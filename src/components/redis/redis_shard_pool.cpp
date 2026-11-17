#include "redis_shard_pool.hpp"

#include <utility>

#include "components/ingest/ingestor.hpp"

namespace tickshelf {

RespRedisShard::RespRedisShard(std::string host, std::uint16_t port)
    : host_(std::move(host)), port_(port) {}

bool RespRedisShard::ensure_connected() {
    return client_.connected() || client_.connect(host_, port_);
}

void RespRedisShard::interrupt() { client_.interrupt(); }

bool RespRedisShard::publish_and_claim(const std::vector<Batch>& batches,
                                       const FlushOptions& options,
                                       std::vector<char>& claimed) {
    if (!ensure_connected()) return false;
    claimed.clear();
    if (!flush_batches(client_, batches, options, &claimed)) {
        client_.close();  // 连接不可信；Engine 重试时重连
        return false;
    }
    claimed.resize(batches.size(), 0);
    if (!options.claim_lease || batches.empty()) return true;

    // 部分 pipeline 已生效时，SET NX 对自己已落的租约返回 Null。补 GET 校验，
    // 避免把"自己的批"误判为"他人持有"而漏写存储。
    std::vector<std::size_t> unresolved;
    unresolved.reserve(batches.size());
    for (std::size_t i = 0; i < batches.size(); ++i)
        if (!claimed[i]) unresolved.push_back(i);
    if (unresolved.empty()) return true;

    std::vector<std::vector<std::string>> commands;
    commands.reserve(unresolved.size());
    for (std::size_t index : unresolved)
        commands.push_back({"GET", lease_key_of(batch_id_of(batches[index]))});

    std::vector<RespValue> replies;
    if (!client_.exec(commands, replies) || replies.size() != commands.size()) {
        client_.close();
        return false;
    }
    for (std::size_t i = 0; i < replies.size(); ++i) {
        const RespValue& reply = replies[i];
        if (reply.type == RespValue::Type::Error) {
            client_.close();
            return false;
        }
        if (reply.type == RespValue::Type::Bulk && reply.str == options.worker_id)
            claimed[unresolved[i]] = 1;
    }
    return true;
}

bool RespRedisShard::finish(const std::vector<std::string>& batch_ids) {
    if (batch_ids.empty()) return true;
    if (!ensure_connected()) return false;
    if (!finish_written_batch_ids(client_, batch_ids)) {
        client_.close();
        return false;
    }
    return true;
}

bool RespRedisShard::release(const std::vector<std::string>& batch_ids) {
    if (batch_ids.empty()) return true;
    if (!ensure_connected()) return false;
    if (!release_claim_ids(client_, batch_ids)) {
        client_.close();
        return false;
    }
    return true;
}

std::vector<std::unique_ptr<RedisShard>> make_redis_shards(const std::string& host,
                                                           std::uint16_t port,
                                                           std::size_t shards) {
    std::vector<std::unique_ptr<RedisShard>> out;
    out.reserve(shards);
    for (std::size_t i = 0; i < shards; ++i)
        out.push_back(std::make_unique<RespRedisShard>(host, port));
    return out;
}

}  // namespace tickshelf
