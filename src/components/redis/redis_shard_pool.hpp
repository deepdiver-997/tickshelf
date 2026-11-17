#pragma once
// 每分片一条 RESP 连接的 Redis 端口（Engine 的 RedisShard 实现）。
//
// 连接只被对应分片 worker 使用，不共享、不加锁。连接失败由 Engine 按退避重试；
// 本类在每次调用前懒连接，失败即 close，下一次调用重连。
//
// publish_and_claim 复用既有 flush_batches（段/索引/state/lease 命令与旧路径完全
// 一致）。对 SETNX 返回 Null 的批追加一轮 pipelined GET lease:{id}：若值等于本
// worker_id，说明是上一次 pipeline 部分生效时自己已认领的批——不能误判成"他人
// 持有"而跳过落库。只有 GET 到他人 id 才保持未认领。
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "components/engine/engine.hpp"
#include "components/redis/resp_client.hpp"

namespace tickshelf {

class RespRedisShard : public RedisShard {
public:
    RespRedisShard(std::string host, std::uint16_t port);

    bool connected() const { return client_.connected(); }
    bool publish_and_claim(const std::vector<Batch>& batches, const FlushOptions& options,
                           std::vector<char>& claimed) override;
    bool finish(const std::vector<std::string>& batch_ids) override;
    bool release(const std::vector<std::string>& batch_ids) override;
    // 唤醒阻塞中的连接 I/O；被中断的连接不可信，下次调用前会重连。
    void interrupt() override;

private:
    bool ensure_connected();

    std::string host_;
    std::uint16_t port_;
    RespClient client_;
};

// N 个同地址连接（一个分片一条）；返回顺序即 shard index。
std::vector<std::unique_ptr<RedisShard>> make_redis_shards(const std::string& host,
                                                           std::uint16_t port,
                                                           std::size_t shards);

}  // namespace tickshelf
