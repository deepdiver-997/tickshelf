#include "ingestor.hpp"

#include "common/civil.hpp"

namespace tickshelf {

void Ingestor::ingest(const Tick& t) {
    const std::uint64_t sym = pack_symbol(t.symbol);
    const std::uint32_t date = yyyymmdd_from_ms(t.ts_exchange_ms);
    buckets_[sym][date].push_back(t);  // ADR-011：u64 键零分配查找；按值拷贝 POD 便宜
}

std::vector<Batch> Ingestor::drain() {
    std::vector<Batch> out;
    for (auto& [sym, dates] : buckets_) {
        for (auto& [date, ticks] : dates) {
            Batch b;
            // 键/批id 里的 symbol 用去 NUL 填充形式，与读端（ZRANGEBYSCORE 的键）一致
            b.symbol.assign(tick_symbol(ticks.front()));
            b.yyyymmdd = date;
            b.first_ts = ticks.front().ts_exchange_ms;
            b.last_ts = ticks.front().ts_exchange_ms;
            for (const Tick& t : ticks) {  // 乱序输入（M4 混沌）下取 min/max
                if (t.ts_exchange_ms < b.first_ts) b.first_ts = t.ts_exchange_ms;
                if (t.ts_exchange_ms > b.last_ts) b.last_ts = t.ts_exchange_ms;
            }
            b.ticks = std::move(ticks);
            out.push_back(std::move(b));
        }
    }
    buckets_.clear();
    return out;
}

std::size_t Ingestor::pending_ticks() const {
    std::size_t n = 0;
    for (const auto& [sym, dates] : buckets_)
        for (const auto& [date, ticks] : dates) n += ticks.size();
    return n;
}

std::string pack_batch(const std::vector<Tick>& ticks) {
    std::string out;
    out.resize(ticks.size() * kTickBytes);
    for (std::size_t i = 0; i < ticks.size(); ++i)
        encode_tick(ticks[i], reinterpret_cast<std::uint8_t*>(out.data()) + i * kTickBytes);
    return out;
}

void build_flush_commands(const std::vector<Batch>& batches, const FlushOptions& opt,
                          std::vector<std::vector<std::string>>& commands,
                          std::vector<std::size_t>& claimed_idx) {
    commands.clear();
    claimed_idx.clear();
    commands.reserve(batches.size() * 4 + 2);

    // 最新投影聚合：每 symbol 取本 flush 内 ts 最大的 tick（ADR-016）
    std::map<std::string, Tick> newest;  // symbol（去 NUL 形式）→ 最新 tick
    for (const Batch& b : batches)
        for (const Tick& t : b.ticks) {
            const std::string sym(tick_symbol(t));
            auto it = newest.find(sym);
            if (it == newest.end() || t.ts_exchange_ms > it->second.ts_exchange_ms)
                newest[sym] = t;
        }

    for (const Batch& b : batches) {
        const std::string id = batch_id_of(b);
        const std::string packed = pack_batch(b.ticks);
        if (opt.claim_lease) {
            claimed_idx.push_back(commands.size());
            commands.push_back({"SET", lease_key_of(id), opt.worker_id, "NX", "EX",
                                std::to_string(opt.lease_ttl_s)});
            commands.push_back({"SET", state_key_of(id), "pending"});
        }
        // 段式 v2（ADR-017）：批 → 一个原始字节段 + 一条索引，命令数与 tick 数无关
        commands.push_back({"SET", seg_key_of(id), std::move(packed)});
        commands.push_back({"ZADD", idx_key(b.symbol), std::to_string(b.first_ts),
                            idx_member(b.yyyymmdd, b.first_ts, b.last_ts)});
        if (!opt.claim_lease) commands.push_back({"SET", state_key_of(id), "pending"});
    }

    if (!newest.empty()) {
        std::vector<std::string> hset{"HSET", latest_key()};
        hset.reserve(2 + newest.size() * 2);
        for (auto& [sym, t] : newest) {
            std::string val;
            val.resize(kTickBytes);
            encode_tick(t, reinterpret_cast<std::uint8_t*>(val.data()));
            hset.push_back(sym);
            hset.push_back(std::move(val));
        }
        commands.push_back(std::move(hset));
    }
}

bool flush_batches(RespClient& rc, const std::vector<Batch>& batches, const FlushOptions& opt,
                   std::vector<char>* claimed) {
    if (!rc.connected() || batches.empty()) return false;

    std::vector<std::vector<std::string>> commands;
    std::vector<std::size_t> claimed_idx;
    build_flush_commands(batches, opt, commands, claimed_idx);

    std::vector<RespValue> replies;
    if (!rc.exec(commands, replies)) return false;
    if (replies.size() != commands.size()) return false;
    for (const RespValue& r : replies)
        if (r.type == RespValue::Type::Error) return false;

    if (claimed != nullptr && opt.claim_lease) {
        claimed->reserve(batches.size());
        for (std::size_t idx : claimed_idx)
            (*claimed).push_back(replies[idx].type != RespValue::Type::Null ? 1 : 0);
    }
    return true;
}

bool finish_written_batches(RespClient& rc, const std::vector<Batch>& batches) {
    if (!rc.connected() || batches.empty()) return false;
    std::vector<std::vector<std::string>> commands;
    commands.reserve(batches.size() * 2);
    for (const Batch& b : batches) {
        const std::string id = batch_id_of(b);
        commands.push_back({"SET", state_key_of(id), "done"});
        commands.push_back({"DEL", lease_key_of(id)});
    }
    std::vector<RespValue> replies;
    if (!rc.exec(commands, replies) || replies.size() != commands.size()) return false;
    for (const RespValue& r : replies)
        if (r.type == RespValue::Type::Error) return false;
    return true;
}

bool release_claims(RespClient& rc, const std::vector<Batch>& batches) {
    if (!rc.connected() || batches.empty()) return false;
    std::vector<std::vector<std::string>> commands;
    commands.reserve(batches.size());
    for (const Batch& b : batches) commands.push_back({"DEL", lease_key_of(batch_id_of(b))});
    std::vector<RespValue> replies;
    if (!rc.exec(commands, replies) || replies.size() != commands.size()) return false;
    return true;
}

bool publish_stats(RespClient& rc, const std::string& component, const std::string& body,
                   std::uint64_t ttl_s) {
    if (!rc.connected()) return false;
    std::vector<RespValue> replies;
    return rc.exec({{"SET", "stats:" + component, body, "EX", std::to_string(ttl_s)},}, replies);
}

}  // namespace tickshelf
