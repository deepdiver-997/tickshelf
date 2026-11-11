#include "lease_persister.hpp"

#include <cstdio>
#include <cstdlib>

#include "ingestor.hpp"  // 键辅助：seg_key_of / state_key_of / lease_key_of

namespace tickshelf {
namespace {

bool is_bulk(const RespValue& v) { return v.type == RespValue::Type::Bulk; }

}  // namespace

LeasePersister::LeasePersister(RespClient& rc, SqliteTickStore& store, Options opt)
    : rc_(rc), store_(store), opt_(std::move(opt)) {}

bool LeasePersister::parse_batch_id(const std::string& id, std::string& symbol,
                                    std::string& yyyymmdd, std::uint64_t& first_ts,
                                    std::uint64_t& last_ts) {
    // {symbol}:{yyyymmdd}:{first}-{last}
    const std::size_t c1 = id.find(':');
    if (c1 == std::string::npos) return false;
    const std::size_t c2 = id.find(':', c1 + 1);
    if (c2 == std::string::npos) return false;
    const std::size_t dash = id.find('-', c2 + 1);
    if (dash == std::string::npos) return false;
    symbol = id.substr(0, c1);
    yyyymmdd = id.substr(c1 + 1, c2 - c1 - 1);
    if (symbol.empty() || yyyymmdd.size() != 8) return false;
    first_ts = std::strtoull(id.substr(c2 + 1, dash - c2 - 1).c_str(), nullptr, 10);
    last_ts = std::strtoull(id.substr(dash + 1).c_str(), nullptr, 10);
    return true;
}

bool LeasePersister::scan_pending_state_keys(std::vector<std::string>& out) {
    out.clear();
    std::string cursor = "0";
    do {
        std::vector<RespValue> r;
        if (!rc_.exec({{"SCAN", cursor, "MATCH", "state:*", "COUNT",
                        std::to_string(opt_.scan_count)}},
                      r) ||
            r.size() != 1 || r[0].type != RespValue::Type::Array || r[0].array.size() != 2) {
            return false;
        }
        cursor = r[0].array[0].str;
        if (r[0].array[1].type != RespValue::Type::Array) return false;
        std::vector<std::vector<std::string>> gets;
        for (const RespValue& k : r[0].array[1].array) gets.push_back({"GET", k.str});
        if (!gets.empty()) {
            std::vector<RespValue> vals;
            if (!rc_.exec(gets, vals) || vals.size() != gets.size()) return false;
            for (std::size_t i = 0; i < gets.size(); ++i) {
                if (is_bulk(vals[i]) && vals[i].str == "pending") {
                    // gets[i][1] 是完整键名（含 "state:" 前缀）；process_batch 吃裸批 id
                    const std::string& full_key = gets[i][1];
                    constexpr const char* kPrefix = "state:";
                    if (full_key.rfind(kPrefix, 0) == 0) out.push_back(full_key.substr(6));
                }
            }
        }
    } while (cursor != "0");
    return true;
}

int LeasePersister::process_batch(const std::string& id) {
    std::string symbol, yyyymmdd;
    std::uint64_t first_ts = 0, last_ts = 0;
    if (!parse_batch_id(id, symbol, yyyymmdd, first_ts, last_ts)) return -1;

    // RTT 合并（ADR-012）：认领+读批一条 pipeline。claim 是唯一仲裁点：SET NX 失败
    // （Null 回复）= 别的 worker 在做，同 pipeline 的读回结果作废（无害读）。
    std::vector<RespValue> r;
    if (!rc_.exec({{"SET", lease_key_of(id), opt_.worker_id, "NX", "EX",
                    std::to_string(opt_.lease_ttl_s)},
                   {"GET", seg_key_of(id)}},  // 段式 v2（ADR-017）：段键 = "seg:" + 批id
                  r) ||
        r.size() != 2) {
        return -1;
    }
    if (r[0].type == RespValue::Type::Null) {
        ++stats_.skipped;
        return 0;
    }
    // Null 段 = "标记落了、段没落"（classic 半管道崩溃窗口）→ 空批路径标 done
    if (r[1].type != RespValue::Type::Bulk && r[1].type != RespValue::Type::Null) return -1;

    // 读批：段是本批 tick 的全体（1:1），逐 64B 解码。无重叠脏读问题（旧 ZSET
    // 区间读的邻批重叠在段式下天然消失），幂等 upsert 兜底保留。
    std::vector<Tick> ticks;
    const std::size_t n = r[1].str.size() / kTickBytes;
    ticks.reserve(n);
    for (std::size_t i = 0; i < n; ++i)
        ticks.push_back(decode_tick(reinterpret_cast<const std::uint8_t*>(r[1].str.data()) +
                                    i * kTickBytes));
    if (ticks.empty()) ++stats_.empty_batches;

    // 幂等落库 → 标记 done（无条件 SET，与淘汰无竞态）→ 释放租约（TTL 是兜底）。
    const std::int64_t ins = store_.upsert_batch(ticks);
    if (ins < 0) return -1;  // SQLite 故障：租约留给 TTL 过期，下轮重做
    stats_.ticks_upserted += static_cast<std::uint64_t>(ins);
    if (!rc_.exec({{"SET", state_key_of(id), "done"}, {"DEL", lease_key_of(id)}}, r) ||
        r.size() != 2) {
        return -1;  // 崩在中途：state 已 done 则不重做；lease 残留靠 TTL 兜底
    }
    ++stats_.claimed;
    return 1;
}

int LeasePersister::poll_once() {
    ++stats_.polls;
    std::vector<std::string> pending;
    if (!scan_pending_state_keys(pending)) return -1;
    stats_.last_backlog = pending.size();
    int done = 0;
    for (const std::string& id : pending) {
        const int r = process_batch(id);
        if (r < 0) return -1;
        done += r;
    }
    return done;
}

}  // namespace tickshelf
