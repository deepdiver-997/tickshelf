#include "reader.hpp"

#include <algorithm>
#include <cstdlib>
#include <map>

#include "common/civil.hpp"
#include "common/endian.hpp"

namespace tickshelf {
namespace {

bool is_bulk(const RespValue& v) { return v.type == RespValue::Type::Bulk; }

}  // namespace

RangeReader::RangeReader(RespClient& rc, TickStore& store)
    : RangeReader(rc, store, Options{}) {}

RangeReader::RangeReader(RespClient& rc, TickStore& store, Options opt)
    : rc_(rc), store_(store), opt_(opt) {}

bool RangeReader::read_hot_side(const std::string& symbol, std::uint64_t from_ms,
                                std::uint64_t to_ms,
                                std::vector<std::pair<std::uint64_t, std::string>>& out) {
    // 1) 索引候选：score=批首 ts，取 first ≤ to 的段；member 携带 last，过滤 last ≥ from
    std::vector<RespValue> r;
    if (!rc_.exec({{"ZRANGEBYSCORE", idx_key(symbol), "-inf", std::to_string(to_ms)}}, r) ||
        r.size() != 1 || r[0].type != RespValue::Type::Array)
        return false;

    struct SegRef {
        std::string seg_key;
        std::uint64_t last;
    };
    std::vector<SegRef> segs;
    for (const RespValue& m : r[0].array) {
        if (m.type != RespValue::Type::Bulk) continue;
        // member = "{date}:{first}-{last}"
        const std::size_t c1 = m.str.find(':');
        const std::size_t dash = m.str.find('-', c1 == std::string::npos ? 0 : c1);
        if (c1 == std::string::npos || dash == std::string::npos) continue;
        const std::string date = m.str.substr(0, c1);
        const std::uint64_t first =
            std::strtoull(m.str.substr(c1 + 1, dash - c1 - 1).c_str(), nullptr, 10);
        const std::uint64_t last = std::strtoull(m.str.substr(dash + 1).c_str(), nullptr, 10);
        if (last < from_ms) continue;  // 段整体在查询窗口之前
        segs.push_back({seg_key(symbol, static_cast<std::uint32_t>(std::stoul(date)), first, last),
                        last});
    }
    if (segs.empty()) return true;

    // 2) 命中段整段取回（原始字节；查询范围覆盖不满整段时多读的部分在合并层按 ts 过滤）
    std::vector<std::vector<std::string>> gets;
    gets.reserve(segs.size());
    for (const SegRef& s : segs) gets.push_back({"GET", s.seg_key});
    std::vector<RespValue> vals;
    if (!rc_.exec(gets, vals) || vals.size() != gets.size()) return false;

    out.reserve(out.size() + 4096);
    for (std::size_t i = 0; i < vals.size(); ++i) {
        if (!is_bulk(vals[i])) continue;
        const std::string& seg = vals[i].str;
        const std::size_t n = seg.size() / kTickBytes;
        for (std::size_t k = 0; k < n; ++k) {
            const std::uint8_t* p = reinterpret_cast<const std::uint8_t*>(seg.data()) +
                                    k * kTickBytes;
            const std::uint64_t ts = get_u64(p + 8);  // tick 内 ts_exchange_ms 偏移 8
            if (ts < from_ms || ts > to_ms) continue;
            out.emplace_back(ts, seg.substr(k * kTickBytes, kTickBytes));
        }
    }
    return true;
}

bool RangeReader::read(const std::string& symbol, std::uint64_t from_ms, std::uint64_t to_ms,
                       QueryResult& out) {
    out = QueryResult{};
    if (to_ms < from_ms) return true;

    // 1) 热侧（段 verbatim）+ 耐久侧（解码）
    std::vector<std::pair<std::uint64_t, std::string>> entries;  // (ts, 64B tick 字节)
    out.redis_ok = read_hot_side(symbol, from_ms, to_ms, entries);
    if (!out.redis_ok && !rc_.connected()) {
        out.redis_ok = read_hot_side(symbol, from_ms, to_ms, entries);
    }
    const std::vector<Tick> durable = store_.query_range(symbol, from_ms, to_ms);
    if (store_.query_failed()) return false;
    for (const Tick& t : durable) {
        std::string bytes(kTickBytes, '\0');
        encode_tick(t, reinterpret_cast<std::uint8_t*>(bytes.data()));
        entries.emplace_back(t.ts_exchange_ms, std::move(bytes));
    }

    // 2) 按 ts 排序去重（热/耐久可能都有同一 tick：字节相同，任取一份）
    std::sort(entries.begin(), entries.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
    out.payload.reserve(entries.size() * kTickBytes);
    out.ts.reserve(entries.size());
    std::uint64_t prev_ts = 0;
    bool first = true;
    for (auto& [ts, bytes] : entries) {
        if (!first && ts == prev_ts) continue;  // 去重
        out.payload.append(bytes);
        out.ts.push_back(ts);
        prev_ts = ts;
        first = false;
    }

    // 3) 缺口检测（期望网格 step_ms）
    const std::uint64_t step = opt_.step_ms;
    auto push_missing = [&](std::uint64_t lo, std::uint64_t hi) {
        if (lo > hi) return;
        out.missing.emplace_back(lo, hi);
    };
    if (out.ts.empty()) {
        push_missing(from_ms, to_ms);
    } else {
        std::uint64_t prev = out.ts.front();
        if (prev > from_ms) push_missing(from_ms, prev - step);
        for (std::size_t i = 1; i < out.ts.size(); ++i) {
            const std::uint64_t cur = out.ts[i];
            if (cur > prev + step) push_missing(prev + step, cur - step);
            prev = cur;
        }
        if (prev < to_ms) push_missing(prev + step, to_ms);
    }
    if (out.missing.size() > opt_.max_missing_ranges) out.missing.resize(opt_.max_missing_ranges);

    // 4) 回补触发（懒，ADR-014）
    if (opt_.trigger_backfill) {
        for (const auto& [lo, hi] : out.missing) {
            const std::string key =
                "backfill:" + symbol + ":" + std::to_string(lo) + "-" + std::to_string(hi);
            std::vector<RespValue> r;
            if (rc_.exec({{"SET", key, "requested", "NX", "EX",
                           std::to_string(opt_.backfill_ttl_s)}},
                          r) &&
                r.size() == 1 && r[0].type == RespValue::Type::Simple) {
                out.backfill_requested.push_back(key);
            }
        }
    }
    return true;
}

bool read_latest(RespClient& rc, const std::string& symbol, bool& found, Tick& out) {
    found = false;
    std::vector<RespValue> r;
    if (!rc.exec({{"HGET", latest_key(), symbol}}, r) || r.size() != 1) return false;
    if (r[0].type != RespValue::Type::Bulk || r[0].str.size() != kTickBytes) return true;  // 无投影
    out = decode_tick(reinterpret_cast<const std::uint8_t*>(r[0].str.data()));
    found = true;
    return true;
}

}  // namespace tickshelf
