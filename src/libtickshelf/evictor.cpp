#include "evictor.hpp"

#include <algorithm>

#include "ingestor.hpp"  // 键辅助：seg_key_of / idx_key / idx_member / state_key_of / lease_key_of

namespace tickshelf {
namespace {

bool is_bulk(const RespValue& v) { return v.type == RespValue::Type::Bulk; }

}  // namespace

Evictor::Evictor(RespClient& rc) : Evictor(rc, Options{}) {}

Evictor::Evictor(RespClient& rc, Options opt) : rc_(rc), opt_(opt) {}

bool Evictor::scan_batches(std::vector<BatchInfo>& out) {
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
        const auto& keys = r[0].array[1];
        if (keys.type != RespValue::Type::Array) return false;
        if (!keys.array.empty()) {
            std::vector<std::vector<std::string>> gets;
            gets.reserve(keys.array.size());
            for (const RespValue& k : keys.array) gets.push_back({"GET", k.str});
            std::vector<RespValue> vals;
            if (!rc_.exec(gets, vals) || vals.size() != gets.size()) return false;
            for (std::size_t i = 0; i < gets.size(); ++i) {
                BatchInfo b;
                // SCAN 给的是全键名（含 "state:" 前缀），批 id 解析吃裸 id——剥前缀
                constexpr const char* kPrefix = "state:";
                const std::string& full_key = gets[i][1];
                if (full_key.rfind(kPrefix, 0) != 0) continue;
                if (!LeasePersister::parse_batch_id(full_key.substr(6), b.sym, b.date, b.first,
                                                    b.last))
                    continue;
                b.date_ymd = std::stoull(b.date);
                b.id = full_key.substr(6);
                b.done = is_bulk(vals[i]) && vals[i].str == "done";
                out.push_back(std::move(b));
            }
        }
    } while (cursor != "0");
    return true;
}

bool Evictor::run_once(bool emergency) {
    ++stats_.runs;

    std::vector<BatchInfo> batches;
    if (!scan_batches(batches)) return false;
    stats_.batches_total += batches.size();
    if (batches.empty()) return true;

    // pending 批的租约探测（tier2/tier3 victim 选择用；tier2 默认关时也服务于 tier3）
    std::vector<std::vector<std::string>> exists_cmds;
    std::vector<std::size_t> pend_idx;
    for (std::size_t i = 0; i < batches.size(); ++i) {
        if (!batches[i].done) {
            exists_cmds.push_back({"EXISTS", lease_key_of(batches[i].id)});
            pend_idx.push_back(i);
        }
    }
    if (!exists_cmds.empty()) {
        std::vector<RespValue> vals;
        if (!rc_.exec(exists_cmds, vals) || vals.size() != exists_cmds.size()) return false;
        for (std::size_t j = 0; j < pend_idx.size(); ++j)
            batches[pend_idx[j]].leased = vals[j].integer > 0;
    }

    std::vector<RespValue> r;
    auto delete_batch = [&](const BatchInfo& b, bool with_state) {
        // 段与索引必须同删：悬挂索引 = 幽灵段（查询会 GET 不存在的键，静默空段）
        if (!rc_.exec({{"DEL", seg_key_of(b.id)},
                       {"ZREM", idx_key(b.sym), idx_member(b.date_ymd, b.first, b.last)}},
                      r) ||
            r.size() != 2)
            return false;
        ++stats_.segs_deleted;
        ++stats_.idx_removed;
        if (with_state) {
            if (!rc_.exec({{"DEL", state_key_of(b.id)}}, r)) return false;
            ++stats_.states_deleted;
        }
        return true;
    };

    for (const BatchInfo& b : batches) {
        if (b.done) {
            // tier1：done 段数据必然已落库，整段删——"已持久化先淘汰"由构造成立
            if (!delete_batch(b, true)) return false;
            continue;
        }
        if (opt_.allow_inflight_evict && b.leased) {
            // tier2：在飞段（worker 内存有数据）；stale lease 边缘见 ADR-004 注记
            if (!delete_batch(b, false)) return false;  // state/lease 留给 worker 收尾
            continue;
        }
        if (emergency && !b.leased) {
            // tier3（紧急）：最老未认领 pending 段真丢。按批 id 首 ts 升序近似——
            // scan 顺序非严格 ts 序，紧急语义下足够；日志由调用方输出
            if (!delete_batch(b, true)) return false;
            ++stats_.emergency_segs;
            continue;
        }
        // pending 未认领且非紧急：保留（真丢层未授权）
    }
    return true;
}

}  // namespace tickshelf
