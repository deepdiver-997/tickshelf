#pragma once
// 淘汰（段式 v2，ADR-017）：段与批 1:1（段键 = "seg:" + 批id），段的 tick 全体
// 同生共死——done 批整段删（数据必然已落库），不再需要水位机。
// （ADR-004 的水位是逐 tick ZSET 下"批区间混住 done/pending 成员"的产物；
//   段式下"已持久化先淘汰"由构造直接成立：只删 done 段。）
//
// 层间代价（ADR-004，段粒度）：
//   tier1 done（免费）——默认唯一动作；
//   tier2 租约在飞（几乎免费）——allow_inflight 时删在飞段（worker 内存有数据）。
//        边界：worker 死于淘汰后该批丢失（stale lease，实测教训）→ 默认关；
//   tier3 紧急（真丢）——emergency 时删最老未认领 pending 段，默认关，日志必须响。
//
// 索引一致性：删段必须同步 ZREM idx:{sym}（查询走索引，悬挂索引 = 幽灵段）。
#include <cstdint>
#include <string>
#include <vector>

#include "lease_persister.hpp"  // parse_batch_id / lease_key_of
#include "ingestor.hpp"         // state_key（ADR-019 状态 HASH）
#include "resp_client.hpp"

namespace tickshelf {

class Evictor {
public:
    struct Options {
        bool allow_inflight_evict = false;  // tier2 默认关（ADR-004 注记）
        std::uint32_t scan_count = 200;
    };

    struct Stats {
        std::uint64_t runs = 0;
        std::uint64_t segs_deleted = 0;      // 删掉的段数（含整段数据）
        std::uint64_t states_deleted = 0;    // state 键 GC
        std::uint64_t idx_removed = 0;       // 索引成员 ZREM
        std::uint64_t emergency_segs = 0;    // tier3 真丢（调用方必须告警）
        std::uint64_t batches_total = 0;     // 扫到的批总数（观测）
    };

    explicit Evictor(RespClient& rc);
    Evictor(RespClient& rc, Options opt);

    // 一轮淘汰。emergency=true 时允许 tier3。false = 连接/协议故障。
    bool run_once(bool emergency = false);

    const Stats& stats() const { return stats_; }

private:
    struct BatchInfo {
        std::string id, sym, date;
        std::uint64_t date_ymd = 0;  // yyyymmdd（idx member 用）
        std::uint64_t first = 0, last = 0;
        bool done = false;
        bool leased = false;
    };

    bool scan_batches(std::vector<BatchInfo>& out);

    RespClient& rc_;
    Options opt_;
    Stats stats_;
};

}  // namespace tickshelf
