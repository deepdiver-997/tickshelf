#pragma once
// 租约落盘（ADR-002）：SCAN 候选批 → SETNX 租约（唯一仲裁点）→ ZRANGEBYSCORE 读批
// → SQLite 幂等 upsert → SET state done。
//
// 核心认知：正确性不依赖租约——at-least-once + 唯一键幂等 = 效果上的 exactly-once。
// 租约只是减少重复劳动的效率优化；查候选批可以读到脏数据（区间重叠/重复 tick），
// claim 才是仲裁点，多实例安全。租约过期但 worker 还活着的双活窗口由幂等兜底。
//
// M1 简化：批级处理（500ms 窗口的批 <1s 内完成），不做分块续期（ADR-002 的
// 分块+续期留给批大到跨 TTL 的场景）；批内崩溃窗口 = 崩溃后重做，幂等兜底。
#include <cstdint>
#include <string>
#include <vector>

#include "resp_client.hpp"
#include "tick_store.hpp"

namespace tickshelf {

class LeasePersister {
public:
    struct Options {
        std::string worker_id = "worker";  // 租约值（观测用，不参与正确性）
        std::uint64_t lease_ttl_s = 60;
        std::uint32_t scan_count = 100;
    };

    struct Stats {
        std::uint64_t polls = 0;
        std::uint64_t claimed = 0;      // 抢到租约并处理完的批
        std::uint64_t skipped = 0;      // 租约被别的 worker 占有
        std::uint64_t empty_batches = 0;  // 区间内无数据（批标记有但 ZSET 空）
        std::uint64_t ticks_upserted = 0;  // 新插入行数（不含幂等忽略）
        std::uint64_t last_backlog = 0;  // 最近一轮 SCAN 到的 pending 批数（负载暴露，ADR-013）
    };

    LeasePersister(RespClient& rc, TickStore& store, Options opt);

    // 单轮：SCAN state:* → GET 过滤 pending → 逐批认领处理。返回本轮处理批数。
    int poll_once();

    const Stats& stats() const { return stats_; }

    // 批id 解析：{symbol}:{yyyymmdd}:{first}-{last}
    // M1 约定 symbol 为不含 ':'/'-' 的 ASCII 短名（feedgen/接入方保证）。
    static bool parse_batch_id(const std::string& id, std::string& symbol, std::string& yyyymmdd,
                               std::uint64_t& first_ts, std::uint64_t& last_ts);

private:
    bool scan_pending_state_keys(std::vector<std::string>& out);  // state:* 且值==pending
    // 1 = claimed 并处理完；0 = 租约被他占（跳过）；-1 = 连接/协议错误
    int process_batch(const std::string& id);

    RespClient& rc_;
    TickStore& store_;
    Options opt_;
    Stats stats_;
};

}  // namespace tickshelf
