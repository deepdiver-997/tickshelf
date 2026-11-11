#pragma once
// 查询组件（ADR-015 + 段式 v2 ADR-017）：索引查段 → 只回命中段的原始字节。
//
// 读路径（热侧）：
//   ZRANGEBYSCORE idx:{sym} -inf to → 候选段 member（过滤 last ≥ from）
//   → pipeline GET seg:… → **段字节 verbatim 进响应载荷**（服务端零解码零重编码，
//     逐 tick 只抽 8B 的 ts 做缺口检测——实测教训：整份复制 + Lua 过滤既慢又有
//     定点数→浮点的精度损失；这里原始字节直达客户端，精度无损天然成立）。
// 耐久侧（SQLite）逐行解码后编码进载荷，与热侧按 ts 去重合并。
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "ingestor.hpp"  // 键空间：idx/backfill 约定
#include "resp_client.hpp"
#include "tick_store.hpp"

namespace tickshelf {

struct QueryResult {
    std::string payload;                            // 64B 对齐 tick 区（升序）
    std::vector<std::uint64_t> ts;                  // 全部 tick 的交易所 ts（升序去重）
    std::size_t tick_count() const { return ts.size(); }
    std::vector<std::pair<std::uint64_t, std::uint64_t>> missing;  // 缺口区间（升序，截断于上限）
    std::vector<std::string> backfill_requested;    // 本次实际发出的回补请求键
    bool redis_ok = true;  // Redis 侧读失败时 false（结果只剩耐久侧，调用方酌情降级）
};

class RangeReader {
public:
    struct Options {
        std::uint64_t step_ms = 1;            // 期望 ts 网格
        std::size_t max_missing_ranges = 64;  // 缺口区间上限（超限截前 N，按位置序）
        bool trigger_backfill = true;
        std::uint64_t backfill_ttl_s = 600;
    };

    explicit RangeReader(RespClient& rc, TickStore& store);
    RangeReader(RespClient& rc, TickStore& store, Options opt);

    // false = Redis 连接/协议故障（out 仍含耐久侧结果，redis_ok=false）
    bool read(const std::string& symbol, std::uint64_t from_ms, std::uint64_t to_ms,
              QueryResult& out);

private:
    bool read_hot_side(const std::string& symbol, std::uint64_t from_ms, std::uint64_t to_ms,
                       std::vector<std::pair<std::uint64_t, std::string>>& out);

    RespClient& rc_;
    TickStore& store_;
    Options opt_;
};

// 最新价快速路径（ADR-016）：HGET latest（field=symbol）→ 64B tick。
// found=false 且返回 true = 投影里还没有该 symbol（如尚未有任何 flush）。
// 返回 false = 连接/协议故障。
bool read_latest(RespClient& rc, const std::string& symbol, bool& found, Tick& out);

}  // namespace tickshelf
