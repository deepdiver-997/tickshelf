#pragma once
// 落库后端接口（ADR-020）：从 SqliteTickStore 显式化提取——rule of three 到点：
// 第二个存储形态（分片 ShardedTickStore）真实落地，才提接口，不做预抽象。
// 语义契约（两个实现都必须满足）：
//   upsert 幂等——UNIQUE(symbol, ts_exchange_ms)，重复不产生第二行；
//   返回新插入行数（幂等忽略的不计），-1 = 故障；
//   upsert_flush = flush 原子事务（多批一次 COMMIT，ADR-012 修订）；
//   query_range 按 ts 升序返回该 symbol 的全部行。
#include <cstdint>
#include <string>
#include <vector>

#include "common/tick.hpp"

namespace tickshelf {

class TickStore {
public:
    virtual ~TickStore() = default;
    // path = ":memory:" 或文件路径（分片实现按自身规则派生分片文件名）。
    virtual bool open(const std::string& path) = 0;
    virtual std::int64_t upsert_batch(const std::vector<Tick>& ticks) = 0;
    virtual std::int64_t upsert_flush(const std::vector<const std::vector<Tick>*>& batches) = 0;
    virtual std::vector<Tick> query_range(const std::string& symbol, std::uint64_t from_ms,
                                          std::uint64_t to_ms) = 0;
    virtual bool query_failed() const = 0;
    virtual std::uint64_t count() const = 0;
};

}  // namespace tickshelf
