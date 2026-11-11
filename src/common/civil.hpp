#pragma once
// 天数 ↔ 公历日期（Howard Hinnant civil 算法，UTC）。
// 行情键 ts:{symbol}:{yyyymmdd} 的日期段由此换算（design.md 数据模型节）。
#include <cstdint>

namespace tickshelf {

inline constexpr std::int64_t kMsPerDay = 86400000;

// days since 1970-01-01 → 公历日期
void civil_from_days(std::int64_t z, int& y, unsigned& m, unsigned& d);

// 公历日期 → days since 1970-01-01
std::int64_t days_from_civil(int y, unsigned m, unsigned d);

// 交易所时间戳（ms）→ yyyymmdd（UTC 日界）
std::uint32_t yyyymmdd_from_ms(std::uint64_t ts_exchange_ms);

}  // namespace tickshelf
