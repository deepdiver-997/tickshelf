#pragma once
// 定点价类型：i64，比例 1e-4（protocol.md）。12345000 = 1234.5000。
// 全程整数运算，回避浮点比较；字符串化仅供日志/调试。
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <string>

namespace tickshelf {

using price_t = std::int64_t;

inline constexpr price_t kPriceScale = 10000;

inline price_t price_from_double(double v) {
    return static_cast<price_t>(std::llround(v * static_cast<double>(kPriceScale)));
}

inline double price_to_double(price_t p) {
    return static_cast<double>(p) / static_cast<double>(kPriceScale);
}

inline std::string price_to_string(price_t p) {
    const bool neg = p < 0;
    const price_t a = neg ? -p : p;
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%s%lld.%04lld", neg ? "-" : "",
                  static_cast<long long>(a / kPriceScale), static_cast<long long>(a % kPriceScale));
    return buf;
}

}  // namespace tickshelf
