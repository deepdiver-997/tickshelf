#include <doctest/doctest.h>

#include <random>

#include "common/civil.hpp"

using namespace tickshelf;

TEST_CASE("civil: 已知锚点") {
    CHECK(days_from_civil(1970, 1, 1) == 0);
    CHECK(days_from_civil(2000, 3, 1) == 11017);  // Hinnant 参考实现文档值

    int y = 0;
    unsigned m = 0, d = 0;
    civil_from_days(0, y, m, d);
    CHECK(y == 1970);
    CHECK(m == 1);
    CHECK(d == 1);
    civil_from_days(11017, y, m, d);
    CHECK(y == 2000);
    CHECK(m == 3);
    CHECK(d == 1);
}

TEST_CASE("civil: ms → yyyymmdd（UTC 日界）") {
    CHECK(yyyymmdd_from_ms(0) == 19700101);
    CHECK(yyyymmdd_from_ms(kMsPerDay - 1) == 19700101);
    CHECK(yyyymmdd_from_ms(kMsPerDay) == 19700102);
    CHECK(yyyymmdd_from_ms(2 * kMsPerDay - 1) == 19700102);
}

TEST_CASE("civil: 随机日期往返互逆") {
    std::mt19937 rng(7);
    for (int i = 0; i < 500; ++i) {
        const int y = 1970 + static_cast<int>(rng() % 131);
        const unsigned m = 1 + rng() % 12;
        const unsigned d = 1 + rng() % 28;
        const auto z = days_from_civil(y, m, d);
        int y2 = 0;
        unsigned m2 = 0, d2 = 0;
        civil_from_days(z, y2, m2, d2);
        CHECK(y2 == y);
        CHECK(m2 == m);
        CHECK(d2 == d);
    }
    // 闰日：2024-02-29 存在且往返一致
    const auto leap = days_from_civil(2024, 2, 29);
    int y = 0;
    unsigned m = 0, d = 0;
    civil_from_days(leap, y, m, d);
    CHECK(y == 2024);
    CHECK(m == 2);
    CHECK(d == 29);
}
