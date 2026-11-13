// BasicIntervalSet / 区间运算纯函数 的单元测试（覆盖账地基，需求文档 §9 第 1 步）。
#include <doctest/doctest.h>

#include <algorithm>
#include <vector>

#include "libtickshelf/interval_set.hpp"

namespace {

using namespace tickshelf;
using IS = BasicIntervalSet<UnixMsDomain>;
using R = BasicRange<UnixMsDomain>;

R r(std::int64_t s, std::int64_t e) { return {s, e}; }

std::vector<R> segs(const IS& s) { return s.segments(); }

bool eq(std::vector<R> a, std::vector<R> b) {
    std::sort(a.begin(), a.end(), [](const R& x, const R& y) { return x.start < y.start; });
    std::sort(b.begin(), b.end(), [](const R& x, const R& y) { return x.start < y.start; });
    return a == b;
}

}  // namespace

TEST_CASE("UnixMsDomain: 域操作语义") {
    CHECK(UnixMsDomain::less(1, 2));
    CHECK(UnixMsDomain::adjacent(11, 11));   // [_,11) 与 [11,_) 无缝
    CHECK_FALSE(UnixMsDomain::adjacent(11, 12));
    CHECK(UnixMsDomain::valid_range(6, 11));
    CHECK_FALSE(UnixMsDomain::valid_range(5, 5));   // 零长
    CHECK_FALSE(UnixMsDomain::valid_range(9, 3));   // 反向
}

TEST_CASE("IntervalSet: insert 融合各形态") {
    IS s;
    CHECK(s.insert(r(6, 11)));
    CHECK(eq(segs(s), {r(6, 11)}));

    SUBCASE("右侧相接融合") {
        CHECK(s.insert(r(11, 14)));
        CHECK(eq(segs(s), {r(6, 14)}));
    }
    SUBCASE("左侧相接融合") {
        CHECK(s.insert(r(3, 6)));
        CHECK(eq(segs(s), {r(3, 11)}));
    }
    SUBCASE("跨越重叠融合") {
        CHECK(s.insert(r(2, 13)));
        CHECK(eq(segs(s), {r(2, 13)}));
    }
    SUBCASE("被包含不变") {
        CHECK(s.insert(r(7, 9)));
        CHECK(eq(segs(s), {r(6, 11)}));
    }
    SUBCASE("不相交各自成段") {
        CHECK(s.insert(r(20, 25)));
        CHECK(eq(segs(s), {r(6, 11), r(20, 25)}));
    }
    SUBCASE("桥接两段") {
        CHECK(s.insert(r(20, 25)));
        CHECK(s.insert(r(11, 20)));  // 同时与 [6,11) 相接、[20,25) 相接
        CHECK(eq(segs(s), {r(6, 25)}));
    }
    SUBCASE("非法区间拒绝且无副作用") {
        CHECK_FALSE(s.insert(r(5, 5)));
        CHECK_FALSE(s.insert(r(9, 3)));
        CHECK(eq(segs(s), {r(6, 11)}));
    }
}

TEST_CASE("IntervalSet: erase 挖洞") {
    IS s;
    CHECK(s.insert(r(0, 20)));

    SUBCASE("中挖洞留两侧余量") {
        CHECK(s.erase(r(5, 8)));
        CHECK(eq(segs(s), {r(0, 5), r(8, 20)}));
    }
    SUBCASE("切左端") {
        CHECK(s.erase(r(0, 5)));
        CHECK(eq(segs(s), {r(5, 20)}));
    }
    SUBCASE("切右端") {
        CHECK(s.erase(r(15, 20)));
        CHECK(eq(segs(s), {r(0, 15)}));
    }
    SUBCASE("整段移除") {
        CHECK(s.erase(r(0, 20)));
        CHECK(s.empty());
    }
    SUBCASE("大于集合范围") {
        CHECK(s.erase(r(30, 40)));
        CHECK(eq(segs(s), {r(0, 20)}));
    }
    SUBCASE("挖洞后回填的余量与旧邻段融合") {
        CHECK(s.insert(r(25, 30)));   // [0,20) [25,30)
        CHECK(s.erase(r(18, 27)));    // 余量 [18,20) 与 [0,18) 融合，[27,30) 独立
        CHECK(eq(segs(s), {r(0, 18), r(27, 30)}));
    }
    SUBCASE("非法区间拒绝") {
        CHECK_FALSE(s.erase(r(7, 7)));
        CHECK_FALSE(s.erase(r(9, 1)));
        CHECK(eq(segs(s), {r(0, 20)}));
    }
}

TEST_CASE("IntervalSet: covered / covered_segments") {
    IS s;
    CHECK(s.insert(r(6, 11)));
    CHECK(s.insert(r(13, 15)));

    CHECK(s.covered(r(6, 11)));
    CHECK(s.covered(r(7, 10)));
    CHECK(s.covered(r(13, 15)));
    CHECK_FALSE(s.covered(r(11, 13)));  // 洞
    CHECK_FALSE(s.covered(r(10, 14)));  // 跨洞
    CHECK_FALSE(s.covered(r(5, 7)));    // 左溢
    CHECK_FALSE(s.covered(r(14, 16)));  // 右溢
    CHECK_FALSE(s.covered(r(11, 11)));  // 零长
    CHECK_FALSE(s.covered(r(6, 10)) == false);

    SUBCASE("covered_segments 裁剪与多段") {
        CHECK(s.covered_segments(r(6, 11)) == std::vector<R>{r(6, 11)});
        CHECK(s.covered_segments(r(10, 14)) == std::vector<R>{r(10, 11), r(13, 14)});
        CHECK(s.covered_segments(r(1, 30)) == std::vector<R>{r(6, 11), r(13, 15)});
        CHECK(s.covered_segments(r(11, 13)).empty());
        CHECK(s.covered_segments(r(0, 2)).empty());
    }
    SUBCASE("相邻段在 covered 中无缝") {
        IS t;
        t.insert(r(6, 11));
        t.insert(r(11, 14));
        CHECK(t.run_count() == 1);  // 插入即融合
        CHECK(t.covered(r(6, 14)));
        CHECK(t.covered_segments(r(6, 14)) == std::vector<R>{r(6, 14)});
    }
}

TEST_CASE("subtract_range: r − 有序段集") {
    using VS = std::vector<R>;
    CHECK(tickshelf::subtract_range<UnixMsDomain>(r(6, 11), {}) == VS{r(6, 11)});
    CHECK(tickshelf::subtract_range<UnixMsDomain>(r(6, 11), {r(6, 11)}) == VS{});
    CHECK(tickshelf::subtract_range<UnixMsDomain>(r(6, 11), {r(7, 9)}) == VS{r(6, 7), r(9, 11)});
    CHECK(tickshelf::subtract_range<UnixMsDomain>(r(6, 11), {r(4, 8), r(10, 13)}) ==
          VS{r(8, 10)});
    CHECK(tickshelf::subtract_range<UnixMsDomain>(r(6, 11), {r(0, 30)}) == VS{});
    CHECK(tickshelf::subtract_range<UnixMsDomain>(r(6, 11), {r(6, 8), r(8, 11)}) == VS{});
}

TEST_CASE("normalize_runs: 乱序/重叠/相邻 → 有序最大段") {
    using VS = std::vector<R>;
    std::vector<R> v{r(11, 14), r(6, 9), r(9, 11), r(20, 22), r(21, 30), r(1, 2)};
    tickshelf::normalize_runs<UnixMsDomain>(&v);
    CHECK(v == VS{r(1, 2), r(6, 14), r(20, 30)});
    std::vector<R> bad{r(9, 9), r(5, 3), r(6, 11)};
    tickshelf::normalize_runs<UnixMsDomain>(&bad);
    CHECK(bad == VS{r(6, 11)});  // 非法段兜底丢弃
}
