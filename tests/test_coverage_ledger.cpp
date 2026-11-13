// 区间覆盖账测试（.local-docs/区间记账_需求文档.md §5 场景表 + §8 测试要求）：
//   12 条验收场景逐一精确断言（区间边界 + 依赖集合）；
//   property fuzz：固定种子随机序列，独立暴力 oracle（逐 cell 依赖集）对账；
//   碎片回归 1000 轮；不变量检查器真阳性自测；恢复配方纯函数级演练。
#include <doctest/doctest.h>

#include <algorithm>
#include <array>
#include <map>
#include <ostream>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "libtickshelf/coverage_ledger.hpp"

namespace tickshelf {
// doctest 断言失败时的可读输出
inline std::ostream& operator<<(std::ostream& os, const BasicRange<UnixMsDomain>& x) {
    return os << "[" << x.start << "," << x.end << ")";
}
inline std::ostream& operator<<(std::ostream& os, const BasicDepSpan<UnixMsDomain>& x) {
    os << "[" << x.range.start << "," << x.range.end << "){";
    for (std::size_t i = 0; i < x.deps.size(); ++i) os << (i ? "," : "") << x.deps[i];
    return os << "}";
}
inline void print_ranges(std::ostream& os, const std::vector<BasicRange<UnixMsDomain>>& v) {
    os << "{";
    for (std::size_t i = 0; i < v.size(); ++i) os << (i ? "," : "") << v[i];
    os << "}";
}
inline void print_spans(std::ostream& os, const std::vector<BasicDepSpan<UnixMsDomain>>& v) {
    os << "{";
    for (std::size_t i = 0; i < v.size(); ++i) os << (i ? "," : "") << v[i];
    os << "}";
}
inline std::ostream& operator<<(std::ostream& os, const BasicLedgerSnapshotEntry<UnixMsDomain>& x) {
    os << x.symbol << "[" << x.start << "," << x.end << "){";
    for (std::size_t i = 0; i < x.deps.size(); ++i) os << (i ? "," : "") << x.deps[i];
    return os << "}";
}
inline std::ostream& operator<<(std::ostream& os, const BasicPlanResult<UnixMsDomain>& p) {
    os << "absent";
    print_ranges(os, p.absent);
    os << " cond";
    print_spans(os, p.conditional);
    os << " sat";
    print_ranges(os, p.satisfied);
    return os;
}
inline std::ostream& operator<<(std::ostream& os, const BasicSettleResult<UnixMsDomain>& s) {
    os << "resolved";
    print_ranges(os, s.resolved);
    os << " degraded";
    print_spans(os, s.degraded);
    return os;
}
inline std::ostream& operator<<(std::ostream& os, const BasicReleaseResult<UnixMsDomain>& s) {
    os << "orphaned";
    print_spans(os, s.orphaned);
    os << " reclaimed";
    print_ranges(os, s.reclaimed);
    return os;
}
}  // namespace tickshelf

namespace {

using namespace tickshelf;
using Led = BasicCoverageLedger<UnixMsDomain>;
using R = BasicRange<UnixMsDomain>;
using Span = BasicDepSpan<UnixMsDomain>;
using Rq = RequestId;
using SnapEntry = Led::SnapshotEntry;

template <typename... Ts>
std::vector<Rq> D(Ts... ids) {
    return std::vector<Rq>{std::string(ids)...};
}
R r(std::int64_t s, std::int64_t e) { return {s, e}; }

// 某 symbol 的条目视图（start, end, deps）——精确状态断言用
struct Ent {
    std::int64_t s, e;
    std::vector<std::string> deps;
    bool operator==(const Ent&) const = default;
};
std::ostream& operator<<(std::ostream& os, const Ent& x) {
    os << "[" << x.s << "," << x.e << "){";
    for (std::size_t i = 0; i < x.deps.size(); ++i) os << (i ? "," : "") << x.deps[i];
    return os << "}";
}
std::vector<Ent> ents(const Led& led, std::string_view sym) {
    std::vector<Ent> out;
    for (const auto& sn : led.export_snapshot())
        if (sn.symbol == sym) out.push_back({sn.start, sn.end, sn.deps});
    return out;
}

// ---- 暴力 oracle：逐 cell 依赖集（与被测实现完全独立的对账基准）----
struct Oracle {
    static constexpr std::int64_t kUniv = 64;
    std::array<std::set<Rq>, kUniv> cells;  // symbol "S" 的逐 cell 在飞依赖
    std::array<bool, kUniv> done{};

    void reserve(std::int64_t a, std::int64_t b, const Rq& id) {
        for (std::int64_t c = a; c < b; ++c) cells[static_cast<std::size_t>(c)].insert(id);
    }

    Led::ReleaseResult release(const Rq& id) {
        Led::ReleaseResult out;
        std::int64_t i = 0;
        while (i < kUniv) {
            if (!cells[static_cast<std::size_t>(i)].count(id)) {
                ++i;
                continue;
            }
            std::set<Rq> rest = cells[static_cast<std::size_t>(i)];
            rest.erase(id);
            std::int64_t j = i + 1;
            if (rest.empty()) {
                while (j < kUniv && cells[static_cast<std::size_t>(j)].count(id) &&
                       cells[static_cast<std::size_t>(j)].size() == 1)
                    ++j;
                out.reclaimed.push_back({i, j});
                for (std::int64_t c = i; c < j; ++c) cells[static_cast<std::size_t>(c)].clear();
            } else {
                while (j < kUniv && cells[static_cast<std::size_t>(j)].count(id)) {
                    std::set<Rq> rj = cells[static_cast<std::size_t>(j)];
                    rj.erase(id);
                    if (rj != rest) break;
                    ++j;
                }
                out.orphaned.push_back({{i, j}, {rest.begin(), rest.end()}});
                for (std::int64_t c = i; c < j; ++c) cells[static_cast<std::size_t>(c)] = rest;
            }
            i = j;
        }
        return out;
    }

    Led::SettleResult settle(std::int64_t a, std::int64_t b, const Rq& id) {
        Led::SettleResult out;
        // 1) completed cells：id 移除；无人剩 → resolved
        std::int64_t i = a;
        while (i < b) {
            if (!cells[static_cast<std::size_t>(i)].count(id)) {
                ++i;
                continue;
            }
            std::set<Rq> rest = cells[static_cast<std::size_t>(i)];
            rest.erase(id);
            std::int64_t j = i + 1;
            if (rest.empty()) {
                while (j < b && cells[static_cast<std::size_t>(j)].count(id) &&
                       cells[static_cast<std::size_t>(j)].size() == 1)
                    ++j;
                out.resolved.push_back({i, j});
                for (std::int64_t c = i; c < j; ++c) cells[static_cast<std::size_t>(c)].clear();
            } else {
                while (j < b && cells[static_cast<std::size_t>(j)].count(id)) {
                    std::set<Rq> rj = cells[static_cast<std::size_t>(j)];
                    rj.erase(id);
                    if (rj != rest) break;
                    ++j;
                }
                for (std::int64_t c = i; c < j; ++c) cells[static_cast<std::size_t>(c)] = rest;
            }
            i = j;
        }
        // 2) 未完成 cells（id 在、不在 [a,b)）：有其他依赖 → degraded（id 保留在账上）
        i = 0;
        while (i < kUniv) {
            if ((i >= a && i < b) || !cells[static_cast<std::size_t>(i)].count(id)) {
                ++i;
                continue;
            }
            std::set<Rq> rest = cells[static_cast<std::size_t>(i)];
            rest.erase(id);
            if (rest.empty()) {
                ++i;
                continue;
            }
            std::int64_t j = i + 1;
            while (j < kUniv && !(j >= a && j < b) && cells[static_cast<std::size_t>(j)].count(id)) {
                std::set<Rq> rj = cells[static_cast<std::size_t>(j)];
                rj.erase(id);
                if (rj != rest) break;
                ++j;
            }
            out.degraded.push_back({{i, j}, {rest.begin(), rest.end()}});
            i = j;
        }
        return out;
    }

    Led::PlanResult plan(std::int64_t a, std::int64_t b) const {
        Led::PlanResult out;
        std::int64_t i = a;
        while (i < b) {
            const auto& ci = cells[static_cast<std::size_t>(i)];
            std::int64_t j = i + 1;
            if (done[static_cast<std::size_t>(i)]) {
                while (j < b && done[static_cast<std::size_t>(j)]) ++j;
                out.satisfied.push_back({i, j});
            } else if (ci.empty()) {
                while (j < b && !done[static_cast<std::size_t>(j)] &&
                       cells[static_cast<std::size_t>(j)].empty())
                    ++j;
                out.absent.push_back({i, j});
            } else {
                while (j < b && !done[static_cast<std::size_t>(j)] &&
                       cells[static_cast<std::size_t>(j)] == ci)
                    ++j;
                out.conditional.push_back({{i, j}, {ci.begin(), ci.end()}});
            }
            i = j;
        }
        return out;
    }

    std::vector<SnapEntry> snapshot() const {
        std::vector<SnapEntry> out;
        std::int64_t i = 0;
        while (i < kUniv) {
            const auto& ci = cells[static_cast<std::size_t>(i)];
            if (ci.empty()) {
                ++i;
                continue;
            }
            std::int64_t j = i + 1;
            while (j < kUniv && cells[static_cast<std::size_t>(j)] == ci) ++j;
            out.push_back({"S", i, j, {ci.begin(), ci.end()}});
            i = j;
        }
        return out;
    }
};

std::vector<SnapEntry> snap_of_sym(const Led& led, std::string_view sym) {
    std::vector<SnapEntry> out;
    for (const auto& e : led.export_snapshot())
        if (e.symbol == sym) out.push_back(e);
    return out;
}

}  // namespace

// doctest：容器断言失败时打印元素（复用上面各类型的 operator<<）
template <typename T>
static doctest::String vec_str(const std::vector<T>& v) {
    std::ostringstream os;
    os << "{";
    for (std::size_t i = 0; i < v.size(); ++i) os << (i ? "," : "") << v[i];
    os << "}";
    return doctest::String(os.str().c_str());
}
namespace doctest {
template <>
struct StringMaker<std::vector<R>> {
    static String convert(const std::vector<R>& v) { return vec_str(v); }
};
template <>
struct StringMaker<std::vector<Span>> {
    static String convert(const std::vector<Span>& v) { return vec_str(v); }
};
template <>
struct StringMaker<std::vector<Ent>> {
    static String convert(const std::vector<Ent>& v) { return vec_str(v); }
};
template <>
struct StringMaker<std::vector<SnapEntry>> {
    static String convert(const std::vector<SnapEntry>& v) { return vec_str(v); }
};
}  // namespace doctest

// ============ 场景表 §5（1–6、7–10、12 逐一精确断言） ============

TEST_CASE("场景1: 空账 + 请求 [6,11)") {
    Led led;
    InMemoryDoneIndex done;
    Led::PlanResult pr;
    REQUIRE(led.plan("X", r(6, 11), done, &pr));
    CHECK(pr.absent == std::vector<R>{r(6, 11)});
    CHECK(pr.conditional.empty());
    CHECK(pr.satisfied.empty());
}

TEST_CASE("场景2: [6,11) 全是 DONE") {
    Led led;
    InMemoryDoneIndex done;
    CHECK(done.insert("X", r(6, 11)));
    Led::PlanResult pr;
    REQUIRE(led.plan("X", r(6, 11), done, &pr));
    CHECK(pr.absent.empty());
    CHECK(pr.conditional.empty());
    CHECK(pr.satisfied == std::vector<R>{r(6, 11)});
}

TEST_CASE("场景3-5: A 在飞 [6,11)，B 请求各形态") {
    Led led;
    InMemoryDoneIndex done;
    CHECK(led.reserve("A", "X", {r(6, 11)}));
    Led::PlanResult pr;

    SUBCASE("场景3: B 请求 [8,12) → absent=[11,12), cond=[8,11)+依赖A") {
        REQUIRE(led.plan("X", r(8, 12), done, &pr));
        CHECK(pr.absent == std::vector<R>{r(11, 12)});
        CHECK(pr.conditional == std::vector<Span>{{r(8, 11), D("A")}});
        CHECK(pr.satisfied.empty());
    }
    SUBCASE("场景4: 被真包含 [8,10) → absent=∅, cond=[8,10)") {
        REQUIRE(led.plan("X", r(8, 10), done, &pr));
        CHECK(pr.absent.empty());
        CHECK(pr.conditional == std::vector<Span>{{r(8, 10), D("A")}});
        CHECK(pr.satisfied.empty());
    }
    SUBCASE("场景5: 真包含已有 [6,12) → absent=[11,12), cond=[6,11)") {
        REQUIRE(led.plan("X", r(6, 12), done, &pr));
        CHECK(pr.absent == std::vector<R>{r(11, 12)});
        CHECK(pr.conditional == std::vector<Span>{{r(6, 11), D("A")}});
        CHECK(pr.satisfied.empty());
    }
    SUBCASE("B reserve [8,12) 后的条目形态") {
        CHECK(led.reserve("B", "X", {r(8, 12)}));
        // [6,8){A} 与 [8,11){A,B} deps 不同 → 不融合（I4），各自独立成条目
        CHECK(ents(led, "X") ==
              std::vector<Ent>{{6, 8, D("A")}, {8, 11, D("A", "B")}, {11, 12, D("B")}});
    }
}

TEST_CASE("场景6: 边界相接不相交；同 deps 相邻必须融合（I4 正例）") {
    Led led;
    CHECK(led.reserve("A", "X", {r(6, 11)}));

    SUBCASE("B reserve [11,12)：rc 各自独立，不合并") {
        Led::PlanResult pr;
        InMemoryDoneIndex done;
        REQUIRE(led.plan("X", r(11, 12), done, &pr));
        CHECK(pr.absent == std::vector<R>{r(11, 12)});  // 半开：[6,11) 不含 11
        CHECK(led.reserve("B", "X", {r(11, 12)}));
        CHECK(ents(led, "X") == std::vector<Ent>{{6, 11, D("A")}, {11, 12, D("B")}});
    }
    SUBCASE("同 id 相接 reserve → 融合为一条") {
        CHECK(led.reserve("A", "X", {r(11, 14)}));
        CHECK(ents(led, "X") == std::vector<Ent>{{6, 14, D("A")}});
    }
}

TEST_CASE("场景7: 零长/反向一律拒绝，无副作用") {
    Led led;
    InMemoryDoneIndex done;
    CHECK(led.reserve("A", "X", {r(6, 11)}));
    const auto before = led.export_snapshot();

    CHECK_FALSE(led.reserve("A", "X", {r(5, 5)}));
    CHECK_FALSE(led.reserve("A", "X", {r(9, 3)}));
    CHECK_FALSE(led.reserve("A", "X", {r(1, 4), r(5, 5)}));  // all-or-nothing
    Led::PlanResult pr;
    CHECK_FALSE(led.plan("X", r(5, 5), done, &pr));
    CHECK_FALSE(led.plan("X", r(9, 3), done, &pr));
    Led::SettleResult sr;
    CHECK_FALSE(led.settle("A", "X", {r(5, 5)}, &sr));
    CHECK(led.export_snapshot() == before);
    CHECK(led.check_invariants());
}

TEST_CASE("场景8: 同一 request_id 重复 reserve 幂等") {
    Led led;
    CHECK(led.reserve("A", "X", {r(6, 11)}));
    const auto after_first = led.export_snapshot();
    CHECK(led.reserve("A", "X", {r(6, 11)}));
    CHECK(led.export_snapshot() == after_first);
    CHECK(led.entry_count() == 1);
    CHECK(ents(led, "X") == std::vector<Ent>{{6, 11, D("A")}});
    CHECK(led.check_invariants());
}

TEST_CASE("场景9: A 取消，B 仍依赖 [8,11)") {
    Led led;
    CHECK(led.reserve("A", "X", {r(6, 11)}));
    CHECK(led.reserve("B", "X", {r(8, 12)}));
    Led::ReleaseResult rr;
    REQUIRE(led.release("A", "X", &rr));
    CHECK(rr.orphaned == std::vector<Span>{{r(8, 11), D("B")}});
    CHECK(rr.reclaimed == std::vector<R>{r(6, 8)});
    // B 的两段同 deps 融合回一条
    CHECK(ents(led, "X") == std::vector<Ent>{{8, 12, D("B")}});
    CHECK(led.check_invariants());
}

TEST_CASE("场景10: A 只完成 [6,9)，[9,11) 落空") {
    Led led;
    CHECK(led.reserve("A", "X", {r(6, 11)}));
    CHECK(led.reserve("B", "X", {r(8, 12)}));
    Led::SettleResult sr;
    REQUIRE(led.settle("A", "X", {r(6, 9)}, &sr));
    CHECK(sr.resolved == std::vector<R>{r(6, 8)});      // A 独占段正常转 DONE
    CHECK(sr.degraded == std::vector<Span>{{r(9, 11), D("B")}});
    // [8,9) 完成但 B 仍在 → 保留为 B 的悬空兴趣；[9,11) 未完成 → id 保留（D4）
    CHECK(ents(led, "X") ==
          std::vector<Ent>{{8, 9, D("B")}, {9, 11, D("A", "B")}, {11, 12, D("B")}});

    SUBCASE("DONE 优先：已落库 [6,9) 之后新请求不再对 [8,9) 产生依赖") {
        InMemoryDoneIndex done;
        CHECK(done.insert("X", r(6, 9)));
        Led::PlanResult pr;
        REQUIRE(led.plan("X", r(6, 11), done, &pr));
        CHECK(pr.satisfied == std::vector<R>{r(6, 9)});
        CHECK(pr.conditional == std::vector<Span>{{r(9, 11), D("A", "B")}});
        CHECK(pr.absent.empty());
    }
}

TEST_CASE("settle 语义补充: 空结算 / 重复结算 / 未涉 id") {
    Led led;
    CHECK(led.reserve("A", "X", {r(6, 11)}));
    CHECK(led.reserve("B", "X", {r(8, 12)}));
    const auto base = led.export_snapshot();

    SUBCASE("空 completed = 全部未完成：只对有他人依赖的段发 degraded，条目不动") {
        Led::SettleResult sr;
        CHECK(led.settle("A", "X", {}, &sr));
        CHECK(sr.resolved.empty());
        CHECK(sr.degraded == std::vector<Span>{{r(8, 11), D("B")}});
        CHECK(led.export_snapshot() == base);
    }
    SUBCASE("重复结算同一段：完成部分已无 id，未完成部分重复 degraded（按区间去重是上层职责）") {
        Led::SettleResult sr1, sr2;
        CHECK(led.settle("A", "X", {r(6, 9)}, &sr1));
        CHECK(led.settle("A", "X", {r(6, 9)}, &sr2));
        CHECK(sr2.resolved.empty());
        CHECK(sr2.degraded == std::vector<Span>{{r(9, 11), D("B")}});
    }
    SUBCASE("未涉 id 的 settle 无效果") {
        Led::SettleResult sr;
        CHECK(led.settle("Z", "X", {r(6, 11)}, &sr));
        CHECK(sr.resolved.empty());
        CHECK(sr.degraded.empty());
        CHECK(led.export_snapshot() == base);
    }
}

TEST_CASE("release 语义补充: 未涉 id / 无人接盘即回收") {
    Led led;
    CHECK(led.reserve("A", "X", {r(6, 11)}));
    const auto base = led.export_snapshot();
    Led::ReleaseResult rr;
    CHECK(led.release("Z", "X", &rr));
    CHECK(rr.orphaned.empty());
    CHECK(rr.reclaimed.empty());
    CHECK(led.export_snapshot() == base);

    CHECK(led.release("A", "X", &rr));
    CHECK(rr.orphaned.empty());
    CHECK(rr.reclaimed == std::vector<R>{r(6, 11)});
    CHECK(led.entry_count() == 0);
    CHECK(led.symbol_count() == 0);  // 空 bucket 清理
}

TEST_CASE("reserve 补充: 多 range 相接融合 / 跨空隙多条目") {
    SUBCASE("相接的多 range 融合为一条") {
        Led led;
        CHECK(led.reserve("A", "S", {r(6, 8), r(8, 11)}));
        CHECK(ents(led, "S") == std::vector<Ent>{{6, 11, D("A")}});
    }
    SUBCASE("跨空隙 reserve 切出 5 段") {
        Led led;
        CHECK(led.reserve("C", "S", {r(6, 8)}));
        CHECK(led.reserve("C", "S", {r(10, 12)}));
        CHECK(led.reserve("B", "S", {r(7, 11)}));
        CHECK(ents(led, "S") ==
              std::vector<Ent>{{6, 7, D("C")},
                               {7, 8, D("B", "C")},
                               {8, 10, D("B")},
                               {10, 11, D("B", "C")},
                               {11, 12, D("C")}});
        CHECK(led.check_invariants());
    }
}

TEST_CASE("多 symbol 隔离与逐 symbol 调用（决定 D5）") {
    Led led;
    CHECK(led.reserve("A", "S1", {r(0, 10)}));
    CHECK(led.reserve("A", "S2", {r(0, 10)}));
    CHECK(led.symbol_count() == 2);
    Led::ReleaseResult rr;
    CHECK(led.release("A", "S1", &rr));
    CHECK(rr.reclaimed == std::vector<R>{r(0, 10)});
    CHECK(led.symbol_count() == 1);  // S1 的空 bucket 已清理
    CHECK(led.entry_count() == 1);   // S2 不受影响
    CHECK(led.release("A", "S2", &rr));
    CHECK(led.entry_count() == 0);
}

// ============ 冻结稿场景 12：碎片回归（§8.3） ============

TEST_CASE("场景12: [7,8)/[9,10) 反复插入删除 1000 轮不残留碎片") {
    for (const auto& [lo, hi] : {std::pair<std::int64_t, std::int64_t>{7, 8}, {9, 10}}) {
        CAPTURE(lo);
        CAPTURE(hi);
        Led led;
        CHECK(led.reserve("A", "S", {r(6, 11)}));
        for (int i = 0; i < 1000; ++i) {
            CHECK(led.reserve("B", "S", {r(lo, hi)}));
            CHECK(led.entry_count() == 3);
            Led::ReleaseResult rr;
            CHECK(led.release("B", "S", &rr));
            CHECK(rr.orphaned == std::vector<Span>{{r(lo, hi), D("A")}});
            CHECK(rr.reclaimed.empty());
            CHECK(ents(led, "S") == std::vector<Ent>{{6, 11, D("A")}});
            REQUIRE(led.check_invariants());
        }
    }
}

// ============ 不变量检查器：真阳性自测（§8.4） ============

TEST_CASE("不变量检查器: 手工坏结构逐一触发 I1/I2/I3/I4/键一致") {
    using Bucket = CoverageBucket<UnixMsDomain>;
    using E = BasicLedgerEntry<UnixMsDomain>;
    using Deps = std::set<Rq>;
    Bucket ok;
    ok.emplace(6, E{6, 8, Deps{"A"}, 1});
    ok.emplace(8, E{8, 11, Deps{"A", "B"}, 2});
    ok.emplace(20, E{20, 25, Deps{"A"}, 1});
    CHECK(coverage_entries_invariants_ok<UnixMsDomain>(ok));

    {  // I1 重叠
        Bucket b = ok;
        b.emplace(7, E{7, 9, Deps{"C"}, 1});
        CHECK_FALSE(coverage_entries_invariants_ok<UnixMsDomain>(b));
    }
    {  // I2 零长
        Bucket b = ok;
        b.erase(20);
        b.emplace(20, E{20, 20, Deps{"C"}, 1});
        CHECK_FALSE(coverage_entries_invariants_ok<UnixMsDomain>(b));
    }
    {  // 僵尸条目（deps 空）
        Bucket b = ok;
        b.erase(20);
        b.emplace(20, E{20, 25, Deps{}, 0});
        CHECK_FALSE(coverage_entries_invariants_ok<UnixMsDomain>(b));
    }
    {  // I3 rc != |deps|
        Bucket b = ok;
        b.erase(20);
        b.emplace(20, E{20, 25, Deps{"C"}, 2});
        CHECK_FALSE(coverage_entries_invariants_ok<UnixMsDomain>(b));
    }
    {  // I4 相邻同 deps 未融合
        Bucket b = ok;
        b.erase(20);
        b.emplace(20, E{20, 25, Deps{"A"}, 1});
        b.emplace(25, E{25, 30, Deps{"A"}, 1});
        CHECK_FALSE(coverage_entries_invariants_ok<UnixMsDomain>(b));
    }
    {  // map 键与条目 start 不一致
        Bucket b = ok;
        b.erase(6);
        b.emplace(7, E{6, 8, Deps{"A"}, 1});
        CHECK_FALSE(coverage_entries_invariants_ok<UnixMsDomain>(b));
    }
}

// ============ 快照导出 / 恢复 ============

TEST_CASE("快照导出/恢复: 往返 + 幂等 + 乱序无关 + 非法整体拒绝") {
    Led led;
    CHECK(led.reserve("A", "S1", {r(6, 11)}));
    CHECK(led.reserve("B", "S1", {r(8, 12)}));
    CHECK(led.reserve("C", "S2", {r(0, 5)}));
    auto snap = led.export_snapshot();
    REQUIRE(snap.size() == 4);
    CHECK(snap[0] == SnapEntry{"S1", 6, 8, D("A")});
    CHECK(snap[1] == SnapEntry{"S1", 8, 11, D("A", "B")});
    CHECK(snap[2] == SnapEntry{"S1", 11, 12, D("B")});
    CHECK(snap[3] == SnapEntry{"S2", 0, 5, D("C")});

    Led led2;
    CHECK(led2.restore(snap));
    CHECK(led2.export_snapshot() == snap);
    CHECK(led2.check_invariants());

    SUBCASE("幂等：重复 restore 状态不变") {
        CHECK(led2.restore(snap));
        CHECK(led2.export_snapshot() == snap);
    }
    SUBCASE("乱序无关：条目与依赖顺序不影响终态") {
        auto shuffled = snap;
        std::mt19937 rng(7);
        std::shuffle(shuffled.begin(), shuffled.end(), rng);
        for (auto& e : shuffled) std::shuffle(e.deps.begin(), e.deps.end(), rng);
        Led led3;
        CHECK(led3.restore(shuffled));
        CHECK(led3.export_snapshot() == snap);
    }
    SUBCASE("非法条目整体拒绝且无副作用") {
        for (const SnapEntry& bad_extra :
             {SnapEntry{"S9", 5, 5, D("X")},        // 零长
              SnapEntry{"", 1, 2, D("X")},          // 空 symbol
              SnapEntry{"S9", 1, 2, std::vector<Rq>{}},  // 空 deps
              SnapEntry{"S9", 9, 3, D("X")}}) {     // 反向
            auto bad = snap;
            bad.push_back(bad_extra);
            Led led3;
            CHECK_FALSE(led3.restore(bad));
            CHECK(led3.entry_count() == 0);
        }
    }
}

// ============ 恢复配方演练（文件头「启动恢复配方」第 1–3 步，纯函数级） ============

TEST_CASE("恢复配方演练: 崩溃后按真实存储对账") {
    // 旧世界：DONE [0,6)；A 承诺 [6,11)；B 承诺 [8,12)。随后进程崩溃。
    InMemoryDoneIndex done_old;
    CHECK(done_old.insert("S1", r(0, 6)));
    Led led;
    CHECK(led.reserve("A", "S1", {r(6, 11)}));
    CHECK(led.reserve("B", "S1", {r(8, 12)}));
    auto snap = led.export_snapshot();
    auto done_segs = done_old.index_of("S1")->segments();

    // 新世界：载 DONE → restore INFLIGHT
    InMemoryDoneIndex done_new;
    for (const auto& s : done_segs) CHECK(done_new.insert("S1", s));
    Led led2;
    CHECK(led2.restore(snap));
    CHECK(led2.check_invariants());

    // 存储探测（测试双）：崩溃前 A 的任务实际落库 [0,9)（[9,11) 未落库，B 的 [11,12) 未落库）
    const R stored{0, 9};
    for (const auto& e : snap) {
        if (e.symbol != "S1") continue;
        // 与真实覆盖求交（clamp 到 stored）
        const std::int64_t lo = std::max(e.start, stored.start);
        const std::int64_t hi = std::min(e.end, stored.end);
        if (lo >= hi) continue;  // 全无 → 重派或 release，此处交给下方 release 政策
        CHECK(done_new.insert("S1", r(lo, hi)));  // 补记 DONE
        for (const auto& id : e.deps) {           // 逐依赖 settle（幂等）
            Led::SettleResult sr;
            REQUIRE(led2.settle(id, "S1", {r(lo, hi)}, &sr));
        }
    }
    CHECK(led2.check_invariants());
    // A 的未完成段 [9,11) 仍挂着 A（D4 重试安全）；任务系统已丢 → 终态 release
    Led::ReleaseResult rr;
    CHECK(led2.release("A", "S1", &rr));
    CHECK(rr.orphaned == std::vector<Span>{{r(9, 11), D("B")}});
    CHECK(led2.release("B", "S1", &rr));
    // 对账时 B 对已落库 [8,9) 的兴趣已随 settle 消解（[8,9) 是 [8,11){A,B} 的依赖之一），
    // 剩余悬空段 [9,12) 在 release(B) 后相邻融合回收
    CHECK(rr.reclaimed == std::vector<R>{r(9, 12)});
    CHECK(led2.entry_count() == 0);

    // 对账后世界与真值一致：[0,9) 免费，[9,20) 需购买
    Led::PlanResult pr;
    REQUIRE(led2.plan("S1", r(0, 20), done_new, &pr));
    CHECK(pr.satisfied == std::vector<R>{r(0, 9)});
    CHECK(pr.absent == std::vector<R>{r(9, 20)});
    CHECK(pr.conditional.empty());
}

// ============ 冻结稿 §8.2 property fuzz：oracle 对账 ============

TEST_CASE("property fuzz: 随机 reserve/settle/release/plan，逐步 oracle 对账 + 不变量") {
    std::mt19937 rng(20261113);
    Oracle orc;
    InMemoryDoneIndex done_idx;
    Led led;
    std::set<Rq> live;
    std::map<Rq, int> resv_count;  // (id, range) 净计数（碎片上界用）
    int next_id = 0;
    std::int64_t live_resv = 0;

    auto new_id = [&]() { return Rq("R") + std::to_string(next_id++); };
    auto pick_id = [&]() {
        return live.empty() ? new_id()
                            : *std::next(live.begin(), static_cast<long>(rng() % live.size()));
    };
    auto rand_point = [&]() { return static_cast<std::int64_t>(rng() % Oracle::kUniv); };
    // 端点分布刻意覆盖：相接/完全重合（短段高发）、包含、部分重叠、零长、反向（§8.2）
    auto rand_range = [&]() {
        std::int64_t a = rand_point();
        std::int64_t b;
        const int k = static_cast<int>(rng() % 10);
        if (k == 0) {
            b = a;  // 零长（无效）
        } else if (k <= 4) {
            b = std::min<std::int64_t>(a + 1 + static_cast<std::int64_t>(rng() % 3),
                                       Oracle::kUniv);  // 短段：相接/重合高发
        } else {
            b = rand_point();
            if (b < a) {
                if (k == 5) {
                    /* 保持反向（无效） */
                } else {
                    std::swap(a, b);
                }
            }
        }
        return R{a, b};
    };

    for (int step = 0; step < 4000; ++step) {
        const int op = static_cast<int>(rng() % 100);
        if (op < 6) {
            // DONE 维护（settle 落库 insert / 淘汰 erase 的模拟）
            R q = rand_range();
            if (q.start < q.end) {
                if (rng() % 2 == 0) {
                    CHECK(done_idx.insert("S", q));
                    for (std::int64_t c = q.start; c < q.end; ++c) orc.done[c] = true;
                } else {
                    CHECK(done_idx.erase("S", q));
                    for (std::int64_t c = q.start; c < q.end; ++c) orc.done[c] = false;
                }
            }
        } else if (op < 42) {
            // reserve
            const Rq id = pick_id();
            const R q = rand_range();
            std::vector<R> one{q};
            if (q.start >= q.end) {
                CHECK_FALSE(led.reserve(id, "S", one));
            } else {
                CHECK(led.reserve(id, "S", one));
                orc.reserve(q.start, q.end, id);
                if (!live.count(id)) live.insert(id);
                resv_count[id] += 1;
                ++live_resv;
                if (rng() % 100 < 20) {  // I5：立即重复 reserve，状态必须不变
                    const auto after_first = led.export_snapshot();
                    CHECK(led.reserve(id, "S", one));
                    CHECK(led.export_snapshot() == after_first);
                }
            }
        } else if (op < 72) {
            // release
            if (!live.empty()) {
                const Rq id = pick_id();
                Led::ReleaseResult rr;
                REQUIRE(led.release(id, "S", &rr));
                const auto expect = orc.release(id);
                CHECK(rr.orphaned == expect.orphaned);
                CHECK(rr.reclaimed == expect.reclaimed);
                live.erase(id);
                live_resv -= resv_count[id];
                resv_count.erase(id);
            }
        } else if (op < 90) {
            // settle（任务终态结算）
            if (!live.empty()) {
                const Rq id = pick_id();
                const R q = rand_range();
                std::vector<R> one{q};
                Led::SettleResult sr;
                if (q.start >= q.end) {
                    CHECK_FALSE(led.settle(id, "S", one, &sr));
                } else {
                    CHECK(led.settle(id, "S", one, &sr));
                    const auto expect = orc.settle(q.start, q.end, id);
                    CHECK(sr.resolved == expect.resolved);
                    CHECK(sr.degraded == expect.degraded);
                }
            }
        } else {
            // plan 探针
            const R q = rand_range();
            if (q.start < q.end) {
                Led::PlanResult pr;
                CHECK(led.plan("S", q, done_idx, &pr));
                CHECK(pr == orc.plan(q.start, q.end));
            }
        }

        // ---- 每步：不变量 + 全量状态 oracle 对账 + 碎片上界（场景 11）----
        REQUIRE(led.check_invariants());
        CHECK(snap_of_sym(led, "S") == orc.snapshot());
        if (live_resv > 0) CHECK(led.entry_count() <= 4 * live_resv + 4);
    }

    // 收尾：全部 release → 账目归零，不残留任何碎片
    for (const auto& id : live) {
        Led::ReleaseResult rr;
        REQUIRE(led.release(id, "S", &rr));
        const auto expect = orc.release(id);
        CHECK(rr.orphaned == expect.orphaned);
        CHECK(rr.reclaimed == expect.reclaimed);
    }
    live.clear();
    REQUIRE(led.check_invariants());
    CHECK(led.entry_count() == 0);
    CHECK(orc.snapshot().empty());
    Led::PlanResult pr;
    CHECK(led.plan("S", R{0, Oracle::kUniv}, done_idx, &pr));
    CHECK(pr == orc.plan(0, Oracle::kUniv));
}
