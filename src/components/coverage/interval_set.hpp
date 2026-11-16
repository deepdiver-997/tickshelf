#pragma once
// 有序半开区间集合（持续合并）+ 覆盖账的域策略与区间运算原语
// （.local-docs/区间记账_需求文档.md §9 第 1 步："Range 运算是全部逻辑的地基"）。
//
// 本文件是区间覆盖账（coverage_ledger.hpp）与 DONE 覆盖缓存共用的地基：
//   - Domain 策略：把点类型与「排序 / 相邻 / 合法性」三个域操作显式化，使覆盖账
//     不绑定毫秒时间戳（使用者可传入自己的比较与相邻判定）。不需要「拆分」钩子：
//     全部切分只发生在调用方提供的端点或既有条目边界处，本结构从不发明新点，
//     因此也不需要后继算术——顺带消除了点运算溢出问题。
//   - BasicIntervalSet：maximally-merged 的半开区间集合（std::map<start, end>）。
//     DONE 覆盖缓存（InMemoryDoneIndex）的原语；插入自动融合重叠段与相邻段，
//     挖洞后的余量也回填融合——任意时刻不存在「相邻却未合并」的两段。
//   - subtract_range / normalize_runs：有序段列表上的纯函数运算（可直连单测）。
//
// 复杂度（冻结稿 §7）：有序 map，insert/erase/covered O(log n + k)。
// 不需要区间树——目标量级单 symbol 几百条目，线性扫重叠段足够。
//
// 模板说明：本项目此前零模板；此处泛型化是覆盖账的明确需求（不局限单一场景）。
// 刻意只用一层无约束的策略类型（静态多态、零开销），无 concepts/SFINAE。
// 模板实现必须在头文件内（同 fixed.hpp 的 header-only 先例），故本文件无 .cpp。
#include <algorithm>
#include <cstddef>
#include <map>
#include <vector>

namespace tickshelf {

// ---- 域策略：点类型 + 三个域操作（冻结稿 §2.1 的泛型化）----
// UnixMsDomain：毫秒时间戳（int64，冻结稿推荐单位）。
// adjacent 用相等判定：半开区间 [s1,e1) 与 [s2,e2) 在 e1 == s2 时中间无空隙。
// 量化/对齐域可自定义（例如按交易日对齐的域允许跨周末视为相邻）。
struct UnixMsDomain {
    using point = std::int64_t;
    static bool less(point a, point b) noexcept { return a < b; }
    // 两段是否无缝相接（可按 I4 融合的相邻判定）
    static bool adjacent(point e1, point s2) noexcept { return e1 == s2; }
    // 半开区间合法性：start < end。零长/反向一律拒绝（冻结稿场景 7）。
    static bool valid_range(point s, point e) noexcept { return s < e; }
};

// 域上的相等（由全序导出，不要求 point 自带 operator==）
template <typename D>
inline bool point_eq(typename D::point a, typename D::point b) noexcept {
    return !D::less(a, b) && !D::less(b, a);
}

template <typename Domain>
struct BasicRange {
    typename Domain::point start = 0;
    typename Domain::point end = 0;  // 半开 [start, end)
    bool operator==(const BasicRange&) const = default;
};

// r − runs：从 r 中减去有序不相交段集。
// 契约：runs 有序、互不重叠；未与 r 裁剪也安全（超出部分被忽略），产出 ⊆ r 且有序。
template <typename Domain>
inline std::vector<BasicRange<Domain>> subtract_range(
    BasicRange<Domain> r, const std::vector<BasicRange<Domain>>& runs) {
    using Point = typename Domain::point;
    std::vector<BasicRange<Domain>> out;
    Point cur = r.start;
    for (const auto& seg : runs) {
        if (Domain::less(cur, seg.start)) out.push_back({cur, seg.start});
        if (Domain::less(cur, seg.end)) cur = seg.end;
        if (!Domain::less(cur, r.end)) break;
    }
    if (Domain::less(cur, r.end)) out.push_back({cur, r.end});
    return out;
}

// 乱序/重叠/相邻的段列表 → 有序不相交最大段（相邻融合用 Domain::adjacent）。
// 非法段（零长/反向）直接丢弃——调用方入口已校验时这是死代码，仅作兜底。
template <typename Domain>
inline void normalize_runs(std::vector<BasicRange<Domain>>* runs) {
    std::sort(runs->begin(), runs->end(), [](const auto& a, const auto& b) {
        if (Domain::less(a.start, b.start)) return true;
        if (Domain::less(b.start, a.start)) return false;
        return Domain::less(a.end, b.end);
    });
    std::vector<BasicRange<Domain>> out;
    for (auto seg : *runs) {
        if (!Domain::valid_range(seg.start, seg.end)) continue;
        if (!out.empty()) {
            auto& last = out.back();
            if (Domain::less(seg.start, last.end) || Domain::adjacent(last.end, seg.start)) {
                if (Domain::less(last.end, seg.end)) last.end = seg.end;
                continue;
            }
        }
        out.push_back(seg);
    }
    *runs = std::move(out);
}

// ---- BasicIntervalSet：持续合并的有序半开区间集合 ----
// 自不变量：runs_ 按 start 有序；任意两段不重叠且不相邻（最大合并形态）。
// 作为纯集合，相邻段融合永远正确（覆盖账的条目不能这样融——deps 全等才行，
// 那条规则在 coverage_ledger.hpp 的 I4）。
template <typename Domain>
class BasicIntervalSet {
public:
    using Range = BasicRange<Domain>;
    using Point = typename Domain::point;

    // 插入（自动与重叠/相邻段融合）。非法区间返回 false 且无副作用。
    bool insert(Range r) {
        if (!Domain::valid_range(r.start, r.end)) return false;
        Point lo = r.start, hi = r.end;
        auto it = runs_.lower_bound(lo);
        if (it != runs_.begin()) {
            auto p = std::prev(it);
            // p 的右端越过 lo（重叠/相接）或域定义为相邻 → 从 p 开始吞并
            if (!Domain::less(p->second, lo) || Domain::adjacent(p->second, lo)) it = p;
        }
        while (it != runs_.end()) {
            const Point s = it->first, e = it->second;
            const bool mergeable =
                Domain::less(s, hi) || point_eq<Domain>(s, hi) || Domain::adjacent(hi, s);
            if (!mergeable) break;
            if (Domain::less(s, lo)) lo = s;
            if (Domain::less(hi, e)) hi = e;
            it = runs_.erase(it);
        }
        runs_.emplace(lo, hi);
        return true;
    }

    // 挖洞 [r.start, r.end)：两侧余量保留为段。非法区间返回 false 且无副作用。
    bool erase(Range r) {
        if (!Domain::valid_range(r.start, r.end)) return false;
        auto it = runs_.lower_bound(r.start);
        if (it != runs_.begin()) {
            auto p = std::prev(it);
            if (Domain::less(r.start, p->second)) it = p;  // p 越过洞左界 → 与洞相交
        }
        std::vector<Range> frags;  // 余量经 insert 回填（自动与邻段融合，保持最大合并）
        while (it != runs_.end() && Domain::less(it->first, r.end)) {
            const Point s = it->first, e = it->second;
            it = runs_.erase(it);
            if (Domain::less(s, r.start)) frags.push_back({s, r.start});
            if (Domain::less(r.end, e)) frags.push_back({r.end, e});
        }
        for (const Range& f : frags) insert(f);
        return true;
    }

    // r 是否被完整覆盖（某一段 span 了整个 r）
    bool covered(Range r) const {
        if (!Domain::valid_range(r.start, r.end)) return false;
        auto it = runs_.upper_bound(r.start);  // first start > r.start
        if (it == runs_.begin()) return false;
        --it;  // start <= r.start 的最后一段
        return !Domain::less(it->second, r.end);
    }

    // r 内已覆盖子段：有序、不相交、已与 r 裁剪。DoneIndex 的实现基础。
    std::vector<Range> covered_segments(Range r) const {
        std::vector<Range> out;
        if (!Domain::valid_range(r.start, r.end)) return out;
        auto it = runs_.lower_bound(r.start);
        if (it != runs_.begin()) {
            auto p = std::prev(it);
            if (Domain::less(r.start, p->second)) it = p;  // p 越过 r 左界 → 相交
        }
        for (; it != runs_.end() && Domain::less(it->first, r.end); ++it) {
            const Point lo = Domain::less(it->first, r.start) ? r.start : it->first;
            const Point hi = Domain::less(r.end, it->second) ? r.end : it->second;
            out.push_back({lo, hi});
        }
        return out;
    }

    std::vector<Range> segments() const {
        std::vector<Range> out;
        out.reserve(runs_.size());
        for (const auto& [s, e] : runs_) out.push_back({s, e});
        return out;
    }
    std::size_t run_count() const { return runs_.size(); }
    bool empty() const { return runs_.empty(); }
    void clear() { runs_.clear(); }

private:
    std::map<Point, Point> runs_;  // start → end
};

using IntervalSet = BasicIntervalSet<UnixMsDomain>;

}  // namespace tickshelf
