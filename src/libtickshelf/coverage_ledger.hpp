#pragma once
// 区间覆盖账（coverage ledger）——.local-docs/区间记账_需求文档.md（v0.1 冻结稿）的实现。
// 纯数据 + 纯函数：无 IO、无锁、无持久化（冻结稿 §1 非目标）。并发由调用方对同一
// symbol 的 plan+reserve 串行化（§6），settle/release 也必须走同一临界区。
//
// 回答三个问题（§1）：
//   plan    —— 新请求的净新增区间是什么（只为这些付额度）？
//   plan.conditional —— 依赖了谁（哪些区间、哪些在飞请求）？
//   settle / release —— 某请求完成/取消时，哪些区间失去支撑？
//
// ===== 对冻结稿的实现决定（勘误与补全，全文见需求文档「实现附录」）=====
//   D1 I4 的 "state" 是草案残留：§2.3 条目无 state 字段（本结构只存 INFLIGHT，
//      §2.2），合并条件 = deps 全等（rc 相等由 deps 全等蕴含）。
//   D2 DoneIndex 的唯一虚方法是 covered_segments()：冻结稿 §6 的 bool covered()
//      无法支撑 DONE 优先的三段运算（不知 DONE 边界无法切分）；covered() 退化为
//      由它派生的便捷方法。
//   D3 PlanResult.conditional 携带依赖方（DepSpan{range, deps}）——冻结稿场景 3
//      的 "cond=[8,11)+依赖A" 需要依赖归因，纯 Range 列表放不下。
//   D4 settle 按字面语义：未完成段的请求自身 deps 保留（重试安全——重试期间若被
//      并发 plan 判成 absent 会双花）。语义 = 任务终态结算：completed 未提及的
//      在飞段一律视为未完成并产出 degraded；重试走新任务尝试（幂等 reserve +
//      再次 settle）或终态 release。终态失败上层必须补 release(id)，否则 id 的
//      deps 永久残留；此时他人先后收到 degraded + orphaned 两个事件（按区间去重）。
//   D5 settle/release 补 symbol 参数（本实现内部按 symbol 分桶）；跨 symbol 请求
//      由上层逐 symbol 调用，与 §6 的 per-symbol 临界区纪律天然对齐。
//   D6 reserve 参数名 ranges（冻结稿 §6 草图叫 absent 有误导）：语义是全量幂等
//      登记——与在飞相交的段加依赖（= conditional 部分），缺口新建条目（= absent
//      部分）。协议：plan → 扣额度成功 → reserve(全跨度)。
//   D7 错误处理按项目惯例：无异常，bool 返回（false = 参数非法，out 不动），
//      多 range 预校验 all-or-nothing（冻结稿场景 7 允许"返回错误码"）。
//
// ===== 启动恢复配方（上层账本职责，本层提供原语）=====
//   1) 载 DONE：存储侧索引（tickshelf 里即 idx:{sym} ZSET 一类的段索引）一次性
//      全量加载进 BasicInMemoryDoneIndex（合并后每 symbol 段数很小）。之后
//      settle 落库即 insert 增量维护；**淘汰路径必须同步 erase**——漏删则 plan
//      永远报 satisfied，查询拿到缺失数据的"免费"承诺（本层最大操作风险）。
//   2) 重建 INFLIGHT：上层持久化的事件流/快照 → ledger.restore()（幂等）。
//   3) 对账：逐在飞条目探测真实存储——全覆盖 → done.insert(覆盖段) + 逐依赖
//      settle；部分覆盖 → settle(覆盖部分)，缺口按 degraded/orphaned 政策处置；
//      全无 → 任务系统仍在则重派（账本不动），已丢则 release（同租约 TTL +
//      reaper 纪律）。测试里有纯函数级演练（test_coverage_ledger.cpp 恢复配方）。
//
// 复杂度（§7）：per-symbol 有序 map，plan O(log n + k)；量级 = 单 symbol 几百
// 条目，线性扫重叠段足够，不需要区间树。条目结构 deps 是 std::set：I5 幂等
// （重复 reserve/release 是 no-op）直接来自集合语义；refcount 为冗余校验字段（I3）。
#include <algorithm>
#include <cstddef>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "interval_set.hpp"

namespace tickshelf {

using RequestId = std::string;  // 语义 id（UUID/业务号均可），set 导出即有序

// ---- INFLIGHT 条目（冻结稿 §2.3）----
// refcount 冗余存一份（== deps.size()），供 I3 不变量交叉校验；deps 是唯一权威。
template <typename Domain>
struct BasicLedgerEntry {
    typename Domain::point start = 0;
    typename Domain::point end = 0;  // 半开 [start, end)
    std::set<RequestId> deps;        // 谁依赖"这段会被拉下来"（含原拉取方）
    std::size_t refcount = 0;        // == deps.size()（I3）
};

template <typename Domain>
using CoverageBucket = std::map<typename Domain::point, BasicLedgerEntry<Domain>>;  // start → 条目

// ---- 结果类型（§4；conditional/degraded/orphaned 携带依赖方，见 D3）----
template <typename Domain>
struct BasicDepSpan {
    BasicRange<Domain> range;
    std::vector<RequestId> deps;  // 有序（set 导出）
    bool operator==(const BasicDepSpan&) const = default;
};

template <typename Domain>
struct BasicPlanResult {
    std::vector<BasicRange<Domain>> absent;              // 需扣额度 + 新建任务
    std::vector<BasicDepSpan<Domain>> conditional;       // 不扣额度，登记依赖（依赖谁）
    std::vector<BasicRange<Domain>> satisfied;           // DONE 已覆盖，免费
    bool operator==(const BasicPlanResult&) const = default;
};

template <typename Domain>
struct BasicSettleResult {
    std::vector<BasicRange<Domain>> resolved;            // 完成且无其他依赖 → 移出 INFLIGHT（数据转 DONE）
    std::vector<BasicDepSpan<Domain>> degraded;          // 未完成 → 这些其他依赖方失去支撑
    bool operator==(const BasicSettleResult&) const = default;
};

template <typename Domain>
struct BasicReleaseResult {
    std::vector<BasicDepSpan<Domain>> orphaned;          // 他人仍依赖 → 上层决定接盘/降级
    std::vector<BasicRange<Domain>> reclaimed;           // 无人依赖，已删除
    bool operator==(const BasicReleaseResult&) const = default;
};

// ---- 不变量检查器（§3，独立函数形式；测试与调试常开）----
// I1 有序不重叠 / I2 无零长度 / I3 rc==|deps| 且 deps 非空 / I4 相邻且 deps 全等必须已融合。
// 自由函数形式 + 公开条目类型：测试可手工构造坏结构验证检查器本身（真阳性自测）。
template <typename Domain>
bool coverage_entries_invariants_ok(const CoverageBucket<Domain>& es) {
    using D = Domain;
    bool first = true;
    typename D::point prev_end = 0;
    std::set<RequestId> prev_deps;
    for (const auto& [start, e] : es) {
        if (!point_eq<D>(start, e.start)) return false;              // map 键与条目一致
        if (!D::valid_range(e.start, e.end)) return false;           // I2（含零长/反向）
        if (e.deps.empty()) return false;                            // rc==0 的条目必删（僵尸）
        if (e.refcount != e.deps.size()) return false;               // I3
        if (!first) {
            if (D::less(e.start, prev_end)) return false;            // I1 重叠
            if (D::adjacent(prev_end, e.start) && prev_deps == e.deps) return false;  // I4 漏融合
        }
        prev_end = e.end;
        prev_deps = e.deps;
        first = false;
    }
    return true;
}

// ---- DONE 覆盖由外部提供（§6）----
// 唯一虚方法 covered_segments（D2）。实现方是存储侧的覆盖索引/缓存。
template <typename Domain>
struct BasicDoneIndex {
    virtual ~BasicDoneIndex() = default;
    // 返回 r 内已覆盖子段：有序、不相交、已与 r 求交裁剪（不要求合并相邻段）。
    virtual std::vector<BasicRange<Domain>> covered_segments(std::string_view sym,
                                                             BasicRange<Domain> r) const = 0;
    // 便捷方法（冻结稿 §6 的 covered）：r 是否被完整覆盖。
    bool covered(std::string_view sym, BasicRange<Domain> r) const {
        if (!Domain::valid_range(r.start, r.end)) return false;
        auto segs = covered_segments(sym, r);
        return segs.size() == 1 && !Domain::less(r.start, segs[0].start) &&
               !Domain::less(segs[0].end, r.end);
    }
};

// 进程内 DONE 缓存（启动恢复配方第 1 步）：启动从存储索引一次性加载（insert 自动
// 合并），settle 落库后增量 insert，**淘汰路径必须 erase**。存储侧始终是真源，
// 这里只是 plan() 消费的加速结构。
template <typename Domain>
class BasicInMemoryDoneIndex final : public BasicDoneIndex<Domain> {
public:
    bool insert(std::string_view sym, BasicRange<Domain> r) {
        return per_sym_[std::string(sym)].insert(r);
    }
    // 淘汰时调用。符号不存在或无交集 = 已不是 DONE → 成功（语义：确保未标记）。
    bool erase(std::string_view sym, BasicRange<Domain> r) {
        auto it = per_sym_.find(sym);
        return it == per_sym_.end() ? true : it->second.erase(r);
    }
    std::vector<BasicRange<Domain>> covered_segments(std::string_view sym,
                                                     BasicRange<Domain> r) const override {
        auto it = per_sym_.find(sym);
        return it == per_sym_.end() ? std::vector<BasicRange<Domain>>{}
                                    : it->second.covered_segments(r);
    }
    const BasicIntervalSet<Domain>* index_of(std::string_view sym) const {  // 观测/测试
        auto it = per_sym_.find(sym);
        return it == per_sym_.end() ? nullptr : &it->second;
    }

private:
    std::map<std::string, BasicIntervalSet<Domain>, std::less<>> per_sym_;
};

// ---- 快照条目（export/restore；序列化与落盘归上层，本层不做 IO）----
template <typename Domain>
struct BasicLedgerSnapshotEntry {
    std::string symbol;
    typename Domain::point start = 0;
    typename Domain::point end = 0;
    std::vector<RequestId> deps;
    bool operator==(const BasicLedgerSnapshotEntry&) const = default;
};

// ---- 区间覆盖账（冻结稿 §6 CoverageLedger）----
template <typename Domain>
class BasicCoverageLedger {
public:
    using Range = BasicRange<Domain>;
    using Entry = BasicLedgerEntry<Domain>;
    using Bucket = CoverageBucket<Domain>;
    using DepSpan = BasicDepSpan<Domain>;
    using PlanResult = BasicPlanResult<Domain>;
    using SettleResult = BasicSettleResult<Domain>;
    using ReleaseResult = BasicReleaseResult<Domain>;
    using SnapshotEntry = BasicLedgerSnapshotEntry<Domain>;
    using Point = typename Domain::point;

    // ---- plan（§4.1）：三段式净新增，DONE 优先于 INFLIGHT ----
    // 前置：与随后的 reserve 在同一 symbol 临界区内完成，否则两个并发请求都会
    // 判定"未覆盖"而双花（§6）。
    bool plan(std::string_view sym, Range r, const BasicDoneIndex<Domain>& done,
              PlanResult* out) const {
        if (!Domain::valid_range(r.start, r.end)) return false;
        *out = {};
        out->satisfied = done.covered_segments(sym, r);  // satisfied = r ∩ DONE
        auto gaps = subtract_range<Domain>(r, out->satisfied);  // 缺口 = r − DONE
        const Bucket* es = find_bucket(sym);
        if (es == nullptr) {
            out->absent = std::move(gaps);
            return true;
        }
        for (const Range& gap : gaps) {
            std::vector<Range> inflight_pieces;
            for (auto it = overlap_begin(*es, gap.start);
                 it != es->end() && Domain::less(it->first, gap.end); ++it) {
                const Point lo = Domain::less(gap.start, it->first) ? it->first : gap.start;
                const Point hi = Domain::less(it->second.end, gap.end) ? it->second.end : gap.end;
                inflight_pieces.push_back({lo, hi});
                out->conditional.push_back(
                    {{lo, hi}, {it->second.deps.begin(), it->second.deps.end()}});
            }
            auto absent = subtract_range<Domain>(gap, inflight_pieces);
            out->absent.insert(out->absent.end(), absent.begin(), absent.end());
        }
        return true;
    }

    // ---- reserve（§4.2）：登记承诺。前置：额度已扣成功（本层不扣减）----
    // ranges = 请求全跨度（D6）。任一 range 非法 → 整体拒绝（D7）。
    bool reserve(const RequestId& id, std::string_view sym, const std::vector<Range>& ranges) {
        for (const Range& r : ranges)
            if (!Domain::valid_range(r.start, r.end)) return false;
        auto runs = ranges;
        normalize_runs<Domain>(&runs);
        for (const Range& r : runs)
            apply_dep_change(id, sym, r, /*add=*/true, nullptr);
        return true;
    }

    // ---- settle（§4.3）：任务终态结算（协议语义见 D4 与文件头注记）----
    // completed = 任务实际完成的区间，可乱序可重叠（内部规范化）。
    bool settle(const RequestId& id, std::string_view sym, const std::vector<Range>& completed,
                SettleResult* out) {
        for (const Range& r : completed)
            if (!Domain::valid_range(r.start, r.end)) return false;
        *out = {};
        auto runs = completed;
        normalize_runs<Domain>(&runs);
        Bucket* es = find_bucket(sym);
        if (es == nullptr) return true;  // 该 symbol 无在飞承诺：完成段无主、无 degraded
        // 1) 完成段：切分 + 移除 id；deps 空 → resolved（条目删除，数据转 DONE）
        for (const Range& c : runs)
            apply_dep_change(id, sym, c, /*add=*/false, &out->resolved);
        // 2) 未完成段：id 条目 − completed，产出 degraded（id 自身保留在 deps，D4）
        std::vector<DepSpan> degraded;
        for (const auto& [start, e] : *es) {
            if (!e.deps.count(id)) continue;
            std::vector<RequestId> others;
            for (const auto& d : e.deps)
                if (d != id) others.push_back(d);
            if (others.empty()) continue;  // 无人可通知（条目保留，等重试或终态 release）
            auto rem = subtract_range<Domain>(Range{e.start, e.end}, runs);
            for (const Range& p : rem) degraded.push_back({p, others});
        }
        coalesce_run_list(&out->resolved);  // 相邻 resolved 段合并输出（相邻条目同次清空）
        coalesce_span_list(&degraded);
        out->degraded = std::move(degraded);
        drop_bucket_if_empty(sym);
        return true;
    }

    // ---- release / cancel（§4.4）：请求退出。取消不是简单删条目 ----
    // 【退款顺序】上层先处置 orphaned（接盘/降级），再结算退款（§4.4）。
    bool release(const RequestId& id, std::string_view sym, ReleaseResult* out) {
        *out = {};
        Bucket* es = find_bucket(sym);
        if (es == nullptr) return true;
        // 先收集再改：遍历中 erase 不安全
        std::vector<Point> affected;
        for (const auto& [start, e] : *es)
            if (e.deps.count(id)) affected.push_back(start);
        std::vector<DepSpan> orphaned;
        std::vector<Range> reclaimed;
        Point wlo = 0, whi = 0;
        bool touched = false;
        for (const Point s : affected) {
            auto eit = es->find(s);
            Entry& e = eit->second;
            std::set<RequestId> others = e.deps;
            others.erase(id);
            if (!touched) {
                wlo = e.start;
                whi = e.end;
                touched = true;
            } else {
                if (Domain::less(e.start, wlo)) wlo = e.start;
                if (Domain::less(whi, e.end)) whi = e.end;
            }
            if (others.empty()) {
                reclaimed.push_back({e.start, e.end});
                es->erase(eit);
            } else {
                orphaned.push_back({{e.start, e.end}, {others.begin(), others.end()}});
                e.deps = std::move(others);
                e.refcount = e.deps.size();
            }
        }
        if (touched) coalesce_window(*es, wlo, whi);
        coalesce_span_list(&orphaned);
        normalize_runs<Domain>(&reclaimed);
        out->orphaned = std::move(orphaned);
        out->reclaimed = std::move(reclaimed);
        drop_bucket_if_empty(sym);
        return true;
    }

    // ---- 不变量（§3）：全 symbol 扫描。测试与调试常开，生产可关 ----
    bool check_invariants() const {
        for (const auto& [sym, es] : symbols_)
            if (!coverage_entries_invariants_ok<Domain>(es)) return false;
        return true;
    }

    std::size_t entry_count() const {
        std::size_t n = 0;
        for (const auto& [sym, es] : symbols_) n += es.size();
        return n;
    }
    std::size_t symbol_count() const { return symbols_.size(); }

    // ---- 快照导出 / 恢复（启动恢复配方第 2 步的原语）----
    std::vector<SnapshotEntry> export_snapshot() const {
        std::vector<SnapshotEntry> out;
        for (const auto& [sym, es] : symbols_)
            for (const auto& [start, e] : es)
                out.push_back({sym, e.start, e.end, {e.deps.begin(), e.deps.end()}});
        return out;
    }
    // 幂等恢复：逐依赖走 reserve 语义（与条目/依赖顺序无关，I4 融合保证同终态）。
    // 任一条目非法（空 symbol / 零长反向 / 空 deps）→ 整体拒绝返回 false，无副作用。
    bool restore(const std::vector<SnapshotEntry>& entries) {
        for (const auto& e : entries)
            if (e.symbol.empty() || !Domain::valid_range(e.start, e.end) || e.deps.empty())
                return false;
        for (const auto& e : entries)
            for (const auto& id : e.deps)
                apply_dep_change(id, e.symbol, Range{e.start, e.end}, /*add=*/true, nullptr);
        return check_invariants();
    }

private:
    static Entry make_entry(Point a, Point b, std::set<RequestId> deps) {
        Entry e;
        e.start = a;
        e.end = b;
        e.deps = std::move(deps);
        e.refcount = e.deps.size();
        return e;
    }

    Bucket* find_bucket(std::string_view sym) {
        auto it = symbols_.find(sym);
        return it == symbols_.end() ? nullptr : &it->second;
    }
    const Bucket* find_bucket(std::string_view sym) const {
        auto it = symbols_.find(sym);
        return it == symbols_.end() ? nullptr : &it->second;
    }
    void drop_bucket_if_empty(std::string_view sym) {
        auto it = symbols_.find(sym);
        if (it != symbols_.end() && it->second.empty()) symbols_.erase(it);
    }

    // 首个与 [lo, hi) 可能重叠的条目：key <= lo 且 end > lo 的最后一条，否则
    // key > lo 的第一条（后者 end > key > lo，重叠与否由 walk 条件 key < hi 决定）。
    // 相邻条目不重叠且有序 ⇒ 后续条目只要 key < hi 就必然与 [lo, hi) 相交。
    static typename Bucket::iterator overlap_begin(Bucket& es, Point lo) {
        auto it = es.upper_bound(lo);
        if (it != es.begin()) {
            auto p = std::prev(it);
            if (Domain::less(lo, p->second.end)) return p;
        }
        return it;
    }
    static typename Bucket::const_iterator overlap_begin(const Bucket& es, Point lo) {
        auto it = es.upper_bound(lo);
        if (it != es.begin()) {
            auto p = std::prev(it);
            if (Domain::less(lo, p->second.end)) return p;
        }
        return it;
    }

    // I4 融合窗口：从 key<=from 的条目前一条起，向右融合「相邻 ∧ deps 全等」；
    // 处理完左成员越过 to 的条目对后停。多退一条是必须的：deps 改动的条目与其
    // 左邻可能新满足融合条件（如 release 后 deps 变回与左邻相同）。
    static void coalesce_window(Bucket& es, Point from, Point to) {
        auto it = es.upper_bound(from);
        if (it != es.begin()) --it;
        if (it != es.begin()) --it;
        while (it != es.end()) {
            auto nxt = std::next(it);
            if (nxt == es.end()) break;
            if (Domain::adjacent(it->second.end, nxt->first) &&
                it->second.deps == nxt->second.deps) {
                it->second.end = nxt->second.end;
                it->second.refcount = it->second.deps.size();
                es.erase(nxt);
            } else {
                ++it;
            }
            if (it != es.end() && Domain::less(to, it->first)) break;
        }
    }

    // 受影响区间重建（reserve 与 settle 完成段共用的唯一切分路径）：
    //   吞并与 [a,b) 相交的条目 → 以全部边界为切点重分段 → 逐段处理：
    //     add=true（reserve）：有主段 deps+id（幂等），无主段新建 deps={id}；
    //     add=false（settle 完成段）：段 ⊆ [a,b) 时移除 id，deps 空 → 记 resolved
    //       并删除；否则（两侧余量段）原样重插（id 保留，D4 重试安全）。
    //   最后按 I4 融合整个改动窗口（含两侧余量段与其外邻的接缝）。
    void apply_dep_change(const RequestId& id, std::string_view sym, Range r, bool add,
                          std::vector<Range>* resolved) {
        Bucket& es = symbols_[std::string(sym)];
        std::vector<Entry> consumed;
        auto it = overlap_begin(es, r.start);
        while (it != es.end() && Domain::less(it->first, r.end)) {
            consumed.push_back(std::move(it->second));
            it = es.erase(it);
        }
        std::vector<Point> cuts{r.start, r.end};
        for (const Entry& c : consumed) {
            cuts.push_back(c.start);
            cuts.push_back(c.end);
        }
        std::sort(cuts.begin(), cuts.end());
        cuts.erase(std::unique(cuts.begin(), cuts.end()), cuts.end());
        for (std::size_t i = 0; i + 1 < cuts.size(); ++i) {
            const Point a = cuts[i], b = cuts[i + 1];
            // 覆盖 [a,b) 的原条目（切点含全部条目边界 ⇒ 至多一个）
            const Entry* owner = nullptr;
            for (const Entry& c : consumed)
                if (!Domain::less(a, c.start) && !Domain::less(c.end, b)) {
                    owner = &c;
                    break;
                }
            if (owner == nullptr) {
                if (add) es.emplace(a, make_entry(a, b, {id}));
                continue;  // 无主完成段（数据不在任何承诺内）→ 忽略
            }
            const bool inside = !Domain::less(a, r.start) && !Domain::less(r.end, b);  // [a,b) ⊆ [r.start, r.end)
            if (!inside || !add) {
                if (!add && inside && owner->deps.count(id)) {
                    // 完成段：移除 id；无人依赖 → resolved（数据转 DONE，条目删除）
                    std::set<RequestId> deps = owner->deps;
                    deps.erase(id);
                    if (deps.empty()) {
                        if (resolved != nullptr) resolved->push_back({a, b});
                        continue;
                    }
                    es.emplace(a, make_entry(a, b, std::move(deps)));
                } else {
                    es.emplace(a, make_entry(a, b, owner->deps));  // 余量/他人条目原样
                }
            } else {
                std::set<RequestId> deps = owner->deps;
                deps.insert(id);  // I5：已在集合内则无变化
                es.emplace(a, make_entry(a, b, std::move(deps)));
            }
        }
        coalesce_window(es, cuts.front(), cuts.back());
    }

    // 输出规范化：相邻段融合（resolved/reclaimed）
    static void coalesce_run_list(std::vector<Range>* runs) { normalize_runs<Domain>(runs); }

    // 输出规范化：相邻且 deps 全等的跨度合并（conditional/degraded/orphaned）
    static void coalesce_span_list(std::vector<DepSpan>* spans) {
        std::sort(spans->begin(), spans->end(), [](const DepSpan& a, const DepSpan& b) {
            if (Domain::less(a.range.start, b.range.start)) return true;
            if (Domain::less(b.range.start, a.range.start)) return false;
            return a.deps < b.deps;
        });
        std::vector<DepSpan> out;
        for (auto& s : *spans) {
            if (!out.empty() && Domain::adjacent(out.back().range.end, s.range.start) &&
                out.back().deps == s.deps) {
                out.back().range.end = s.range.end;
                continue;
            }
            out.push_back(std::move(s));
        }
        *spans = std::move(out);
    }

    std::map<std::string, Bucket, std::less<>> symbols_;  // symbol → 有序条目表
};

using DoneIndex = BasicDoneIndex<UnixMsDomain>;
using InMemoryDoneIndex = BasicInMemoryDoneIndex<UnixMsDomain>;
using CoverageLedger = BasicCoverageLedger<UnixMsDomain>;  // 冻结稿 §6 命名映射

}  // namespace tickshelf
