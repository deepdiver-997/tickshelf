#pragma once
// 分片落库（ADR-020）：N 个独立 SQLite 文件 + N 个独占写线程。
//
// 路由 = 纯函数 FNV-1a(symbol) % N——**没有路由数据库、没有互斥锁**：路由是函数
// 不是状态，中心路由库只会把要并行化的写路径重新串行化，还多出一个崩溃一致性面。
// 一个 symbol 永远落同一个分片（无跨分片重复行）；改 N = 重哈希迁移，备案不做。
//
// 外部语义与单文件 SqliteTickStore 完全一致（同步 upsert_flush / query_range）；
// 内部：demux（每批跨多股 → 按路由拆到分片缓冲）→ 每分片有界队列（满则阻塞 =
// 背压，⑤的运行时对应物）→ 独占写线程并行执行（SQLite 单写者/文件，进程同构
// 曲线的拐点 N=8 见 benchmarks.md）→ 调用方等全部 future 聚合返回。
//
// 生命周期约束：单进程内读写不同时使用（queryd 只读 / daemon·persister 只写，
// 本项目的进程形态天然满足）；跨进程读写靠 WAL（ADR-005 注记）。
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "sqlite_store.hpp"
#include "tick_store.hpp"

namespace tickshelf {

class ShardedTickStore : public TickStore {
public:
    ShardedTickStore() = default;
    ~ShardedTickStore() override;
    ShardedTickStore(const ShardedTickStore&) = delete;
    ShardedTickStore& operator=(const ShardedTickStore&) = delete;

    // 分片数（open 前设置；N=1 退化为单文件、路径原样——行为与 SqliteTickStore 一致）
    void set_shards(int n) { shard_count_ = n > 1 ? n : 1; }

    // path：N=1 时原样；N>1 时分片文件 = path + "." + i（i = 0..N-1，各自盖版本戳）
    bool open(const std::string& path) override;
    std::int64_t upsert_batch(const std::vector<Tick>& ticks) override;
    std::int64_t upsert_flush(const std::vector<const std::vector<Tick>*>& batches) override;
    std::vector<Tick> query_range(const std::string& symbol, std::uint64_t from_ms,
                                  std::uint64_t to_ms) override;
    bool query_failed() const override { return query_failed_; }
    std::uint64_t count() const override;

private:
    // 分片 = 文件 + 独占写线程 + 有界作业队列（完整类型放头文件：vector<unique_ptr<Shard>>
    // 作为成员，隐式析构在含本头的 TU 里实例化时需要 Shard 完整）。
    // 队列访问模式恒为 SPSC（upsert_flush 同步 ⇒ 单提交者串行 + 单写线程），mutex+cv
    // 在 flush 节奏（~2 次/s/分片）下无竞争——队列深度结构性 ≤1（锁步：提交即等 future），
    // 冲突在任何存储速度下都不可见。换无锁 SPSC（ADR-010 备案的 quantify 复用点）的
    // 触发条件：flush 节奏达千次/秒级（工作线程持久化逐 tick 交接）或队列操作进入
    // stall P99。完整分析（含批提交均摊与队列流量的解耦证明）见 design.md ADR-020 附录。
    struct Shard {
        SqliteTickStore store;
        std::thread worker;
        std::mutex mu;
        std::condition_variable cv;
        struct Job {
            std::shared_ptr<const std::vector<std::vector<Tick>>> payload;  // 保活 demux 缓冲
            std::vector<const std::vector<Tick>*> batches;
            std::promise<std::int64_t> done;
        };
        std::deque<Job> jobs;
        bool stop = false;
    };

    // 路由哈希：规范化输入 = 去 NUL 后的实际 symbol 字节（写侧 strnlen(8)、读侧
    // string::size()——两侧同一规范形式，否则读写落到不同分片）
    std::size_t shard_of(const char* symbol, std::size_t len) const;
    void worker_loop(Shard& s);

    int shard_count_ = 1;  // 配置值（open 前设置）
    std::vector<std::unique_ptr<Shard>> shards_;
    mutable bool query_failed_ = false;
};

// 工厂（薄入口装配用）：shards<=1 → 单文件 SqliteTickStore（路径原样，兼容全部
// 现有工具与演练脚本）；>1 → ShardedTickStore。调用方拿到 TickStore 后统一 open(path)。
std::unique_ptr<TickStore> make_tick_store(int shards);

}  // namespace tickshelf
