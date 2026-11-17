#pragma once
// 分片持久化引擎（ADR-023）：把 Redis 发布与耐久落库从 tickshelfd 的收包线程
// 解耦到每分片一个 worker。
//
// 线程模型（务必遵守）：
//   - submit()/tick()/wait_idle()/drain()/stats() 只在源线程调用；
//   - 每分片一个 worker，独占该分片的 RedisShard 与 StorageShard；
//   - 源线程拥有 staging（每分片一个 Ingestor）与 sealed_pending；worker 只碰
//     队列、pending_records 与自身统计（atomic）。
//
// 载荷：封口后的 Slice 内部仍是 std::vector<Batch>，复用既有批 id/段/索引/state
// 协议；重试不重新分组，批 id 稳定。
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <vector>

#include "components/engine/slice.hpp"
#include "components/engine/write_policy.hpp"
#include "components/ingest/ingestor.hpp"

namespace tickshelf {

// 每分片的 Redis 端口（真实实现包装 RespClient + 既有 flush/finish/release；
// 测试用 fake 注入失败/部分认领）。只被对应分片 worker 调用。
class RedisShard {
public:
    virtual ~RedisShard() = default;
    virtual bool publish_and_claim(const std::vector<Batch>& batches, const FlushOptions& options,
                                   std::vector<char>& claimed) = 0;
    virtual bool finish(const std::vector<std::string>& batch_ids) = 0;
    virtual bool release(const std::vector<std::string>& batch_ids) = 0;
};

// 每分片的存储端口（真实实现包装 SqliteTickStore；未来 clickhouse/parquet 适配器
// 也实现这个接口）。只被对应分片 worker 调用，允许阻塞。
class StorageShard {
public:
    virtual ~StorageShard() = default;
    virtual std::int64_t upsert_flush(const std::vector<const std::vector<Tick>*>& batches) = 0;
    // Successful calls commit input to a record store, not a counting/Null sink.
    // This capability alone makes no power-loss or synchronous=FULL guarantee.
    virtual bool persists_records() const { return true; }
};

struct EngineConfig {
    std::size_t shards = 1;
    WriteMode write_mode = WriteMode::PublishOnly;
    std::uint64_t batch_records = 8192;       // 软上限：按 (symbol,date) 分组后可能略超
    std::uint64_t batch_bytes = 4ull << 20;   // 0 = 不按字节封口
    std::uint64_t batch_ms = 100;             // 0 = 只按容量封口
    std::size_t inflight = 8;                 // 每分片 ready 队列容量（Slice 数）
    std::uint64_t max_pending_records = 65536;  // 每分片 staging+queued+in-flight 上限
    std::uint64_t dedup_bytes = 0;            // 0 = 关闭去重
    std::string worker_id = "daemon";
    std::uint64_t lease_ttl_s = 60;
    std::uint64_t retry_ms = 50;
    std::uint64_t max_retry_ms = 1000;
    std::uint64_t finish_attempts = 3;
    std::uint64_t shutdown_ms = 10000;
};

struct ShardStats {
    std::uint64_t submitted_records = 0;
    std::uint64_t accepted_records = 0;
    std::uint64_t rejected_records = 0;
    std::uint64_t dedup_dropped = 0;
    std::uint64_t sealed_slices = 0;
    std::uint64_t published_records = 0;       // successful publish input, before claim filtering
    std::uint64_t committed_input_records = 0; // successful persistent-store input (includes duplicates)
    std::uint64_t inserted_unique_records = 0; // persistent store's actual new-row count
    std::uint64_t sink_consumed_records = 0;   // successful nonpersistent/Null-store input
    std::uint64_t durable_records = 0;         // legacy alias of committed_input_records, not a sync guarantee
    std::uint64_t publish_only_records = 0;
    std::uint64_t skipped_records = 0;
    std::uint64_t degraded_records = 0;
    std::uint64_t retries = 0;
    std::uint64_t finish_pending_records = 0;
    std::uint64_t queue_depth = 0;
    std::uint64_t queue_peak = 0;
    std::uint64_t pending_records = 0;
};

struct EngineStats {
    std::uint64_t submitted_records = 0;
    std::uint64_t accepted_records = 0;
    std::uint64_t rejected_records = 0;
    std::uint64_t dedup_dropped = 0;
    std::uint64_t sealed_slices = 0;
    std::uint64_t published_records = 0;       // successful publish input, before claim filtering
    std::uint64_t committed_input_records = 0; // successful persistent-store input (includes duplicates)
    std::uint64_t inserted_unique_records = 0; // persistent store's actual new-row count
    std::uint64_t sink_consumed_records = 0;   // successful nonpersistent/Null-store input
    std::uint64_t durable_records = 0;         // legacy alias of committed_input_records, not a sync guarantee
    std::uint64_t publish_only_records = 0;
    std::uint64_t skipped_records = 0;
    std::uint64_t degraded_records = 0;
    std::uint64_t retries = 0;
    std::uint64_t finish_pending_records = 0;
    std::uint64_t queue_depth = 0;
    std::uint64_t queue_peak = 0;
    std::uint64_t pending_records = 0;
    std::vector<ShardStats> shards;
};

struct DrainReport {
    bool complete = false;          // legacy alias of queue_complete, NOT storage completion
    bool queue_complete = false;    // includes drained staging; finish may still be pending
    bool storage_complete = false;  // all forwarded input committed by persistent shards
    std::uint64_t pending_records = 0;
    std::uint64_t published_records = 0;       // successful publish input, before claim filtering
    std::uint64_t committed_input_records = 0; // successful persistent-store input (includes duplicates)
    std::uint64_t inserted_unique_records = 0; // persistent store's actual new-row count
    std::uint64_t sink_consumed_records = 0;   // successful nonpersistent/Null-store input
    std::uint64_t durable_records = 0;         // legacy alias of committed_input_records, not a sync guarantee
    std::uint64_t degraded_records = 0;
    std::uint64_t publish_only_records = 0;
};

class Engine {
public:
    Engine(EngineConfig config, std::vector<std::unique_ptr<RedisShard>> redis,
           std::vector<std::unique_ptr<StorageShard>> storage);
    ~Engine();
    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;

    // 源线程：接收 ticks 的前缀；返回 accepted 数，未接受的尾部由调用方保留重试。
    std::size_t submit(std::span<const Tick> ticks, std::uint64_t now_ms);

    // 源线程：年龄封口 + 尝试把 sealed_pending 推入 ready 队列。每个 poll 轮次调用。
    void tick(std::uint64_t now_ms);

    // 源线程：只等已封口队列完成（不含 staging），不证明已存储：publish-only、
    // Null sink、skipped、degraded 都可能完成队列。完整存储状态见 drain 报告。
    bool wait_idle(std::chrono::milliseconds timeout);

    // 源线程：封口 staging，限时等待；超时唤醒退避并放弃尚未启动的队列 I/O。
    // 已进入 adapter 的阻塞 I/O 无法被此 deadline 打断；join 仍会等待它返回。
    DrainReport drain(std::chrono::milliseconds timeout);

    // 幂等停止（析构会调用）；等价于 drain(shutdown_ms)。
    void stop();

    EngineStats stats() const;
    std::uint64_t pending_records() const;  // 只统计已封口切片

private:
    struct Shard;

    void worker_loop(Shard& shard);
    void process(Shard& shard, const SlicePtr& slice);
    void seal(Shard& shard, std::uint64_t now_ms);
    void complete(Shard& shard, const SlicePtr& slice);
    bool can_accept(const Shard& shard) const;
    void observe_queue_peak(Shard& shard);
    bool retry_wait(std::uint64_t delay_ms);
    DrainReport drain_report() const;

    EngineConfig config_;
    std::vector<std::unique_ptr<RedisShard>> redis_;
    std::vector<std::unique_ptr<StorageShard>> storage_;
    std::vector<std::unique_ptr<Shard>> shards_;
    std::atomic<bool> stopping_{false};
    std::atomic<bool> stopped_{false};
    mutable std::mutex done_mutex_;
    std::condition_variable done_cv_;
    std::uint64_t pending_total_ = 0;  // 与 done_mutex_ 同锁
};

}  // namespace tickshelf
