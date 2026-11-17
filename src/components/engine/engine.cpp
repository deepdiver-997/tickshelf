#include "engine.hpp"

#include <algorithm>
#include <cassert>
#include <stdexcept>
#include <thread>
#include <utility>

#include "bounded_queue.hpp"
#include "components/storage/shard_route.hpp"
#include "dedupe.hpp"

namespace tickshelf {

struct Engine::Shard {
    Shard(std::size_t index_, const EngineConfig& config)
        : index(index_), ready(config.inflight), dedupe(config.dedup_bytes) {}

    const std::size_t index;
    Ingestor staging;                 // 仅源线程
    std::uint64_t staging_records = 0;  // 仅源线程
    std::uint64_t staging_since_ms = 0; // 仅源线程
    SlicePtr sealed_pending;          // 仅源线程：ready 满时暂存
    BoundedQueue<SlicePtr> ready;     // 源线程 push / worker pop
    DedupeCache dedupe;               // 仅源线程
    std::thread worker;
    std::atomic<std::uint64_t> pending_records{0};  // staging+queued+in-flight
    std::uint64_t next_sequence = 0;                // 仅源线程

    std::atomic<std::uint64_t> submitted{0};
    std::atomic<std::uint64_t> accepted{0};
    std::atomic<std::uint64_t> rejected{0};
    std::atomic<std::uint64_t> dedup_dropped{0};
    std::atomic<std::uint64_t> sealed_slices{0};
    std::atomic<std::uint64_t> published_records{0};
    std::atomic<std::uint64_t> committed_input_records{0};
    std::atomic<std::uint64_t> inserted_unique_records{0};
    std::atomic<std::uint64_t> sink_consumed_records{0};
    std::atomic<std::uint64_t> publish_only_records{0};
    std::atomic<std::uint64_t> skipped_records{0};
    std::atomic<std::uint64_t> degraded_records{0};
    std::atomic<std::uint64_t> retries{0};
    std::atomic<std::uint64_t> finish_pending_records{0};
    std::atomic<std::uint64_t> queue_peak{0};
};

Engine::Engine(EngineConfig config, std::vector<std::unique_ptr<RedisShard>> redis,
               std::vector<std::unique_ptr<StorageShard>> storage)
    : config_(std::move(config)), redis_(std::move(redis)), storage_(std::move(storage)) {
    if (config_.shards == 0) config_.shards = 1;
    if (config_.batch_records == 0) config_.batch_records = 1;
    if (config_.inflight == 0) config_.inflight = 1;
    if (config_.retry_ms == 0) config_.retry_ms = 1;
    config_.max_retry_ms = std::max(config_.max_retry_ms, config_.retry_ms);
    if (config_.finish_attempts == 0) config_.finish_attempts = 1;
    if (config_.shutdown_ms == 0) config_.shutdown_ms = 1;
    config_.max_pending_records = std::max(config_.max_pending_records, config_.batch_records);

    if (redis_.size() != config_.shards)
        throw std::invalid_argument("engine: redis shard count mismatch");
    if (config_.write_mode == WriteMode::WriteThrough && storage_.size() != config_.shards)
        throw std::invalid_argument("engine: storage shard count mismatch");

    shards_.reserve(config_.shards);
    for (std::size_t i = 0; i < config_.shards; ++i)
        shards_.push_back(std::make_unique<Shard>(i, config_));
    for (auto& shard : shards_)
        shard->worker = std::thread([this, shard = shard.get()] { worker_loop(*shard); });
}

Engine::~Engine() { stop(); }

bool Engine::can_accept(const Shard& shard) const {
    // sealed_pending 非空 = 该分片队列已满且还有一片没入队；不再接收新 tick。
    return !stopping_.load(std::memory_order_relaxed) && !shard.sealed_pending &&
           shard.pending_records.load(std::memory_order_relaxed) < config_.max_pending_records;
}

std::size_t Engine::submit(std::span<const Tick> ticks, std::uint64_t now_ms) {
    if (ticks.empty() || stopping_.load(std::memory_order_relaxed)) return 0;
    std::size_t accepted = 0;
    for (const Tick& tick : ticks) {
        Shard& shard = *shards_[shard_of(tick, shards_.size())];
        shard.submitted.fetch_add(1, std::memory_order_relaxed);
        if (!can_accept(shard)) {
            shard.rejected.fetch_add(1, std::memory_order_relaxed);
            break;
        }
        if (shard.dedupe.enabled() && shard.dedupe.check_and_insert(tick)) {
            shard.dedup_dropped.fetch_add(1, std::memory_order_relaxed);
            shard.accepted.fetch_add(1, std::memory_order_relaxed);
            ++accepted;
            continue;
        }
        if (shard.staging_records == 0) shard.staging_since_ms = now_ms;
        shard.staging.ingest(tick);
        ++shard.staging_records;
        shard.pending_records.fetch_add(1, std::memory_order_relaxed);
        shard.accepted.fetch_add(1, std::memory_order_relaxed);
        ++accepted;

        const bool by_records = shard.staging_records >= config_.batch_records;
        const bool by_bytes =
            config_.batch_bytes > 0 && shard.staging_records * kTickBytes >= config_.batch_bytes;
        if (by_records || by_bytes) seal(shard, now_ms);
    }
    return accepted;
}

void Engine::tick(std::uint64_t now_ms) {
    if (stopping_.load(std::memory_order_relaxed)) return;
    for (auto& ptr : shards_) {
        Shard& shard = *ptr;
        if (shard.sealed_pending) {
            if (shard.ready.try_push(shard.sealed_pending)) {
                shard.sealed_pending.reset();
                observe_queue_peak(shard);
            }
        }
        if (shard.sealed_pending) continue;
        if (shard.staging_records > 0 && config_.batch_ms > 0 &&
            now_ms >= shard.staging_since_ms &&
            now_ms - shard.staging_since_ms >= config_.batch_ms) {
            seal(shard, now_ms);
        }
    }
}

void Engine::seal(Shard& shard, std::uint64_t now_ms) {
    if (shard.staging_records == 0) return;
    if (shard.sealed_pending) return;  // 源线程不变量：有暂存片时不再产生新片
    auto batches = shard.staging.drain();
    shard.staging_records = 0;
    shard.staging_since_ms = now_ms;
    if (batches.empty()) return;

    auto slice = make_slice(static_cast<std::uint32_t>(shard.index), shard.next_sequence++,
                            std::move(batches));
    shard.sealed_slices.fetch_add(1, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lock(done_mutex_);
        pending_total_ += slice->records;
    }
    if (!shard.ready.try_push(slice)) shard.sealed_pending = std::move(slice);
    observe_queue_peak(shard);
}

void Engine::observe_queue_peak(Shard& shard) {
    const auto depth = static_cast<std::uint64_t>(shard.ready.size());
    auto peak = shard.queue_peak.load(std::memory_order_relaxed);
    while (depth > peak &&
           !shard.queue_peak.compare_exchange_weak(peak, depth, std::memory_order_relaxed)) {
    }
}

void Engine::complete(Shard& shard, const SlicePtr& slice) {
    const auto before = shard.pending_records.fetch_sub(slice->records, std::memory_order_relaxed);
    assert(before >= slice->records);
    (void)before;
    {
        std::lock_guard<std::mutex> lock(done_mutex_);
        if (pending_total_ >= slice->records)
            pending_total_ -= slice->records;
        else
            pending_total_ = 0;  // 防御：正常路径不可达
    }
    done_cv_.notify_all();
}

void Engine::process(Shard& shard, const SlicePtr& slice) {
    FlushOptions options;
    options.claim_lease = config_.write_mode == WriteMode::WriteThrough;
    options.worker_id = config_.worker_id + "-" + std::to_string(shard.index);
    options.lease_ttl_s = config_.lease_ttl_s;

    std::vector<char> claimed;
    for (;;) {
        if (stopping_.load(std::memory_order_acquire)) return;
        claimed.clear();
        if (redis_[shard.index]->publish_and_claim(slice->batches, options, claimed)) break;
        if (stopping_.load(std::memory_order_relaxed)) return;  // 未发布：保留 pending 计数
        ++slice->attempts;
        shard.retries.fetch_add(1, std::memory_order_relaxed);
        const auto shift = std::min<std::uint32_t>(slice->attempts, 6);
        std::uint64_t delay = config_.retry_ms;
        for (std::uint32_t i = 0; i < shift; ++i)
            delay = delay >= config_.max_retry_ms / 2
                        ? config_.max_retry_ms : delay * 2;
        if (!retry_wait(delay)) return;
    }
    shard.published_records.fetch_add(slice->records, std::memory_order_relaxed);
    slice->claimed = claimed;

    if (config_.write_mode == WriteMode::PublishOnly) {
        shard.publish_only_records.fetch_add(slice->records, std::memory_order_relaxed);
        complete(shard, slice);
        return;
    }

    std::vector<const std::vector<Tick>*> mine;
    std::vector<std::string> mine_ids;
    std::uint64_t mine_records = 0;
    for (std::size_t i = 0; i < slice->batches.size(); ++i) {
        if (i >= claimed.size() || claimed[i] == 0) continue;
        mine.push_back(&slice->batches[i].ticks);
        mine_ids.push_back(batch_id_of(slice->batches[i]));
        mine_records += slice->batches[i].ticks.size();
    }
    if (mine.empty()) {
        // 所有批都被别的 worker 认领：数据已在 Redis，本片无需落库。
        shard.skipped_records.fetch_add(slice->records, std::memory_order_relaxed);
        complete(shard, slice);
        return;
    }
    const std::uint64_t skipped = slice->records - mine_records;
    if (skipped > 0) shard.skipped_records.fetch_add(skipped, std::memory_order_relaxed);

    if (stopping_.load(std::memory_order_acquire)) return; // published, but not stored
    const std::int64_t inserted = storage_[shard.index]->upsert_flush(mine);
    if (inserted < 0) {
        // 数据仍在 Redis、state pending；释放自己的租约，让 persister 补偿。
        if (!stopping_.load(std::memory_order_acquire)) redis_[shard.index]->release(mine_ids);
        shard.degraded_records.fetch_add(mine_records, std::memory_order_relaxed);
        complete(shard, slice);
        return;
    }

    if (!storage_[shard.index]->persists_records()) {
        // A Null/counting sink must never mark real Redis records as stored/done.
        shard.sink_consumed_records.fetch_add(mine_records, std::memory_order_relaxed);
        if (!stopping_.load(std::memory_order_acquire)) redis_[shard.index]->release(mine_ids);
        complete(shard, slice);
        return;
    }
    shard.committed_input_records.fetch_add(mine_records, std::memory_order_relaxed);
    shard.inserted_unique_records.fetch_add(static_cast<std::uint64_t>(inserted),
                                            std::memory_order_relaxed);
    bool finished = false;
    for (std::uint64_t attempt = 0; attempt < config_.finish_attempts; ++attempt) {
        if (stopping_.load(std::memory_order_acquire)) break;
        if (redis_[shard.index]->finish(mine_ids)) {
            finished = true;
            break;
        }
        if (attempt + 1 < config_.finish_attempts && !retry_wait(config_.retry_ms)) break;
    }
    if (!finished) shard.finish_pending_records.fetch_add(mine_records, std::memory_order_relaxed);
    // Commit input and newly inserted rows are distinct; finish is Redis bookkeeping.
    complete(shard, slice);
}

bool Engine::retry_wait(std::uint64_t delay_ms) {
    std::unique_lock<std::mutex> lock(done_mutex_);
    return !done_cv_.wait_for(lock, std::chrono::milliseconds(delay_ms),
                              [&] { return stopping_.load(std::memory_order_acquire); });
}

void Engine::worker_loop(Shard& shard) {
    for (;;) {
        SlicePtr slice;
        if (!shard.ready.pop_wait(slice)) return;
        // close() normally drains a queue. Shutdown must not start one potentially
        // blocking adapter call per remaining slice after the drain budget expires.
        if (stopping_.load(std::memory_order_acquire)) return;
        process(shard, slice);
    }
}

DrainReport Engine::drain_report() const {
    const auto current = stats();
    DrainReport report;
    report.pending_records = current.pending_records;
    report.published_records = current.published_records;
    report.committed_input_records = current.committed_input_records;
    report.inserted_unique_records = current.inserted_unique_records;
    report.sink_consumed_records = current.sink_consumed_records;
    report.durable_records = current.durable_records;
    report.degraded_records = current.degraded_records;
    report.publish_only_records = current.publish_only_records;
    report.complete = report.queue_complete = report.pending_records == 0;
    report.storage_complete = report.queue_complete && config_.write_mode == WriteMode::WriteThrough &&
        current.committed_input_records == current.accepted_records - current.dedup_dropped &&
        std::all_of(storage_.begin(), storage_.end(),
                    [](const auto& store) { return store->persists_records(); });
    return report;
}

bool Engine::wait_idle(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(done_mutex_);
    return done_cv_.wait_for(lock, timeout, [&] { return pending_total_ == 0; });
}

DrainReport Engine::drain(std::chrono::milliseconds timeout) {
    if (stopped_.load(std::memory_order_acquire)) return drain_report();

    const auto deadline = std::chrono::steady_clock::now() + timeout;

    struct PendingSlice {
        Shard* shard;
        SlicePtr slice;
    };
    std::vector<PendingSlice> pending;
    for (auto& ptr : shards_) {
        Shard& shard = *ptr;
        if (shard.sealed_pending) pending.push_back({&shard, shard.sealed_pending});
        if (shard.staging_records > 0) {
            auto batches = shard.staging.drain();
            shard.staging_records = 0;
            if (!batches.empty()) {
                auto slice = make_slice(static_cast<std::uint32_t>(shard.index),
                                        shard.next_sequence++, std::move(batches));
                shard.sealed_slices.fetch_add(1, std::memory_order_relaxed);
                {
                    std::lock_guard<std::mutex> lock(done_mutex_);
                    pending_total_ += slice->records;
                }
                pending.push_back({&shard, std::move(slice)});
            }
        }
        shard.sealed_pending.reset();
    }

    for (auto& item : pending) {
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now());
        if (remaining.count() <= 0 ||
            !item.shard->ready.push_wait_for(item.slice, remaining)) {
            item.shard->sealed_pending = item.slice;
        }
    }

    {
        std::unique_lock<std::mutex> lock(done_mutex_);
        done_cv_.wait_until(lock, deadline, [&] { return pending_total_ == 0; });
    }

    // drain 期间不提前置 stopping：worker 仍可在整个 deadline 内重试发布。
    // 超时后才停止重试并排空/放弃，未完成的 pending 会如实出现在报告里。
    {
        std::lock_guard<std::mutex> lock(done_mutex_);
        stopping_.store(true, std::memory_order_release);
    }
    done_cv_.notify_all(); // interrupt retry backoff
    // Redis 端口可被中断：阻塞中的 publish/finish/release 立即以失败返回，
    // join 不再等满一个连接超时。Storage 的阻塞提交不可中断，等它完成当前调用。
    for (auto& port : redis_) port->interrupt();
    for (auto& ptr : shards_) ptr->ready.close();
    for (auto& ptr : shards_)
        if (ptr->worker.joinable()) ptr->worker.join();
    stopped_.store(true, std::memory_order_release);

    return drain_report();
}

void Engine::stop() {
    if (!stopped_.load(std::memory_order_acquire))
        (void)drain(std::chrono::milliseconds(config_.shutdown_ms));
}

EngineStats Engine::stats() const {
    EngineStats out;
    for (const auto& ptr : shards_) {
        const Shard& shard = *ptr;
        ShardStats st;
        st.submitted_records = shard.submitted.load(std::memory_order_relaxed);
        st.accepted_records = shard.accepted.load(std::memory_order_relaxed);
        st.rejected_records = shard.rejected.load(std::memory_order_relaxed);
        st.dedup_dropped = shard.dedup_dropped.load(std::memory_order_relaxed);
        st.sealed_slices = shard.sealed_slices.load(std::memory_order_relaxed);
        st.published_records = shard.published_records.load(std::memory_order_relaxed);
        st.committed_input_records = shard.committed_input_records.load(std::memory_order_relaxed);
        st.inserted_unique_records = shard.inserted_unique_records.load(std::memory_order_relaxed);
        st.sink_consumed_records = shard.sink_consumed_records.load(std::memory_order_relaxed);
        st.durable_records = st.committed_input_records;
        st.publish_only_records = shard.publish_only_records.load(std::memory_order_relaxed);
        st.skipped_records = shard.skipped_records.load(std::memory_order_relaxed);
        st.degraded_records = shard.degraded_records.load(std::memory_order_relaxed);
        st.retries = shard.retries.load(std::memory_order_relaxed);
        st.finish_pending_records = shard.finish_pending_records.load(std::memory_order_relaxed);
        st.queue_depth = static_cast<std::uint64_t>(shard.ready.size());
        st.queue_peak = shard.queue_peak.load(std::memory_order_relaxed);
        st.pending_records = shard.pending_records.load(std::memory_order_relaxed);

        out.submitted_records += st.submitted_records;
        out.accepted_records += st.accepted_records;
        out.rejected_records += st.rejected_records;
        out.dedup_dropped += st.dedup_dropped;
        out.sealed_slices += st.sealed_slices;
        out.published_records += st.published_records;
        out.committed_input_records += st.committed_input_records;
        out.inserted_unique_records += st.inserted_unique_records;
        out.sink_consumed_records += st.sink_consumed_records;
        out.durable_records += st.durable_records;
        out.publish_only_records += st.publish_only_records;
        out.skipped_records += st.skipped_records;
        out.degraded_records += st.degraded_records;
        out.retries += st.retries;
        out.finish_pending_records += st.finish_pending_records;
        out.queue_depth += st.queue_depth;
        out.queue_peak += st.queue_peak;
        out.pending_records += st.pending_records;
        out.shards.push_back(st);
    }
    return out;
}

std::uint64_t Engine::pending_records() const {
    std::lock_guard<std::mutex> lock(done_mutex_);
    return pending_total_;
}

}  // namespace tickshelf
