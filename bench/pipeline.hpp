#pragma once

#include <functional>
#include <optional>

#include "config.hpp"
#include "hash.hpp"
#include "metrics.hpp"
#include "store.hpp"
#include "libtickshelf/resp_client.hpp"

namespace tickshelf::bench {

// Single-threaded and synchronous: deadlines bound queue waiting, not the
// duration of an in-flight Redis operation or store transaction. Bytes become
// immutable after encoding; ingress time and an optional SHA are process-local.
class Pipeline {
public:
    using Clock = std::function<std::uint64_t()>;
    Pipeline(const Config& config, Metrics& metrics, Store& store, Clock now = clock_ns);
    Pipeline(const Pipeline&) = delete;
    Pipeline& operator=(const Pipeline&) = delete;

    void add(const std::vector<Record>& records);
    void tick();
    // Explicit final drain, including all partial batches. Errors propagate;
    // neither destruction nor an error path implicitly commits pending records.
    void flush();
    std::uint64_t checksum() const { return checksum_; }
    std::uint64_t max_queue_age_ns() const;
    // Ceil to poll's millisecond precision. An already-due queue returns zero;
    // an empty pipeline returns idle_cap_ms. Synchronous I/O is not preempted.
    int poll_timeout_ms(int idle_cap_ms = 100) const;

private:
    enum class FlushReason { Rows, Bytes, Age, Tail };
    struct QueuePeak {
        std::size_t records = 0, bytes = 0;
        void observe(std::size_t count, std::size_t size);
    };

    std::optional<std::uint64_t> oldest_ingress_ns() const;
    bool due(const std::vector<EncodedRecord>& records) const;
    void update_metrics();
    void note_flush(const char* stage, FlushReason reason,
                    const std::vector<EncodedRecord>& records);
    void flush_hash(FlushReason reason);
    void flush_redis(FlushReason reason);
    void flush_store(FlushReason reason);
    void exec(const std::vector<std::vector<std::string>>& commands,
              std::vector<RespValue>& replies, const char* stage);

    const Config& config_;
    Metrics& metrics_;
    Store& store_;
    Clock now_;
    Deduper deduper_;
    RespClient redis_;
    std::vector<EncodedRecord> encoded_, hash_pending_, hash_scratch_;
    std::vector<EncodedRecord> redis_pending_, redis_scratch_, redis_decoded_, store_pending_;
    QueuePeak hash_peak_, redis_peak_, store_peak_;
    std::size_t hash_bytes_ = 0, redis_bytes_ = 0, store_bytes_ = 0;
    std::uint64_t redis_id_ = 0, checksum_ = kFnvOffset;
};

}  // namespace tickshelf::bench
