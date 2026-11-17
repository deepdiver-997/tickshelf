#include "pipeline.hpp"

#include <algorithm>
#include <map>
#include <stdexcept>
#include <utility>

namespace tickshelf::bench {
namespace {
void require(bool value, const std::string& message) {
    if (!value) throw std::runtime_error(message);
}
std::uint64_t age_ns(std::uint64_t now, std::uint64_t ingress) {
    return now >= ingress ? now - ingress : 0;
}
void batch_stat(Metrics& metrics, const std::string& name, std::uint64_t value,
                bool first) {
    metrics.inc(name + "_sum", value);
    auto& minimum = metrics.counters[name + "_min"];
    minimum = first ? value : std::min(minimum, value);
    auto& maximum = metrics.counters[name + "_max"];
    maximum = std::max(maximum, value);
}
}  // namespace

Pipeline::Pipeline(const Config& config, Metrics& metrics, Store& store, Clock now)
    : config_(config), metrics_(metrics), store_(store), now_(std::move(now)),
      deduper_(config, metrics) {
    require(static_cast<bool>(now_), "pipeline clock is required");
    if (config.redis == "publish" || config.redis == "roundtrip")
        require(redis_.connect(config.redis_host, static_cast<std::uint16_t>(config.redis_port)),
                "cannot connect private Redis");
    encoded_.reserve(config.batch_records);
    hash_pending_.reserve(config.hash_batch_records);
    hash_scratch_.reserve(config.hash_batch_records);
    redis_pending_.reserve(config.redis_batch_records);
    redis_scratch_.reserve(config.redis_batch_records);
    if (config.redis == "roundtrip") redis_decoded_.reserve(config.redis_batch_records);
    // Do not eagerly allocate a million-row vector for byte-limited workloads.
    store_pending_.reserve(std::min<std::uint32_t>(config.transaction_rows, 1024));
    for (const char* name : {"received", "parsed", "forwarded", "dedup_dropped",
             "committed_attempts", "sink_consumed", "rejected", "pipeline_transactions",
             "transaction_rows_sum", "transaction_rows_min", "transaction_rows_max",
             "transaction_bytes_sum", "transaction_bytes_min", "transaction_bytes_max"})
        metrics_.counters.try_emplace(name, 0);
    // Actual-work evidence for ablation tables. A disabled network/hash stage
    // still retains queue handoff and canonical encoding; do not call it free.
    metrics_.counters["ablation_work_schema"] = 1;
    for (const char* name : {"record_encoded_records", "redis_build_records",
             "redis_publish_records", "redis_readback_records", "redis_network_execs",
             "redis_cleanup_batches", "dedup_lookup_records", "dedup_candidate_comparisons",
             "hash_bypassed_records"})
        metrics_.counters.try_emplace(name, 0);
    for (const char* stage : {"hash", "redis", "store"})
        for (const char* reason : {"rows", "bytes", "age", "tail"})
            metrics_.counters.try_emplace(std::string(stage) + "_flush_" + reason, 0);
    update_metrics();
}

void Pipeline::QueuePeak::observe(std::size_t count, std::size_t size) {
    records = std::max(records, count);
    bytes = std::max(bytes, size);
}

void Pipeline::add(const std::vector<Record>& records) {
    // All records from one inbound frame retain the same ingress timestamp,
    // including time spent encoding, hashing, Redis roundtripping and writing.
    const auto ingress = now_();
    encoded_.clear();
    {
        Timer timer(metrics_, "record_encode");
        for (const auto& record : records) {
            auto bytes = encode_record(record);
            require(!bytes.empty() && bytes.size() <= kMaxRecordBytes, "invalid canonical record");
            metrics_.inc("canonical_bytes", bytes.size());
            metrics_.inc("record_encoded_records");
            encoded_.push_back({record.symbol, record.time, std::move(bytes), {}, false, ingress});
        }
    }
    for (auto& record : encoded_) {
        if (!hash_pending_.empty() && hash_bytes_ + record.bytes.size() + 4 > kMaxPayloadBytes)
            flush_hash(FlushReason::Bytes);
        hash_bytes_ += record.bytes.size() + 4;
        hash_pending_.push_back(std::move(record));
        hash_peak_.observe(hash_pending_.size(), hash_bytes_);
        if (hash_pending_.size() >= config_.hash_batch_records) flush_hash(FlushReason::Rows);
    }
    encoded_.clear();
    update_metrics();
}

std::optional<std::uint64_t> Pipeline::oldest_ingress_ns() const {
    std::optional<std::uint64_t> oldest;
    for (const auto* queue : {&hash_pending_, &redis_pending_, &store_pending_}) {
        if (!queue->empty() && (!oldest || queue->front().ingress_ns < *oldest))
            oldest = queue->front().ingress_ns;
    }
    return oldest;
}

std::uint64_t Pipeline::max_queue_age_ns() const {
    const auto oldest = oldest_ingress_ns();
    return oldest ? age_ns(now_(), *oldest) : 0;
}

int Pipeline::poll_timeout_ms(int idle_cap_ms) const {
    const int cap = std::max(0, idle_cap_ms);
    const auto oldest = oldest_ingress_ns();
    if (!oldest) return cap;
    const auto age = age_ns(now_(), *oldest);
    const auto limit = static_cast<std::uint64_t>(config_.transaction_ms) * 1000000;
    if (age >= limit) return 0;
    const auto remaining_ms = (limit - age + 999999) / 1000000;
    return static_cast<int>(std::min<std::uint64_t>(static_cast<std::uint64_t>(cap), remaining_ms));
}

bool Pipeline::due(const std::vector<EncodedRecord>& records) const {
    return !records.empty() && age_ns(now_(), records.front().ingress_ns) >=
        static_cast<std::uint64_t>(config_.transaction_ms) * 1000000;
}

void Pipeline::tick() {
    // Re-evaluate each actual remaining front after every synchronous stage.
    // Expiring an old store batch must not flush younger hash/Redis records.
    if (due(hash_pending_)) flush_hash(FlushReason::Age);
    if (due(redis_pending_)) flush_redis(FlushReason::Age);
    if (due(store_pending_)) flush_store(FlushReason::Age);
    update_metrics();
}

void Pipeline::flush() {
    flush_hash(FlushReason::Tail);
    flush_redis(FlushReason::Tail);
    flush_store(FlushReason::Tail);
    update_metrics();
}

void Pipeline::update_metrics() {
    auto& counters = metrics_.counters;
    counters["buffered_records"] = hash_pending_.size() + redis_pending_.size() + store_pending_.size();
    counters["max_queue_age_ns"] = max_queue_age_ns();
    auto queue_metrics = [&](const char* name, const QueuePeak& peak,
                             const std::vector<EncodedRecord>& pending) {
        const std::string prefix(name);
        counters[prefix + "_pending_records_peak"] = peak.records;
        counters[prefix + "_pending_bytes_peak"] = peak.bytes;
        counters[prefix + "_pending_capacity"] = pending.capacity();
    };
    queue_metrics("hash", hash_peak_, hash_pending_);
    queue_metrics("redis", redis_peak_, redis_pending_);
    queue_metrics("store", store_peak_, store_pending_);
    counters["hash_scratch_capacity"] = hash_scratch_.capacity();
    counters["redis_scratch_capacity"] = redis_scratch_.capacity();
}

void Pipeline::note_flush(const char* stage, FlushReason reason,
                          const std::vector<EncodedRecord>& records) {
    const char* reason_name = "tail";
    switch (reason) {
    case FlushReason::Rows: reason_name = "rows"; break;
    case FlushReason::Bytes: reason_name = "bytes"; break;
    case FlushReason::Age: reason_name = "age"; break;
    case FlushReason::Tail: break;
    }
    metrics_.inc(std::string(stage) + "_flush_" + reason_name);
    metrics_.time(std::string(stage) + "_queue_oldest_age", age_ns(now_(), records.front().ingress_ns));
}

void Pipeline::flush_hash(FlushReason reason) {
    if (hash_pending_.empty()) return;
    note_flush("hash", reason, hash_pending_);
    hash_scratch_.clear();
    hash_scratch_.swap(hash_pending_);
    hash_bytes_ = 0;
    std::string error;
    const bool ok = deduper_.apply(hash_scratch_, error);
    require(ok, error);
    {
        Timer timer(metrics_, "consume_checksum");
        for (const auto& record : hash_scratch_) checksum_ = fnv_extend(checksum_, record.bytes);
    }
    metrics_.inc("forwarded", hash_scratch_.size());
    for (auto& record : hash_scratch_) {
        if (!redis_pending_.empty() && redis_bytes_ + record.bytes.size() + 4 > kMaxPayloadBytes)
            flush_redis(FlushReason::Bytes);
        redis_bytes_ += record.bytes.size() + 4;
        redis_pending_.push_back(std::move(record));
        redis_peak_.observe(redis_pending_.size(), redis_bytes_);
        if (redis_pending_.size() >= config_.redis_batch_records) flush_redis(FlushReason::Rows);
    }
    hash_scratch_.clear();
}

void Pipeline::exec(const std::vector<std::vector<std::string>>& commands,
                    std::vector<RespValue>& replies, const char* stage) {
    Timer timer(metrics_, stage);
    require(redis_.exec(commands, replies) && replies.size() == commands.size(), "Redis pipeline transport failure");
    for (const auto& reply : replies)
        require(reply.type != RespValue::Type::Error, "Redis command failed: " + reply.str);
    metrics_.inc("redis_commands", commands.size());
    metrics_.inc("redis_network_execs");
}

void Pipeline::flush_redis(FlushReason reason) {
    if (redis_pending_.empty()) return;
    note_flush("redis", reason, redis_pending_);
    redis_scratch_.clear();
    redis_scratch_.swap(redis_pending_);
    redis_bytes_ = 0;
    auto& records = redis_scratch_;
    if (config_.redis != "off") {
        std::string payload;
        std::vector<std::vector<std::string>> commands;
        const std::string key = "iqbench:batch:" + std::to_string(redis_id_++);
        {
            Timer timer(metrics_, "redis_build");
            metrics_.inc("redis_build_records", records.size());
            std::map<std::string, std::uint64_t> symbols;
            std::size_t payload_bytes = 0;
            for (const auto& record : records) payload_bytes += record.bytes.size() + 4;
            payload.reserve(payload_bytes);
            for (const auto& record : records) {
                const auto len = record.bytes.size();
                for (unsigned i = 0; i < 4; ++i) payload.push_back(static_cast<char>(len >> (8 * i)));
                payload += record.bytes;
                auto [it, inserted] = symbols.emplace(record.symbol, record.time);
                if (!inserted) it->second = std::min(it->second, record.time);
            }
            commands.push_back({"SET", key, payload});
            for (const auto& [symbol, time] : symbols)
                commands.push_back({"ZADD", "iqbench:index:" + symbol, std::to_string(time), key});
            // Real exec encodes once internally; do not double-encode for timing.
            if (config_.redis == "encode") {
                Timer encoding(metrics_, "redis_encode_only");
                std::string wire;
                for (const auto& command : commands) encode_command(command, wire);
                metrics_.inc("redis_encoded_bytes", wire.size());
                metrics_.counters["redis_encode_checksum"] ^= fnv_extend(kFnvOffset, wire);
            }
        }
        if (config_.redis != "encode") {
            std::vector<RespValue> replies;
            exec(commands, replies, "redis_publish_wait");
            metrics_.inc("redis_publish_records", records.size());
            if (config_.redis == "roundtrip") {
                exec({{"GET", key}}, replies, "redis_read_wait");
                // Only whole-payload equality makes cached SHA/time metadata
                // transferable. Persist bytes copied from the actual GET reply,
                // never the original in-memory records or a re-encoded book.
                require(replies[0].type == RespValue::Type::Bulk && replies[0].str == payload,
                        "Redis roundtrip payload mismatch");
                std::string error;
                {
                    Timer timer(metrics_, "redis_decode");
                    const bool ok = decode_encoded_records(replies[0].str, redis_decoded_, error);
                    require(ok, "Redis readback decode failed: " + error);
                    require(redis_decoded_.size() == records.size(), "Redis readback count mismatch");
                    for (std::size_t i = 0; i < records.size(); ++i) {
                        redis_decoded_[i].sha256 = records[i].sha256;
                        redis_decoded_[i].has_sha256 = records[i].has_sha256;
                        redis_decoded_[i].ingress_ns = records[i].ingress_ns;
                    }
                    metrics_.inc("redis_readback_records", redis_decoded_.size());
                    records.swap(redis_decoded_);
                    redis_decoded_.clear();
                }
            }
            std::vector<std::vector<std::string>> cleanup{{"DEL", key}};
            for (std::size_t i = 1; i < commands.size(); ++i)
                cleanup.push_back({"ZREM", commands[i][1], key});
            exec(cleanup, replies, "redis_cleanup_wait");
            metrics_.inc("redis_cleanup_batches");
        }
    }
    for (auto& record : records) {
        require(record.bytes.size() <= config_.transaction_bytes, "one record exceeds transaction byte limit");
        if (!store_pending_.empty() && store_bytes_ + record.bytes.size() > config_.transaction_bytes)
            flush_store(FlushReason::Bytes);
        store_bytes_ += record.bytes.size();
        store_pending_.push_back(std::move(record));
        store_peak_.observe(store_pending_.size(), store_bytes_);
        if (store_pending_.size() >= config_.transaction_rows) flush_store(FlushReason::Rows);
    }
    records.clear();
}

void Pipeline::flush_store(FlushReason reason) {
    if (store_pending_.empty()) return;
    note_flush("store", reason, store_pending_);
    std::string error;
    {
        Timer timer(metrics_, "store_total");
        const bool ok = store_.write(store_pending_, error);
        require(ok, "store write failed: " + error);
    }
    metrics_.time("store_complete_oldest_age", age_ns(now_(), store_pending_.front().ingress_ns));
    const bool first = metrics_.counters["pipeline_transactions"] == 0;
    batch_stat(metrics_, "transaction_rows", store_pending_.size(), first);
    batch_stat(metrics_, "transaction_bytes", store_bytes_, first);
    metrics_.inc("pipeline_transactions");
    metrics_.inc(config_.backend == "null" ? "sink_consumed" : "committed_attempts", store_pending_.size());
    // No shared oldest timestamp exists to reset: other stages may still hold
    // older records while this transaction completes inside an upstream drain.
    store_pending_.clear();
    store_bytes_ = 0;
}

}  // namespace tickshelf::bench
