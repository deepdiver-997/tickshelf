// Opt-in, in-process Engine experiment. No TTAP/TCP ingress and no production Null CLI.
#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "common/tick.hpp"
#include "components/engine/engine.hpp"
#include "components/redis/redis_shard_pool.hpp"
#include "components/storage/shard_route.hpp"
#include "components/storage/sqlite_store.hpp"

namespace {
using namespace tickshelf;
using Clock = std::chrono::steady_clock;

std::uint64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now().time_since_epoch()).count();
}
std::string quote(const std::string& value) {
    std::string result = "\"";
    for (unsigned char c : value) {
        if (c == '"' || c == '\\') { result += '\\'; result += static_cast<char>(c); }
        else if (c < 32) throw std::runtime_error("control character in path/argument");
        else result += static_cast<char>(c);
    }
    return result + '"';
}
std::uint64_t number(const std::string& value, std::uint64_t low, std::uint64_t high) {
    std::uint64_t result = 0;
    const auto parsed = std::from_chars(value.data(), value.data() + value.size(), result);
    if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size() || result < low || result > high)
        throw std::runtime_error("integer outside supported range: " + value);
    return result;
}
struct Options {
    std::string input, output, redis = "null", storage = "sqlite", fifo = "off";
    std::uint64_t port = 0, shards = 2, batch = 256, inflight = 4, pending = 4096;
    std::uint64_t dedup_bytes = 1 << 20, timeout_ms = 30000;
};
Options parse(int argc, char** argv) {
    Options o;
    std::vector<std::string> seen;
    for (int i = 1; i < argc; ++i) {
        std::string key = argv[i];
        if (i + 1 == argc) throw std::runtime_error("missing value for " + key);
        for (const auto& old : seen) if (old == key) throw std::runtime_error("duplicate option " + key);
        seen.push_back(key);
        const std::string value = argv[++i];
        if (key == "--input") o.input = value;
        else if (key == "--output") o.output = value;
        else if (key == "--redis") o.redis = value;
        else if (key == "--storage") o.storage = value;
        else if (key == "--fifo") o.fifo = value;
        else if (key == "--redis-port") o.port = number(value, 1, 65535);
        else if (key == "--shards") o.shards = number(value, 1, 64);
        else if (key == "--batch-records") o.batch = number(value, 1, 65536);
        else if (key == "--inflight") o.inflight = number(value, 1, 256);
        else if (key == "--max-pending-records") o.pending = number(value, 1, 1048576);
        else if (key == "--dedup-bytes") o.dedup_bytes = number(value, 1, 64ull << 20);
        else if (key == "--timeout-ms") o.timeout_ms = number(value, 1, 300000);
        else throw std::runtime_error("unknown option " + key);
    }
    if (o.input.empty() || o.output.empty() || (o.redis != "real" && o.redis != "null") ||
        (o.storage != "sqlite" && o.storage != "null") || (o.fifo != "on" && o.fifo != "off") ||
        (o.redis == "real" && (o.port == 0 || o.port == 6379)) || o.pending < o.batch)
        throw std::runtime_error("invalid options (real Redis requires an explicit private, non-6379 port)");
    return o;
}
std::uint64_t tick_hash(const Tick& tick) {
    std::array<std::uint8_t, kTickBytes> bytes{};
    encode_tick(tick, bytes.data());
    std::uint64_t hash = 14695981039346656037ull;
    for (auto byte : bytes) { hash ^= byte; hash *= 1099511628211ull; }
    return hash;
}
// Sum of per-record FNV hashes is order independent across shard workers/batch grouping.
struct Observation { std::uint64_t count = 0, checksum = 0; };
void observe(Observation& out, const std::vector<const std::vector<Tick>*>& batches) {
    for (const auto* batch : batches) for (const auto& tick : *batch) {
        ++out.count;
        out.checksum += tick_hash(tick);
    }
}
class NullRedis final : public RedisShard {
public:
    bool publish_and_claim(const std::vector<Batch>& batches, const FlushOptions&,
                           std::vector<char>& claimed) override {
        claimed.assign(batches.size(), 1); // all claims, no lease conflict or durability
        return true;
    }
    bool finish(const std::vector<std::string>&) override { return true; }
    bool release(const std::vector<std::string>&) override { return true; }
};
class ObservedStorage final : public StorageShard {
public:
    ObservedStorage(Observation& observation, const std::string& path, SqliteConnectionInfo* info)
        : observation_(observation) {
        if (info) {
            store_ = std::make_unique<SqliteTickStore>();
            if (!store_->open(path) || !store_->connection_info(*info))
                throw std::runtime_error("open/read actual SQLite connection PRAGMA failed");
        }
    }
    bool persists_records() const override { return store_ != nullptr; }
    std::int64_t upsert_flush(const std::vector<const std::vector<Tick>*>& batches) override {
        const auto result = store_ ? store_->upsert_flush(batches) : 0;
        if (result >= 0 && !store_) observe(observation_, batches);
        return result;
    }
private:
    Observation& observation_;
    std::unique_ptr<SqliteTickStore> store_;
};
std::vector<Tick> load(const std::string& path) {
    const auto size = std::filesystem::file_size(path);
    if (size == 0 || size % kTickBytes || size > 64ull * 1000000)
        throw std::runtime_error("input must contain 1..1000000 canonical 64-byte ticks");
    std::ifstream input(path, std::ios::binary);
    if (!input) throw std::runtime_error("cannot read input");
    std::vector<Tick> ticks;
    ticks.reserve(size / kTickBytes);
    std::array<std::uint8_t, kTickBytes> bytes{};
    while (input.read(reinterpret_cast<char*>(bytes.data()), bytes.size())) {
        auto tick = decode_tick(bytes.data());
        std::array<std::uint8_t, kTickBytes> canonical{};
        encode_tick(tick, canonical.data());
        if (bytes != canonical || tick_symbol(tick).empty()) throw std::runtime_error("noncanonical tick input");
        ticks.push_back(tick);
    }
    if (!input.eof() || ticks.size() != size / kTickBytes) throw std::runtime_error("short input read");
    return ticks;
}
void counters(const EngineStats& s) {
#define FIELD(name) std::cout << "\"" #name "\":" << s.name << ','
    FIELD(submitted_records); FIELD(accepted_records); FIELD(rejected_records); FIELD(dedup_dropped);
    FIELD(sealed_slices); FIELD(published_records); FIELD(committed_input_records);
    FIELD(inserted_unique_records); FIELD(sink_consumed_records); FIELD(durable_records);
    FIELD(publish_only_records); FIELD(skipped_records); FIELD(degraded_records); FIELD(retries);
    FIELD(finish_pending_records); FIELD(queue_depth); FIELD(queue_peak);
#undef FIELD
    std::cout << "\"pending_records\":" << s.pending_records;
}
} // namespace

int main(int argc, char** argv) {
    try {
        const auto o = parse(argc, argv);
        auto ticks = load(o.input); // file decode is outside the Engine timer, disclosed below
        if (!std::filesystem::create_directory(o.output)) throw std::runtime_error("output must be a new directory");
        EngineConfig config;
        config.shards = o.shards;
        config.write_mode = WriteMode::WriteThrough;
        config.batch_records = o.batch;
        config.batch_bytes = 0;
        config.batch_ms = 10;
        config.inflight = o.inflight;
        config.max_pending_records = o.pending;
        config.dedup_bytes = o.fifo == "on" ? o.dedup_bytes : 0;
        config.worker_id = "engine-ablation";
        config.retry_ms = 1;
        config.max_retry_ms = 10;
        config.shutdown_ms = o.timeout_ms;
        std::vector<std::unique_ptr<RedisShard>> redis;
        if (o.redis == "real") redis = make_redis_shards("127.0.0.1", static_cast<std::uint16_t>(o.port), o.shards);
        else for (std::size_t i = 0; i < o.shards; ++i) redis.push_back(std::make_unique<NullRedis>());
        std::vector<Observation> observations(o.shards);
        std::vector<SqliteConnectionInfo> infos(o.storage == "sqlite" ? o.shards : 0);
        std::vector<std::unique_ptr<StorageShard>> storage;
        const std::string db = (std::filesystem::path(o.output) / "ticks.db").string();
        for (std::size_t i = 0; i < o.shards; ++i)
            storage.push_back(std::make_unique<ObservedStorage>(observations[i], shard_path(db, i, o.shards),
                              infos.empty() ? nullptr : &infos[i]));
        Engine engine(config, std::move(redis), std::move(storage));
        const auto start = Clock::now();
        const auto deadline = start + std::chrono::milliseconds(o.timeout_ms);
        std::size_t accepted = 0;
        while (accepted < ticks.size() && Clock::now() < deadline) {
            engine.tick(now_ms());
            const auto n = engine.submit(std::span<const Tick>(ticks).subspan(accepted), now_ms());
            accepted += n;
            if (n == 0) engine.wait_idle(std::chrono::milliseconds(1)); // timed CV; retry/tick above
        }
        const auto submitted_at = Clock::now();
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - submitted_at);
        const auto report = engine.drain(std::max(remaining, std::chrono::milliseconds(0)));
        const auto finished = Clock::now(); // includes finish/release retries and worker join
        const auto s = engine.stats();
        Observation observed;
        for (const auto& observation : observations) {
            observed.count += observation.count;
            observed.checksum += observation.checksum;
        }
        const bool ok = accepted == ticks.size() && report.queue_complete &&
            s.degraded_records == 0 && s.skipped_records == 0 && s.finish_pending_records == 0 &&
            (o.storage != "sqlite" || report.storage_complete);
        std::cout << std::setprecision(12) << "{\"schema\":1,\"status\":" << quote(ok ? "PASS" : "FAIL")
                  << ",\"label\":\"in-process-real-Engine-no-TTAP-ingress\",\"build_type\":"
#ifdef NDEBUG
                  << "\"release-like-NDEBUG\""
#else
                  << "\"debug\""
#endif
                  << ",\"parameters\":{\"redis\":" << quote(o.redis) << ",\"storage\":" << quote(o.storage)
                  << ",\"fifo\":" << quote(o.fifo) << ",\"records\":" << ticks.size()
                  << ",\"shards\":" << o.shards << ",\"batch_records\":" << o.batch
                  << ",\"batch_bytes\":0,\"batch_ms\":10,\"inflight\":" << o.inflight
                  << ",\"max_pending_records\":" << o.pending << ",\"dedup_bytes_per_shard\":" << config.dedup_bytes
                  << ",\"timeout_ms\":" << o.timeout_ms << "},\"timing\":{\"elapsed_s\":"
                  << std::chrono::duration<double>(finished - start).count() << ",\"submit_s\":"
                  << std::chrono::duration<double>(submitted_at - start).count() << ",\"drain_s\":"
                  << std::chrono::duration<double>(finished - submitted_at).count()
                  << ",\"scope\":\"submit-through-finish-and-join-including-timed-CV-backpressure; Null-only-consumption-checksum; excludes-input-decode-storage-open-and-SQLite-oracle\"},\"counters\":{";
        counters(s);
        std::cout << "},\"queue_complete\":" << (report.queue_complete ? "true" : "false")
                  << ",\"storage_complete\":" << (report.storage_complete ? "true" : "false")
                  << ",\"observation\":{\"scope\":\"Null-only-count-and-full-byte-checksum; SQLite-verified-independently-after-timer\",\"input_count\":" << observed.count << ",\"checksum_sum_fnv1a64\":\""
                  << std::hex << std::setw(16) << std::setfill('0') << observed.checksum << std::dec
                  << "\",\"persistent\":" << (o.storage == "sqlite" ? "true" : "false")
                  << "},\"sqlite_connections\":[";
        for (std::size_t i = 0; i < infos.size(); ++i) {
            if (i) std::cout << ',';
            const auto& info = infos[i];
            std::cout << "{\"shard\":" << i << ",\"path\":" << quote(shard_path(db, i, o.shards))
                      << ",\"version\":" << quote(info.version) << ",\"journal_mode\":" << quote(info.journal_mode)
                      << ",\"synchronous\":" << info.synchronous << ",\"fullfsync\":" << info.fullfsync
                      << ",\"checkpoint_fullfsync\":" << info.checkpoint_fullfsync
                      << ",\"wal_autocheckpoint\":" << info.wal_autocheckpoint << '}';
        }
        std::cout << "]}\n";
        return ok ? 0 : 1;
    } catch (const std::exception& error) {
        std::cerr << "engine_ablation: " << error.what() << '\n';
        return 2;
    }
}
