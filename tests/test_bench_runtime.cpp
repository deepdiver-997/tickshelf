#include <doctest/doctest.h>

#include "pipeline.hpp"

#include <algorithm>
#include <array>
#include <functional>
#include <set>
#include <stdexcept>
#include <vector>

using namespace tickshelf::bench;

namespace {
constexpr std::uint64_t ms = 1000000;

class FakeStore final : public Store {
public:
    bool write(const std::vector<EncodedRecord>& records, std::string& error) override {
        ++attempts;
        if (on_write) on_write();
        if (throws) throw std::runtime_error("injected store exception");
        if (fails) { error = "injected store failure"; return false; }
        batches.push_back(records);
        return true;
    }
    bool finish(std::string&) override { return true; }
    bool export_rows(std::ostream&, std::string&) override { return true; }
    bool export_range(const std::string&, std::uint64_t, std::uint64_t,
                      std::ostream&, std::string&) override { return true; }
    std::string info_json() const override { return "{}"; }
    std::size_t rows() const {
        std::size_t count = 0;
        for (const auto& batch : batches) count += batch.size();
        return count;
    }
    std::vector<EncodedRecord> flattened() const {
        std::vector<EncodedRecord> records;
        for (const auto& batch : batches) records.insert(records.end(), batch.begin(), batch.end());
        return records;
    }
    std::vector<std::vector<EncodedRecord>> batches;
    std::function<void()> on_write;
    std::size_t attempts = 0;
    bool fails = false, throws = false;
};

Config runtime_config() {
    Config config;
    config.redis = "off";
    config.backend = "null";
    config.hash = "off";
    config.transaction_ms = 100;
    return config;
}

std::vector<Record> records(std::uint64_t begin, std::size_t count,
                            GeneratorOptions options = {}) {
    std::vector<Record> result;
    for (std::size_t i = 0; i < count; ++i) result.push_back(generate_record(begin + i, options));
    return result;
}

std::uint64_t checksum(const std::vector<EncodedRecord>& records) {
    auto value = kFnvOffset;
    for (const auto& record : records) value = fnv_extend(value, record.bytes);
    return value;
}
}  // namespace

TEST_CASE("bench runtime: committed fronts cannot expire newer remaining records") {
    auto config = runtime_config();
    SUBCASE("hash remainder") {
        config.hash_batch_records = 2;
        config.redis_batch_records = 1;
        config.transaction_rows = 1;
    }
    SUBCASE("Redis remainder") {
        config.hash_batch_records = 1;
        config.redis_batch_records = 2;
        config.transaction_rows = 2;
    }
    SUBCASE("store remainder") {
        config.hash_batch_records = 1;
        config.redis_batch_records = 1;
        config.transaction_rows = 2;
    }
    Metrics metrics;
    FakeStore store;
    std::uint64_t now = ms;
    Pipeline pipeline(config, metrics, store, [&] { return now; });
    pipeline.add(records(0, 1));
    CHECK(store.rows() == 0);
    now = 91 * ms;
    pipeline.add(records(1, 2));
    REQUIRE(store.rows() == 2);
    CHECK(metrics.counters["buffered_records"] == 1);
    now = 101 * ms;
    pipeline.tick();
    CHECK(store.rows() == 2);
    CHECK(pipeline.max_queue_age_ns() == 10 * ms);
    CHECK(pipeline.poll_timeout_ms() == 90);
    now = 191 * ms - 1;
    pipeline.tick();
    CHECK(store.rows() == 2);
    CHECK(pipeline.poll_timeout_ms() == 1);
    now = 191 * ms;
    CHECK(pipeline.poll_timeout_ms() == 0);
    pipeline.tick();
    CHECK(store.rows() == 3);
    CHECK(metrics.counters["buffered_records"] == 0);
    CHECK(pipeline.poll_timeout_ms() == 100);
    CHECK(pipeline.max_queue_age_ns() == 0);
    const auto written = store.flattened();
    REQUIRE(written.size() == 3);
    CHECK(written[0].ingress_ns == ms);
    CHECK(written[1].ingress_ns == 91 * ms);
    CHECK(written[2].ingress_ns == 91 * ms);
    CHECK(pipeline.checksum() == checksum(written));
}

TEST_CASE("bench runtime: a due store queue does not drain a younger upstream queue") {
    auto config = runtime_config();
    config.transaction_rows = 10;
    SUBCASE("younger hash") {
        config.hash_batch_records = 2;
        config.redis_batch_records = 1;
    }
    SUBCASE("younger Redis") {
        config.hash_batch_records = 1;
        config.redis_batch_records = 2;
    }
    Metrics metrics;
    FakeStore store;
    std::uint64_t now = ms;
    Pipeline pipeline(config, metrics, store, [&] { return now; });
    pipeline.add(records(0, 2));
    now = 91 * ms;
    pipeline.add(records(2, 1));
    now = 101 * ms;
    pipeline.tick();
    REQUIRE(store.rows() == 2);
    CHECK(metrics.counters["store_flush_age"] == 1);
    CHECK(metrics.counters["buffered_records"] == 1);
    CHECK(pipeline.max_queue_age_ns() == 10 * ms);
    CHECK(pipeline.poll_timeout_ms() == 90);
    now = 191 * ms;
    pipeline.tick();
    CHECK(store.rows() == 3);
    CHECK(metrics.counters["store_flush_age"] == 2);
    CHECK(metrics.counters["buffered_records"] == 0);
}

TEST_CASE("bench runtime: zero-origin injectable clock and low-rate polling obey deadlines") {
    auto config = runtime_config();
    config.transaction_ms = 7;
    config.hash_batch_records = 8;
    config.redis_batch_records = 9;
    config.transaction_rows = 10;
    Metrics metrics;
    FakeStore store;
    std::uint64_t now = 0;
    Pipeline pipeline(config, metrics, store, [&] { return now; });
    CHECK(pipeline.poll_timeout_ms(17) == 17);
    pipeline.add(records(0, 1));
    CHECK(pipeline.poll_timeout_ms() == 7);
    now = 5 * ms;
    CHECK(pipeline.poll_timeout_ms() == 2);
    CHECK(pipeline.poll_timeout_ms(1) == 1);
    now = 6 * ms + 1;
    pipeline.tick();
    CHECK(store.rows() == 0);
    CHECK(pipeline.poll_timeout_ms() == 1);
    now = 7 * ms;
    CHECK(pipeline.poll_timeout_ms() == 0);
    pipeline.tick();
    REQUIRE(store.rows() == 1);
    CHECK(store.batches[0][0].ingress_ns == 0);
    for (const char* stage : {"hash", "redis", "store"}) {
        CHECK(metrics.counters[std::string(stage) + "_flush_age"] == 1);
        const auto& histogram = metrics.stages.at(std::string(stage) + "_queue_oldest_age");
        CHECK(histogram.count == 1);
        CHECK(histogram.sum_ns == 7 * ms);
    }
    CHECK(metrics.counters["transaction_rows_sum"] == 1);
    CHECK(metrics.counters["pipeline_transactions"] == 1);
    CHECK(pipeline.poll_timeout_ms(17) == 17);
    pipeline.flush();
    pipeline.flush();
    CHECK(store.attempts == 1);
    CHECK(metrics.counters["store_flush_tail"] == 0);
}

TEST_CASE("bench runtime: different batch widths preserve metadata order checksum and tail") {
    auto config = runtime_config();
    config.hash_batch_records = 3;
    config.redis_batch_records = 5;
    config.transaction_rows = 4;
    config.batch_records = 7;
    config.backend = "sqlite_compact";  // FakeStore still exercises durable-backend accounting.
    SUBCASE("hash off") { config.hash = "off"; }
    SUBCASE("hash only") { config.hash = "only"; }
    SUBCASE("dedup") { config.hash = "dedup"; }
    Metrics metrics;
    FakeStore store;
    std::uint64_t now = ms;
    Pipeline pipeline(config, metrics, store, [&] { return now; });
    std::vector<EncodedRecord> expected;
    std::set<std::string> seen;
    for (std::uint64_t begin = 0; begin < 73;) {
        now += ms;
        const auto count = std::min<std::uint64_t>(begin % 7 + 1, 73 - begin);
        std::vector<Record> batch;
        for (std::uint64_t i = 0; i < count; ++i) {
            auto record = generate_record(logical_id(begin + i, 5), {54, 5, 42, true});
            auto bytes = encode_record(record);
            if (config.hash != "dedup" || seen.insert(bytes).second)
                expected.push_back({record.symbol, record.time, bytes, {}, false, now});
            batch.push_back(std::move(record));
        }
        pipeline.add(batch);
        begin += count;
    }
    CHECK(store.rows() <= expected.size());
    pipeline.flush();
    const auto written = store.flattened();
    REQUIRE(written.size() == expected.size());
    std::uint64_t total_bytes = 0;
    for (std::size_t i = 0; i < written.size(); ++i) {
        CHECK(written[i].bytes == expected[i].bytes);
        CHECK(written[i].symbol == expected[i].symbol);
        CHECK(written[i].time == expected[i].time);
        CHECK(written[i].ingress_ns == expected[i].ingress_ns);
        total_bytes += written[i].bytes.size();
        CHECK(written[i].has_sha256 == (config.hash != "off"));
        if (written[i].has_sha256) {
            std::array<std::uint8_t, 32> digest{};
            unsigned size = 0;
            REQUIRE(EVP_Digest(written[i].bytes.data(), written[i].bytes.size(), digest.data(),
                               &size, EVP_sha256(), nullptr) == 1);
            REQUIRE(size == digest.size());
            CHECK(written[i].sha256 == digest);
        }
    }
    CHECK(pipeline.checksum() == checksum(expected));
    CHECK(metrics.counters["forwarded"] == expected.size());
    CHECK(metrics.counters["committed_attempts"] == expected.size());
    CHECK(metrics.counters["sink_consumed"] == 0);
    CHECK(metrics.counters["pipeline_transactions"] == store.batches.size());
    CHECK(metrics.counters["transaction_rows_sum"] == written.size());
    CHECK(metrics.counters["transaction_rows_min"] == written.size() % 4);
    CHECK(metrics.counters["transaction_rows_max"] == 4);
    CHECK(metrics.counters["transaction_bytes_sum"] == total_bytes);
    CHECK(metrics.counters["store_flush_tail"] == 1);
    CHECK(metrics.counters["buffered_records"] == 0);
    CHECK(metrics.counters["hash_pending_records_peak"] <= 3);
    CHECK(metrics.counters["redis_pending_records_peak"] <= 5);
    CHECK(metrics.counters["store_pending_records_peak"] <= 4);
    CHECK(metrics.counters["hash_pending_capacity"] == 3);
    CHECK(metrics.counters["hash_scratch_capacity"] == 3);
    CHECK(metrics.counters["redis_pending_capacity"] == 5);
    CHECK(metrics.counters["redis_scratch_capacity"] == 5);
    const auto attempts = store.attempts;
    const auto final_checksum = pipeline.checksum();
    pipeline.flush();
    CHECK(store.attempts == attempts);
    CHECK(pipeline.checksum() == final_checksum);
}

TEST_CASE("bench runtime: byte-limited transactions report actual rows bytes and flush reasons") {
    auto config = runtime_config();
    config.hash_batch_records = 1;
    config.redis_batch_records = 1;
    config.transaction_rows = 100;
    const auto input = records(0, 10, {54, 10, 42, false});
    const auto record_bytes = encode_record(input.front()).size();
    config.transaction_bytes = record_bytes * 3;
    Metrics metrics;
    FakeStore store;
    std::uint64_t now = ms;
    Pipeline pipeline(config, metrics, store, [&] { return now; });
    pipeline.add({input.begin(), input.begin() + 3});
    CHECK(store.attempts == 0);  // Exactly equal bytes fit; the next record splits.
    pipeline.add({input.begin() + 3, input.end()});
    REQUIRE(store.batches.size() == 3);
    pipeline.flush();
    REQUIRE(store.batches.size() == 4);
    for (std::size_t i = 0; i < store.batches.size(); ++i) {
        CHECK(store.batches[i].size() == (i == 3 ? 1 : 3));
        std::uint64_t bytes = 0;
        for (const auto& record : store.batches[i]) bytes += record.bytes.size();
        CHECK(bytes <= config.transaction_bytes);
    }
    CHECK(metrics.counters["store_flush_bytes"] == 3);
    CHECK(metrics.counters["store_flush_rows"] == 0);
    CHECK(metrics.counters["store_flush_tail"] == 1);
    CHECK(metrics.counters["transaction_rows_sum"] == 10);
    CHECK(metrics.counters["transaction_rows_min"] == 1);
    CHECK(metrics.counters["transaction_rows_max"] == 3);
    CHECK(metrics.counters["transaction_bytes_sum"] == record_bytes * 10);
    CHECK(metrics.counters["transaction_bytes_min"] == record_bytes);
    CHECK(metrics.counters["transaction_bytes_max"] == record_bytes * 3);
    CHECK(metrics.counters["store_pending_bytes_peak"] == config.transaction_bytes);
    CHECK(pipeline.checksum() == checksum(store.flattened()));
}

TEST_CASE("bench runtime: stage payload byte caps bound reusable queues") {
    auto config = runtime_config();
    config.batch_records = 10;
    config.transaction_rows = 16;
    config.transaction_bytes = 8 * 1024 * 1024;
    const char* stage = nullptr;
    SUBCASE("hash payload") {
        config.hash_batch_records = 8192;
        config.redis_batch_records = 1;
        stage = "hash";
    }
    SUBCASE("Redis payload") {
        config.hash_batch_records = 1;
        config.redis_batch_records = 8192;
        stage = "redis";
    }
    REQUIRE(stage != nullptr);
    Metrics metrics;
    FakeStore store;
    std::uint64_t now = ms;
    Pipeline pipeline(config, metrics, store, [&] { return now; });
    for (std::uint64_t begin = 0; begin < 70; begin += 10) {
        auto batch = records(begin, 10);
        for (auto& record : batch) record.symbol = std::string(60000, 'S');
        pipeline.add(batch);
    }
    pipeline.flush();
    CHECK(store.rows() == 70);
    CHECK(metrics.counters[std::string(stage) + "_flush_bytes"] == 1);
    CHECK(metrics.counters[std::string(stage) + "_pending_bytes_peak"] <= kMaxPayloadBytes);
    CHECK(metrics.counters[std::string(stage) + "_pending_capacity"] == 8192);
    CHECK(metrics.counters[std::string(stage) + "_scratch_capacity"] == 8192);
    CHECK(metrics.counters["store_pending_records_peak"] <= 16);
    CHECK(pipeline.checksum() == checksum(store.flattened()));
}

TEST_CASE("bench runtime: synchronous writes are not preempted and one ingress batch keeps its clock") {
    auto config = runtime_config();
    config.hash_batch_records = 1;
    config.redis_batch_records = 1;
    config.transaction_rows = 1;
    Metrics metrics;
    FakeStore store;
    std::uint64_t now = 10 * ms;
    store.on_write = [&] { now += 250 * ms; };
    Pipeline pipeline(config, metrics, store, [&] { return now; });
    pipeline.add(records(0, 3));
    CHECK(now == 760 * ms);
    REQUIRE(store.rows() == 3);
    for (const auto& record : store.flattened()) CHECK(record.ingress_ns == 10 * ms);
    CHECK(metrics.stages.at("store_queue_oldest_age").max_ns == 500 * ms);
    CHECK(metrics.stages.at("store_complete_oldest_age").max_ns == 750 * ms);
    CHECK(metrics.stages.at("store_complete_oldest_age").sum_ns == 1500 * ms);
    CHECK(pipeline.max_queue_age_ns() == 0);
    CHECK(metrics.counters["store_flush_rows"] == 3);
    CHECK(metrics.counters["store_flush_age"] == 0);
}

TEST_CASE("bench runtime: store failures propagate without success counters or implicit retry") {
    auto config = runtime_config();
    config.hash_batch_records = 1;
    config.redis_batch_records = 1;
    config.transaction_rows = 1;
    config.backend = "segment_rocksdb";
    Metrics metrics;
    FakeStore store;
    std::uint64_t now = ms;
    SUBCASE("false with error") {
        store.fails = true;
        Pipeline pipeline(config, metrics, store, [&] { return now; });
        CHECK_THROWS_WITH(pipeline.add(records(0, 1)), "store write failed: injected store failure");
    }
    SUBCASE("exception") {
        store.throws = true;
        Pipeline pipeline(config, metrics, store, [&] { return now; });
        CHECK_THROWS_WITH(pipeline.add(records(0, 1)), "injected store exception");
    }
    SUBCASE("explicit tail failure") {
        config.hash_batch_records = 100;
        config.redis_batch_records = 100;
        config.transaction_rows = 100;
        store.fails = true;
        Pipeline pipeline(config, metrics, store, [&] { return now; });
        pipeline.add(records(0, 1));
        CHECK(store.attempts == 0);
        CHECK_THROWS_WITH(pipeline.flush(), "store write failed: injected store failure");
    }
    CHECK(store.attempts == 1);
    CHECK(store.rows() == 0);
    CHECK(metrics.counters["committed_attempts"] == 0);
    CHECK(metrics.counters["pipeline_transactions"] == 0);
    CHECK(metrics.counters["transaction_rows_sum"] == 0);
}

TEST_CASE("bench runtime: oversized transaction records and invalid canonical data propagate") {
    auto config = runtime_config();
    config.hash_batch_records = 1;
    config.redis_batch_records = 1;
    config.transaction_rows = 1;
    config.transaction_bytes = 1024;
    Metrics metrics;
    FakeStore store;
    std::uint64_t now = ms;
    Pipeline pipeline(config, metrics, store, [&] { return now; });
    auto batch = records(0, 1);
    SUBCASE("canonical record larger than transaction budget") {
        batch[0].symbol.assign(2048, 'X');
        CHECK_THROWS_WITH(pipeline.add(batch), "one record exceeds transaction byte limit");
    }
    SUBCASE("invalid canonical masks") {
        batch[0].nulls = 0xffffffff;
        CHECK_THROWS(pipeline.add(batch));
    }
    CHECK(store.attempts == 0);
    CHECK(metrics.counters["pipeline_transactions"] == 0);
}
