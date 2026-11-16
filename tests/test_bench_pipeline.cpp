#include <doctest/doctest.h>
#include "config.hpp"
#include "hash.hpp"
#include "protocol.hpp"
#include "components/storage/sqlite_store.hpp"
#include <algorithm>
#include <array>
#include <deque>
#include <filesystem>
#include <fstream>
#include <unistd.h>

using namespace tickshelf::bench;

TEST_CASE("bench protocol: split frames and corruption are bounded") {
    const auto frame = encode_message({MessageType::Json, 123, "{\"ticks\":[]}"});
    const auto end = encode_message({MessageType::End, 123, {}});
    for (std::size_t split = 0; split <= frame.size(); ++split) {
        MessageParser parser;
        std::vector<Message> messages;
        std::string error;
        CHECK(parser.feed(std::string_view(frame).substr(0, split), messages, error));
        CHECK(parser.feed(std::string_view(frame).substr(split), messages, error));
        REQUIRE(messages.size() == 1);
        CHECK(messages[0].sequence == 123);
        CHECK(messages[0].payload == "{\"ticks\":[]}");
        CHECK_FALSE(parser.incomplete());
    }
    MessageParser many;
    std::vector<Message> messages;
    std::string error;
    CHECK(many.feed(frame + end, messages, error));
    CHECK(messages.size() == 2);
    for (const std::size_t offset : {0U, 4U, 5U, 6U, 16U, 20U, 24U}) {
        auto broken = frame;
        broken[offset] ^= 0x7f;
        MessageParser parser;
        messages.clear();
        const bool accepted = parser.feed(broken, messages, error);
        CHECK((!accepted || parser.incomplete()));
        CHECK(messages.empty());
    }
    auto huge = frame;
    huge[19] = 0x7f;
    MessageParser parser;
    CHECK_FALSE(parser.feed(huge, messages, error));
    CHECK_THROWS(encode_message({MessageType::End, 0, "bad"}));
}

TEST_CASE("bench dedup: exact equality survives forced digest collisions") {
    Config config;
    config.hash = "dedup";
    config.dedup_max_entries = 2;
    Metrics metrics;
    Deduper deduper(config, metrics, [](std::string_view, std::string& digest) { digest = "collision"; return true; });
    std::string error;
    std::vector<EncodedRecord> batch{{"A", 1, "a"}, {"A", 1, "b"}, {"A", 1, "a"}};
    const auto* storage = batch.data();
    const auto capacity = batch.capacity();
    REQUIRE(deduper.apply(batch, error));
    CHECK(batch.data() == storage);
    CHECK(batch.capacity() == capacity);
    // This metric now counts one canonical copy, not the old doubled payload
    // plus duplicated digest strings. Fixed cache overhead is reported apart.
    CHECK(metrics.counters["dedup_payload_bytes"] == 2);
    REQUIRE(batch.size() == 2);
    CHECK(batch[0].bytes == "a");
    CHECK(batch[1].bytes == "b");
    CHECK(metrics.counters["dedup_dropped"] == 1);
    batch = {{"A", 1, "c"}};
    REQUIRE(deduper.apply(batch, error));
    CHECK(metrics.counters["dedup_evictions"] == 1);
    batch = {{"A", 1, "a"}};
    CHECK(deduper.apply(batch, error));
    CHECK(batch.size() == 1);
    CHECK(metrics.counters["dedup_entries"] == 2);
}

TEST_CASE("bench hash: per-record and batch SHA256 have identical results") {
    Config a, b;
    b.hash_mode = "record";
    Metrics ma, mb;
    Deduper da(a, ma), db(b, mb);
    std::vector<EncodedRecord> va{{"A", 1, "abc"}, {"A", 2, "def"}, {"A", 1, "abc"}};
    auto vb = va;
    std::string error;
    REQUIRE(da.apply(va, error));
    REQUIRE(db.apply(vb, error));
    CHECK(va.size() == vb.size());
    CHECK(ma.counters["hash_invocations"] == 3);
    CHECK(mb.counters["hash_invocations"] == 3);
    CHECK(ma.counters["hash_api_calls"] == 3);
    CHECK(mb.counters["hash_api_calls"] == 3);
    CHECK(ma.counters["hash_context_allocations"] == 1);
    CHECK(mb.counters["hash_context_allocations"] == 3);
    CHECK(ma.counters["dedup_dropped"] == mb.counters["dedup_dropped"]);
}

TEST_CASE("bench dedup: forced collisions retain exact FIFO one-entry replay semantics") {
    Config config;
    config.dedup_max_entries = 1;
    Metrics metrics;
    Deduper deduper(config, metrics, [](std::string_view, std::string& value) {
        value.assign(80, 'x'); // Opaque long overrides remain supported.
        return true;
    });
    std::string error;
    std::vector<EncodedRecord> records = {{"x", 1, "a"}, {"x", 1, "b"}, {"x", 1, "a"},
                                          {"x", 1, "b"}, {"x", 1, "b"}};
    REQUIRE(deduper.apply(records, error));
    REQUIRE(records.size() == 4);
    CHECK(records[0].bytes == "a");
    CHECK(records[1].bytes == "b");
    CHECK(records[2].bytes == "a");
    CHECK(records[3].bytes == "b");
    for (const auto& record : records) CHECK_FALSE(record.has_sha256);
    CHECK(metrics.counters["dedup_dropped"] == 1);
    CHECK(metrics.counters["dedup_evictions"] == 3);
    CHECK(metrics.counters["dedup_entries"] == 1);
    CHECK(metrics.counters["dedup_payload_bytes"] == 1);
    records = {{"x", 1, "b"}};
    REQUIRE(deduper.apply(records, error));
    CHECK(records.empty());
    records = {{"x", 1, "a"}};
    REQUIRE(deduper.apply(records, error));
    CHECK(records.size() == 1);
    CHECK(metrics.counters["dedup_evictions"] == 4);
    CHECK(metrics.counters["hash_context_allocations"] == 1); // Real, reused long-override normalization context.
}

TEST_CASE("bench dedup: byte budgets and entry bounds agree with a single-copy FIFO model") {
    const auto collision = +[](std::string_view, std::string& value) { value = "constant"; return true; };
    Config config;
    config.dedup_max_entries = 3;
    Metrics probe_metrics;
    Deduper probe(config, probe_metrics, collision);
    std::vector<EncodedRecord> probe_records = {{"x", 0, ""}};
    std::string error;
    REQUIRE(probe.apply(probe_records, error));
    const auto overhead = probe_metrics.counters["dedup_cache_bytes"];
    REQUIRE(overhead > 0);
    const std::array<std::string, 5> values = {"", "abcdefg", std::string(1024, 'a'),
                                              std::string(2048, 'b'), std::string(512, 'c')};
    for (const std::size_t budget : {std::size_t(0), std::size_t(1), std::size_t(overhead - 1),
             std::size_t(overhead), std::size_t(overhead + 7), std::size_t(2 * overhead + 1031),
             std::size_t(4 * overhead + 10000), std::size_t(256ULL * 1024 * 1024)}) {
        INFO("byte budget: ", budget);
        Metrics metrics;
        Deduper deduper(config, metrics, collision, budget);
        std::deque<std::string> model;
        std::size_t payload_bytes = 0, evictions = 0, dropped = 0;
        for (unsigned batch = 0; batch < 12; ++batch) {
            std::vector<EncodedRecord> records;
            std::vector<std::string> expected;
            for (unsigned i = 0; i < 7; ++i) {
                const auto& value = values[(batch * 7 + i * i) % values.size()];
                records.push_back({"same-symbol", 1, value});
                if (std::find(model.begin(), model.end(), value) != model.end()) { ++dropped; continue; }
                expected.push_back(value);
                model.push_back(value);
                payload_bytes += value.size();
                while (model.size() > config.dedup_max_entries || payload_bytes + overhead * model.size() > budget) {
                    payload_bytes -= model.front().size();
                    model.pop_front();
                    ++evictions;
                }
            }
            const auto* storage = records.data();
            REQUIRE(deduper.apply(records, error));
            CHECK(records.data() == storage);
            REQUIRE(records.size() == expected.size());
            for (std::size_t i = 0; i < records.size(); ++i) CHECK(records[i].bytes == expected[i]);
            CHECK(metrics.counters["dedup_entries"] == model.size());
            CHECK(metrics.counters["dedup_payload_bytes"] == payload_bytes);
            CHECK(metrics.counters["dedup_cache_bytes"] == payload_bytes + overhead * model.size());
            CHECK(metrics.counters["dedup_cache_bytes"] <= budget);
            CHECK(metrics.counters["dedup_evictions"] == evictions);
            CHECK(metrics.counters["dedup_dropped"] == dropped);
        }
    }
}

TEST_CASE("bench dedup: digest failure is atomic for cache records and SHA metadata") {
    Config config;
    config.dedup_max_entries = 1;
    Metrics metrics;
    Deduper deduper(config, metrics, [](std::string_view bytes, std::string& value) {
        value = "collision";
        return bytes != "fail";
    });
    std::string error;
    std::vector<EncodedRecord> records = {{"seed", 1, "seed"}};
    REQUIRE(deduper.apply(records, error));
    const auto cached_bytes = metrics.counters["dedup_cache_bytes"];
    records = {{"new", 2, "not-cached"}, {"bad", 3, "fail"}};
    records[0].sha256.fill(0xa5);
    records[0].has_sha256 = true;
    records[0].ingress_ns = 1234;
    const auto* storage = records.data();
    CHECK_FALSE(deduper.apply(records, error));
    CHECK_FALSE(error.empty());
    REQUIRE(records.size() == 2);
    CHECK(records.data() == storage);
    CHECK(records[0].symbol == "new");
    CHECK(records[0].bytes == "not-cached");
    CHECK(records[0].has_sha256);
    CHECK(records[0].sha256[0] == 0xa5);
    CHECK(records[0].ingress_ns == 1234);
    CHECK(records[1].bytes == "fail");
    CHECK(metrics.counters["dedup_entries"] == 1);
    CHECK(metrics.counters["dedup_cache_bytes"] == cached_bytes);
    CHECK(metrics.counters["dedup_evictions"] == 0);
    CHECK(metrics.counters["hash_invocations"] == 2); // seed + completed prefix, not failed digest.
    CHECK(metrics.counters["hash_input_bytes"] == 14);
    CHECK(metrics.counters["hash_context_allocations"] == 0); // Override allocates no real SHA context.
    records = {{"seed", 1, "seed"}};
    REQUIRE(deduper.apply(records, error));
    CHECK(error.empty());
    CHECK(records.empty()); // Failed batch must not evict the original FIFO entry.
    records = {{"new", 2, "not-cached"}};
    REQUIRE(deduper.apply(records, error));
    CHECK(records.size() == 1); // Nor may it publish its successful digest prefix.
}

TEST_CASE("bench hash: cached SHA bytes and digest checksums match real SHA256") {
    constexpr std::array<std::uint8_t, 32> abc_sha = {
        0xba,0x78,0x16,0xbf,0x8f,0x01,0xcf,0xea,0x41,0x41,0x40,0xde,0x5d,0xae,0x22,0x23,
        0xb0,0x03,0x61,0xa3,0x96,0x17,0x7a,0x9c,0xb4,0x10,0xff,0x61,0xf2,0x00,0x15,0xad};
    for (const auto* mode : {"batch", "record"}) {
        for (const auto* operation : {"only", "dedup"}) {
            Config config;
            config.hash_mode = mode;
            config.hash = operation;
            Metrics metrics;
            Deduper deduper(config, metrics);
            std::vector<EncodedRecord> records = {{"x", 1, "abc"}, {"y", 2, "def"}, {"x", 1, "abc"}};
            records[0].ingress_ns = 123;
            std::string error;
            REQUIRE(deduper.apply(records, error));
            REQUIRE(records.size() == (config.hash == "only" ? 3 : 2));
            CHECK(records[0].sha256 == abc_sha);
            CHECK(records[0].ingress_ns == 123);
            for (const auto& record : records) {
                std::array<std::uint8_t, 32> independent{};
                unsigned length = 0;
                REQUIRE(EVP_Digest(record.bytes.data(), record.bytes.size(), independent.data(),
                                   &length, EVP_sha256(), nullptr) == 1);
                CHECK(length == 32);
                CHECK(record.has_sha256);
                CHECK(record.sha256 == independent);
            }
            if (config.hash == "only") {
                auto expected = kFnvOffset;
                for (const auto& record : records)
                    expected = fnv_extend(expected, std::string_view(reinterpret_cast<const char*>(record.sha256.data()), 32));
                CHECK(metrics.counters["hash_digest_checksum"] == expected);
            }
            CHECK(metrics.counters["hash_invocations"] == 3);
            CHECK(metrics.counters["hash_api_calls"] == 3);
            CHECK(metrics.counters["hash_input_bytes"] == 9);
            CHECK(metrics.counters["hash_context_allocations"] == (config.hash_mode == "batch" ? 1 : 3));
        }
    }
}

TEST_CASE("bench hash: off FNV and test overrides never claim trusted SHA metadata") {
    for (const auto* operation : {"off", "only", "dedup"}) {
        Config config;
        config.hash = operation;
        config.hash_algorithm = "fnv1a64";
        Metrics metrics;
        Deduper deduper(config, metrics);
        std::vector<EncodedRecord> records = {{"x", 1, "abc"}};
        records[0].has_sha256 = true;
        records[0].ingress_ns = 987;
        std::string error = "old";
        REQUIRE(deduper.apply(records, error));
        CHECK(error.empty());
        REQUIRE(records.size() == 1);
        CHECK_FALSE(records[0].has_sha256);
        CHECK(records[0].ingress_ns == 987);
        CHECK(metrics.counters["hash_context_allocations"] == 0);
        CHECK(metrics.counters["hash_invocations"] == (config.hash == "off" ? 0 : 1));
        if (config.hash == "only") {
            const auto hash = fnv_extend(kFnvOffset, "abc");
            std::string digest(8, '\0');
            for (unsigned i = 0; i < 8; ++i) digest[i] = static_cast<char>(hash >> (i * 8));
            CHECK(metrics.counters["hash_digest_checksum"] == fnv_extend(kFnvOffset, digest));
        }
    }
    Config config;
    config.hash = "only";
    Metrics metrics;
    Deduper deduper(config, metrics, [](std::string_view, std::string& value) { value.assign(32, '\0'); return true; });
    std::vector<EncodedRecord> records = {{"x", 1, "abc"}};
    records[0].has_sha256 = true;
    std::string error;
    REQUIRE(deduper.apply(records, error));
    CHECK_FALSE(records[0].has_sha256);
    CHECK(metrics.counters["hash_context_allocations"] == 0);
    CHECK(metrics.counters["hash_digest_checksum"] == fnv_extend(kFnvOffset, std::string(32, '\0')));
}

TEST_CASE("bench dedup: cache owns long bytes across source destruction and digest-map rehash") {
    Config config;
    config.dedup_max_entries = 4096;
    Metrics metrics;
    Deduper deduper(config, metrics);
    std::string error;
    constexpr unsigned total = 2048;
    std::size_t payload_bytes = 0;
    for (unsigned first = 0; first < total; first += 128) {
        std::vector<EncodedRecord> records;
        for (unsigned i = first; i < first + 128; ++i) {
            auto bytes = std::string(1024, static_cast<char>('a' + i % 26)) + std::to_string(i);
            payload_bytes += bytes.size();
            records.push_back({"same-key", 42, std::move(bytes)});
        }
        const auto* storage = records.data();
        REQUIRE(deduper.apply(records, error));
        REQUIRE(records.size() == 128);
        CHECK(records.data() == storage);
        for (const auto& record : records) CHECK(record.has_sha256);
        // Destroy/overwrite every original allocation; the cache must own it.
        for (auto& record : records) record.bytes.assign(record.bytes.size(), 'x');
    }
    CHECK(metrics.counters["dedup_entries"] == total);
    CHECK(metrics.counters["dedup_payload_bytes"] == payload_bytes);
    for (unsigned first = 0; first < total; first += 128) {
        std::vector<EncodedRecord> replay;
        for (unsigned i = first; i < first + 128; ++i)
            replay.push_back({"same-key", 42, std::string(1024, static_cast<char>('a' + i % 26)) + std::to_string(i)});
        REQUIRE(deduper.apply(replay, error));
        CHECK(replay.empty());
    }
    CHECK(metrics.counters["dedup_dropped"] == total);
    CHECK(metrics.counters["dedup_evictions"] == 0);
    CHECK(metrics.counters["hash_context_allocations"] == 1);
    CHECK(metrics.counters["hash_invocations"] == 2 * total);
    CHECK(metrics.counters["hash_input_bytes"] == 2 * payload_bytes);
}

TEST_CASE("bench config: strict types, duplicate keys, paths and no side effects") {
    char temp[] = "/tmp/tickshelf-config-test.XXXXXX";
    const auto* dir = ::mkdtemp(temp);
    REQUIRE(dir != nullptr);
    const auto root = std::filesystem::path(dir);
    const auto file = root / "config.json";
    Config config;
    std::string error;
    auto load = [&](const std::string& json) {
        { std::ofstream out(file); out << json; }
        return load_config(file.string(), config, error);
    };
    CHECK(load("{\"directory\":\"data\",\"hash_batch_records\":7,\"redis_batch_records\":11}"));
    CHECK(config.directory == (root / "data").string());
    CHECK(config.hash_batch_records == 7);
    CHECK(config.redis_batch_records == 11);
    CHECK_FALSE(std::filesystem::exists(config.directory));
    CHECK_FALSE(load("{\"directory\":\"a\",\"directory\":\"b\"}"));
    CHECK_FALSE(load("{\"directory\":\"a\",\"typo\":1}"));
    CHECK_FALSE(load("{\"directory\":\"a\",\"rate\":-1}"));
    CHECK_FALSE(load("{\"directory\":\"a\",\"records\":1.5}"));
    CHECK_FALSE(load("{\"directory\":\"a\",\"port\":4294967296}"));
    CHECK_FALSE(load("{\"directory\":\"a\",\"reuse_statement\":1}"));
    CHECK_FALSE(load("{\"directory\":\"a\",\"duplicate_every\":1}"));
    CHECK_FALSE(load("{\"directory\":\"a\",\"host\":\"0.0.0.0\"}"));
    CHECK_FALSE(load(std::string(65537, ' ')));
    CHECK(error == "config exceeds 64KiB");
    std::filesystem::remove_all(root);
}

TEST_CASE("bench production connection probe: read actual state without changing it") {
    tickshelf::SqliteTickStore closed;
    tickshelf::SqliteConnectionInfo info;
    CHECK_FALSE(closed.connection_info(info));
    char temp[] = "/tmp/tickshelf-connection-test.XXXXXX";
    const auto* dir = ::mkdtemp(temp);
    REQUIRE(dir != nullptr);
    {
        tickshelf::SqliteTickStore store;
        REQUIRE(store.open((std::filesystem::path(dir) / "fresh.db").string()));
        REQUIRE(store.connection_info(info));
        CHECK(info.journal_mode == "wal");
        CHECK(info.synchronous >= 0);
        CHECK(info.synchronous <= 3);
        CHECK_FALSE(info.version.empty());
        tickshelf::SqliteConnectionInfo again;
        REQUIRE(store.connection_info(again));
        CHECK(again.synchronous == info.synchronous);
        CHECK(again.wal_autocheckpoint == info.wal_autocheckpoint);
        CHECK(store.count() == 0);
    }
    std::filesystem::remove_all(dir);
}

TEST_CASE("bench histogram: bounded percentile bins") {
    Histogram h;
    CHECK(h.percentile(99) == 0);
    for (unsigned i = 0; i < 1000; ++i) h.add(i);
    CHECK(h.count == 1000);
    CHECK(h.sum_ns == 499500);
    CHECK(h.percentile(50) >= 499);
    CHECK(h.percentile(95) >= 949);
    CHECK(h.percentile(99) <= h.max_ns);
}
