#include <doctest/doctest.h>
#include <simdjson.h>
#include <sqlite3.h>
#include <openssl/evp.h>
#include "../bench/store.hpp"

#include <cerrno>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <sys/wait.h>
#include <unistd.h>
#ifdef TICKSHELF_HAVE_ROCKSDB
#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>
#endif

namespace {
using namespace tickshelf::bench;
namespace fs = std::filesystem;
struct Directory {
    fs::path path;
    Directory() {
        const auto pattern = (fs::temp_directory_path() / "tickshelf_rocks_record_XXXXXX").string();
        std::vector<char> name(pattern.begin(), pattern.end());
        name.push_back('\0');
        const auto* made = ::mkdtemp(name.data());
        if (!made) throw std::runtime_error("mkdtemp failed");
        path = made;
    }
    ~Directory() { std::error_code error; fs::remove_all(path, error); }
};
struct Json {
    std::string storage;
    simdjson::dom::parser parser;
    simdjson::dom::element value;
    explicit Json(std::string text) : storage(std::move(text)) {
        if (const auto error = parser.parse(storage).get(value)) throw std::runtime_error(simdjson::error_message(error));
    }
};
StoreOptions options_for(const fs::path& path, const std::string& backend = "rocksdb") {
    StoreOptions options;
    options.directory = path.string();
    options.backend = backend;
    return options;
}
EncodedRecord row(std::string symbol = "A", std::uint64_t time = 0, std::int64_t value = 1) {
    Record record;
    record.symbol = std::move(symbol);
    record.time = time;
    record.present = 1;
    record.values[0] = value;
    return {record.symbol, record.time, encode_record(record)};
}
[[maybe_unused]] std::string hex(std::string_view value) {
    constexpr char digits[] = "0123456789abcdef";
    std::string out;
    for (unsigned char c : value) { out += digits[c >> 4]; out += digits[c & 15]; }
    return out;
}
std::unique_ptr<Store> opened(const StoreOptions& options, Metrics& metrics) {
    std::string error = "stale";
    auto store = open_store(options, metrics, error);
    REQUIRE_MESSAGE(store != nullptr, error);
    CHECK(error.empty());
    return store;
}
std::set<std::string> exported(Store& store, bool range = false, const std::string& symbol = "A",
                               std::uint64_t from = 0, std::uint64_t to = std::numeric_limits<std::uint64_t>::max()) {
    std::string error = "stale";
    std::ostringstream out;
    REQUIRE_MESSAGE((range ? store.export_range(symbol, from, to, out, error) : store.export_rows(out, error)), error);
    CHECK(error.empty());
    std::set<std::string> result;
    std::istringstream in(out.str());
    std::string line;
    while (std::getline(in, line)) {
        Json json(line);
        const auto bytes = std::string(json.value["hex"].get_string().value());
        CHECK(result.emplace(bytes).second);
    }
    return result;
}
}

TEST_CASE("bench_rocks_record: SQLite workload API counters match actual calls and exclude export/info") {
    for (const std::string backend : {"sqlite", "sqlite_compact"}) for (const bool reuse : {true, false})
    for (const std::string model : {"light", "heavy"}) {
        CAPTURE(backend); CAPTURE(reuse); CAPTURE(model);
        Directory directory;
        Metrics metrics;
        auto options = options_for(directory.path, backend);
        options.reuse_statement = reuse;
        options.transaction_model = model;
        auto store = opened(options, metrics);
        const std::uint64_t heavy = model == "heavy";
        CHECK(metrics.counters.at("sqlite_prepare_calls") == (reuse ? 1 + heavy : 0));
        std::string error;
        REQUIRE(store->write({row(), row(), row("B", 2)}, error));
        REQUIRE(store->write({row()}, error));
        CHECK(metrics.counters.at("sqlite_begin_calls") == 2);
        CHECK(metrics.counters.at("sqlite_commit_calls") == 2);
        CHECK(metrics.counters.at("sqlite_exec_calls") == 4);
        CHECK(metrics.counters.at("sqlite_rollback_calls") == 0);
        CHECK(metrics.counters.at("sqlite_begin_calls") == metrics.stages.at("sqlite_begin").count);
        CHECK(metrics.counters.at("sqlite_commit_calls") == metrics.stages.at("sqlite_commit").count);
        CHECK(metrics.counters.at("sqlite_prepare_calls") == metrics.stages.at("sqlite_prepare").count);
        CHECK(metrics.counters.at("sqlite_step_calls") == metrics.stages.at("sqlite_step").count);
        CHECK(metrics.counters.at("sqlite_prepare_calls") == (reuse ? 1 + heavy : 2 * (1 + heavy)));
        CHECK(metrics.counters.at("sqlite_step_calls") == 4 + 2 * heavy);
        CHECK(metrics.counters.at("sqlite_reset_calls") == 4 + 2 * heavy);
        CHECK(metrics.counters.at("sqlite_clear_bindings_calls") == 4 + 2 * heavy);
        CHECK(metrics.counters.at("sqlite_bind_calls") == 4 * (backend == "sqlite_compact" ? 4 : 3) + 8 * heavy);
        CHECK(metrics.counters.at("sqlite_bind_blob_calls") == 4 * (backend == "sqlite_compact" ? 3 : 2));
        CHECK(metrics.counters.at("sqlite_bind_text_calls") == 4);
        CHECK(metrics.counters.at("sqlite_bind_int64_calls") == 8 * heavy);
        CHECK(metrics.counters.at("sqlite_changes_calls") == 4);
        const auto before = metrics.counters;
        CHECK(exported(*store).size() == 2);
        CHECK(exported(*store, true).size() == 1);
        Json info(store->info_json());
        CHECK(info.storage.find("NOT syscalls") != std::string::npos);
        for (const auto& [name, count] : before)
            if (name.starts_with("sqlite_")) CHECK(metrics.counters.at(name) == count);
        REQUIRE(store->finish(error));
    }
}

#ifdef TICKSHELF_HAVE_ROCKSDB
namespace {
using Rows = std::map<std::string, std::string>;
fs::path database(const StoreOptions& options) { return fs::path(options.directory) / "full-record-rocksdb-v1"; }
std::unique_ptr<rocksdb::DB> fixture(const StoreOptions& options, bool create = false) {
    rocksdb::Options rocks;
    rocks.create_if_missing = create;
    rocks.use_fsync = true;
    std::unique_ptr<rocksdb::DB> db;
    const auto status = rocksdb::DB::Open(rocks, database(options).string(), &db);
    REQUIRE_MESSAGE(status.ok(), status.ToString());
    return db;
}
Rows rows(rocksdb::DB& db) {
    Rows result;
    std::unique_ptr<rocksdb::Iterator> it(db.NewIterator(rocksdb::ReadOptions{}));
    for (it->SeekToFirst(); it->Valid(); it->Next()) result.emplace(it->key().ToString(), it->value().ToString());
    REQUIRE(it->status().ok());
    return result;
}
std::string be(std::uint64_t value) {
    std::string result(8, '\0');
    for (unsigned i = 0; i < 8; ++i) result[i] = static_cast<char>(value >> (56 - 8 * i));
    return result;
}
}

TEST_CASE("bench_rocks_record: exact collision-safe dedup, full Rocks values, indexed binary/uint64 range and replay") {
    for (const std::string model : {"light", "heavy"}) {
        Directory directory;
        auto options = options_for(directory.path);
        options.transaction_model = model;
        options.test_digest = [](std::string_view, std::string& digest) { digest.assign(32, '\0'); return true; };
        Metrics metrics;
        auto store = opened(options, metrics);
        const auto maximum = std::numeric_limits<std::uint64_t>::max();
        const auto high = std::uint64_t{1} << 63;
        auto a = row("A", 7), b = row("A", 7, 2);
        auto c = row(std::string("N\0\xc3\xbf\"", 5), maximum), d = row("A", 0), e = row("A", high), f = row("A", high - 1);
        a.has_sha256 = true; a.sha256.fill(0x77); // Override beats a trusted cache.
        const auto iterators_before = metrics.counters.at("rocksdb_iterator_creations");
        std::string error;
        REQUIRE_MESSAGE(store->write({a, b, a, c, d, e, f, b}, error), error);
        REQUIRE_MESSAGE(store->write({a, b, c}, error), error);
        CHECK(metrics.counters.at("rocksdb_iterator_creations") - iterators_before == 2);
        CHECK(metrics.counters.at("rocksdb_seek_calls") == 2); // One digest bucket scan per batch, not per record.
        CHECK(metrics.counters.at("rocksdb_write_calls") == 2);
        CHECK(metrics.counters.at("rocksdb_write_batches") == 2);
        CHECK(metrics.counters.at("rocksdb_put_ops") == (model == "heavy" ? 22 : 14)); // 6*(D/R[/T]) + 2*S [+2*M].
        CHECK(metrics.counters.at("inserted") == 6);
        CHECK(metrics.counters.at("ignored") == 5);
        CHECK(metrics.counters.at("store_digest_override_calls") == 11);
        CHECK(metrics.counters.at("rocksdb_dedup_pending_hits") == 2);
        CHECK(metrics.counters.at("rocksdb_dedup_candidate_rows") == 6);
        CHECK(metrics.counters.at("sqlite_step_calls") == 0);
        CHECK(metrics.counters.at("file_write_bytes") == 0);
        const std::set<std::string> expected{hex(a.bytes), hex(b.bytes), hex(c.bytes), hex(d.bytes), hex(e.bytes), hex(f.bytes)};
        CHECK(exported(*store) == expected);
        CHECK(exported(*store, true, "A", 7, 7) == std::set<std::string>{hex(a.bytes), hex(b.bytes)});
        CHECK(exported(*store, true, "A", high - 1, high) == std::set<std::string>{hex(e.bytes), hex(f.bytes)});
        CHECK(exported(*store, true, c.symbol, maximum, maximum) == std::set<std::string>{hex(c.bytes)});
        CHECK(exported(*store, true, "A", 0, 0) == std::set<std::string>{hex(d.bytes)});
        CHECK(exported(*store, true, "A", maximum, maximum).empty());
        CHECK(exported(*store, true, "missing").empty());
        std::ostringstream invalid;
        CHECK_FALSE(store->export_range("A", 9, 8, invalid, error));
        CHECK_FALSE(store->export_range(std::string(65536, 'x'), 0, maximum, invalid, error));
        CHECK(invalid.str().empty());
        REQUIRE_MESSAGE(store->finish(error), error);
        REQUIRE(store->finish(error));
        for (const char* stage : {"rocksdb_flush", "rocksdb_wait_compact", "rocksdb_sync_wal", "rocksdb_close", "rocksdb_reopen_readonly"})
            CHECK(metrics.stages.at(stage).count == 1);
        const auto writes_before_export = metrics.counters.at("rocksdb_write_batches");
        CHECK(exported(*store) == expected);
        CHECK(metrics.counters.at("rocksdb_write_batches") == writes_before_export);
        CHECK_FALSE(store->write({row("X")}, error));
        CHECK(error.find("finished") != std::string::npos);
        Json info(store->info_json());
        CHECK(info.value["backend"].get_string().value() == "rocksdb");
        CHECK(info.value["physical_rows"].get_uint64().value() == 6);
        CHECK(info.value["symbol_index_rows"].get_uint64().value() == 6);
        CHECK(info.value["time_symbol_index_rows"].get_uint64().value() == (model == "heavy" ? 6 : 0));
        CHECK(info.value["physical_key_rows"].get_uint64().value() == (model == "heavy" ? 22 : 14));
        CHECK(info.value["persisted_counts"]["attempted"].get_uint64().value() == 11);
        CHECK(info.value["rocksdb"]["options"]["compression"].get_string().value() == "none");
        CHECK(info.value["rocksdb"]["options"]["write_buffer_size"].get_uint64().value() > 0);
        CHECK(info.value["rocksdb"]["options"]["block_cache_capacity"].get_uint64().value() > 0);
        CHECK(info.value["rocksdb"]["finish_background_state"]["rocksdb.num-running-compactions"].get_uint64().value() == 0);
        CHECK(info.value["rocksdb"]["finish_background_state"]["rocksdb.num-running-flushes"].get_uint64().value() == 0);
        CHECK_FALSE(info.value["physical_power_loss_tested"].get_bool().value());
        CHECK(info.value["hardware_plp"].get_string().value() == "unknown");
        store.reset();
        auto db = fixture(options);
        const auto contents = rows(*db);
        std::set<std::string> payloads;
        std::uint64_t ranges = 0, times = 0, metadata = 0;
        for (const auto& [key, value] : contents) {
            if (key[0] == 'D') { CHECK(key.size() == 41); payloads.insert(hex(value)); }
            if (key[0] == 'R') { CHECK(value.size() == 32); ++ranges; }
            if (key[0] == 'T') { CHECK(value.size() == 32); ++times; }
            if (key[0] == 'M') { CHECK(value.size() == 48); ++metadata; }
        }
        CHECK(payloads == expected);
        CHECK(ranges == 6); CHECK(times == (model == "heavy" ? 6 : 0)); CHECK(metadata == (model == "heavy" ? 2 : 0));
        db.reset();
        for (const auto& entry : fs::recursive_directory_iterator(directory.path))
            CHECK(entry.path().extension() != ".segments");
        Metrics replay;
        store = opened(options, replay);
        CHECK(exported(*store) == expected);
        REQUIRE_MESSAGE(store->write({a, b, c, a}, error), error);
        CHECK(replay.counters.at("inserted") == 0);
        CHECK(replay.counters.at("ignored") == 4);
        Json replay_info(store->info_json());
        CHECK(replay_info.value["persisted_counts"]["transactions"].get_uint64().value() == 3);
        CHECK(replay_info.value["persisted_counts"]["attempted"].get_uint64().value() == 15);
    }
}

TEST_CASE("bench_rocks_record: flushed Bloom prefix seeks preserve dedup and total-order long binary symbol ranges") {
    Directory directory;
    auto options = options_for(directory.path);
    options.transaction_model = "heavy";
    Metrics initial;
    auto store = opened(options, initial);
    const std::string symbol = std::string(45, 'S') + std::string("\0\xc3\xbf", 3);
    std::vector<EncodedRecord> batch;
    std::set<std::string> expected;
    for (std::uint64_t i = 0; i < 64; ++i) {
        batch.push_back(row(symbol, i * 7, static_cast<std::int64_t>(i)));
        expected.insert(hex(batch.back().bytes));
    }
    std::string error;
    REQUIRE(store->write(batch, error));
    REQUIRE(store->finish(error)); // Materialize filtered SST, close, reopen read-only.
    store.reset();
    Metrics replay;
    store = opened(options, replay);
    std::vector<EncodedRecord> duplicates(batch.rbegin(), batch.rend());
    duplicates.insert(duplicates.end(), batch.begin(), batch.end());
    const auto extra = row(symbol, std::numeric_limits<std::uint64_t>::max(), 777);
    duplicates.push_back(extra); duplicates.push_back(extra);
    REQUIRE_MESSAGE(store->write(duplicates, error), error);
    CHECK(replay.counters.at("rocksdb_seek_calls") == 65);
    CHECK(replay.counters.at("rocksdb_iterator_creations") == 1);
    CHECK(replay.counters.at("rocksdb_dedup_candidate_rows") == 64);
    CHECK(replay.counters.at("inserted") == 1);
    CHECK(replay.counters.at("ignored") == 129);
    expected.insert(hex(extra.bytes));
    CHECK(exported(*store, true, symbol) == expected);
    CHECK(exported(*store, true, symbol, 7, 7) == std::set<std::string>{hex(batch[1].bytes)});
    CHECK(exported(*store, true, "").empty());
    Json info(store->info_json());
    CHECK(info.value["rocksdb"]["options"]["digest_prefix_bytes"].get_uint64().value() == 33);
    CHECK(info.value["rocksdb"]["options"]["bloom_bits_per_key"].get_uint64().value() == 10);
    CHECK(info.value["rocksdb"]["options"]["whole_key_filtering"].get_bool().value());
    CHECK(info.value["rocksdb"]["options"]["other_read_options"]["total_order_seek"].get_bool().value());
    REQUIRE(store->finish(error));
    store.reset();
    Metrics recovered;
    store = opened(options, recovered);
    CHECK(exported(*store) == expected);
}

TEST_CASE("bench_rocks_record: trusted SHA256 cache reuse and missing-cache hashing retain the shared contract") {
    Directory directory;
    Metrics metrics;
    auto store = opened(options_for(directory.path), metrics);
    auto cached = row();
    unsigned size = 0;
    REQUIRE(EVP_Digest(cached.bytes.data(), cached.bytes.size(), cached.sha256.data(), &size, EVP_sha256(), nullptr) == 1);
    REQUIRE(size == 32);
    cached.has_sha256 = true;
    auto uncached = cached;
    uncached.has_sha256 = false;
    uncached.sha256.fill(0xff);
    std::string error;
    REQUIRE(store->write({cached, uncached, cached}, error));
    CHECK(metrics.counters.at("store_digest_cache_hits") == 2);
    CHECK(metrics.counters.at("store_digest_computed") == 1);
    CHECK(metrics.counters.at("store_digest_bytes") == cached.bytes.size());
    CHECK(metrics.counters.at("inserted") == 1);
    CHECK(metrics.counters.at("ignored") == 2);
    CHECK(exported(*store).size() == 1);
}

TEST_CASE("bench_rocks_record: batch validation/digest failure is precommit and does not poison or leak state") {
    for (const std::string model : {"light", "heavy"}) {
        Directory directory;
        auto options = options_for(directory.path);
        options.transaction_model = model;
        options.test_digest = [](std::string_view bytes, std::string& digest) {
            digest.assign(bytes.substr(6, 1) == "B" ? 31 : 32, '\0'); return true;
        };
        Metrics metrics;
        auto store = opened(options, metrics);
        const auto a = row();
        std::string error;
        auto bad = a;
        bad.time = 9;
        CHECK_FALSE(store->write({a, bad}, error));
        bad = a; bad.bytes.resize(kMaxRecordBytes + 1);
        CHECK_FALSE(store->write({a, bad}, error));
        CHECK_FALSE(store->write({a, row("B")}, error));
        CHECK(error.find("exactly 32 bytes") != std::string::npos);
        CHECK(metrics.counters.at("rocksdb_write_calls") == 0);
        CHECK(exported(*store).empty());
        REQUIRE_MESSAGE(store->write({a}, error), error);
        REQUIRE(store->finish(error));
        store.reset();
        Metrics recovery;
        store = opened(options, recovery);
        CHECK(exported(*store) == std::set<std::string>{hex(a.bytes)});
        Json info(store->info_json());
        CHECK(info.value["persisted_counts"]["transactions"].get_uint64().value() == 1);
        CHECK(info.value["persisted_counts"]["attempted"].get_uint64().value() == 1);
    }
}

TEST_CASE("bench_rocks_record: uncertain write status poisons until strict reopen and never permits export repair") {
    Directory directory;
    auto options = options_for(directory.path);
    options.transaction_model = "heavy";
    options.test_rocks_write_status_error = true;
    Metrics metrics;
    auto store = opened(options, metrics);
    std::string error;
    CHECK_FALSE(store->write({row(), row("B")}, error));
    CHECK(error.find("uncertain") != std::string::npos);
    CHECK_FALSE(store->write({row("C")}, error));
    CHECK_FALSE(store->finish(error));
    std::ostringstream out;
    CHECK_FALSE(store->export_rows(out, error));
    CHECK_FALSE(store->export_range("A", 0, 1, out, error));
    CHECK(out.str().empty());
    CHECK(metrics.counters.at("transactions") == 0);
    CHECK(metrics.counters.at("rocksdb_write_calls") == 1);
    Json poisoned(store->info_json());
    CHECK(poisoned.value["rocksdb"]["poisoned"].get_bool().value());
    CHECK(poisoned.value["physical_rows"].is_null());
    CHECK(poisoned.value["persisted_counts"].is_null());
    store.reset();
    options.test_rocks_write_status_error = false;
    Metrics recovered;
    store = opened(options, recovered);
    CHECK(exported(*store).size() == 2); // Atomic batch was accepted, despite unacknowledged result.
    REQUIRE(store->write({row(), row("B")}, error));
    CHECK(recovered.counters.at("inserted") == 0);
    CHECK(recovered.counters.at("ignored") == 2);
}

TEST_CASE("bench_rocks_record: DB lock rejects independent writers and instance mutex serializes dedup through commit") {
    Directory directory;
    auto options = options_for(directory.path);
    options.test_digest = [](std::string_view, std::string& digest) { digest.assign(32, '\0'); return true; };
    Metrics metrics, other;
    auto store = opened(options, metrics);
    std::string error;
    CHECK(open_store(options, other, error) == nullptr);
    CHECK_FALSE(error.empty());
    const std::vector<EncodedRecord> batch{row(), row("B"), row(), row("A", 0, 2)};
    bool ok1 = true, ok2 = true;
    std::string error1, error2;
    auto write = [&](bool& ok, std::string& message) {
        for (int i = 0; i < 8; ++i) if (!store->write(batch, message)) { ok = false; break; }
    };
    std::thread a([&] { write(ok1, error1); }), b([&] { write(ok2, error2); });
    a.join(); b.join();
    REQUIRE_MESSAGE(ok1, error1); REQUIRE_MESSAGE(ok2, error2);
    CHECK(metrics.counters.at("inserted") == 3);
    CHECK(metrics.counters.at("ignored") == 61);
    CHECK(exported(*store).size() == 3);
    store.reset();
    store = opened(options, other);
    CHECK(exported(*store).size() == 3);
}

TEST_CASE("bench_rocks_record: strict recovery rejects malformed missing altered orphan and extra keys without logical repair") {
    for (int damage = 0; damage < 24; ++damage) {
        CAPTURE(damage);
        Directory directory;
        auto options = options_for(directory.path);
        options.transaction_model = "heavy";
        Metrics metrics;
        auto store = opened(options, metrics);
        std::string error;
        REQUIRE(store->write({row(), row("B", 9)}, error));
        REQUIRE(store->write({row("A", 1)}, error));
        store.reset();
        auto db = fixture(options);
        const auto contents = rows(*db);
        const auto find = [&](char prefix) {
            for (const auto& entry : contents) if (entry.first[0] == prefix) return std::pair<std::string, std::string>{entry.first, entry.second};
            throw std::runtime_error("fixture prefix missing");
        };
        auto d = find('D'), r = find('R'), t = find('T'), m = find('M');
        auto state = contents.at("S");
        rocksdb::WriteBatch batch;
        if (damage == 0) REQUIRE(batch.Delete("F").ok());
        if (damage == 1) REQUIRE(batch.Put("F", "foreign-v2").ok());
        if (damage == 2) REQUIRE(batch.Delete("S").ok());
        if (damage == 3) REQUIRE(batch.Put("S", "short").ok());
        if (damage == 4) REQUIRE(batch.Put("UNKNOWN", "extra").ok());
        if (damage == 5) REQUIRE(batch.Delete(d.first).ok());
        if (damage == 6) { d.second.back() ^= 0x40; REQUIRE(batch.Put(d.first, d.second).ok()); }
        if (damage == 7) { d.second.resize(23); REQUIRE(batch.Put(d.first, d.second).ok()); }
        if (damage == 8) REQUIRE(batch.Delete(r.first).ok());
        if (damage == 9) { r.second[0] ^= 1; REQUIRE(batch.Put(r.first, r.second).ok()); }
        if (damage == 10) { REQUIRE(batch.Delete(r.first).ok()); r.first[3] ^= 1; REQUIRE(batch.Put(r.first, r.second).ok()); }
        if (damage == 11) { r.first.replace(r.first.size() - 8, 8, be(99)); REQUIRE(batch.Put(r.first, r.second).ok()); }
        if (damage == 12) REQUIRE(batch.Delete(t.first).ok());
        if (damage == 13) { t.first[8] ^= 1; REQUIRE(batch.Put(t.first, t.second).ok()); }
        if (damage == 14) REQUIRE(batch.Delete(m.first).ok());
        if (damage == 15) { m.second.back() ^= 1; REQUIRE(batch.Put(m.first, m.second).ok()); }
        if (damage == 16) REQUIRE(batch.Put("M" + be(99), m.second).ok());
        if (damage == 17) { state.replace(0, 8, be(4)); REQUIRE(batch.Put("S", state).ok()); }
        if (damage == 18) { state.replace(16, 8, be(std::numeric_limits<std::uint64_t>::max())); REQUIRE(batch.Put("S", state).ok()); }
        if (damage == 19) { REQUIRE(batch.Put(d.first.substr(0, 33) + be(0), d.second).ok()); }
        if (damage == 20) { REQUIRE(batch.Delete(d.first).ok()); d.first[1] ^= 1; REQUIRE(batch.Put(d.first, d.second).ok()); }
        if (damage == 21) REQUIRE(batch.Put("D", d.second).ok());
        if (damage == 22) { r.first[1] = '\xff'; REQUIRE(batch.Put(r.first, r.second).ok()); }
        if (damage == 23) { m.second.replace(16, 8, be(std::numeric_limits<std::uint64_t>::max())); REQUIRE(batch.Put(m.first, m.second).ok()); }
        rocksdb::WriteOptions write; write.sync = true;
        REQUIRE(db->Write(write, &batch).ok());
        const auto damaged = rows(*db);
        db.reset();
        CHECK(open_store(options, metrics, error) == nullptr);
        CHECK_FALSE(error.empty());
        db = fixture(options);
        CHECK(rows(*db) == damaged);
    }
}

TEST_CASE("bench_rocks_record: incompatible model missing CURRENT and existing unstamped DB are never initialized as fresh") {
    for (int damage = 0; damage < 3; ++damage) {
        Directory directory;
        auto options = options_for(directory.path);
        Metrics metrics;
        std::string error;
        if (damage == 2) {
            fs::create_directories(database(options));
            auto db = fixture(options, true);
            REQUIRE(db->Put(rocksdb::WriteOptions{}, "foreign", "payload").ok());
        } else {
            auto store = opened(options, metrics);
            REQUIRE(store->write({row()}, error));
            store.reset();
            if (damage == 0) options.transaction_model = "heavy";
            else fs::rename(database(options) / "CURRENT", database(options) / "owned-CURRENT-backup");
        }
        CHECK(open_store(options, metrics, error) == nullptr);
        CHECK_FALSE(error.empty());
    }
}

TEST_CASE("bench_rocks_record: crash boundary commits payload indexes metadata and ID state atomically") {
    for (const std::string point : {"before_write", "after_write"}) {
        Directory directory;
        auto options = options_for(directory.path);
        options.transaction_model = "heavy";
        Metrics metrics;
        auto store = opened(options, metrics);
        std::string error;
        REQUIRE(store->write({row()}, error));
        store.reset();
        const auto child = ::fork();
        REQUIRE(child >= 0);
        if (child == 0) {
            options.failpoint = point;
            Metrics child_metrics;
            auto child_store = open_store(options, child_metrics, error);
            if (!child_store) ::_exit(70);
            child_store->write({row("B"), row("C")}, error);
            ::_exit(71);
        }
        int status = 0;
        pid_t waited;
        do { waited = ::waitpid(child, &status, 0); } while (waited < 0 && errno == EINTR);
        REQUIRE(waited == child); REQUIRE(WIFSIGNALED(status)); CHECK(WTERMSIG(status) == SIGKILL);
        Metrics recovered;
        store = opened(options, recovered);
        CHECK(exported(*store).size() == (point == "after_write" ? 3 : 1));
        REQUIRE(store->write({row("B"), row("C")}, error));
        CHECK(exported(*store).size() == 3);
    }
}
#else
TEST_CASE("bench_rocks_record: disabled factory is explicit and never creates a fallback database") {
    Directory directory;
    auto options = options_for(directory.path / "not-created");
    Metrics metrics;
    std::string error;
    CHECK(open_store(options, metrics, error) == nullptr);
    CHECK(error.find("unsupported backend rocksdb") != std::string::npos);
    CHECK(error.find("TICKSHELF_HAVE_ROCKSDB") != std::string::npos);
    CHECK_FALSE(fs::exists(options.directory));
}
#endif
