#include <doctest/doctest.h>
#include <openssl/evp.h>
#include <simdjson.h>
#include <sqlite3.h>
#include "../bench/config.hpp"
#include "../bench/store.hpp"

#include <filesystem>
#include <fstream>
#include <limits>
#include <set>
#include <sstream>
#include <stdexcept>
#include <unistd.h>

namespace {
using namespace tickshelf::bench;
namespace fs = std::filesystem;
struct Directory {
    fs::path path;
    Directory() {
        const auto pattern = (fs::temp_directory_path() / "tickshelf_store_lookup_XXXXXX").string();
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
std::vector<std::string> backends() {
    return {"sqlite_compact"
#ifdef TICKSHELF_HAVE_ROCKSDB
        , "rocksdb"
#endif
    };
}
StoreOptions options_for(const fs::path& path, const std::string& backend, const std::string& mode = "assume_unique") {
    StoreOptions options;
    options.directory = path.string();
    options.backend = backend;
    options.store_lookup = mode;
    return options;
}
fs::path database(const StoreOptions& options) {
    const fs::path base(options.directory);
    if (options.backend == "rocksdb")
        return base / (options.store_lookup == "exact" ? "full-record-rocksdb-v1" : "full-record-rocksdb-assume-unique-v1");
    return (options.store_lookup == "exact" ? base : base / "sqlite-compact-assume-unique-v1") / "compact-record.sqlite3";
}
std::unique_ptr<Store> opened(const StoreOptions& options, Metrics& metrics) {
    std::string error = "stale";
    auto store = open_store(options, metrics, error);
    REQUIRE_MESSAGE(store != nullptr, error);
    CHECK(error.empty());
    return store;
}
std::string hex(std::string_view bytes) {
    constexpr char digits[] = "0123456789abcdef";
    std::string out;
    for (unsigned char c : bytes) { out += digits[c >> 4]; out += digits[c & 15]; }
    return out;
}
std::multiset<std::string> exported(Store& store, bool range = false, const std::string& symbol = "A",
                                   std::uint64_t from = 0, std::uint64_t to = std::numeric_limits<std::uint64_t>::max()) {
    std::ostringstream out;
    std::string error;
    REQUIRE_MESSAGE((range ? store.export_range(symbol, from, to, out, error) : store.export_rows(out, error)), error);
    CHECK(error.empty());
    std::multiset<std::string> result;
    std::istringstream input(out.str());
    std::string line;
    while (std::getline(input, line)) {
        Json json(line);
        result.emplace(std::string(json.value["hex"].get_string().value()));
    }
    return result;
}
std::vector<EncodedRecord> rows() {
    std::vector<EncodedRecord> result;
    for (std::uint64_t id = 0; id < 6; ++id) {
        auto record = generate_record(id, {3, 5, 42, true});
        if (id < 2) { record.symbol = "A"; record.time = 7; }
        if (id == 2) { record.symbol = std::string("B\0\xc3\xbf", 4); record.time = std::numeric_limits<std::uint64_t>::max(); }
        result.push_back({record.symbol, record.time, encode_record(record)});
    }
    return result;
}
std::multiset<std::string> expected(const std::vector<EncodedRecord>& records) {
    std::multiset<std::string> result;
    for (const auto& record : records) result.insert(hex(record.bytes));
    return result;
}
std::int64_t scalar(const fs::path& path, const char* sql) {
    sqlite3* db = nullptr;
    REQUIRE(sqlite3_open_v2(path.c_str(), &db, SQLITE_OPEN_READONLY, nullptr) == SQLITE_OK);
    sqlite3_stmt* statement = nullptr;
    const int prepared = sqlite3_prepare_v2(db, sql, -1, &statement, nullptr);
    const int rc = prepared == SQLITE_OK ? sqlite3_step(statement) : prepared;
    const auto value = rc == SQLITE_ROW ? sqlite3_column_int64(statement, 0) : 0;
    const std::string error = sqlite3_errmsg(db);
    sqlite3_finalize(statement);
    sqlite3_close(db);
    REQUIRE_MESSAGE(rc == SQLITE_ROW, error);
    return value;
}
void put_config(const fs::path& path, const std::string& text) {
    REQUIRE_FALSE(fs::exists(path));
    std::ofstream output(path, std::ios::binary);
    output << text;
    output.close();
    REQUIRE(output.good());
}
}

TEST_CASE("bench_store_lookup: default exact and strict config print load check roundtrip") {
    Directory directory;
    CHECK(Config{}.store_lookup == "exact");
    CHECK(StoreOptions{}.store_lookup == "exact");
    unsigned serial = 0;
    for (const auto& backend : backends()) for (const std::string mode : {"exact", "assume_unique"}) {
        Config config;
        config.directory = (directory.path / "case").string();
        config.backend = backend;
        config.store_lookup = mode;
        const auto path = directory.path / (std::to_string(serial++) + ".json");
        put_config(path, config_json(config));
        Config loaded;
        std::string error;
        REQUIRE_MESSAGE(load_config(path.string(), loaded, error), error);
        REQUIRE_MESSAGE(validate_config(loaded, error), error);
        CHECK(loaded.store_lookup == mode);
        CHECK(config_json(loaded) == config_json(config));
    }
    const auto old_path = directory.path / "old-config.json";
    put_config(old_path, "{\"directory\":\"old-case\",\"backend\":\"sqlite_compact\"}");
    Config old;
    std::string error;
    REQUIRE_MESSAGE(load_config(old_path.string(), old, error), error);
    CHECK(old.store_lookup == "exact");
    CHECK_FALSE(fs::exists(old.directory)); // config checking is side-effect-free.
    for (const auto* suffix : {",\"store_lookup\":\"invalid\"", ",\"store_lookup\":\"EXACT\"",
                              ",\"store_lookup\":false", ",\"store_lookup\":7",
                              ",\"store_lookup\":\"exact\",\"store_lookup\":\"exact\"",
                              ",\"store_lookups\":\"exact\""}) {
        const auto path = directory.path / (std::to_string(serial++) + ".json");
        put_config(path, std::string("{\"directory\":\"unused\",\"backend\":\"sqlite_compact\"") + suffix + "}");
        Config invalid;
        CHECK_FALSE(load_config(path.string(), invalid, error));
        CHECK_FALSE(error.empty());
    }
}

TEST_CASE("bench_store_lookup: unsupported backends repeats capture and invalid selectors fail before storage") {
    Directory directory;
    Config config;
    config.directory = (directory.path / "not-created").string();
    config.backend = "sqlite_compact";
    config.store_lookup = "assume_unique";
    std::string error;
    REQUIRE(validate_config(config, error));
    config.duplicate_every = 2;
    CHECK_FALSE(validate_config(config, error));
    CHECK(error.find("duplicate_every=0") != std::string::npos);
    config.duplicate_every = 0;
    config.input_file = "capture.bin";
    config.input_manifest = "capture-manifest.json";
    CHECK_FALSE(validate_config(config, error));
    CHECK(error.find("input_manifest") != std::string::npos);
    config.input_file.clear(); config.input_manifest.clear();
    Metrics metrics;
    for (const std::string backend : {"sqlite", "null", "segment", "segment_rocksdb"}) {
        config.backend = backend;
        CHECK_FALSE(validate_config(config, error));
        CHECK(error.find("store_lookup=assume_unique") != std::string::npos);
        auto options = options_for(config.directory, backend);
        CHECK(open_store(options, metrics, error) == nullptr);
        CHECK(error.find("store_lookup=assume_unique") != std::string::npos);
        CHECK_FALSE(fs::exists(config.directory));
    }
    auto invalid = options_for(config.directory, "sqlite_compact", "not-an-enum");
    CHECK(open_store(invalid, metrics, error) == nullptr);
    CHECK(error.find("store_lookup") != std::string::npos);
    CHECK_FALSE(fs::exists(config.directory));
#ifndef TICKSHELF_HAVE_ROCKSDB
    config.backend = "rocksdb";
    CHECK_FALSE(validate_config(config, error));
    CHECK(error.find("ROCKSDB") != std::string::npos);
    CHECK(open_store(options_for(config.directory, "rocksdb"), metrics, error) == nullptr);
    CHECK(error.find("TICKSHELF_HAVE_ROCKSDB") != std::string::npos);
    CHECK_FALSE(fs::exists(config.directory));
#endif
}

TEST_CASE("bench_store_lookup: unique full content hash cache indexes transactions and zero lookup work survive reopen") {
    for (const auto& backend : backends()) for (const std::string model : {"light", "heavy"})
    for (const bool reuse : {true, false}) {
        if (backend == "rocksdb" && !reuse) continue; // SQLite-only lifetime switch.
        CAPTURE(backend); CAPTURE(model); CAPTURE(reuse);
        Directory directory;
        auto options = options_for(directory.path, backend);
        options.transaction_model = model;
        options.reuse_statement = reuse;
        // The separate selector must not re-enable candidate SQL in assume_unique.
        if (backend == "sqlite_compact" && !reuse) options.sqlite_compact_lookup = "separate";
        auto batch = rows();
        const auto contents = expected(batch);
        REQUIRE(std::set<std::string>(contents.begin(), contents.end()).size() == batch.size());
        std::uint64_t recomputed_bytes = 0;
        for (std::size_t i = 0; i < batch.size(); ++i) {
            if (i < 2) {
                unsigned size = 0;
                REQUIRE(EVP_Digest(batch[i].bytes.data(), batch[i].bytes.size(), batch[i].sha256.data(), &size, EVP_sha256(), nullptr) == 1);
                REQUIRE(size == 32);
                batch[i].has_sha256 = true;
            } else recomputed_bytes += batch[i].bytes.size();
        }
        Metrics metrics;
        auto store = opened(options, metrics);
        std::string error;
        REQUIRE_MESSAGE(store->write({batch.begin(), batch.begin() + 3}, error), error);
        REQUIRE_MESSAGE(store->write({batch.begin() + 3, batch.end()}, error), error);
        CHECK(metrics.counters.at("backend_lookup_assume_unique") == 1);
        CHECK(metrics.counters.at("backend_lookup_exact") == 0);
        CHECK(metrics.counters.at("backend_lookup_calls") == 0);
        CHECK(metrics.counters.at("backend_candidates") == 0);
        CHECK(metrics.counters.at("backend_candidates_observable") == 1);
        CHECK(metrics.counters.at("backend_digest_cached") == 2);
        CHECK(metrics.counters.at("backend_digest_recomputed") == 4);
        CHECK(metrics.counters.at("store_digest_cache_hits") == 2);
        CHECK(metrics.counters.at("store_digest_computed") == 4);
        CHECK(metrics.counters.at("store_digest_bytes") == recomputed_bytes);
        CHECK(metrics.stages.at("store_digest_compute").count == 4);
        CHECK(metrics.counters.at("backend_inserted") == batch.size());
        CHECK(metrics.counters.at("backend_ignored") == 0);
        CHECK(metrics.counters.at("transactions") == 2);
        if (backend == "sqlite_compact") {
            CHECK(metrics.counters.at("sqlite_lookup_prepare_calls") == 0);
            CHECK(metrics.counters.at("sqlite_lookup_step_calls") == 0);
            CHECK(metrics.counters.at("sqlite_lookup_rows") == 0);
            CHECK(metrics.counters.at("sqlite_begin_calls") == 2);
            CHECK(metrics.counters.at("sqlite_commit_calls") == 2);
            CHECK(scalar(database(options), "PRAGMA user_version") == 3);
            CHECK(scalar(database(options), "SELECT count(*) FROM records") == 6);
            CHECK(scalar(database(options), "SELECT count(*) FROM records INDEXED BY records_digest WHERE length(digest)=32") == 6);
            CHECK(scalar(database(options), "SELECT count(*) FROM pragma_index_list('records') WHERE \"unique\"<>0") == 0);
            CHECK(scalar(database(options), "SELECT count(*) FROM pragma_index_list('records') WHERE name='records_symbol_time'") == 1);
            if (model == "heavy") {
                CHECK(scalar(database(options), "SELECT count(*) FROM pragma_index_list('records') WHERE name='records_time_symbol'") == 1);
                CHECK(scalar(database(options), "SELECT count(*) FROM write_batches WHERE attempted=3 AND inserted=3 AND ignored=0") == 2);
            }
        } else {
            CHECK(metrics.counters.at("rocksdb_seek_calls") == 0);
            CHECK(metrics.counters.at("rocksdb_iterator_creations") == 0);
            CHECK(metrics.counters.at("rocksdb_dedup_pending_hits") == 0);
            CHECK(metrics.counters.at("rocksdb_write_batches") == 2);
            CHECK(metrics.counters.at("symbol_index_rows") == 6);
            CHECK(metrics.counters.at("time_symbol_index_rows") == (model == "heavy" ? 6 : 0));
            CHECK(metrics.counters.at("rocksdb_state_rows") == 2);
            CHECK(metrics.counters.at("metadata_kv_rows") == (model == "heavy" ? 2 : 0));
        }
        const auto lookup_before = metrics.counters.at("backend_lookup_calls");
        CHECK(exported(*store) == contents);
        CHECK(exported(*store, true, "A", 7, 7) == std::multiset<std::string>{hex(batch[0].bytes), hex(batch[1].bytes)});
        CHECK(exported(*store, true, batch[2].symbol, batch[2].time, batch[2].time) == std::multiset<std::string>{hex(batch[2].bytes)});
        CHECK(exported(*store, true, "missing").empty());
        CHECK(metrics.counters.at("backend_lookup_calls") == lookup_before); // Export Seeks are not dedup work.
        Json info(store->info_json());
        CHECK(info.value["store_lookup"].get_string().value() == "assume_unique");
        CHECK_FALSE(info.value["production"].get_bool().value());
        CHECK(info.value["synthetic_only"].get_bool().value());
        CHECK(info.value["physical_duplicate_rows"].is_null());
        REQUIRE_MESSAGE(store->finish(error), error);
        store.reset();
        Metrics recovered;
        store = opened(options, recovered);
        CHECK(recovered.counters.at("backend_digest_recomputed") == 0); // Recovery is outside workload counts.
        CHECK(recovered.counters.at("backend_lookup_calls") == 0);
        CHECK(exported(*store) == contents);
        CHECK(exported(*store, true, "A", 7, 7).size() == 2);
    }
}

TEST_CASE("bench_store_lookup: exact collision dedup stays default while diagnostic retains all distinct colliding payloads") {
    for (const auto& backend : backends()) for (const std::string mode : {"exact", "assume_unique"}) {
        Directory directory;
        auto options = options_for(directory.path, backend, mode);
        options.test_digest = [](std::string_view, std::string& digest) { digest.assign(32, '\0'); return true; };
        Metrics metrics;
        auto store = opened(options, metrics);
        const auto batch = rows();
        const auto a = batch[0], b = batch[1];
        std::string error;
        REQUIRE_MESSAGE(store->write({a, b}, error), error);
        REQUIRE_MESSAGE(store->write(mode == "exact" ? std::vector<EncodedRecord>{a, b, a} : std::vector<EncodedRecord>{batch[2]}, error), error);
        CHECK(metrics.counters.at("backend_inserted") == (mode == "exact" ? 2 : 3));
        CHECK(metrics.counters.at("backend_ignored") == (mode == "exact" ? 3 : 0));
        CHECK(metrics.counters.at("backend_digest_recomputed") == 0); // Overrides are not actual SHA256 calls.
        CHECK(metrics.counters.at("store_digest_override_calls") == (mode == "exact" ? 5 : 3));
        CHECK(exported(*store, true, "A", 7, 7) == std::multiset<std::string>{hex(a.bytes), hex(b.bytes)});
        if (mode == "exact") {
            CHECK(metrics.counters.at("backend_lookup_exact") == 1);
            CHECK(metrics.counters.at("backend_lookup_calls") == (backend == "rocksdb" ? 2 : 5));
            if (backend == "sqlite_compact") {
                CHECK(scalar(database(options), "PRAGMA user_version") == 2);
                CHECK(metrics.counters.at("backend_candidates_observable") == 0);
            } else {
                CHECK(metrics.counters.at("backend_candidates") == 2);
                Json info(store->info_json());
                CHECK(info.value["rocksdb"]["format_stamp"].get_string().value() == "tickshelf-bench/full-record-rocksdb/v1/RCD1/light");
            }
        } else {
            CHECK(metrics.counters.at("backend_lookup_calls") == 0);
            CHECK(exported(*store).size() == 3);
        }
        REQUIRE(store->finish(error));
        store.reset();
        Metrics recovered;
        store = opened(options, recovered);
        CHECK(exported(*store).size() == (mode == "exact" ? 2 : 3));
    }
}

TEST_CASE("bench_store_lookup: separate exact SQL reports materialized candidates without counting export work") {
    Directory directory;
    auto options = options_for(directory.path, "sqlite_compact", "exact");
    options.sqlite_compact_lookup = "separate";
    options.test_digest = [](std::string_view, std::string& digest) { digest.assign(32, '\0'); return true; };
    Metrics metrics;
    auto store = opened(options, metrics);
    const auto batch = rows();
    std::string error;
    REQUIRE(store->write({batch[0], batch[1], batch[0]}, error));
    REQUIRE(store->write({batch[1], batch[0]}, error));
    CHECK(metrics.counters.at("backend_lookup_calls") == 5);
    CHECK(metrics.counters.at("backend_candidates") == 5);
    CHECK(metrics.counters.at("backend_candidates_observable") == 1);
    CHECK(metrics.counters.at("backend_lookup_calls") == metrics.counters.at("sqlite_lookup_rows"));
    CHECK(metrics.counters.at("backend_candidates") == metrics.counters.at("sqlite_lookup_candidate_rows"));
    CHECK(metrics.counters.at("backend_inserted") == 2);
    CHECK(metrics.counters.at("backend_ignored") == 3);
    CHECK(exported(*store).size() == 2);
    CHECK(metrics.counters.at("backend_lookup_calls") == 5);
    CHECK(metrics.counters.at("backend_candidates") == 5);
}

TEST_CASE("bench_store_lookup: exact and diagnostic directories coexist but cross-mode format imports fail") {
    for (const auto& backend : backends()) {
        Directory directory;
        const auto exact = options_for(directory.path / "case", backend, "exact");
        const auto diagnostic = options_for(directory.path / "case", backend);
        Metrics exact_metrics, diagnostic_metrics;
        auto exact_store = opened(exact, exact_metrics), diagnostic_store = opened(diagnostic, diagnostic_metrics);
        const auto batch = rows();
        std::string error;
        REQUIRE(exact_store->write({batch[0]}, error));
        REQUIRE(diagnostic_store->write({batch[1]}, error));
        CHECK(database(exact) != database(diagnostic));
        CHECK(exported(*exact_store) == std::multiset<std::string>{hex(batch[0].bytes)});
        CHECK(exported(*diagnostic_store) == std::multiset<std::string>{hex(batch[1].bytes)});
        REQUIRE(exact_store->finish(error)); REQUIRE(diagnostic_store->finish(error));
        exact_store.reset(); diagnostic_store.reset();
        auto import_exact = options_for(directory.path / "import-exact", backend, "exact");
        fs::create_directories(database(import_exact).parent_path());
        fs::rename(database(diagnostic), database(import_exact));
        CHECK(open_store(import_exact, exact_metrics, error) == nullptr);
        CHECK_FALSE(error.empty());
        auto import_diagnostic = options_for(directory.path / "import-diagnostic", backend);
        fs::create_directories(database(import_diagnostic).parent_path());
        fs::rename(database(exact), database(import_diagnostic));
        CHECK(open_store(import_diagnostic, diagnostic_metrics, error) == nullptr);
        CHECK_FALSE(error.empty());
    }
}

TEST_CASE("bench_store_lookup: accidental repeats remain visible to the oracle and strict recovery rejects them") {
    for (const auto& backend : backends()) for (const int repetition : {0, 1, 2}) {
        CAPTURE(backend); CAPTURE(repetition);
        Directory directory;
        auto options = options_for(directory.path, backend);
        options.transaction_model = "heavy";
        Metrics metrics;
        auto store = opened(options, metrics);
        const auto batch = rows();
        std::string error;
        if (repetition != 0) {
            REQUIRE(store->write({batch[0], batch[1]}, error));
            if (repetition == 2) {
                REQUIRE(store->finish(error));
                store.reset();
                store = opened(options, metrics);
            }
            REQUIRE(store->write({batch[0]}, error));
        } else REQUIRE(store->write({batch[0], batch[1], batch[0]}, error));
        CHECK(metrics.counters.at("backend_lookup_calls") == 0);
        CHECK(metrics.counters.at("backend_inserted") == 3);
        CHECK(metrics.counters.at("backend_ignored") == 0);
        const auto actual = exported(*store);
        CHECK(actual.size() == 3);
        CHECK(actual.count(hex(batch[0].bytes)) == 2);
        CHECK(actual != std::multiset<std::string>{hex(batch[0].bytes), hex(batch[1].bytes)});
        CHECK(exported(*store, true, "A", 7, 7) == actual); // No range-query masking.
        Json info(store->info_json());
        CHECK(info.value["physical_duplicate_rows"].is_null());
        REQUIRE(store->finish(error));
        store.reset();
        Metrics recovered;
        CHECK(open_store(options, recovered, error) == nullptr);
        CHECK(error.find("duplicate") != std::string::npos);
    }
}
