#include <doctest/doctest.h>
#include <simdjson.h>
#include <sqlite3.h>
#include <openssl/evp.h>
#ifdef TICKSHELF_HAVE_ROCKSDB
#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>
#endif

#include <array>
#include <cerrno>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <sys/wait.h>
#include <unistd.h>

#include "../bench/store.hpp"

using namespace tickshelf::bench;
namespace fs = std::filesystem;

namespace {

// All I/O is confined to a directory this fixture successfully created. No
// Redis connection, fixed shared test path, or cleanup of pre-existing paths.
struct StoreDirectory {
    fs::path path;
    StoreDirectory() {
        const auto pattern = (fs::temp_directory_path() / "tickshelf_bench_store_XXXXXX").string();
        std::vector<char> name(pattern.begin(), pattern.end());
        name.push_back('\0');
        const char* created = ::mkdtemp(name.data());
        if (!created) throw std::runtime_error("mkdtemp failed");
        path = created;
    }
    ~StoreDirectory() {
        std::error_code error;
        fs::remove_all(path, error);
    }
    StoreDirectory(const StoreDirectory&) = delete;
    StoreDirectory& operator=(const StoreDirectory&) = delete;
};

struct Json {
    std::string storage;
    simdjson::dom::parser parser;
    simdjson::dom::element value;
    explicit Json(std::string text) : storage(std::move(text)) {
        const auto error = parser.parse(storage).get(value);
        if (error) throw std::runtime_error(std::string("invalid JSON: ") + simdjson::error_message(error));
    }
};

EncodedRecord encoded(const Record& record) { return {record.symbol, record.time, encode_record(record)}; }
EncodedRecord sample(std::uint64_t id) { return encoded(generate_record(id, GeneratorOptions{54, 5, 42, true})); }
std::string hexadecimal(std::string_view bytes) {
    const char digits[] = "0123456789abcdef";
    std::string out;
    for (unsigned char c : bytes) { out += digits[c >> 4]; out += digits[c & 15]; }
    return out;
}

struct ExportedRow {
    std::string symbol;
    std::uint64_t time;
    std::string hex;
};
std::vector<ExportedRow> parsed_rows(const std::string& text) {
    std::vector<ExportedRow> rows;
    std::istringstream in(text);
    std::string line;
    while (std::getline(in, line)) {
        Json json(line);
        CHECK(json.value.get_object().value().size() == 3);
        rows.push_back({std::string(json.value["symbol"].get_string().value()),
                        json.value["time"].get_uint64().value(),
                        std::string(json.value["hex"].get_string().value())});
    }
    return rows;
}
std::vector<ExportedRow> exported(Store& store) {
    std::ostringstream out;
    std::string error = "stale";
    REQUIRE_MESSAGE(store.export_rows(out, error), error);
    CHECK(error.empty());
    return parsed_rows(out.str());
}
std::vector<ExportedRow> ranged(Store& store, const std::string& symbol, std::uint64_t from, std::uint64_t to) {
    std::ostringstream out;
    std::string error = "stale";
    REQUIRE_MESSAGE(store.export_range(symbol, from, to, out, error), error);
    CHECK(error.empty());
    return parsed_rows(out.str());
}
std::multiset<std::string> hexes(const std::vector<ExportedRow>& rows) {
    std::multiset<std::string> result;
    for (const auto& row : rows) result.insert(row.hex);
    return result;
}
std::multiset<std::string> expected_hexes(const std::vector<EncodedRecord>& records) {
    std::multiset<std::string> result;
    for (const auto& record : records) result.insert(hexadecimal(record.bytes));
    return result;
}
std::unique_ptr<Store> opened(const StoreOptions& options, Metrics& metrics) {
    std::string error = "stale";
    auto store = open_store(options, metrics, error);
    REQUIRE_MESSAGE(store != nullptr, error);
    CHECK(error.empty());
    return store;
}
StoreOptions options_for(const std::string& backend, const fs::path& directory) {
    StoreOptions options;
    options.backend = backend;
    options.directory = directory.string();
    return options;
}
void execute_sql(const fs::path& path, const char* sql) {
    sqlite3* db = nullptr;
    const int opened_rc = sqlite3_open(path.c_str(), &db);
    REQUIRE(opened_rc == SQLITE_OK);
    const int rc = sqlite3_exec(db, sql, nullptr, nullptr, nullptr);
    const std::string error = sqlite3_errmsg(db);
    sqlite3_close(db);
    REQUIRE_MESSAGE(rc == SQLITE_OK, error);
}
std::int64_t sql_int(const fs::path& path, const char* sql) {
    sqlite3* db = nullptr;
    REQUIRE(sqlite3_open_v2(path.c_str(), &db, SQLITE_OPEN_READONLY, nullptr) == SQLITE_OK);
    sqlite3_stmt* stmt = nullptr;
    const int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        const std::string error = sqlite3_errmsg(db);
        sqlite3_close(db);
        FAIL(error);
    }
    const int step = sqlite3_step(stmt);
    std::int64_t result = 0;
    if (step == SQLITE_ROW) result = sqlite3_column_int64(stmt, 0);
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    REQUIRE(step == SQLITE_ROW);
    return result;
}
std::string file_contents(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("cannot open fixture file");
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}
void append_file(const fs::path& path, std::string_view bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::app);
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    out.close();
    REQUIRE(out.good());
}

std::vector<std::string> storage_backends(bool include_null = false) {
    std::vector<std::string> result{"sqlite", "sqlite_compact", "segment", "segment_day"};
#ifdef TICKSHELF_HAVE_ROCKSDB
    result.emplace_back("segment_rocksdb");
    result.emplace_back("rocksdb");
#endif
    if (include_null) result.emplace_back("null");
    return result;
}
std::vector<std::string> segment_backends() {
    std::vector<std::string> result{"segment"};
#ifdef TICKSHELF_HAVE_ROCKSDB
    result.emplace_back("segment_rocksdb");
#endif
    return result;
}
bool is_segment(const std::string& backend) { return backend == "segment" || backend == "segment_rocksdb"; }
fs::path segment_directory(const StoreOptions& options) {
    return options.backend == "segment_rocksdb" ? fs::path(options.directory) / "segment-rocksdb-v1" : fs::path(options.directory);
}

} // namespace

TEST_CASE("bench_store: linked dependencies preserve standard filesystem exception identity") {
    bool caught = false;
    try {
        throw fs::filesystem_error("RTTI linkage probe", std::make_error_code(std::errc::io_error));
    } catch (const std::exception& error) {
        caught = true;
        CHECK(std::string(error.what()).find("RTTI linkage probe") != std::string::npos);
    } catch (...) {
        // A mixed-RTTI static archive shadowed libc++ typeinfo on this Mac.
    }
    CHECK(caught);
}

TEST_CASE("bench_store: invalid backend/options/path return nullptr with diagnostics") {
    StoreDirectory directory;
    Metrics metrics;
    std::string error;
    auto options = options_for("invalid", directory.path);
    CHECK(open_store(options, metrics, error) == nullptr);
    CHECK_FALSE(error.empty());
    options.backend = "sqlite";
    options.synchronous = "OFF";
    CHECK(open_store(options, metrics, error) == nullptr);
    options.synchronous = "FULL";
    options.autocheckpoint = -1;
    CHECK(open_store(options, metrics, error) == nullptr);
    options.autocheckpoint = 0;
    options.transaction_model = "sleep";
    CHECK(open_store(options, metrics, error) == nullptr);
    options.transaction_model = "light";
    options.failpoint = "after_append";
    CHECK(open_store(options, metrics, error) == nullptr);
    options.backend = "segment";
    options.failpoint = "unknown";
    CHECK(open_store(options, metrics, error) == nullptr);
    options.failpoint.clear();
    options.synchronous = "NORMAL";
    CHECK(open_store(options, metrics, error) == nullptr);
    options.backend = "sqlite";
    options.synchronous = "FULL";
    options.directory.clear();
    CHECK(open_store(options, metrics, error) == nullptr);
    const auto file = directory.path / "not-a-directory";
    { std::ofstream out(file); out << "owned fixture"; }
    options.directory = file.string();
    CHECK(open_store(options, metrics, error) == nullptr);
    CHECK_FALSE(error.empty());
}

TEST_CASE("bench_store: null consumes every byte and never creates its directory") {
    StoreDirectory directory;
    auto options = options_for("null", directory.path / "must-not-exist");
    Metrics metrics;
    auto store = opened(options, metrics);
    const std::vector<EncodedRecord> records{sample(0), sample(1), sample(0)};
    std::string error = "stale";
    REQUIRE(store->write({}, error));
    REQUIRE(store->write(records, error));
    CHECK(error.empty());
    REQUIRE(store->finish(error));
    CHECK(exported(*store).empty());
    CHECK_FALSE(fs::exists(options.directory));
    std::uint64_t expected = 14695981039346656037ULL, bytes = 0;
    for (const auto& record : records) {
        for (unsigned char c : record.bytes) expected = (expected ^ c) * 1099511628211ULL;
        bytes += record.bytes.size();
    }
    Json info(store->info_json());
    CHECK(info.value["backend"].get_string().value() == "null");
    CHECK_FALSE(info.value["durable"].get_bool().value());
    CHECK_FALSE(info.value["writes_database"].get_bool().value());
    CHECK(info.value["checksum"].get_uint64().value() == expected);
    CHECK(metrics.counters.at("null_consumed_bytes") == bytes);
    CHECK(metrics.counters.at("null_consumed_rows") == 3);
    CHECK(metrics.counters.at("file_write_bytes") == 0);
    CHECK(metrics.counters.at("sqlite_step_calls") == 0);
    std::ostringstream range_out;
    CHECK_FALSE(store->export_range(records.front().symbol, 0, std::numeric_limits<std::uint64_t>::max(), range_out, error));
    CHECK(error.find("does not persist") != std::string::npos);
    CHECK(range_out.str().empty());
    CHECK_FALSE(fs::exists(options.directory));
}

TEST_CASE("bench_store: length/header rejection is all-or-nothing for each backend") {
    for (const std::string& backend : storage_backends(true)) {
        CAPTURE(backend);
        StoreDirectory directory;
        Metrics metrics;
        auto store = opened(options_for(backend, directory.path), metrics);
        const auto good = sample(0);
        auto bad = good;
        bad.bytes.resize(kMaxRecordBytes + 1);
        std::string error;
        CHECK_FALSE(store->write({good, bad}, error));
        CHECK_FALSE(error.empty());
        bad.bytes.clear();
        CHECK_FALSE(store->write({good, bad}, error));
        bad = good;
        bad.symbol = "different";
        CHECK_FALSE(store->write({good, bad}, error));
        bad = good;
        ++bad.time;
        CHECK_FALSE(store->write({good, bad}, error));
        bad = good;
        bad.bytes[0] = 'X';
        CHECK_FALSE(store->write({good, bad}, error));
        Record large;
        large.symbol.assign(60000, 'x');
        large.time = 1;
        const auto large_encoded = encoded(large);
        std::vector<EncodedRecord> over_payload((64ULL * 1024 * 1024) / large_encoded.bytes.size() + 1, large_encoded);
        CHECK_FALSE(store->write(over_payload, error));
        CHECK_FALSE(error.empty());
        CHECK(exported(*store).empty());
        CHECK(metrics.counters.at("inserted") == 0);
        REQUIRE_MESSAGE(store->write({good}, error), error); // Rejection does not poison a store.
        REQUIRE(store->finish(error));
        CHECK(exported(*store).size() == (backend == "null" ? 0 : 1));
    }
}

TEST_CASE("bench_store: store transactions may exceed wire-frame byte and row limits") {
    for (const std::string& backend : storage_backends(true)) {
        CAPTURE(backend);
        StoreDirectory directory;
        const auto options = options_for(backend, directory.path);
        Metrics metrics;
        auto store = opened(options, metrics);
        Record record;
        record.symbol.assign(600, 'x');
        record.time = std::numeric_limits<std::uint64_t>::max();
        const auto canonical = encoded(record);
        const std::vector<EncodedRecord> batch(9000, canonical);
        REQUIRE(batch.size() > kMaxBatchRecords);
        REQUIRE(batch.size() * canonical.bytes.size() > kMaxPayloadBytes);
        std::string error;
        REQUIRE_MESSAGE(store->write(batch, error), error);
        REQUIRE_MESSAGE(store->finish(error), error);
        if (backend == "null") {
            CHECK(metrics.counters.at("null_consumed_rows") == batch.size());
        } else {
            const std::uint64_t expected_rows =
            (is_segment(backend) || backend == "segment_day") ? batch.size() : 1;
            Json info(store->info_json());
            CHECK(info.value["physical_rows"].get_uint64().value() == expected_rows);
            store.reset();
            Metrics recovery_metrics;
            store = opened(options, recovery_metrics);
            Json recovered(store->info_json());
            CHECK(recovered.value["physical_rows"].get_uint64().value() == expected_rows);
            if (backend == "segment") {
                CHECK(sql_int(directory.path / "segment-index.sqlite3", "SELECT count(*) FROM segments") == 1);
                CHECK(sql_int(directory.path / "segment-index.sqlite3", "SELECT records FROM segments") == 9000);
            }
        }
    }
}

TEST_CASE("bench_store: SQLite exact content dedup, replay, timestamp collision, and statement reuse") {
    for (const std::string backend : {"sqlite", "sqlite_compact"}) {
    CAPTURE(backend);
    for (const bool reuse : {true, false}) {
        CAPTURE(reuse);
        StoreDirectory directory;
        auto options = options_for(backend, directory.path / "nested" / "created");
        options.reuse_statement = reuse;
        options.autocheckpoint = 7;
        Metrics metrics;
        auto store = opened(options, metrics);
        auto altered = generate_record(0, GeneratorOptions{54, 5, 42, true});
        ++altered.values[0]; // Same symbol and time, genuinely different full record.
        const auto a = sample(0), b = encoded(altered);
        Record unusual = generate_record(1, GeneratorOptions{});
        unusual.symbol = std::string("quote\" slash\\ line\n null\0", 25);
        unusual.time = std::numeric_limits<std::uint64_t>::max();
        const auto c = encoded(unusual);
        std::string error;
        REQUIRE_MESSAGE(store->write({a, a, b}, error), error);
        REQUIRE_MESSAGE(store->write({a, a, b}, error), error);
        REQUIRE_MESSAGE(store->write({c}, error), error);
        CHECK(metrics.counters.at("attempted") == 7);
        CHECK(metrics.counters.at("inserted") == 3);
        CHECK(metrics.counters.at("ignored") == 4);
        CHECK(metrics.counters.at("transactions") == 3);
        CHECK(metrics.counters.at("sqlite_prepare_calls") == (reuse ? 1 : 3));
        CHECK(metrics.stages.at("sqlite_prepare").count == (reuse ? 1 : 3));
        CHECK(metrics.stages.at("sqlite_begin").count == 3);
        CHECK(metrics.stages.at("sqlite_step").count == 7);
        CHECK(metrics.stages.at("sqlite_commit").count == 3);
        REQUIRE(store->finish(error));
        REQUIRE(store->finish(error));
        const auto rows = exported(*store);
        CHECK(hexes(rows) == expected_hexes({a, b, c}));
        bool saw_uint64 = false;
        for (const auto& row : rows) {
            if (row.hex == hexadecimal(c.bytes)) {
                CHECK(row.symbol == unusual.symbol);
                CHECK(row.time == unusual.time);
                saw_uint64 = true;
            }
        }
        CHECK(saw_uint64);
        Json info(store->info_json());
        CHECK(info.value["durable"].get_bool().value());
        CHECK_FALSE(info.value["physical_power_loss_tested"].get_bool().value());
        CHECK(info.value["hardware_plp"].get_string().value() == "unknown");
        CHECK(info.value["sqlite"]["pragmas"]["journal_mode"].get_string().value() == "wal");
        CHECK(info.value["sqlite"]["pragmas"]["synchronous"].get_int64().value() == 2);
        CHECK(info.value["sqlite"]["pragmas"]["wal_autocheckpoint"].get_int64().value() == 7);
        CHECK_FALSE(info.value["sqlite"]["version"].get_string().value().empty());
        CHECK(info.value["sqlite"]["compile_options"].get_array().value().size() > 0);
        CHECK((info.storage.find("WITHOUT ROWID") != std::string::npos) == (backend == "sqlite"));
        CHECK(info.value["physical_rows"].get_uint64().value() == 3);
        store.reset();
        Metrics reopened_metrics;
        auto reopened = opened(options, reopened_metrics);
        REQUIRE(reopened->write({a, b, c}, error));
        CHECK(reopened_metrics.counters.at("inserted") == 0);
        CHECK(reopened_metrics.counters.at("ignored") == 3);
        CHECK(hexes(exported(*reopened)) == expected_hexes({a, b, c}));
    }
    }
}

TEST_CASE("bench_store: SQLite failed mid-transaction rolls back and reusable statement recovers") {
    StoreDirectory directory;
    Metrics metrics;
    auto store = opened(options_for("sqlite", directory.path), metrics);
    const auto database = directory.path / "full-record.sqlite3";
    execute_sql(database, "CREATE TRIGGER reject_fixture BEFORE INSERT ON records "
                          "WHEN NEW.symbol='600001.SH' BEGIN SELECT RAISE(ABORT,'fixture failure'); END");
    std::string error;
    CHECK_FALSE(store->write({sample(0), sample(1)}, error));
    CHECK_FALSE(error.empty());
    CHECK(exported(*store).empty());
    CHECK(metrics.counters.at("inserted") == 0);
    CHECK(metrics.counters.at("transactions") == 0);
    execute_sql(database, "DROP TRIGGER reject_fixture");
    REQUIRE_MESSAGE(store->write({sample(0), sample(1)}, error), error);
    CHECK(hexes(exported(*store)) == expected_hexes({sample(0), sample(1)}));
}

TEST_CASE("bench_store: explicit NORMAL diagnostic and actual heavy SQL/schema costs") {
    StoreDirectory directory;
    Metrics metrics;
    auto options = options_for("sqlite", directory.path);
    options.synchronous = "NORMAL";
    options.transaction_model = "heavy";
    options.reuse_statement = false;
    options.autocheckpoint = 0;
    auto store = opened(options, metrics);
    std::string error;
    REQUIRE(store->write({sample(0), sample(0), sample(1)}, error));
    REQUIRE(store->write({sample(1)}, error));
    REQUIRE(store->finish(error));
    Json info(store->info_json());
    CHECK_FALSE(info.value["durable"].get_bool().value());
    CHECK(info.value["sqlite"]["pragmas"]["synchronous"].get_int64().value() == 1);
    CHECK(info.value["sqlite"]["pragmas"]["wal_autocheckpoint"].get_int64().value() == 0);
    CHECK(info.storage.find("records_time_symbol") != std::string::npos);
    CHECK(info.storage.find("write_batches") != std::string::npos);
    CHECK(metrics.counters.at("metadata_sql_rows") == 2);
    CHECK(metrics.counters.at("sqlite_step_calls") == 6);
    CHECK(metrics.counters.at("sqlite_prepare_calls") == 4); // insert + metadata, once per transaction.
    const auto database = directory.path / "full-record.sqlite3";
    CHECK(sql_int(database, "SELECT count(*) FROM write_batches") == 2);
    CHECK(sql_int(database, "SELECT sum(attempted) FROM write_batches") == 4);
    CHECK(sql_int(database, "SELECT sum(inserted) FROM write_batches") == 2);
    CHECK(sql_int(database, "SELECT sum(ignored) FROM write_batches") == 2);
}

TEST_CASE("bench_store: segment complete readback, segment-granularity index and explicit physical duplicates") {
    for (const bool heavy : {false, true}) {
        CAPTURE(heavy);
        StoreDirectory directory;
        auto options = options_for("segment", directory.path);
        options.transaction_model = heavy ? "heavy" : "light";
        options.reuse_statement = !heavy;
        options.autocheckpoint = 0;
        Metrics metrics;
        auto store = opened(options, metrics);
        const std::vector<EncodedRecord> records{sample(0), sample(1), sample(0)};
        std::string error;
        REQUIRE_MESSAGE(store->write(records, error), error);
        REQUIRE_MESSAGE(store->write(records, error), error);
        Json before(store->info_json());
        CHECK(before.value["physical_duplicate_rows"].is_null());
        CHECK(before.value["physical_rows"].get_uint64().value() == 6);
        CHECK(metrics.counters.at("attempted") == 6);
        CHECK(metrics.counters.at("inserted") == 6);
        CHECK(metrics.counters.at("ignored") == 0);
        CHECK(metrics.counters.at("index_rows") == 2);
        CHECK(metrics.counters.at("symbol_index_rows") == 4);
        CHECK(metrics.counters.at("sqlite_prepare_calls") == (heavy ? 6 : 2));
        CHECK(metrics.counters.at("sqlite_step_calls") == (heavy ? 8 : 6));
        CHECK(metrics.counters.at("metadata_sql_rows") == (heavy ? 2 : 0));
        CHECK(metrics.stages.at("file_append").count == 2);
        CHECK(metrics.stages.at("file_sync").count == 2);
        CHECK(metrics.stages.at("index_commit").count == 2);
        CHECK(metrics.stages.at("sqlite_commit").count == 2);
        CHECK(metrics.counters.at("file_write_bytes") == fs::file_size(directory.path / "records.segments"));
        CHECK(sql_int(directory.path / "segment-index.sqlite3", "SELECT count(*) FROM segments") == 2);
        CHECK(sql_int(directory.path / "segment-index.sqlite3", "SELECT sum(records) FROM segments") == 6);
        CHECK(sql_int(directory.path / "segment-index.sqlite3", "SELECT count(*) FROM segment_symbols") == 4);
        REQUIRE(store->finish(error));
        const std::vector<EncodedRecord> expected{sample(0), sample(1), sample(0), sample(0), sample(1), sample(0)};
        CHECK(hexes(exported(*store)) == expected_hexes(expected));
        Json info(store->info_json());
        CHECK(info.value["durable"].get_bool().value());
        CHECK_FALSE(info.value["physical_power_loss_tested"].get_bool().value());
        CHECK(info.value["hardware_plp"].get_string().value() == "unknown");
        CHECK(info.value["physical_duplicate_rows"].get_uint64().value() == 4);
        CHECK(info.value["sqlite"]["pragmas"]["synchronous"].get_int64().value() == 2);
        CHECK(metrics.counters.at("segment_duplicate_physical_rows") == 4);
        store.reset();
        Metrics recovered_metrics;
        auto recovered = opened(options, recovered_metrics);
        CHECK(hexes(exported(*recovered)) == expected_hexes(expected));
        CHECK(recovered_metrics.counters.at("inserted") == 0);
        REQUIRE(recovered->write({sample(2)}, error));
        Json after_write(recovered->info_json());
        CHECK(after_write.value["physical_duplicate_rows"].is_null());
        CHECK(exported(*recovered).size() == 7);
    }
}

TEST_CASE("bench_store: indexed inclusive ranges handle unordered time, symbols, exact duplicates, and uint64 bounds") {
    for (const std::string& backend : storage_backends()) {
        for (const bool reuse : {true, false}) {
            CAPTURE(backend);
            CAPTURE(reuse);
            StoreDirectory directory;
            auto options = options_for(backend, directory.path);
            options.reuse_statement = reuse;
            options.transaction_model = reuse ? "light" : "heavy";
            Metrics metrics;
            auto store = opened(options, metrics);
            const auto make = [](const std::string& symbol, std::uint64_t time, std::int64_t value = 1) {
                Record record;
                record.symbol = symbol;
                record.time = time;
                record.present = 1;
                record.values[0] = value;
                return encoded(record);
            };
            const auto maximum = std::numeric_limits<std::uint64_t>::max();
            const auto high = std::uint64_t{1} << 63;
            const auto a0 = make("A", 0), a5 = make("A", 5), a50 = make("A", 50);
            const auto a50_other = make("A", 50, 2), a75 = make("A", 75), a100 = make("A", 100);
            const auto a250 = make("A", 250), a_high_before = make("A", high - 1);
            const auto a_high = make("A", high), a_max = make("A", maximum);
            const auto b1 = make("B", 1), b20 = make("B", 20), b60 = make("B", 60);
            const std::string unusual_symbol("C\"\0", 3);
            const auto c50 = make(unusual_symbol, 50);
            const std::vector<EncodedRecord> first{a100, b60, a5, a50, a50_other, a50, b1, a0, a_max, a_high, a_high_before};
            const std::vector<EncodedRecord> second{a50, a5, a100, a75, c50, b60};
            const std::vector<EncodedRecord> third{a250, b20};
            std::string error;
            REQUIRE_MESSAGE(store->write(first, error), error);
            REQUIRE_MESSAGE(store->write(second, error), error);
            REQUIRE_MESSAGE(store->write(third, error), error);
            REQUIRE(store->finish(error));
            const auto prepares = metrics.counters.at("sqlite_prepare_calls");
            const auto candidates = metrics.counters.at("range_candidate_segments");
            const auto decoded = metrics.counters.at("range_decoded_rows");
            CHECK(hexes(ranged(*store, "A", 50, 50)) == expected_hexes({a50, a50_other}));
            if (backend == "segment") {
                CHECK(metrics.counters.at("range_candidate_segments") - candidates == 2);
                CHECK(metrics.counters.at("range_decoded_rows") - decoded == first.size() + second.size());
                CHECK(sql_int(directory.path / "segment-index.sqlite3", "SELECT count(*) FROM segments") == 3);
                CHECK(sql_int(directory.path / "segment-index.sqlite3", "SELECT count(*) FROM segment_symbols") == 7);
                CHECK(sql_int(directory.path / "segment-index.sqlite3",
                    "SELECT count(*) FROM segment_symbols WHERE segment_offset=0 AND symbol='A' "
                    "AND min_time=x'0000000000000000' AND max_time=x'ffffffffffffffff'") == 1);
                Json info(store->info_json());
                CHECK(info.value["symbol_index_rows"].get_uint64().value() == 7);
                CHECK(info.value["sqlite"]["pragmas"]["user_version"].get_uint64().value() == 2);
                CHECK(info.storage.find("segment_symbols_lookup") != std::string::npos);
            }
            const std::vector<EncodedRecord> all_a{a0, a5, a50, a50_other, a75, a100, a250, a_high_before, a_high, a_max};
            for (int pass = 0; pass < 2; ++pass) { // Repeated reads never carry dedup state across calls.
                CHECK(hexes(ranged(*store, "A", 5, 100)) == expected_hexes({a5, a50, a50_other, a75, a100}));
                CHECK(hexes(ranged(*store, "A", 0, maximum)) == expected_hexes(all_a));
                CHECK(hexes(ranged(*store, "A", 0, 0)) == expected_hexes({a0}));
                CHECK(hexes(ranged(*store, "A", maximum, maximum)) == expected_hexes({a_max}));
                CHECK(hexes(ranged(*store, "A", high - 1, high)) == expected_hexes({a_high_before, a_high}));
                CHECK(hexes(ranged(*store, "B", 20, 60)) == expected_hexes({b20, b60}));
                const auto unusual = ranged(*store, unusual_symbol, 50, 50);
                CHECK(hexes(unusual) == expected_hexes({c50}));
                REQUIRE(unusual.size() == 1);
                CHECK(unusual.front().symbol == unusual_symbol);
                CHECK(ranged(*store, "A", 6, 49).empty());
                CHECK(ranged(*store, "missing", 0, maximum).empty());
            }
            CHECK(metrics.counters.at("sqlite_prepare_calls") == prepares); // Query prepares are not write prepare metrics.
            std::ostringstream reversed;
            CHECK_FALSE(store->export_range("A", 100, 5, reversed, error));
            CHECK_FALSE(error.empty());
            CHECK(reversed.str().empty());
            std::ostringstream oversized;
            CHECK_FALSE(store->export_range(std::string(65536, 'x'), 0, maximum, oversized, error));
            CHECK_FALSE(error.empty());
            store.reset();
            Metrics recovered_metrics;
            store = opened(options, recovered_metrics);
            CHECK(hexes(ranged(*store, "A", 50, 50)) == expected_hexes({a50, a50_other}));
            CHECK(hexes(ranged(*store, "A", 0, maximum)) == expected_hexes(all_a));
            CHECK(hexes(ranged(*store, "B", 1, 60)) == expected_hexes({b1, b20, b60}));
            CHECK(hexes(ranged(*store, unusual_symbol, 50, 50)) == expected_hexes({c50}));
        }
    }
}

TEST_CASE("bench_store: sparse range index corruption or omission is fatal before tail truncation") {
    const std::array<const char*, 7> corruptions{
        "DELETE FROM segment_symbols WHERE symbol='600000.SH'",
        "UPDATE segment_symbols SET max_time=x'ffffffffffffffff' WHERE symbol='600000.SH'",
        "UPDATE segment_symbols SET min_time=x'0000000000000000' WHERE symbol='600000.SH'",
        "UPDATE segment_symbols SET symbol='WRONG' WHERE symbol='600000.SH'",
        "INSERT INTO segment_symbols VALUES(0,'EXTRA',x'0000000000000000',x'0000000000000000')",
        "INSERT INTO segment_symbols VALUES(9223372036854775807,'ORPHAN',x'0000000000000000',x'0000000000000000')",
        "DELETE FROM segment_symbols"
    };
    for (const char* sql : corruptions) {
        CAPTURE(sql);
        StoreDirectory directory;
        const auto options = options_for("segment", directory.path);
        const auto data = directory.path / "records.segments";
        const auto database = directory.path / "segment-index.sqlite3";
        Metrics metrics;
        auto store = opened(options, metrics);
        std::string error;
        REQUIRE(store->write({sample(0), sample(1), sample(54)}, error));
        store.reset();
        append_file(data, "unindexed tail must survive a bad sparse index");
        const auto before = file_contents(data);
        execute_sql(database, sql);
        Metrics recovery_metrics;
        CHECK(open_store(options, recovery_metrics, error) == nullptr);
        CHECK(error.find("indexed segment symbol/time range") != std::string::npos);
        CHECK(file_contents(data) == before);
    }
}

TEST_CASE("bench_store: old segment schema without sparse ranges is explicitly rejected") {
    StoreDirectory directory;
    const auto options = options_for("segment", directory.path);
    const auto data = directory.path / "records.segments";
    const auto database = directory.path / "segment-index.sqlite3";
    Metrics metrics;
    auto store = opened(options, metrics);
    std::string error;
    REQUIRE(store->write({sample(0)}, error));
    store.reset();
    const auto before = file_contents(data);
    execute_sql(database, "PRAGMA user_version=1; DROP TABLE segment_symbols");
    Metrics recovery_metrics;
    CHECK(open_store(options, recovery_metrics, error) == nullptr);
    CHECK(error.find("schema version") != std::string::npos);
    CHECK(sql_int(database, "PRAGMA user_version") == 1);
    CHECK(file_contents(data) == before);
}

TEST_CASE("bench_store: sparse rows and segment entry roll back together after partial sparse insertion") {
    for (const bool reuse : {true, false}) {
        CAPTURE(reuse);
        StoreDirectory directory;
        auto options = options_for("segment", directory.path);
        options.reuse_statement = reuse;
        const auto data = directory.path / "records.segments";
        const auto database = directory.path / "segment-index.sqlite3";
        Metrics metrics;
        auto store = opened(options, metrics);
        std::string error;
        REQUIRE(store->write({sample(0)}, error));
        const auto before = file_contents(data);
        execute_sql(database, "CREATE TRIGGER reject_sparse BEFORE INSERT ON segment_symbols "
                              "WHEN NEW.symbol='600002.SH' BEGIN SELECT RAISE(ABORT,'fixture sparse failure'); END");
        CHECK_FALSE(store->write({sample(1), sample(2)}, error));
        CHECK(error.find("fixture sparse failure") != std::string::npos);
        CHECK(metrics.counters.at("inserted") == 1);
        CHECK(metrics.counters.at("symbol_index_rows") == 1);
        CHECK(sql_int(database, "SELECT count(*) FROM segments") == 1);
        CHECK(sql_int(database, "SELECT count(*) FROM segment_symbols") == 1);
        CHECK_FALSE(store->finish(error));
        execute_sql(database, "DROP TRIGGER reject_sparse");
        store.reset();
        Metrics recovered_metrics;
        store = opened(options, recovered_metrics);
        CHECK(file_contents(data) == before);
        CHECK(ranged(*store, sample(1).symbol, 0, std::numeric_limits<std::uint64_t>::max()).empty());
        REQUIRE_MESSAGE(store->write({sample(1), sample(2)}, error), error);
        CHECK(hexes(ranged(*store, sample(2).symbol, 0, std::numeric_limits<std::uint64_t>::max())) == expected_hexes({sample(2)}));
    }
}

TEST_CASE("bench_store: segment takes an exclusive lifetime append/recovery lock") {
    for (const auto& backend : segment_backends()) {
    CAPTURE(backend);
    StoreDirectory directory;
    const auto options = options_for(backend, directory.path);
    Metrics first_metrics, second_metrics;
    auto first = opened(options, first_metrics);
    std::string error;
    auto second = open_store(options, second_metrics, error);
    CHECK(second == nullptr);
    CHECK_FALSE(error.empty());
    first.reset();
    second = opened(options, second_metrics);
    CHECK(second != nullptr);
    }
}

TEST_CASE("bench_store: failed segment index commit requires reopen and discards only its unindexed tail") {
    StoreDirectory directory;
    const auto options = options_for("segment", directory.path);
    const auto data = directory.path / "records.segments";
    const auto database = directory.path / "segment-index.sqlite3";
    Metrics metrics;
    auto store = opened(options, metrics);
    std::string error;
    REQUIRE(store->write({sample(0)}, error));
    const auto confirmed = file_contents(data);
    execute_sql(database, "CREATE TRIGGER reject_fixture BEFORE INSERT ON segments "
                          "BEGIN SELECT RAISE(ABORT,'fixture index failure'); END");
    CHECK_FALSE(store->write({sample(1), sample(2)}, error));
    CHECK(error.find("fixture index failure") != std::string::npos);
    CHECK(fs::file_size(data) > confirmed.size());
    CHECK(metrics.counters.at("inserted") == 1);
    CHECK(metrics.counters.at("index_rows") == 1);
    CHECK(sql_int(database, "SELECT count(*) FROM segments") == 1);
    CHECK_FALSE(store->finish(error));
    CHECK_FALSE(store->write({sample(3)}, error));
    std::ostringstream rejected_export;
    CHECK_FALSE(store->export_rows(rejected_export, error));
    execute_sql(database, "DROP TRIGGER reject_fixture");
    store.reset();
    Metrics recovery_metrics;
    store = opened(options, recovery_metrics);
    CHECK(file_contents(data) == confirmed);
    CHECK(recovery_metrics.counters.at("recovered_tail_bytes") > 0);
    CHECK(hexes(exported(*store)) == expected_hexes({sample(0)}));
    REQUIRE_MESSAGE(store->write({sample(1), sample(2)}, error), error);
    CHECK(hexes(exported(*store)) == expected_hexes({sample(0), sample(1), sample(2)}));
}

TEST_CASE("bench_store: recovery truncates only unindexed tails including valid and corrupt orphans") {
    for (const auto& backend : segment_backends()) {
    CAPTURE(backend);
    for (const bool complete_orphan : {false, true}) {
        CAPTURE(complete_orphan);
        StoreDirectory directory;
        const auto options = options_for(backend, directory.path);
        const auto data = segment_directory(options) / "records.segments";
        Metrics metrics;
        auto store = opened(options, metrics);
        std::string error;
        REQUIRE(store->write({sample(0), sample(1)}, error));
        store.reset();
        const auto confirmed = file_contents(data);
        const auto tail = complete_orphan ? confirmed : std::string("TSG1\x01\x00 orphan/truncated junk", 29);
        append_file(data, tail);
        Metrics recovery_metrics;
        store = opened(options, recovery_metrics);
        CHECK(fs::file_size(data) == confirmed.size());
        CHECK(file_contents(data) == confirmed);
        CHECK(recovery_metrics.counters.at("recovered_tail_bytes") == tail.size());
        CHECK(hexes(exported(*store)) == expected_hexes({sample(0), sample(1)}));
        REQUIRE(store->write({sample(2)}, error));
        CHECK(exported(*store).size() == 3);
    }
    }
}

TEST_CASE("bench_store: indexed segment corruption must fail and must not repair confirmed data") {
    for (const auto& backend : segment_backends()) {
    CAPTURE(backend);
    // Version, length, count, CRC, reserved header, and payload corruption.
    for (const std::uint64_t offset : {4, 8, 16, 24, 28, 40}) {
        CAPTURE(offset);
        StoreDirectory directory;
        const auto options = options_for(backend, directory.path);
        const auto data = segment_directory(options) / "records.segments";
        Metrics metrics;
        auto store = opened(options, metrics);
        std::string error;
        REQUIRE(store->write({sample(0), sample(1)}, error));
        store.reset();
        auto bytes = file_contents(data);
        bytes.at(offset) ^= 0x40;
        { std::ofstream out(data, std::ios::binary | std::ios::trunc); out.write(bytes.data(), bytes.size()); }
        append_file(data, "unindexed tail must NOT be truncated if confirmed data is corrupt");
        const auto damaged = file_contents(data);
        Metrics recovery_metrics;
        auto bad = open_store(options, recovery_metrics, error);
        CHECK(bad == nullptr);
        CHECK(error.find("indexed segment") != std::string::npos);
        CHECK(file_contents(data) == damaged);
    }
    }
}

TEST_CASE("bench_store: indexed truncation is fatal, not treated as a disposable orphan") {
    for (const auto& backend : segment_backends()) {
    CAPTURE(backend);
    StoreDirectory directory;
    const auto options = options_for(backend, directory.path);
    const auto data = segment_directory(options) / "records.segments";
    Metrics metrics;
    auto store = opened(options, metrics);
    std::string error;
    REQUIRE(store->write({sample(0)}, error));
    store.reset();
    const auto truncated_size = fs::file_size(data) - 1;
    fs::resize_file(data, truncated_size);
    Metrics recovery_metrics;
    CHECK(open_store(options, recovery_metrics, error) == nullptr);
    CHECK(error.find("truncated") != std::string::npos);
    CHECK(fs::file_size(data) == truncated_size);
    }
}

TEST_CASE("bench_store: SIGKILL failpoints recover according to committed segment index") {
    for (const auto& backend : segment_backends()) {
    CAPTURE(backend);
    for (const std::string failpoint : {"after_append", "after_sync", "after_index"}) {
        CAPTURE(failpoint);
        StoreDirectory directory;
        auto options = options_for(backend, directory.path);
        Metrics metrics;
        auto store = opened(options, metrics);
        std::string error;
        REQUIRE(store->write({sample(0)}, error));
        store.reset(); // Never fork a process with a live SQLite connection.
        const auto data = segment_directory(options) / "records.segments";
        const auto confirmed_size = fs::file_size(data);
        const pid_t child = ::fork();
        REQUIRE(child >= 0);
        if (child == 0) {
            options.failpoint = failpoint;
            Metrics child_metrics;
            std::string child_error;
            auto child_store = open_store(options, child_metrics, child_error);
            if (!child_store) ::_exit(70);
            child_store->write({sample(1), sample(2)}, child_error);
            ::_exit(71); // A failpoint must not return or run stack unwinding.
        }
        int status = 0;
        pid_t waited;
        do { waited = ::waitpid(child, &status, 0); } while (waited < 0 && errno == EINTR);
        REQUIRE(waited == child);
        REQUIRE(WIFSIGNALED(status));
        CHECK(WTERMSIG(status) == SIGKILL);
        CHECK(fs::file_size(data) > confirmed_size);
        Metrics recovery_metrics;
        store = opened(options, recovery_metrics);
        const bool indexed = failpoint == "after_index";
        CHECK(exported(*store).size() == (indexed ? 3 : 1));
        if (indexed) {
            CHECK(hexes(exported(*store)) == expected_hexes({sample(0), sample(1), sample(2)}));
        } else {
            CHECK(fs::file_size(data) == confirmed_size);
            CHECK(recovery_metrics.counters.at("recovered_tail_bytes") > 0);
            CHECK(hexes(exported(*store)) == expected_hexes({sample(0)}));
        }
        REQUIRE_MESSAGE(store->write({sample(1), sample(2)}, error), error);
        const auto rows = exported(*store);
        CHECK(rows.size() == (indexed ? 5 : 3));
        std::set<std::string> logical;
        for (const auto& row : rows) logical.insert(row.hex);
        CHECK(logical.size() == 3);
        Json info(store->info_json());
        CHECK(info.value["physical_duplicate_rows"].get_uint64().value() == (indexed ? 2 : 0));
        CHECK(info.value["symbol_index_rows"].get_uint64().value() == (indexed ? 5 : 3));
        CHECK(hexes(ranged(*store, sample(1).symbol, sample(1).time, sample(1).time)) == expected_hexes({sample(1)}));
    }
    }
}

TEST_CASE("bench_store: day-run failpoints adopt fsynced frames; metadata flush is the commit point") {
    // after_append/after_sync：元数据只有分配行，恢复逐帧扫描采纳已 fsync 的帧。
    for (const std::string failpoint : {"after_append", "after_sync"}) {
    CAPTURE(failpoint);
    StoreDirectory directory;
    auto options = options_for("segment_day", directory.path);
    Metrics metrics;
    auto store = opened(options, metrics);
    std::string error;
    REQUIRE(store->write({sample(0)}, error));
    REQUIRE(store->finish(error));  // v3：write 落在内存缓冲，finish 保证落盘
    store.reset();
    const auto data_file = [&] {
        const auto layout = fs::path(options.directory) / "segment-day-v1";
        for (const auto& entry : fs::directory_iterator(layout))
            if (entry.path().extension() == ".runs") return entry.path();
        FAIL("no day-run file");
        return layout / "missing";
    }();
    const auto confirmed_size = fs::file_size(data_file);
    const pid_t child = ::fork();
    REQUIRE(child >= 0);
    if (child == 0) {
        options.failpoint = failpoint;
        Metrics child_metrics;
        std::string child_error;
        auto child_store = open_store(options, child_metrics, child_error);
        if (!child_store) ::_exit(70);
        // 同 symbol 的 300 条（id 步长 54）≈ 90KB > 64KiB chunk → 溢出触发 flush
        std::vector<EncodedRecord> batch;
        for (unsigned i = 0; i < 300; ++i) batch.push_back(sample(1 + 54u * i));
        child_store->write(batch, child_error);
        ::_exit(71); // A failpoint must not return or run stack unwinding.
    }
    int status = 0;
    pid_t waited;
    do { waited = ::waitpid(child, &status, 0); } while (waited < 0 && errno == EINTR);
    REQUIRE(waited == child);
    REQUIRE(WIFSIGNALED(status));
    CHECK(WTERMSIG(status) == SIGKILL);
    CHECK(fs::file_size(data_file) > confirmed_size);  // 帧已落盘（after_sync 场景已 fsync）
    Metrics recovery_metrics;
    store = opened(options, recovery_metrics);
    CHECK(recovery_metrics.counters.at("recovered_tail_records") == 300);
    CHECK(recovery_metrics.counters.at("recovered_tail_bytes") > 0);
    const auto rows = exported(*store);
    CHECK(rows.size() == 301);  // 采纳：fsynced 但未提交的帧活下来了
    // 重试重写同一批：采纳过的帧再次写入 → 物理重复，逻辑集合不变。
    std::vector<EncodedRecord> retry_batch;
    for (unsigned i = 0; i < 300; ++i) retry_batch.push_back(sample(1 + 54u * i));
    REQUIRE_MESSAGE(store->write(retry_batch, error), error);
    REQUIRE(store->finish(error));
    const auto retried = exported(*store);
    CHECK(retried.size() == 601);
    std::set<std::string> logical;
    for (const auto& row : retried) logical.insert(row.hex);
    CHECK(logical.size() == 301);
    Json info(store->info_json());
    CHECK(info.value["physical_duplicate_rows"].get_uint64().value() == 300);
    CHECK(hexes(ranged(*store, sample(1).symbol, sample(1).time, sample(1).time)) ==
          expected_hexes({sample(1)}));
    }
}

TEST_CASE("bench_store: day-run after_metadata failpoint commits exactly the flushed batches") {
    StoreDirectory directory;
    auto options = options_for("segment_day", directory.path);
    Metrics metrics;
    auto store = opened(options, metrics);
    std::string error;
    REQUIRE(store->write({sample(0)}, error));
    REQUIRE(store->finish(error));  // v3：显式提交（write 落在内存缓冲）
    store.reset();
    const pid_t child = ::fork();
    REQUIRE(child >= 0);
    if (child == 0) {
        options.failpoint = "after_metadata";
        Metrics child_metrics;
        std::string child_error;
        auto child_store = open_store(options, child_metrics, child_error);
        if (!child_store) ::_exit(70);
        // 同 symbol（id 步长 54）260 条 ≈ 78KB > 64KiB chunk 剩余 → 第 1 批即溢出
        // 触发 flush，after_metadata 在该批提交点后崩溃（后续批不执行）。
        std::vector<EncodedRecord> batch_records;
        for (unsigned i = 0; i < 260; ++i) batch_records.push_back(sample(1 + 54u * i));
        child_store->write(batch_records, child_error);
        ::_exit(71);
    }
    int status = 0;
    pid_t waited;
    do { waited = ::waitpid(child, &status, 0); } while (waited < 0 && errno == EINTR);
    REQUIRE(waited == child);
    REQUIRE(WIFSIGNALED(status));
    CHECK(WTERMSIG(status) == SIGKILL);
    Metrics recovery_metrics;
    store = opened(options, recovery_metrics);
    // 元数据提交事务已完成（提交点）：child 批 1 的 260 帧 + parent 1 条全部持久，
    // 无采纳尾部（无采纳 = 计数键不出现，而不是冒报 0）。
    CHECK_FALSE(recovery_metrics.counters.count("recovered_tail_records"));
    CHECK_FALSE(recovery_metrics.counters.count("recovered_tail_bytes"));
    CHECK(exported(*store).size() == 261);
    REQUIRE(store->write({sample(999999)}, error));
    REQUIRE(store->finish(error));
    CHECK(exported(*store).size() == 262);
}

TEST_CASE("bench_store: compact fixed32 digest collisions compare the full payload in every reuse/model mode") {
    for (const bool reuse : {true, false}) for (const std::string model : {"light", "heavy"}) {
        CAPTURE(reuse); CAPTURE(model);
        StoreDirectory directory;
        auto options = options_for("sqlite_compact", directory.path);
        options.reuse_statement = reuse;
        options.transaction_model = model;
        options.test_digest = [](std::string_view, std::string& digest) { digest.assign(32, '\0'); return true; };
        Metrics metrics;
        auto store = opened(options, metrics);
        auto altered = generate_record(0, GeneratorOptions{54, 5, 42, true});
        ++altered.values[0];
        auto a = sample(0), b = encoded(altered), c = sample(1);
        // Override deliberately takes priority over otherwise trusted caches.
        a.has_sha256 = true; a.sha256.fill(0x71);
        b.has_sha256 = true; b.sha256.fill(0x72);
        std::string error;
        REQUIRE_MESSAGE(store->write({a, b, a, c, b}, error), error);
        REQUIRE_MESSAGE(store->write({c, a, b, a}, error), error);
        CHECK(hexes(exported(*store)) == expected_hexes({a, b, c}));
        CHECK(hexes(ranged(*store, a.symbol, a.time, a.time)) == expected_hexes({a, b}));
        CHECK(metrics.counters.at("inserted") == 3);
        CHECK(metrics.counters.at("ignored") == 6);
        CHECK(metrics.counters.at("store_digest_override_calls") == 9);
        CHECK(metrics.stages.at("store_digest_compute").count == 9);
        const auto db = directory.path / "compact-record.sqlite3";
        CHECK(sql_int(db, "PRAGMA user_version") == 2);
        CHECK(sql_int(db, "SELECT count(*) FROM records") == 3);
        CHECK(sql_int(db, "SELECT count(DISTINCT digest) FROM records") == 1);
        CHECK(sql_int(db, "SELECT count(*) FROM records WHERE typeof(digest)='blob' AND length(digest)=32") == 3);
        CHECK(sql_int(db, "SELECT count(*) FROM pragma_index_list('records') WHERE \"unique\"=1") == 0);
        CHECK(sql_int(db, "SELECT count(*) FROM pragma_index_xinfo('records_symbol_time') WHERE cid=-1") == 1);
        CHECK(sql_int(db, "SELECT count(*) FROM pragma_index_xinfo('records_digest') WHERE cid=-1") == 1);
        REQUIRE(store->finish(error));
        store.reset();
        Metrics replay;
        store = opened(options, replay);
        REQUIRE(store->write({a, b, c, a}, error));
        CHECK(replay.counters.at("inserted") == 0);
        CHECK(replay.counters.at("ignored") == 4);
        CHECK(hexes(exported(*store)) == expected_hexes({a, b, c}));
    }
}

TEST_CASE("bench_store: compact trusts upstream SHA256 cache and separately counts missing-cache hashing") {
    StoreDirectory directory;
    Metrics metrics;
    auto store = opened(options_for("sqlite_compact", directory.path), metrics);
    auto cached = sample(0);
    unsigned size = 0;
    REQUIRE(EVP_Digest(cached.bytes.data(), cached.bytes.size(), cached.sha256.data(), &size, EVP_sha256(), nullptr) == 1);
    REQUIRE(size == cached.sha256.size());
    cached.has_sha256 = true;
    cached.ingress_ns = 1234;
    auto uncached = cached;
    uncached.has_sha256 = false;
    uncached.sha256.fill(0xff); // An invalid cache is never consumed.
    uncached.ingress_ns = 5678;
    std::string error;
    REQUIRE_MESSAGE(store->write({cached, uncached, cached}, error), error);
    CHECK(metrics.counters.at("store_digest_cache_hits") == 2);
    CHECK(metrics.counters.at("store_digest_computed") == 1);
    CHECK(metrics.counters.at("store_digest_bytes") == cached.bytes.size());
    CHECK(metrics.stages.at("store_digest_compute").count == 1);
    CHECK(metrics.counters.at("inserted") == 1);
    CHECK(hexes(exported(*store)) == expected_hexes({cached}));
    Json info(store->info_json());
    CHECK(info.value["physical_rows"].get_uint64().value() == 1);
}

TEST_CASE("bench_store: compact mid-batch SQL or digest failures roll back rows and heavy metadata") {
    for (const bool reuse : {true, false}) for (int injection = 0; injection < 3; ++injection) {
        CAPTURE(reuse); CAPTURE(injection);
        StoreDirectory directory;
        auto options = options_for("sqlite_compact", directory.path);
        options.reuse_statement = reuse;
        options.transaction_model = "heavy";
        if (injection != 0) options.test_digest = injection == 1 ?
            +[](std::string_view bytes, std::string& digest) {
                digest.assign(bytes.substr(6, 9) == "600001.SH" ? 31 : 32, '\0'); return true;
            } :
            +[](std::string_view bytes, std::string& digest) {
                digest.assign(32, '\0'); return bytes.substr(6, 9) != "600001.SH";
            };
        Metrics metrics;
        auto store = opened(options, metrics);
        const auto db = directory.path / "compact-record.sqlite3";
        if (injection == 0) execute_sql(db, "CREATE TRIGGER reject_fixture BEFORE INSERT ON records "
            "WHEN NEW.symbol='600001.SH' BEGIN SELECT RAISE(ABORT,'fixture failure'); END");
        std::string error;
        CHECK_FALSE(store->write({sample(0), sample(1)}, error));
        CHECK_FALSE(error.empty());
        CHECK(sql_int(db, "SELECT count(*) FROM records") == 0);
        CHECK(sql_int(db, "SELECT count(*) FROM write_batches") == 0);
        CHECK(metrics.counters.at("transactions") == 0);
        CHECK(exported(*store).empty());
        if (injection == 0) execute_sql(db, "DROP TRIGGER reject_fixture");
        REQUIRE_MESSAGE(store->write({sample(0), sample(2)}, error), error);
        CHECK(sql_int(db, "SELECT count(*) FROM write_batches") == 1);
        CHECK(hexes(exported(*store)) == expected_hexes({sample(0), sample(2)}));
    }
}

TEST_CASE("bench_store: compact version rejection never changes rows or the SQLite baseline") {
    for (const int version : {1, 3}) {
        StoreDirectory directory;
        auto options = options_for("sqlite_compact", directory.path);
        Metrics metrics;
        auto store = opened(options, metrics);
        std::string error;
        REQUIRE(store->write({sample(0)}, error));
        store.reset();
        const auto db = directory.path / "compact-record.sqlite3";
        const auto pragma = "PRAGMA user_version=" + std::to_string(version);
        execute_sql(db, pragma.c_str());
        Metrics recovery;
        CHECK(open_store(options, recovery, error) == nullptr);
        CHECK(error.find("schema version") != std::string::npos);
        CHECK(sql_int(db, "PRAGMA user_version") == version);
        CHECK(sql_int(db, "SELECT count(*) FROM records") == 1);
        auto baseline = opened(options_for("sqlite", directory.path), recovery);
        CHECK(exported(*baseline).empty());
        CHECK(fs::exists(directory.path / "full-record.sqlite3"));
    }
}

TEST_CASE("bench_store: compact BEGIN IMMEDIATE serializes competing writers without duplicate physical payloads") {
    StoreDirectory directory;
    auto options = options_for("sqlite_compact", directory.path);
    options.test_digest = [](std::string_view, std::string& digest) { digest.assign(32, '\0'); return true; };
    Metrics first_metrics, second_metrics;
    auto first = opened(options, first_metrics), second = opened(options, second_metrics);
    const std::vector<EncodedRecord> batch{sample(0), sample(1), sample(0), sample(2)};
    bool ok1 = true, ok2 = true;
    std::string error1, error2;
    const auto write = [&](Store& store, bool& ok, std::string& error) {
        for (int pass = 0; pass < 8; ++pass) if (!store.write(batch, error)) { ok = false; break; }
    };
    std::thread a([&] { write(*first, ok1, error1); });
    std::thread b([&] { write(*second, ok2, error2); });
    a.join(); b.join();
    REQUIRE_MESSAGE(ok1, error1); REQUIRE_MESSAGE(ok2, error2);
    CHECK(first_metrics.counters.at("inserted") + second_metrics.counters.at("inserted") == 3);
    CHECK(first_metrics.counters.at("ignored") + second_metrics.counters.at("ignored") == 61);
    CHECK(sql_int(directory.path / "compact-record.sqlite3", "SELECT count(*) FROM records") == 3);
    CHECK(hexes(exported(*first)) == expected_hexes({sample(0), sample(1), sample(2)}));
}

TEST_CASE("bench_store: compact rejects unstamped existing schemas and a unique digest index") {
    for (int damage = 0; damage < 2; ++damage) {
        StoreDirectory directory;
        const auto options = options_for("sqlite_compact", directory.path);
        const auto db = directory.path / "compact-record.sqlite3";
        Metrics metrics;
        if (damage == 0) execute_sql(db, "CREATE TABLE foreign_records(payload BLOB PRIMARY KEY)");
        else {
            auto store = opened(options, metrics);
            store.reset();
            execute_sql(db, "DROP INDEX records_digest; CREATE UNIQUE INDEX records_digest ON records(digest)");
        }
        std::string error;
        CHECK(open_store(options, metrics, error) == nullptr);
        CHECK(error.find("schema") != std::string::npos);
        CHECK(sql_int(db, "PRAGMA user_version") == (damage == 0 ? 0 : 2));
    }
}

TEST_CASE("bench_store: both SQLite layouts expose explicit NORMAL FULL and fullfsync settings") {
    for (const std::string backend : {"sqlite", "sqlite_compact"}) for (const std::string sync : {"FULL", "NORMAL"})
    for (const std::string fullfsync : {"default", "on", "off"}) {
        StoreDirectory directory;
        auto options = options_for(backend, directory.path);
        options.synchronous = sync;
        options.fullfsync = fullfsync;
        options.transaction_model = "heavy";
        options.autocheckpoint = 0;
        Metrics metrics;
        auto store = opened(options, metrics);
        std::string error;
        REQUIRE(store->write({sample(0), sample(0), sample(1)}, error));
        REQUIRE(store->finish(error));
        Json info(store->info_json());
        CHECK(info.value["durable"].get_bool().value() == (sync == "FULL"));
        CHECK(info.value["normal_diagnostic"].get_bool().value() == (sync == "NORMAL"));
        CHECK(info.value["sqlite"]["pragmas"]["synchronous"].get_uint64().value() == (sync == "FULL" ? 2 : 1));
        if (fullfsync != "default") CHECK(info.value["sqlite"]["pragmas"]["fullfsync"].get_uint64().value() == (fullfsync == "on" ? 1 : 0));
        CHECK(info.value["sqlite"]["last_checkpoint"]["called"].get_bool().value());
        CHECK(metrics.counters.at("metadata_sql_rows") == 1);
        CHECK(hexes(exported(*store)) == expected_hexes({sample(0), sample(1)}));
        options.fullfsync = "invalid";
        CHECK(open_store(options, metrics, error) == nullptr);
    }
}

TEST_CASE("bench_store: missing SQLite segment index or sparse schema is fatal before tail truncation") {
    for (int damage = 0; damage < 3; ++damage) {
        StoreDirectory directory;
        const auto options = options_for("segment", directory.path);
        Metrics metrics;
        auto store = opened(options, metrics);
        std::string error;
        REQUIRE(store->write({sample(0)}, error));
        store.reset();
        const auto data = directory.path / "records.segments";
        const auto db = directory.path / "segment-index.sqlite3";
        append_file(data, "must not remove this tail when committed index is missing");
        const auto before = file_contents(data);
        if (damage == 0) fs::rename(db, directory.path / "owned-index-backup.sqlite3");
        if (damage == 1) execute_sql(db, "DROP TABLE segment_symbols");
        if (damage == 2) execute_sql(db, "DROP INDEX segment_symbols_lookup");
        CHECK(open_store(options, metrics, error) == nullptr);
        CHECK(error.find("missing") != std::string::npos);
        CHECK(file_contents(data) == before);
    }
}

#ifdef TICKSHELF_HAVE_ROCKSDB
namespace {
using RocksRows = std::vector<std::pair<std::string, std::string>>;
std::unique_ptr<rocksdb::DB> fixture_rocks(const StoreOptions& options) {
    rocksdb::Options rocks_options;
    rocks_options.create_if_missing = false;
    rocks_options.use_fsync = true;
    std::unique_ptr<rocksdb::DB> db;
    const auto status = rocksdb::DB::Open(rocks_options, (segment_directory(options) / "index.rocksdb").string(), &db);
    REQUIRE_MESSAGE(status.ok(), status.ToString());
    return db;
}
RocksRows rocks_rows(rocksdb::DB& db) {
    RocksRows result;
    std::unique_ptr<rocksdb::Iterator> it(db.NewIterator(rocksdb::ReadOptions{}));
    for (it->SeekToFirst(); it->Valid(); it->Next()) result.emplace_back(it->key().ToString(), it->value().ToString());
    REQUIRE(it->status().ok());
    return result;
}
std::uint64_t fixture_be(std::string_view bytes) {
    std::uint64_t value = 0;
    for (unsigned char c : bytes) value = (value << 8) | c;
    return value;
}
std::string fixture_be(std::uint64_t value) {
    std::string bytes(8, '\0');
    for (unsigned i = 0; i < 8; ++i) bytes[i] = static_cast<char>(value >> (56 - 8 * i));
    return bytes;
}
}

TEST_CASE("bench_store: RocksDB reports actual KV batch costs and finishes all persistent work before export") {
    for (const std::string model : {"light", "heavy"}) {
        StoreDirectory directory;
        auto options = options_for("segment_rocksdb", directory.path);
        options.transaction_model = model;
        Metrics metrics;
        auto store = opened(options, metrics);
        std::string error;
        REQUIRE(store->write({sample(0), sample(1), sample(0)}, error));
        REQUIRE(store->write({sample(54), sample(2)}, error));
        CHECK(metrics.counters.at("rocksdb_write_batches") == 2);
        CHECK(metrics.counters.at("rocksdb_put_ops") == (model == "heavy" ? 12 : 8)); // S/H + 2 ranges, plus M/C.
        CHECK(metrics.counters.at("metadata_kv_rows") == (model == "heavy" ? 2 : 0));
        CHECK(metrics.counters.at("sqlite_step_calls") == 0);
        CHECK(metrics.counters.at("sqlite_prepare_calls") == 0);
        CHECK(metrics.counters.at("rocksdb_key_bytes") > 0);
        CHECK(metrics.counters.at("rocksdb_value_bytes") > 0);
        CHECK(metrics.counters.at("rocksdb_writebatch_bytes") > metrics.counters.at("rocksdb_key_bytes") + metrics.counters.at("rocksdb_value_bytes"));
        REQUIRE_MESSAGE(store->finish(error), error);
        REQUIRE(store->finish(error));
        CHECK(metrics.stages.at("rocksdb_flush").count == 1);
        CHECK(metrics.stages.at("rocksdb_wait_compact").count == 1);
        CHECK(metrics.stages.at("rocksdb_sync_wal").count == 1);
        CHECK(metrics.stages.at("rocksdb_close").count == 1);
        Json info(store->info_json());
        CHECK(info.value["rocksdb"]["finish_called"].get_bool().value());
        CHECK(info.value["rocksdb"]["write_options"]["sync"].get_bool().value());
        CHECK_FALSE(info.value["rocksdb"]["write_options"]["disableWAL"].get_bool().value());
        CHECK(info.value["rocksdb"]["write_options"]["use_fsync"].get_bool().value());
        CHECK(info.value["rocksdb"]["finish_background_state"]["rocksdb.num-running-flushes"].get_uint64().value() == 0);
        CHECK(info.value["rocksdb"]["finish_background_state"]["rocksdb.num-running-compactions"].get_uint64().value() == 0);
        const auto writes = metrics.counters.at("rocksdb_write_batches");
        CHECK(hexes(exported(*store)) == expected_hexes({sample(0), sample(1), sample(0), sample(54), sample(2)}));
        CHECK(metrics.counters.at("rocksdb_write_batches") == writes);
        CHECK_FALSE(store->write({sample(3)}, error));
        CHECK(error.find("finished") != std::string::npos);
        const auto end = fs::file_size(segment_directory(options) / "records.segments");
        store.reset();
        auto db = fixture_rocks(options);
        const auto rows = rocks_rows(*db);
        std::size_t segments = 0, ranges = 0, metadata = 0, crc = 0;
        for (const auto& [key, value] : rows) {
            if (key == "H") CHECK(fixture_be(value) == end);
            if (key[0] == 'S') ++segments;
            if (key[0] == 'R') ++ranges;
            if (key[0] == 'M') ++metadata;
            if (key[0] == 'C') ++crc;
        }
        CHECK(segments == 2); CHECK(ranges == 4);
        CHECK(metadata == (model == "heavy" ? 2 : 0));
        CHECK(crc == metadata);
        db.reset();
        Metrics replay;
        store = opened(options, replay);
        CHECK(hexes(ranged(*store, sample(0).symbol, 0, std::numeric_limits<std::uint64_t>::max())) == expected_hexes({sample(0), sample(54)}));
        REQUIRE(store->write({sample(3)}, error));
        // The old SQLite-index format remains physically and logically independent.
        Metrics sql_metrics;
        auto sql = opened(options_for("segment", directory.path), sql_metrics);
        CHECK(exported(*sql).empty());
    }
}

TEST_CASE("bench_store: RocksDB strict recovery rejects altered missing extra and orphan index keys before truncating") {
    for (int damage = 0; damage < 13; ++damage) {
        CAPTURE(damage);
        StoreDirectory directory;
        auto options = options_for("segment_rocksdb", directory.path);
        options.transaction_model = "heavy";
        Metrics metrics;
        auto store = opened(options, metrics);
        std::string error;
        REQUIRE(store->write({sample(0), sample(1)}, error));
        REQUIRE(store->write({sample(2)}, error));
        store.reset();
        const auto data = segment_directory(options) / "records.segments";
        append_file(data, "tail must remain byte-identical when committed index validation fails");
        const auto before = file_contents(data);
        auto db = fixture_rocks(options);
        const auto rows = rocks_rows(*db);
        const auto find = [&](char prefix) {
            for (const auto& row : rows) if (row.first[0] == prefix) return row;
            throw std::runtime_error("fixture prefix missing");
        };
        rocksdb::WriteBatch batch;
        auto range = find('R');
        auto segment = find('S');
        if (damage == 0) REQUIRE(batch.Delete(range.first).ok());
        if (damage == 1) { range.second.assign(8, '\xff'); REQUIRE(batch.Put(range.first, range.second).ok()); }
        if (damage == 2) { REQUIRE(batch.Delete(range.first).ok()); range.first[3] ^= 1; REQUIRE(batch.Put(range.first, range.second).ok()); }
        if (damage == 3) { range.first.replace(range.first.size() - 8, 8, fixture_be(999999999)); REQUIRE(batch.Put(range.first, range.second).ok()); }
        if (damage == 4) REQUIRE(batch.Put("H", fixture_be(0)).ok());
        if (damage == 5) REQUIRE(batch.Delete("H").ok());
        if (damage == 6) REQUIRE(batch.Delete(segment.first).ok());
        if (damage == 7) REQUIRE(batch.Put("F", "wrong-format-version").ok());
        if (damage == 8) REQUIRE(batch.Put("UNKNOWN", "bad").ok());
        if (damage == 9) REQUIRE(batch.Delete(find('C').first).ok());
        if (damage == 10) { auto row = find('M'); row.second.back() ^= 1; REQUIRE(batch.Put(row.first, row.second).ok()); }
        if (damage == 11) { segment.second[7] ^= 1; REQUIRE(batch.Put(segment.first, segment.second).ok()); }
        if (damage == 12) { REQUIRE(batch.Delete(range.first).ok()); range.first[range.first.size() - 9] ^= 1; REQUIRE(batch.Put(range.first, range.second).ok()); }
        rocksdb::WriteOptions write; write.sync = true;
        REQUIRE(db->Write(write, &batch).ok());
        db.reset();
        CHECK(open_store(options, metrics, error) == nullptr);
        CHECK_FALSE(error.empty());
        CHECK(file_contents(data) == before);
    }
}

TEST_CASE("bench_store: missing RocksDB directory or CURRENT or format stamp never converts committed data to an orphan") {
    for (int damage = 0; damage < 3; ++damage) {
        StoreDirectory directory;
        const auto options = options_for("segment_rocksdb", directory.path);
        Metrics metrics;
        auto store = opened(options, metrics);
        std::string error;
        REQUIRE(store->write({sample(0)}, error));
        store.reset();
        const auto base = segment_directory(options);
        const auto data = base / "records.segments";
        append_file(data, "must not truncate");
        const auto before = file_contents(data);
        if (damage == 0) fs::rename(base / "index.rocksdb", base / "owned-index-backup");
        if (damage == 1) fs::rename(base / "index.rocksdb" / "CURRENT", base / "owned-current-backup");
        if (damage == 2) {
            auto db = fixture_rocks(options);
            REQUIRE(db->Delete(rocksdb::WriteOptions{}, "F").ok());
        }
        CHECK(open_store(options, metrics, error) == nullptr);
        CHECK_FALSE(error.empty());
        CHECK(file_contents(data) == before);
    }
}

TEST_CASE("bench_store: RocksDB heavy SIGKILL batches commit S R H M C atomically") {
    for (const std::string failpoint : {"after_append", "after_sync", "after_index"}) {
        CAPTURE(failpoint);
        StoreDirectory directory;
        auto options = options_for("segment_rocksdb", directory.path);
        options.transaction_model = "heavy";
        Metrics metrics;
        auto store = opened(options, metrics);
        std::string error;
        REQUIRE(store->write({sample(0)}, error));
        store.reset();
        const auto data = segment_directory(options) / "records.segments";
        const auto original_end = fs::file_size(data);
        const auto child = ::fork();
        REQUIRE(child >= 0);
        if (child == 0) {
            options.failpoint = failpoint;
            Metrics child_metrics;
            auto child_store = open_store(options, child_metrics, error);
            if (!child_store) ::_exit(70);
            child_store->write({sample(1), sample(2)}, error);
            ::_exit(71);
        }
        int status = 0;
        pid_t waited;
        do { waited = ::waitpid(child, &status, 0); } while (waited < 0 && errno == EINTR);
        REQUIRE(waited == child); REQUIRE(WIFSIGNALED(status)); REQUIRE(WTERMSIG(status) == SIGKILL);
        auto db = fixture_rocks(options);
        const auto rows = rocks_rows(*db);
        std::size_t segments = 0, ranges = 0, metadata = 0, crc = 0;
        std::uint64_t high = 0;
        for (const auto& [key, value] : rows) {
            if (key == "H") high = fixture_be(value);
            if (key[0] == 'S') ++segments;
            if (key[0] == 'R') ++ranges;
            if (key[0] == 'M') ++metadata;
            if (key[0] == 'C') ++crc;
        }
        const bool committed = failpoint == "after_index";
        CHECK(segments == (committed ? 2 : 1));
        CHECK(ranges == (committed ? 3 : 1));
        CHECK(metadata == segments); CHECK(crc == segments);
        CHECK(high == (committed ? fs::file_size(data) : original_end));
        db.reset();
        store = opened(options, metrics);
        CHECK(exported(*store).size() == (committed ? 3 : 1));
        CHECK(fs::file_size(data) == high);
    }
}
#else
TEST_CASE("bench_store: disabled RocksDB fails explicitly without creating a fallback directory") {
    StoreDirectory directory;
    auto options = options_for("segment_rocksdb", directory.path / "not-created");
    Metrics metrics;
    std::string error;
    CHECK(open_store(options, metrics, error) == nullptr);
    CHECK(error.find("unsupported") != std::string::npos);
    CHECK(error.find("TICKSHELF_HAVE_ROCKSDB") != std::string::npos);
    CHECK_FALSE(fs::exists(options.directory));
}
#endif

TEST_CASE("bench_store: exporting to a failed stream reports failure for every backend") {
    for (const std::string& backend : storage_backends(true)) {
        StoreDirectory directory;
        Metrics metrics;
        auto store = opened(options_for(backend, directory.path), metrics);
        std::string error;
        REQUIRE(store->write({sample(0)}, error));
        std::ostringstream out;
        out.setstate(std::ios::badbit);
        CHECK_FALSE(store->export_rows(out, error));
        CHECK_FALSE(error.empty());
        CHECK_FALSE(store->export_range(sample(0).symbol, 0, std::numeric_limits<std::uint64_t>::max(), out, error));
        CHECK_FALSE(error.empty());
    }
}
