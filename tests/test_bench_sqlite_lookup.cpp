#include <doctest/doctest.h>
#include <simdjson.h>
#include <sqlite3.h>
#include "../bench/store.hpp"

#include <filesystem>
#include <limits>
#include <set>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unistd.h>

namespace {
using namespace tickshelf::bench;
namespace fs = std::filesystem;
struct Directory {
    fs::path path;
    Directory() {
        const auto pattern = (fs::temp_directory_path() / "tickshelf_sqlite_lookup_XXXXXX").string();
        std::vector<char> name(pattern.begin(), pattern.end());
        name.push_back('\0');
        const auto* created = ::mkdtemp(name.data());
        if (!created) throw std::runtime_error("mkdtemp failed");
        path = created;
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
StoreOptions options_for(const fs::path& path) {
    StoreOptions options;
    options.backend = "sqlite_compact";
    options.directory = path.string();
    options.sqlite_compact_lookup = "separate";
    return options;
}
EncodedRecord row(const std::string& symbol = "A", std::uint64_t time = 7, std::int64_t value = 1) {
    Record record;
    record.symbol = symbol;
    record.time = time;
    record.present = 1;
    record.values[0] = value;
    return {symbol, time, encode_record(record)};
}
std::string hex(std::string_view value) {
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
    std::ostringstream out;
    std::string error = "stale";
    REQUIRE_MESSAGE((range ? store.export_range(symbol, from, to, out, error) : store.export_rows(out, error)), error);
    CHECK(error.empty());
    std::set<std::string> result;
    std::istringstream in(out.str());
    std::string line;
    while (std::getline(in, line)) {
        Json json(line);
        CHECK(result.emplace(std::string(json.value["hex"].get_string().value())).second);
    }
    return result;
}
void sql(const fs::path& path, const std::string& text) {
    sqlite3* db = nullptr;
    REQUIRE(sqlite3_open(path.c_str(), &db) == SQLITE_OK);
    const int rc = sqlite3_exec(db, text.c_str(), nullptr, nullptr, nullptr);
    const std::string error = sqlite3_errmsg(db);
    sqlite3_close(db);
    REQUIRE_MESSAGE(rc == SQLITE_OK, error);
}
std::int64_t scalar(const fs::path& path, const char* text) {
    sqlite3* db = nullptr;
    REQUIRE(sqlite3_open_v2(path.c_str(), &db, SQLITE_OPEN_READONLY, nullptr) == SQLITE_OK);
    sqlite3_stmt* statement = nullptr;
    const int prepared = sqlite3_prepare_v2(db, text, -1, &statement, nullptr);
    const int rc = prepared == SQLITE_OK ? sqlite3_step(statement) : prepared;
    const auto value = rc == SQLITE_ROW ? sqlite3_column_int64(statement, 0) : 0;
    const std::string error = sqlite3_errmsg(db);
    sqlite3_finalize(statement);
    sqlite3_close(db);
    REQUIRE_MESSAGE(rc == SQLITE_ROW, error);
    return value;
}
}

TEST_CASE("bench_sqlite_lookup: default is old subquery and invalid/noncompact selections fail before creating storage") {
    Directory directory;
    StoreOptions options;
    CHECK(options.sqlite_compact_lookup == "subquery");
    options.backend = "sqlite_compact";
    options.directory = (directory.path / "default").string();
    Metrics metrics;
    auto store = opened(options, metrics);
    std::string error;
    REQUIRE(store->write({row(), row(), row("B")}, error));
    CHECK(metrics.counters.at("sqlite_prepare_calls") == 1);
    CHECK(metrics.counters.at("sqlite_step_calls") == 3);
    CHECK(metrics.counters.at("sqlite_bind_calls") == 12);
    CHECK(metrics.counters.at("sqlite_reset_calls") == 3);
    for (const char* name : {"sqlite_lookup_prepare_calls", "sqlite_lookup_step_calls", "sqlite_lookup_bind_calls", "sqlite_lookup_reset_calls"})
        CHECK(metrics.counters.at(name) == 0);
    CHECK(metrics.stages.count("sqlite_lookup") == 0);
    Json info(store->info_json());
    CHECK(info.value["sqlite_compact_lookup"].get_string().value() == "subquery");
    CHECK(info.value["compact_lookup_algorithm"].get_string().value().find("original INSERT SELECT") != std::string_view::npos);
    options.directory = (directory.path / "not-created").string();
    options.sqlite_compact_lookup = "invalid";
    CHECK(open_store(options, metrics, error) == nullptr);
    CHECK(error.find("sqlite_compact_lookup") != std::string::npos);
    CHECK_FALSE(fs::exists(options.directory));
    options.sqlite_compact_lookup = "separate";
    for (const std::string backend : {"sqlite", "null", "segment", "segment_rocksdb", "rocksdb"}) {
        options.backend = backend;
        CHECK(open_store(options, metrics, error) == nullptr);
        CHECK(error.find("requires sqlite_compact") != std::string::npos);
        CHECK_FALSE(fs::exists(options.directory));
    }
}

TEST_CASE("bench_sqlite_lookup: separate exact collision dedup handles pending rows same-time content binary symbols and replay") {
    for (const bool reuse : {true, false}) for (const std::string model : {"light", "heavy"}) {
        CAPTURE(reuse); CAPTURE(model);
        Directory directory;
        auto options = options_for(directory.path);
        options.reuse_statement = reuse;
        options.transaction_model = model;
        options.test_digest = [](std::string_view, std::string& digest) { digest.assign(32, '\0'); return true; };
        Metrics metrics;
        auto store = opened(options, metrics);
        auto a = row(), b = row("A", 7, 2);
        const auto c = row(std::string("B\0\xc3\xbf", 4), std::numeric_limits<std::uint64_t>::max());
        a.has_sha256 = true; a.sha256.fill(0x77);
        b.has_sha256 = true; b.sha256.fill(0x88); // Test override still has priority.
        std::string error;
        REQUIRE_MESSAGE(store->write({a, b, a, c, b}, error), error);
        REQUIRE_MESSAGE(store->write({c, a, b, a}, error), error);
        CHECK(metrics.counters.at("inserted") == 3);
        CHECK(metrics.counters.at("ignored") == 6);
        CHECK(metrics.counters.at("attempted") == 9);
        CHECK(metrics.counters.at("store_digest_override_calls") == 9);
        CHECK(metrics.counters.at("sqlite_lookup_rows") == 9);
        CHECK(metrics.counters.at("sqlite_lookup_matches") == 6);
        CHECK(metrics.counters.at("sqlite_lookup_bind_calls") == 9);
        CHECK(metrics.counters.at("sqlite_lookup_reset_calls") == 9);
        CHECK(metrics.counters.at("sqlite_lookup_prepare_calls") == (reuse ? 1 : 2));
        CHECK(metrics.counters.at("sqlite_prepare_calls") == (reuse ? 2 : 4) + (model == "heavy" ? (reuse ? 1 : 2) : 0));
        CHECK(exported(*store) == std::set<std::string>{hex(a.bytes), hex(b.bytes), hex(c.bytes)});
        CHECK(exported(*store, true, "A", 7, 7) == std::set<std::string>{hex(a.bytes), hex(b.bytes)});
        CHECK(exported(*store, true, c.symbol, c.time, c.time) == std::set<std::string>{hex(c.bytes)});
        const auto db = directory.path / "compact-record.sqlite3";
        CHECK(scalar(db, "SELECT count(*) FROM records") == 3);
        CHECK(scalar(db, "SELECT count(DISTINCT digest) FROM records") == 1);
        CHECK(scalar(db, "SELECT count(*) FROM pragma_index_list('records') WHERE \"unique\"<>0") == 0);
        if (model == "heavy") {
            CHECK(scalar(db, "SELECT count(*) FROM write_batches") == 2);
            CHECK(scalar(db, "SELECT sum(attempted) FROM write_batches") == 9);
            CHECK(scalar(db, "SELECT sum(inserted) FROM write_batches") == 3);
            CHECK(scalar(db, "SELECT sum(ignored) FROM write_batches") == 6);
        }
        REQUIRE(store->finish(error));
        store.reset();
        Metrics replay;
        store = opened(options, replay);
        REQUIRE_MESSAGE(store->write({a, b, c, a}, error), error);
        CHECK(replay.counters.at("inserted") == 0);
        CHECK(replay.counters.at("ignored") == 4);
        CHECK(exported(*store).size() == 3);
    }
}

TEST_CASE("bench_sqlite_lookup: API call timings distinguish lookup subset from insert metadata and aggregate calls") {
    for (const bool reuse : {true, false}) for (const std::string model : {"light", "heavy"}) {
        CAPTURE(reuse); CAPTURE(model);
        Directory directory;
        auto options = options_for(directory.path);
        options.reuse_statement = reuse;
        options.transaction_model = model;
        const std::uint64_t heavy = model == "heavy";
        Metrics metrics;
        auto store = opened(options, metrics);
        CHECK(metrics.counters.at("sqlite_prepare_calls") == (reuse ? 2 + heavy : 0));
        CHECK(metrics.counters.at("sqlite_lookup_prepare_calls") == (reuse ? 1 : 0));
        std::string error;
        REQUIRE(store->write({row(), row()}, error));
        REQUIRE(store->write({row()}, error));
        CHECK(metrics.counters.at("sqlite_prepare_calls") == (reuse ? 2 + heavy : 4 + 2 * heavy));
        CHECK(metrics.counters.at("sqlite_lookup_prepare_calls") == (reuse ? 1 : 2));
        CHECK(metrics.counters.at("sqlite_lookup_step_calls") == 3); // DONE, ROW, ROW.
        CHECK(metrics.counters.at("sqlite_lookup_bind_calls") == 3);
        CHECK(metrics.counters.at("sqlite_lookup_reset_calls") == 3);
        CHECK(metrics.counters.at("sqlite_lookup_clear_bindings_calls") == 3);
        CHECK(metrics.counters.at("sqlite_lookup_rows") == 3);
        CHECK(metrics.counters.at("sqlite_lookup_candidate_rows") == 2);
        CHECK(metrics.counters.at("sqlite_lookup_candidate_bytes") == 2 * row().bytes.size());
        CHECK(metrics.counters.at("sqlite_lookup_matches") == 2);
        CHECK(metrics.counters.at("sqlite_step_calls") == 4 + 2 * heavy);
        CHECK(metrics.counters.at("sqlite_bind_calls") == 7 + 8 * heavy);
        CHECK(metrics.counters.at("sqlite_bind_blob_calls") == 6);
        CHECK(metrics.counters.at("sqlite_bind_text_calls") == 1);
        CHECK(metrics.counters.at("sqlite_bind_int64_calls") == 8 * heavy);
        CHECK(metrics.counters.at("sqlite_reset_calls") == 4 + 2 * heavy);
        CHECK(metrics.counters.at("sqlite_clear_bindings_calls") == 4 + 2 * heavy);
        CHECK(metrics.counters.at("sqlite_changes_calls") == 1);
        CHECK(metrics.counters.at("sqlite_begin_calls") == 2);
        CHECK(metrics.counters.at("sqlite_commit_calls") == 2);
        CHECK(metrics.counters.at("sqlite_exec_calls") == 4);
        CHECK(metrics.counters.at("sqlite_step_calls") == metrics.stages.at("sqlite_step").count);
        CHECK(metrics.counters.at("sqlite_prepare_calls") == metrics.stages.at("sqlite_prepare").count);
        for (const std::string name : {"prepare", "step", "bind", "reset", "clear_bindings"})
            CHECK(metrics.counters.at("sqlite_lookup_" + name + "_calls") == metrics.stages.at("sqlite_lookup_" + name).count);
        const auto before = metrics.counters;
        CHECK(exported(*store).size() == 1);
        Json info(store->info_json());
        CHECK(info.value["sqlite_compact_lookup"].get_string().value() == "separate");
        CHECK(info.value["compact_lookup_algorithm"].get_string().value().find("reset/clear lookup before") != std::string_view::npos);
        CHECK(info.value["lookup_metric_scope"].get_string().value().find("not syscalls") != std::string_view::npos);
        for (const auto& [name, count] : before)
            if (name.starts_with("sqlite_")) CHECK(metrics.counters.at(name) == count);
    }
}

TEST_CASE("bench_sqlite_lookup: midbatch insert metadata and digest errors roll back both statements and can retry") {
    for (const bool reuse : {true, false}) for (const int failure : {0, 1, 2}) {
        CAPTURE(reuse); CAPTURE(failure);
        Directory directory;
        auto options = options_for(directory.path);
        options.reuse_statement = reuse;
        options.transaction_model = "heavy";
        options.test_digest = failure == 2 ?
            +[](std::string_view bytes, std::string& digest) { digest.assign(bytes.substr(6, 1) == "B" ? 31 : 32, '\0'); return true; } :
            +[](std::string_view, std::string& digest) { digest.assign(32, '\0'); return true; };
        Metrics metrics;
        auto store = opened(options, metrics);
        const auto db = directory.path / "compact-record.sqlite3";
        if (failure == 0)
            sql(db, "CREATE TRIGGER reject_lookup_record BEFORE INSERT ON records WHEN NEW.symbol='B' "
                    "BEGIN SELECT RAISE(ABORT,'fixture record failure'); END");
        if (failure == 1)
            sql(db, "CREATE TRIGGER reject_lookup_metadata BEFORE INSERT ON write_batches "
                    "BEGIN SELECT RAISE(ABORT,'fixture metadata failure'); END");
        std::string error;
        CHECK_FALSE(store->write({row(), row("B")}, error));
        CHECK_FALSE(error.empty());
        CHECK(metrics.counters.at("sqlite_rollback_calls") == 1);
        CHECK(metrics.counters.at("transactions") == 0);
        CHECK(metrics.counters.at("inserted") == 0);
        CHECK(exported(*store).empty());
        CHECK(scalar(db, "SELECT count(*) FROM records") == 0);
        CHECK(scalar(db, "SELECT count(*) FROM write_batches") == 0);
        if (failure == 0) sql(db, "DROP TRIGGER reject_lookup_record");
        if (failure == 1) sql(db, "DROP TRIGGER reject_lookup_metadata");
        const auto second = row(failure == 2 ? "C" : "B");
        REQUIRE_MESSAGE(store->write({row(), second, row()}, error), error);
        CHECK(metrics.counters.at("inserted") == 2);
        CHECK(metrics.counters.at("ignored") == 1);
        CHECK(metrics.counters.at("sqlite_lookup_prepare_calls") == (reuse ? 1 : 2));
        CHECK(exported(*store) == std::set<std::string>{hex(row().bytes), hex(second.bytes)});
        CHECK(scalar(db, "SELECT count(*) FROM write_batches") == 1);
        CHECK(scalar(db, "SELECT attempted FROM write_batches") == 3);
        CHECK(scalar(db, "SELECT inserted FROM write_batches") == 2);
        CHECK(scalar(db, "SELECT ignored FROM write_batches") == 1);
        REQUIRE(store->finish(error));
    }
}

TEST_CASE("bench_sqlite_lookup: BEGIN IMMEDIATE serializes independent writers with heavy metadata and forced collisions") {
    for (const bool reuse : {true, false}) {
        Directory directory;
        auto options = options_for(directory.path);
        options.reuse_statement = reuse;
        options.transaction_model = "heavy";
        options.test_digest = [](std::string_view, std::string& digest) { digest.assign(32, '\0'); return true; };
        Metrics first_metrics, second_metrics;
        auto first = opened(options, first_metrics), second = opened(options, second_metrics);
        const std::vector<EncodedRecord> batch{row(), row("A", 7, 2), row(), row("B")};
        bool ok1 = true, ok2 = true;
        std::string error1, error2;
        const auto write = [&](Store& store, bool& ok, std::string& error) {
            for (int i = 0; i < 8; ++i) if (!store.write(batch, error)) { ok = false; break; }
        };
        std::thread a([&] { write(*first, ok1, error1); }), b([&] { write(*second, ok2, error2); });
        a.join(); b.join();
        REQUIRE_MESSAGE(ok1, error1); REQUIRE_MESSAGE(ok2, error2);
        CHECK(first_metrics.counters.at("inserted") + second_metrics.counters.at("inserted") == 3);
        CHECK(first_metrics.counters.at("ignored") + second_metrics.counters.at("ignored") == 61);
        CHECK(first_metrics.counters.at("sqlite_lookup_prepare_calls") + second_metrics.counters.at("sqlite_lookup_prepare_calls") == (reuse ? 2 : 16));
        CHECK(exported(*first).size() == 3);
        CHECK(exported(*second).size() == 3);
        const auto db = directory.path / "compact-record.sqlite3";
        CHECK(scalar(db, "SELECT count(*) FROM records") == 3);
        CHECK(scalar(db, "SELECT count(*) FROM write_batches") == 16);
        CHECK(scalar(db, "SELECT sum(attempted) FROM write_batches") == 64);
        CHECK(scalar(db, "SELECT sum(inserted) FROM write_batches") == 3);
        CHECK(scalar(db, "SELECT sum(ignored) FROM write_batches") == 61);
    }
}

TEST_CASE("bench_sqlite_lookup: subquery and separate can reopen the same unchanged compact schema") {
    Directory directory;
    auto options = options_for(directory.path);
    options.transaction_model = "heavy";
    options.test_digest = [](std::string_view, std::string& digest) { digest.assign(32, '\0'); return true; };
    const auto a = row(), b = row("A", 7, 2);
    std::string schema;
    for (const std::string mode : {"subquery", "separate", "subquery"}) {
        options.sqlite_compact_lookup = mode;
        Metrics metrics;
        auto store = opened(options, metrics);
        std::string error;
        REQUIRE_MESSAGE(store->write({a, b, a}, error), error);
        Json info(store->info_json());
        std::ostringstream tables;
        const simdjson::dom::array definitions = info.value["sqlite"]["schema"].get_array().value();
        for (const auto table : definitions) tables << table.get_string().value() << '\n';
        if (schema.empty()) schema = tables.str();
        CHECK(tables.str() == schema);
        CHECK(exported(*store) == std::set<std::string>{hex(a.bytes), hex(b.bytes)});
        CHECK(scalar(directory.path / "compact-record.sqlite3", "PRAGMA user_version") == 2);
        REQUIRE(store->finish(error));
    }
}
