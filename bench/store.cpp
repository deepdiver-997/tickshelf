#include "store.hpp"
#include "rocks_record_store.hpp"

#include <sqlite3.h>
#include <openssl/evp.h>
#ifdef TICKSHELF_HAVE_ROCKSDB
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/version.h>
#include <rocksdb/write_batch.h>
#endif

#include <algorithm>
#include <functional>
#include <array>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <filesystem>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string_view>
#include <utility>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace tickshelf::bench {
namespace {
namespace fs = std::filesystem;

// A store transaction may combine many <=4 MiB wire frames. The caller caps
// its row count; storage independently bounds canonical bytes at 64 MiB.
constexpr std::uint64_t kMaxStoreBytes = 64ULL * 1024 * 1024;
constexpr std::uint64_t kMinCanonicalBytes = 22;
constexpr std::uint64_t kMaxSegmentPayloadBytes =
    kMaxStoreBytes + 4 * (kMaxStoreBytes / kMinCanonicalBytes);

[[noreturn]] void fail(const std::string& message) { throw std::runtime_error(message); }
[[noreturn]] void system_failure(const std::string& operation) {
    const int code = errno;
    fail(operation + ": " + std::strerror(code));
}

struct File {
    int fd = -1;
    File() = default;
    explicit File(int value) : fd(value) {}
    ~File() { if (fd >= 0) ::close(fd); }
    File(const File&) = delete;
    File& operator=(const File&) = delete;
};

void sync_directory(const fs::path& path) {
    File dir(::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    if (dir.fd < 0) system_failure("open directory " + path.string());
    int rc;
    do { rc = ::fsync(dir.fd); } while (rc < 0 && errno == EINTR);
    if (rc < 0) system_failure("fsync directory " + path.string());
}

fs::path prepare_directory(const std::string& name) {
    if (name.empty()) fail("store directory must not be empty");
    const fs::path directory = fs::absolute(name).lexically_normal();
    std::vector<fs::path> missing;
    for (auto p = directory; !fs::exists(p); p = p.parent_path()) missing.push_back(p);
    if (fs::exists(directory) && !fs::is_directory(directory)) fail("store path is not a directory");
    fs::create_directories(directory);
    if (!fs::is_directory(directory)) fail("store path is not a directory");
    // Persist each newly created directory and its entry in the parent.
    for (const auto& p : missing) {
        sync_directory(p);
        sync_directory(p.parent_path());
    }
    // A caller may have just mkdir'ed the case directory itself. Persist its
    // entry too, even when this function did not create it (startup only).
    if (!directory.parent_path().empty() && directory.parent_path() != directory)
        sync_directory(directory.parent_path());
    return directory;
}

std::uint64_t read_le(std::string_view bytes, std::size_t offset, unsigned count) {
    std::uint64_t value = 0;
    for (unsigned i = 0; i < count; ++i)
        value |= std::uint64_t(static_cast<unsigned char>(bytes[offset + i])) << (8 * i);
    return value;
}
void append_le(std::string& out, std::uint64_t value, unsigned count) {
    for (unsigned i = 0; i < count; ++i) out.push_back(static_cast<char>(value >> (8 * i)));
}
std::array<unsigned char, 8> time_key(std::uint64_t time) {
    std::array<unsigned char, 8> result{};
    for (unsigned i = 0; i < 8; ++i) result[i] = static_cast<unsigned char>(time >> (56 - 8 * i));
    return result;
}
std::uint64_t read_time_key(const void* data, int size) {
    if (size != 8 || !data) fail("invalid stored uint64 time key");
    std::uint64_t result = 0;
    const auto* p = static_cast<const unsigned char*>(data);
    for (int i = 0; i < 8; ++i) result = (result << 8) | p[i];
    return result;
}

// EncodedRecord is trusted canonical input from the codec, not a second parser
// boundary. Check lengths and the small identity header without re-materializing
// book vectors in the timed write path. Recovery/export do fully decode.
std::uint64_t validate_batch(const std::vector<EncodedRecord>& records, Metrics& metrics) {
    Timer timer(metrics, "store_validate");
    std::uint64_t canonical_bytes = 0;
    for (const auto& record : records) {
        const auto size = record.bytes.size();
        if (size > kMaxRecordBytes || size < kMinCanonicalBytes || size > kMaxStoreBytes - canonical_bytes)
            fail("record length or 64 MiB canonical transaction payload exceeds store bounds");
        const std::string_view bytes(record.bytes);
        const auto symbol_size = read_le(bytes, 4, 2);
        if (bytes.substr(0, 4) != "RCD1" || symbol_size != record.symbol.size() ||
            22 + symbol_size > size || bytes.substr(6, symbol_size) != record.symbol ||
            read_le(bytes, 6 + symbol_size, 8) != record.time)
            fail("EncodedRecord canonical header does not match symbol/time");
        canonical_bytes += size;
    }
    return canonical_bytes;
}

std::string hex_bytes(std::string_view bytes) {
    constexpr char digits[] = "0123456789abcdef";
    std::string hex;
    hex.resize(bytes.size() * 2);
    for (std::size_t i = 0; i < bytes.size(); ++i) {
        const auto byte = static_cast<unsigned char>(bytes[i]);
        hex[2 * i] = digits[byte >> 4];
        hex[2 * i + 1] = digits[byte & 15];
    }
    return hex;
}
void emit_row(std::ostream& out, const Record& record, std::string_view bytes) {
    out << "{\"symbol\":" << json_quote(record.symbol) << ",\"time\":" << record.time
        << ",\"hex\":" << json_quote(hex_bytes(bytes)) << "}\n";
    if (!out) fail("failed to export store rows");
}

struct Statement {
    sqlite3_stmt* ptr = nullptr;
    ~Statement() { if (ptr) sqlite3_finalize(ptr); }
    Statement() = default;
    Statement(const Statement&) = delete;
    Statement& operator=(const Statement&) = delete;
};

class Database {
public:
    sqlite3* handle = nullptr;
    explicit Database(const fs::path& path, const StoreOptions& options, int schema_version = 1) {
        try {
            if (sqlite3_open_v2(path.c_str(), &handle,
                                SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
                                nullptr) != SQLITE_OK) db_fail("open " + path.string());
            sqlite3_extended_result_codes(handle, 1);
            check(sqlite3_busy_timeout(handle, 5000), "busy_timeout");
            exec("PRAGMA journal_mode=WAL");
            exec("PRAGMA synchronous=" + options.synchronous);
            if (options.fullfsync != "default") {
                exec(std::string("PRAGMA fullfsync=") + (options.fullfsync == "on" ? "1" : "0"));
                if (scalar_int("PRAGMA fullfsync") != (options.fullfsync == "on" ? 1 : 0))
                    fail("SQLite did not apply requested fullfsync configuration");
            }
            exec("PRAGMA wal_autocheckpoint=" + std::to_string(options.autocheckpoint));
            if (scalar_text("PRAGMA journal_mode") != "wal" ||
                scalar_int("PRAGMA synchronous") != (options.synchronous == "FULL" ? 2 : 1) ||
                scalar_int("PRAGMA wal_autocheckpoint") != options.autocheckpoint)
                fail("SQLite did not apply requested WAL/synchronous/checkpoint configuration");
            exec("PRAGMA foreign_keys=ON");
            if (scalar_int("PRAGMA foreign_keys") != 1) fail("SQLite did not enable foreign keys");
            const auto version = scalar_int("PRAGMA user_version");
            if (version != 0 && version != schema_version)
                fail("unsupported benchmark store schema version: expected " + std::to_string(schema_version) +
                     ", found " + std::to_string(version));
            if (path.filename() == "compact-record.sqlite3" && version == 0 &&
                scalar_int("SELECT count(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'") != 0)
                fail("unsupported unstamped compact store schema");
            exec("PRAGMA user_version=" + std::to_string(schema_version));
        } catch (...) {
            if (handle) sqlite3_close_v2(handle);
            handle = nullptr;
            throw;
        }
    }
    ~Database() { if (handle) sqlite3_close_v2(handle); }
    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    [[noreturn]] void db_fail(const std::string& operation) const {
        fail("SQLite " + operation + ": " + (handle ? sqlite3_errmsg(handle) : "no connection"));
    }
    void check(int rc, const std::string& operation) const {
        if (rc != SQLITE_OK) db_fail(operation);
    }
    void exec(const std::string& sql) const {
        if (sqlite3_exec(handle, sql.c_str(), nullptr, nullptr, nullptr) != SQLITE_OK) db_fail(sql);
    }
    void rollback() const noexcept { sqlite3_exec(handle, "ROLLBACK", nullptr, nullptr, nullptr); }
    void prepare(Statement& statement, const std::string& sql, Metrics* metrics = nullptr, bool lookup = false) const {
        if (statement.ptr) fail("attempt to prepare an already prepared statement");
        const auto start = clock_ns();
        const int rc = sqlite3_prepare_v2(handle, sql.c_str(), -1, &statement.ptr, nullptr);
        if (metrics) {
            const auto elapsed = clock_ns() - start;
            metrics->time("sqlite_prepare", elapsed);
            metrics->inc("sqlite_prepare_calls");
            if (lookup) {
                metrics->time("sqlite_lookup_prepare", elapsed);
                metrics->inc("sqlite_lookup_prepare_calls");
            }
        }
        check(rc, "prepare " + sql);
    }
    std::string scalar_text(const std::string& sql) const {
        Statement stmt;
        prepare(stmt, sql);
        if (sqlite3_step(stmt.ptr) != SQLITE_ROW) db_fail(sql);
        const auto* text = sqlite3_column_text(stmt.ptr, 0);
        return text ? reinterpret_cast<const char*>(text) : "";
    }
    std::int64_t scalar_int(const std::string& sql) const {
        Statement stmt;
        prepare(stmt, sql);
        if (sqlite3_step(stmt.ptr) != SQLITE_ROW) db_fail(sql);
        return sqlite3_column_int64(stmt.ptr, 0);
    }
    std::string info_json() const {
        std::ostringstream out;
        out << "{\"version\":" << json_quote(sqlite3_libversion())
            << ",\"source_id\":" << json_quote(sqlite3_sourceid()) << ",\"compile_options\":[";
        Statement compile;
        prepare(compile, "PRAGMA compile_options");
        bool comma = false;
        int rc;
        while ((rc = sqlite3_step(compile.ptr)) == SQLITE_ROW) {
            if (comma) out << ',';
            comma = true;
            const auto* text = sqlite3_column_text(compile.ptr, 0);
            out << json_quote(text ? reinterpret_cast<const char*>(text) : "");
        }
        if (rc != SQLITE_DONE) db_fail("compile_options");
        out << "],\"pragmas\":{\"journal_mode\":" << json_quote(scalar_text("PRAGMA journal_mode"))
            << ",\"synchronous\":" << scalar_int("PRAGMA synchronous")
            << ",\"synchronous_name\":" << json_quote(scalar_int("PRAGMA synchronous") == 2 ? "FULL" : "NORMAL")
            << ",\"wal_autocheckpoint\":" << scalar_int("PRAGMA wal_autocheckpoint")
            << ",\"page_size\":" << scalar_int("PRAGMA page_size")
            << ",\"cache_size\":" << scalar_int("PRAGMA cache_size")
            << ",\"fullfsync\":" << scalar_int("PRAGMA fullfsync")
            << ",\"checkpoint_fullfsync\":" << scalar_int("PRAGMA checkpoint_fullfsync")
            << ",\"foreign_keys\":" << scalar_int("PRAGMA foreign_keys")
            << ",\"user_version\":" << scalar_int("PRAGMA user_version") << "},\"schema\":[";
        Statement schema;
        prepare(schema, "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name");
        comma = false;
        while ((rc = sqlite3_step(schema.ptr)) == SQLITE_ROW) {
            if (comma) out << ',';
            comma = true;
            out << json_quote(reinterpret_cast<const char*>(sqlite3_column_text(schema.ptr, 0)));
        }
        if (rc != SQLITE_DONE) db_fail("read schema");
        out << "],\"checkpoint_on_finish\":\"PASSIVE\",\"last_checkpoint\":{\"called\":"
            << (checkpoint_called_ ? "true" : "false") << ",\"log_frames\":" << checkpoint_log_
            << ",\"checkpointed_frames\":" << checkpoint_done_ << "}}";
        return out.str();
    }
    void checkpoint(Metrics& metrics) {
        Timer timer(metrics, "sqlite_checkpoint");
        const int rc = sqlite3_wal_checkpoint_v2(handle, nullptr, SQLITE_CHECKPOINT_PASSIVE,
                                                &checkpoint_log_, &checkpoint_done_);
        checkpoint_called_ = true;
        check(rc, "PASSIVE checkpoint");
    }
private:
    bool checkpoint_called_ = false;
    int checkpoint_log_ = -1, checkpoint_done_ = -1;
};

constexpr const char* kHeavySchema =
    "CREATE TABLE IF NOT EXISTS write_batches("
    "id INTEGER PRIMARY KEY, attempted INTEGER NOT NULL, inserted INTEGER NOT NULL,"
    "ignored INTEGER NOT NULL, bytes INTEGER NOT NULL)";
constexpr const char* kHeavyInsert =
    "INSERT INTO write_batches(attempted,inserted,ignored,bytes) VALUES(?1,?2,?3,?4)";

void count_sqlite_bind(Metrics* metrics, const char* kind) {
    if (!metrics) return;
    metrics->inc("sqlite_bind_calls");
    metrics->inc(kind);
}
void bind_int(const Database& db, sqlite3_stmt* stmt, int column, std::uint64_t value, Metrics* metrics = nullptr) {
    if (value > std::uint64_t(std::numeric_limits<sqlite3_int64>::max())) fail("SQLite integer overflow");
    count_sqlite_bind(metrics, "sqlite_bind_int64_calls");
    db.check(sqlite3_bind_int64(stmt, column, static_cast<sqlite3_int64>(value)), "bind integer");
}
void bind_symbol(const Database& db, sqlite3_stmt* stmt, int column, std::string_view symbol, Metrics* metrics = nullptr) {
    if (symbol.size() > std::numeric_limits<std::uint16_t>::max()) fail("symbol exceeds canonical header bounds");
    count_sqlite_bind(metrics, "sqlite_bind_text_calls");
    db.check(sqlite3_bind_text(stmt, column, symbol.data(), static_cast<int>(symbol.size()), SQLITE_TRANSIENT), "bind symbol");
}
void bind_time(const Database& db, sqlite3_stmt* stmt, int column, std::uint64_t time, Metrics* metrics = nullptr) {
    const auto bytes = time_key(time);
    count_sqlite_bind(metrics, "sqlite_bind_blob_calls");
    db.check(sqlite3_bind_blob(stmt, column, bytes.data(), 8, SQLITE_TRANSIENT), "bind time key");
}
void reset_statement(const Database& db, sqlite3_stmt* stmt, Metrics* metrics = nullptr) {
    if (metrics) metrics->inc("sqlite_reset_calls");
    db.check(sqlite3_reset(stmt), "reset statement");
    if (metrics) metrics->inc("sqlite_clear_bindings_calls");
    db.check(sqlite3_clear_bindings(stmt), "clear bindings");
}
void cleanup_statement(sqlite3_stmt* stmt, Metrics& metrics) {
    if (!stmt) return;
    metrics.inc("sqlite_reset_calls");
    sqlite3_reset(stmt);
    metrics.inc("sqlite_clear_bindings_calls");
    sqlite3_clear_bindings(stmt);
}
void step_done(const Database& db, sqlite3_stmt* stmt, Metrics& metrics) {
    Timer timer(metrics, "sqlite_step");
    metrics.inc("sqlite_step_calls");
    if (sqlite3_step(stmt) != SQLITE_DONE) db.db_fail("step");
}
void write_batch_metadata(const Database& db, Statement& reusable, const StoreOptions& options,
                          Metrics& metrics, std::uint64_t attempted, std::uint64_t inserted,
                          std::uint64_t ignored, std::uint64_t bytes) {
    if (options.transaction_model != "heavy") return;
    Statement local;
    auto& statement = options.reuse_statement ? reusable : local;
    if (!statement.ptr) db.prepare(statement, kHeavyInsert, &metrics);
    try {
        bind_int(db, statement.ptr, 1, attempted, &metrics);
        bind_int(db, statement.ptr, 2, inserted, &metrics);
        bind_int(db, statement.ptr, 3, ignored, &metrics);
        bind_int(db, statement.ptr, 4, bytes, &metrics);
        step_done(db, statement.ptr, metrics);
        reset_statement(db, statement.ptr, &metrics);
        metrics.inc("metadata_sql_rows");
    } catch (...) {
        cleanup_statement(statement.ptr, metrics);
        throw;
    }
}

class StoreBase : public Store {
public:
    StoreBase(StoreOptions options, Metrics& metrics) : options_(std::move(options)), metrics_(metrics) {
        for (const char* name : {"attempted", "inserted", "ignored", "transactions", "payload_bytes",
                                 "sqlite_prepare_calls", "sqlite_step_calls", "sqlite_begin_calls", "sqlite_commit_calls",
                                 "sqlite_exec_calls", "sqlite_rollback_calls", "sqlite_bind_calls", "sqlite_bind_blob_calls",
                                 "sqlite_bind_text_calls", "sqlite_bind_int64_calls", "sqlite_reset_calls",
                                 "sqlite_clear_bindings_calls", "sqlite_changes_calls",
                                 "sqlite_lookup_prepare_calls", "sqlite_lookup_step_calls", "sqlite_lookup_bind_calls",
                                 "sqlite_lookup_reset_calls", "sqlite_lookup_clear_bindings_calls", "sqlite_lookup_rows",
                                 "sqlite_lookup_candidate_rows", "sqlite_lookup_candidate_bytes", "sqlite_lookup_matches", "file_write_calls",
                                 "file_write_bytes", "file_sync_calls", "index_rows", "symbol_index_rows",
                                 "metadata_sql_rows", "range_candidate_segments", "range_decoded_rows", "range_emitted_rows",
                                 "store_digest_cache_hits", "store_digest_computed", "store_digest_bytes", "store_digest_override_calls",
                                 "backend_digest_recomputed", "backend_digest_cached", "backend_lookup_calls", "backend_candidates",
                                 "backend_inserted", "backend_ignored",
                                 "rocksdb_write_batches", "rocksdb_put_ops", "rocksdb_key_bytes", "rocksdb_value_bytes",
                                 "rocksdb_writebatch_bytes", "rocksdb_get_calls", "rocksdb_range_iterator_steps",
                                 "rocksdb_range_read_bytes", "metadata_kv_rows"})
            metrics_.counters.try_emplace(name, 0);
        metrics_.counters["backend_lookup_exact"] = options_.store_lookup == "exact";
        metrics_.counters["backend_lookup_assume_unique"] = options_.store_lookup == "assume_unique";
        // The original SQLite subquery does not materialize its candidates in C++.
        metrics_.counters["backend_candidates_observable"] = options_.backend == "sqlite_compact" &&
            (options_.store_lookup == "assume_unique" || options_.sqlite_compact_lookup == "separate");
    }
protected:
    StoreOptions options_;
    Metrics& metrics_;
    std::uint64_t attempted_ = 0, inserted_ = 0, ignored_ = 0, transactions_ = 0;
    std::uint64_t physical_rows_ = 0;
    void attempt(std::size_t count) {
        attempted_ += count;
        metrics_.inc("attempted", count);
    }
    void committed(std::uint64_t inserted, std::uint64_t ignored, std::uint64_t bytes) {
        inserted_ += inserted;
        ignored_ += ignored;
        physical_rows_ += inserted;
        ++transactions_;
        metrics_.inc("inserted", inserted);
        metrics_.inc("ignored", ignored);
        metrics_.inc("backend_inserted", inserted);
        metrics_.inc("backend_ignored", ignored);
        metrics_.inc("payload_bytes", bytes);
        metrics_.inc("transactions");
    }
    std::string counts_json() const {
        return "{\"attempted\":" + std::to_string(attempted_) + ",\"inserted\":" + std::to_string(inserted_) +
            ",\"ignored\":" + std::to_string(ignored_) + ",\"transactions\":" + std::to_string(transactions_) + "}";
    }
};

class NullStore final : public StoreBase {
public:
    NullStore(const StoreOptions& options, Metrics& metrics) : StoreBase(options, metrics) {}
    bool write(const std::vector<EncodedRecord>& records, std::string& error) override {
        error.clear();
        attempt(records.size());
        try {
            const auto bytes = validate_batch(records, metrics_);
            Timer timer(metrics_, "null_consume");
            for (const auto& record : records)
                for (unsigned char byte : record.bytes) checksum_ = (checksum_ ^ byte) * 1099511628211ULL;
            metrics_.inc("null_consumed_bytes", bytes);
            metrics_.inc("null_consumed_rows", records.size());
            metrics_.counters["null_checksum"] = checksum_;
            consumed_bytes_ += bytes;
            consumed_rows_ += records.size();
            return true;
        } catch (const std::exception& e) { error = e.what(); return false; }
    }
    bool finish(std::string& error) override { error.clear(); return true; }
    bool export_rows(std::ostream& out, std::string& error) override {
        error.clear();
        if (!out) { error = "failed to export null store rows"; return false; }
        return true;
    }
    bool export_range(const std::string&, std::uint64_t, std::uint64_t,
                      std::ostream&, std::string& error) override {
        error = "null store does not persist records and cannot export a range";
        return false;
    }
    std::string info_json() const override {
        return "{\"backend\":\"null\",\"durable\":false,\"physical_power_loss_tested\":false,\"hardware_plp\":\"unknown\","
               "\"writes_database\":false,\"sync_method\":\"none\",\"deduplication\":\"none\","
               "\"physical_rows\":0,\"physical_duplicate_rows\":0,\"checksum_algorithm\":\"fnv1a64\",\"checksum\":" +
            std::to_string(checksum_) + ",\"consumed_bytes\":" + std::to_string(consumed_bytes_) +
            ",\"consumed_rows\":" + std::to_string(consumed_rows_) + ",\"counters\":" + counts_json() + "}";
    }
private:
    std::uint64_t checksum_ = 14695981039346656037ULL, consumed_bytes_ = 0, consumed_rows_ = 0;
};

class SqliteStore final : public StoreBase {
public:
    SqliteStore(const StoreOptions& options, Metrics& metrics)
        : StoreBase(options, metrics), directory_(prepare_directory(options.store_lookup == "assume_unique" ?
              (fs::path(options.directory) / "sqlite-compact-assume-unique-v1").string() : options.directory)),
          compact_(options.backend == "sqlite_compact"), assume_unique_(options.store_lookup == "assume_unique"),
          separate_(compact_ && !assume_unique_ && options.sqlite_compact_lookup == "separate"),
          db_(directory_ / (compact_ ? "compact-record.sqlite3" : "full-record.sqlite3"), options, assume_unique_ ? 3 : (compact_ ? 2 : 1)) {
        if (compact_) {
            db_.exec("CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY, payload BLOB NOT NULL,"
                     " symbol TEXT NOT NULL, time BLOB NOT NULL CHECK(typeof(time)='blob' AND length(time)=8),"
                     " digest BLOB NOT NULL CHECK(typeof(digest)='blob' AND length(digest)=32))");
            db_.exec("CREATE INDEX IF NOT EXISTS records_digest ON records(digest)");
            // Secondary indexes contain the small INTEGER rowid, never a copy
            // of the full payload. Digest is deliberately NOT UNIQUE.
            if (db_.scalar_int("SELECT count(*) FROM pragma_table_info('records') WHERE name='id' AND type='INTEGER' AND pk=1") != 1 ||
                db_.scalar_int("SELECT count(*) FROM pragma_table_info('records')") != 5 ||
                db_.scalar_int("SELECT count(*) FROM pragma_table_info('records') WHERE \"notnull\"=1 AND "
                    "((name IN ('payload','digest','time') AND type='BLOB') OR (name='symbol' AND type='TEXT'))") != 4 ||
                db_.scalar_int("SELECT count(*) FROM pragma_index_list('records') WHERE \"unique\"<>0") != 0 ||
                db_.scalar_int("SELECT count(*) FROM pragma_index_list('records') WHERE name='records_digest' AND \"unique\"=0") != 1 ||
                db_.scalar_text("SELECT group_concat(name,',') FROM pragma_index_info('records_digest')") != "digest")
                fail("unsupported compact store schema");
        } else {
            db_.exec("CREATE TABLE IF NOT EXISTS records(payload BLOB PRIMARY KEY, symbol TEXT NOT NULL,"
                     " time BLOB NOT NULL CHECK(length(time)=8)) WITHOUT ROWID");
        }
        db_.exec("CREATE INDEX IF NOT EXISTS records_symbol_time ON records(symbol,time)");
        if (options_.transaction_model == "heavy") {
            db_.exec("CREATE INDEX IF NOT EXISTS records_time_symbol ON records(time,symbol)");
            db_.exec(kHeavySchema);
        }
        physical_rows_ = static_cast<std::uint64_t>(db_.scalar_int("SELECT count(*) FROM records"));
        // No uniqueness constraint is added to the diagnostic format: collisions
        // and accidental repeats remain physical rows for the independent oracle.
        // Reopen validates the zero-repeat precondition outside workload counters.
        if (assume_unique_ && db_.scalar_int("SELECT EXISTS(SELECT 1 FROM records GROUP BY payload HAVING count(*)>1)") != 0)
            fail("assume_unique store contains duplicate exact canonical payloads");
        if (options_.reuse_statement) {
            db_.prepare(insert_, insert_sql(), &metrics_);
            if (separate_lookup()) db_.prepare(lookup_, lookup_sql(), &metrics_, true);
            if (options_.transaction_model == "heavy") db_.prepare(metadata_, kHeavyInsert, &metrics_);
        }
        sync_directory(directory_); // Includes newly created DB and WAL directory entries.
    }
    bool write(const std::vector<EncodedRecord>& records, std::string& error) override {
        error.clear();
        attempt(records.size());
        bool transaction = false;
        try {
            const auto bytes = validate_batch(records, metrics_);
            if (records.empty()) return true;
            { Timer timer(metrics_, "sqlite_begin"); metrics_.inc("sqlite_begin_calls"); metrics_.inc("sqlite_exec_calls"); db_.exec("BEGIN IMMEDIATE"); }
            transaction = true;
            // Both modes reuse every statement within this batch. Only lifetime
            // across transactions differs: prepare once per store vs transaction,
            // never once per record. The default subquery insert is unchanged.
            Statement local, local_lookup;
            auto& statement = options_.reuse_statement ? insert_ : local;
            auto& lookup = options_.reuse_statement ? lookup_ : local_lookup;
            if (!statement.ptr) db_.prepare(statement, insert_sql(), &metrics_);
            if (separate_lookup() && !lookup.ptr) db_.prepare(lookup, lookup_sql(), &metrics_, true);
            std::uint64_t inserted = 0;
            for (const auto& record : records) {
                std::array<unsigned char, 32> separate_digest;
                if (separate_lookup()) {
                    separate_digest = store_detail::record_digest(record, options_, metrics_);
                    // Reset/clear the read cursor before any write; it must not
                    // remain active across INSERT or COMMIT on this connection.
                    if (lookup_contains(lookup.ptr, record, separate_digest)) continue;
                }
                const auto time = time_key(record.time);
                count_sqlite_bind(&metrics_, "sqlite_bind_blob_calls");
                db_.check(sqlite3_bind_blob(statement.ptr, 1, record.bytes.data(),
                                           static_cast<int>(record.bytes.size()), SQLITE_STATIC), "bind payload");
                count_sqlite_bind(&metrics_, "sqlite_bind_text_calls");
                db_.check(sqlite3_bind_text(statement.ptr, 2, record.symbol.data(),
                                           static_cast<int>(record.symbol.size()), SQLITE_STATIC), "bind symbol");
                count_sqlite_bind(&metrics_, "sqlite_bind_blob_calls");
                db_.check(sqlite3_bind_blob(statement.ptr, 3, time.data(), 8, SQLITE_TRANSIENT), "bind time");
                if (compact_) {
                    const auto digest = separate_lookup() ? separate_digest : store_detail::record_digest(record, options_, metrics_);
                    count_sqlite_bind(&metrics_, "sqlite_bind_blob_calls");
                    db_.check(sqlite3_bind_blob(statement.ptr, 4, digest.data(), static_cast<int>(digest.size()),
                                               SQLITE_TRANSIENT), "bind digest");
                }
                // Count the actual SQL-level candidate check, not an extra API
                // call: default subquery lookup is part of the INSERT statement.
                if (compact_ && !assume_unique_ && !separate_lookup()) metrics_.inc("backend_lookup_calls");
                step_done(db_, statement.ptr, metrics_);
                metrics_.inc("sqlite_changes_calls");
                inserted += static_cast<std::uint64_t>(sqlite3_changes(db_.handle));
                reset_statement(db_, statement.ptr, &metrics_);
            }
            write_batch_metadata(db_, metadata_, options_, metrics_, records.size(), inserted,
                                 records.size() - inserted, bytes);
            { Timer timer(metrics_, "sqlite_commit"); metrics_.inc("sqlite_commit_calls"); metrics_.inc("sqlite_exec_calls"); db_.exec("COMMIT"); }
            transaction = false;
            committed(inserted, records.size() - inserted, bytes);
            return true;
        } catch (const std::exception& e) {
            cleanup_statement(insert_.ptr, metrics_);
            reset_lookup(lookup_.ptr, false);
            cleanup_statement(metadata_.ptr, metrics_);
            if (transaction) { metrics_.inc("sqlite_rollback_calls"); metrics_.inc("sqlite_exec_calls"); db_.rollback(); }
            error = e.what();
            return false;
        }
    }
    bool finish(std::string& error) override {
        error.clear();
        try { db_.checkpoint(metrics_); return true; }
        catch (const std::exception& e) { error = e.what(); return false; }
    }
    bool export_rows(std::ostream& out, std::string& error) override {
        error.clear();
        try {
            Statement stmt;
            db_.prepare(stmt, "SELECT symbol,time,payload FROM records ORDER BY payload");
            export_query(stmt.ptr, out);
            return true;
        } catch (const std::exception& e) { error = e.what(); return false; }
    }
    bool export_range(const std::string& symbol, std::uint64_t from, std::uint64_t to,
                      std::ostream& out, std::string& error) override {
        error.clear();
        try {
            Timer timer(metrics_, "export_range");
            if (from > to) fail("range lower bound exceeds upper bound");
            Statement stmt;
            db_.prepare(stmt, "SELECT symbol,time,payload FROM records INDEXED BY records_symbol_time "
                              "WHERE symbol=?1 AND time>=?2 AND time<=?3 ORDER BY time,payload");
            bind_symbol(db_, stmt.ptr, 1, symbol);
            bind_time(db_, stmt.ptr, 2, from);
            bind_time(db_, stmt.ptr, 3, to);
            // The full canonical primary key already provides exact dedup.
            export_query(stmt.ptr, out, true);
            return true;
        } catch (const std::exception& e) { error = e.what(); return false; }
    }
    std::string info_json() const override {
        return "{\"backend\":" + json_quote(compact_ ? "sqlite_compact" : "sqlite") + ",\"durable\":" + std::string(options_.synchronous == "FULL" ? "true" : "false") +
            ",\"physical_power_loss_tested\":false,\"hardware_plp\":\"unknown\",\"normal_diagnostic\":" +
            (options_.synchronous == "NORMAL" ? "true" : "false") + ",\"durability_scope\":" +
            json_quote(options_.synchronous == "FULL" ?
                "SQLite FULL WAL commit with OS sync; no physical PLP claim" :
                "NORMAL diagnostic: WAL is not synced on every commit; no power-loss durability promise") +
            ",\"sync_method\":\"SQLite WAL synchronous=" + options_.synchronous +
            "\",\"database\":" + json_quote((directory_ / (compact_ ? "compact-record.sqlite3" : "full-record.sqlite3")).string()) +
            ",\"transaction_model\":" + json_quote(options_.transaction_model) +
            ",\"store_lookup\":" + json_quote(options_.store_lookup) +
            (assume_unique_ ? ",\"production\":false,\"synthetic_only\":true,\"format_stamp\":\"tickshelf-bench/sqlite-compact-assume-unique/v1/RCD1/user_version=3\",\"uniqueness_contract\":\"prevalidated zero-repeat synthetic input only; writes retain every physical row; independent export oracle and strict reopen detect repeats\"" : "") +
            ",\"backend_lookup_metric_scope\":\"workload only, excluding recovery/export; lookup_calls counts candidate SQL executions (including the original INSERT subquery); candidates counts rows returned for C++ exact comparison, not SQLite internal visits; backend_candidates_observable=0 means internal subquery candidates are unavailable, not measured zero\"" +
            ",\"sqlite_compact_lookup\":" + json_quote(compact_ ? options_.sqlite_compact_lookup : "not_applicable") +
            ",\"compact_lookup_algorithm\":" + json_quote(assume_unique_ ?
                "synthetic assume_unique diagnostic: no candidate SQL; prepared INSERT VALUES retains canonical payload, SHA-256 digest, all indexes and transaction/sync work" :
                (separate_lookup() ?
                    "prepared digest-candidate SELECT; exact canonical byte comparison in C++; reset/clear lookup before prepared INSERT VALUES; both inside BEGIN IMMEDIATE" :
                    (compact_ ? "original INSERT SELECT WHERE NOT EXISTS(digest AND full payload) inside BEGIN IMMEDIATE" : "not applicable"))) +
            ",\"lookup_metric_scope\":\"sqlite_lookup_*_calls are subsets of aggregate sqlite_* workload API counters, not additional calls to add again; lookup prepare/step timings are subsets of aggregate prepare/step, while bind/reset/clear are independently timed; prepare includes open-time lifetime reuse; step counts each ROW/DONE/error sqlite3_step; rows counts record lookups and candidate_bytes counts returned canonical bytes, not physical I/O; library APIs are not syscalls\"" +
            ",\"reuse_statement\":" + (options_.reuse_statement ? "true" : "false") +
            ",\"statement_reuse_scope\":" + json_quote(options_.reuse_statement ?
                "store lifetime, prepared at open; reset/rebind within each batch" :
                "one write transaction, prepared once per statement kind; reset/rebind within each batch") +
            ",\"prepare_metric_scope\":" + json_quote(separate_lookup() ?
                "sqlite_prepare measures workload INSERT/metadata and digest-candidate lookup SELECT prepares, including open-time lifetime reuse; sqlite_lookup_prepare is a subset; export/recovery/schema prepares excluded" :
                "sqlite_prepare measures write-statement prepares only, including open-time prepares for lifetime reuse; query/recovery/schema prepares excluded") +
            ",\"sqlite_api_count_scope\":\"library API calls, NOT syscalls/kernel calls; begin/commit count sqlite3_exec calls in the same scope as corresponding Timer counts; prepare/step count workload API scope (including separate lookup when selected); bind/reset/clear_bindings/changes and error rollback/cleanup count workload API invocations; schema, recovery and export APIs excluded; no per-record JSON logging\"" +
            ",\"deduplication\":" + json_quote(assume_unique_ ?
                "none; synthetic zero-repeat assume_unique diagnostic; fixed32 nonunique SHA-256 digest index retained" :
                (compact_ ?
                    (separate_lookup() ?
                        "nonunique fixed32 SHA-256 candidates then exact full payload comparison; separate prepared SELECT and INSERT VALUES within BEGIN IMMEDIATE" :
                        "nonunique fixed32 SHA-256 candidates then exact full payload comparison; INSERT SELECT NOT EXISTS within BEGIN IMMEDIATE") :
                    "exact canonical payload BLOB primary key, not a digest")) +
            ",\"schema_cost\":" + json_quote(compact_ ?
                "INTEGER PRIMARY KEY payload table; fixed32 nonunique digest index and symbol/time indexes contain rowid only; no UNIQUE(digest,payload)" :
                "WITHOUT ROWID payload key plus full key copied into symbol/time secondary index and any heavy indexes; not compact digest indexing") +
            ",\"digest_cache_contract\":\"private trusted EncodedRecord.sha256 valid only while canonical bytes unchanged; missing SHA-256 computed and separately timed/counted; test override takes precedence\","
            "\"time_encoding\":\"uint64 big-endian 8-byte BLOB\",\"physical_rows\":" + std::to_string(physical_rows_) +
            ",\"physical_duplicate_rows\":" + (assume_unique_ ? "null" : "0") + ",\"counters\":" + counts_json() + ",\"sqlite\":" + db_.info_json() + "}";
    }
private:
    fs::path directory_;
    bool compact_, assume_unique_, separate_;
    Database db_;
    Statement insert_, lookup_, metadata_;
    bool separate_lookup() const { return separate_; }
    static const char* lookup_sql() {
        return "SELECT payload FROM records INDEXED BY records_digest WHERE digest=?1";
    }
    void reset_lookup(sqlite3_stmt* statement, bool checked) {
        if (!statement) return;
        metrics_.inc("sqlite_reset_calls");
        metrics_.inc("sqlite_lookup_reset_calls");
        auto start = clock_ns();
        const int reset_rc = sqlite3_reset(statement);
        metrics_.time("sqlite_lookup_reset", clock_ns() - start);
        // Always clear bindings, even if sqlite3_reset reports the last step's
        // error. Failure cleanup must release SQLITE_STATIC/TRANSIENT payloads.
        metrics_.inc("sqlite_clear_bindings_calls");
        metrics_.inc("sqlite_lookup_clear_bindings_calls");
        start = clock_ns();
        const int clear_rc = sqlite3_clear_bindings(statement);
        metrics_.time("sqlite_lookup_clear_bindings", clock_ns() - start);
        if (checked) {
            db_.check(reset_rc, "reset compact lookup");
            db_.check(clear_rc, "clear compact lookup bindings");
        }
    }
    bool lookup_contains(sqlite3_stmt* statement, const EncodedRecord& record,
                         const std::array<unsigned char, 32>& digest) {
        Timer all(metrics_, "sqlite_lookup");
        metrics_.inc("backend_lookup_calls");
        metrics_.inc("sqlite_lookup_rows");
        bool match = false;
        try {
            count_sqlite_bind(&metrics_, "sqlite_bind_blob_calls");
            metrics_.inc("sqlite_lookup_bind_calls");
            auto start = clock_ns();
            const int bind_rc = sqlite3_bind_blob(statement, 1, digest.data(), static_cast<int>(digest.size()), SQLITE_TRANSIENT);
            metrics_.time("sqlite_lookup_bind", clock_ns() - start);
            db_.check(bind_rc, "bind compact lookup digest");
            for (;;) {
                metrics_.inc("sqlite_step_calls");
                metrics_.inc("sqlite_lookup_step_calls");
                start = clock_ns();
                const int rc = sqlite3_step(statement);
                const auto elapsed = clock_ns() - start;
                // Lookup stages are subsets of aggregate write API stages, not
                // extra invocations. ROW and DONE each count as one sqlite3_step.
                metrics_.time("sqlite_step", elapsed);
                metrics_.time("sqlite_lookup_step", elapsed);
                if (rc == SQLITE_DONE) break;
                if (rc != SQLITE_ROW) db_.db_fail("step compact lookup");
                const int size = sqlite3_column_bytes(statement, 0);
                const auto* data = static_cast<const char*>(sqlite3_column_blob(statement, 0));
                if (!data || size <= 0 || size > static_cast<int>(kMaxRecordBytes)) fail("invalid compact lookup payload length");
                metrics_.inc("sqlite_lookup_candidate_rows");
                metrics_.inc("backend_candidates");
                metrics_.inc("sqlite_lookup_candidate_bytes", static_cast<std::uint64_t>(size));
                if (std::string_view(data, static_cast<std::size_t>(size)) == record.bytes) {
                    match = true;
                    metrics_.inc("sqlite_lookup_matches");
                    break;
                }
            }
        } catch (...) {
            reset_lookup(statement, false);
            throw;
        }
        // Release the read cursor before INSERT VALUES. BEGIN IMMEDIATE still
        // owns the write reservation, so competing connections cannot race the
        // lookup/insert pair; this is not a standalone optimistic Get+Put.
        reset_lookup(statement, true);
        return match;
    }
    const char* insert_sql() const {
        if (assume_unique_ || separate_lookup()) return "INSERT INTO records(payload,symbol,time,digest) VALUES(?1,?2,?3,?4)";
        return compact_ ?
            "INSERT INTO records(payload,symbol,time,digest) SELECT ?1,?2,?3,?4 WHERE NOT EXISTS("
            "SELECT 1 FROM records INDEXED BY records_digest WHERE digest=?4 AND payload=?1)" :
            "INSERT INTO records(payload,symbol,time) VALUES(?1,?2,?3) ON CONFLICT(payload) DO NOTHING";
    }
    void export_query(sqlite3_stmt* statement, std::ostream& out, bool range = false) {
        int rc;
        while ((rc = sqlite3_step(statement)) == SQLITE_ROW) {
            const auto* symbol = reinterpret_cast<const char*>(sqlite3_column_text(statement, 0));
            const auto time = read_time_key(sqlite3_column_blob(statement, 1), sqlite3_column_bytes(statement, 1));
            const auto* data = static_cast<const char*>(sqlite3_column_blob(statement, 2));
            const int size = sqlite3_column_bytes(statement, 2);
            if (!data || size <= 0 || size > static_cast<int>(kMaxRecordBytes)) fail("invalid stored payload length");
            const std::string_view bytes(data, size);
            Record record;
            std::string decode_error;
            if (!decode_record(bytes, record, decode_error)) fail("stored record decode: " + decode_error);
            if (!symbol || record.symbol != std::string_view(symbol, sqlite3_column_bytes(statement, 0)) || record.time != time)
                fail("stored SQLite symbol/time does not match canonical identity");
            emit_row(out, record, bytes);
            if (range) {
                metrics_.inc("range_decoded_rows");
                metrics_.inc("range_emitted_rows");
            }
        }
        if (rc != SQLITE_DONE) db_.db_fail("export rows");
        if (!out) fail("failed to export store rows");
    }
};

constexpr std::size_t kSegmentHeaderBytes = 32;
constexpr auto crc_table() {
    std::array<std::uint32_t, 256> table{};
    for (std::uint32_t i = 0; i < table.size(); ++i) {
        std::uint32_t crc = i;
        for (unsigned bit = 0; bit < 8; ++bit) crc = (crc >> 1) ^ ((crc & 1) ? 0xedb88320U : 0);
        table[i] = crc;
    }
    return table;
}
std::uint32_t crc32(std::string_view bytes) {
    static constexpr auto table = crc_table();
    std::uint32_t crc = 0xffffffffU;
    for (unsigned char byte : bytes) crc = table[(crc ^ byte) & 255] ^ (crc >> 8);
    return crc ^ 0xffffffffU;
}

void sync_file(int fd, Metrics& metrics) {
    Timer timer(metrics, "file_sync");
    int rc;
    do {
        metrics.inc("file_sync_calls");
#if defined(__linux__)
        rc = ::fdatasync(fd);
#else
        rc = ::fsync(fd);
#endif
    } while (rc < 0 && errno == EINTR);
    if (rc < 0) system_failure("sync segment file");
}
void read_at(int fd, std::uint64_t offset, std::string& buffer) {
    std::size_t done = 0;
    while (done < buffer.size()) {
        const auto n = ::pread(fd, buffer.data() + done, buffer.size() - done, static_cast<off_t>(offset + done));
        if (n < 0) { if (errno == EINTR) continue; system_failure("read segment"); }
        if (n == 0) fail("indexed segment is truncated");
        done += static_cast<std::size_t>(n);
    }
}
struct SegmentEntry {
    std::uint64_t offset = 0, length = 0, records = 0;
    std::uint32_t crc = 0;
};
struct TimeRange {
    std::uint64_t min = 0, max = 0;
};
using SymbolRanges = std::map<std::string, TimeRange, std::less<>>;
void add_range(SymbolRanges& ranges, std::string_view symbol, std::uint64_t time) {
    const auto found = ranges.find(symbol);
    if (found == ranges.end()) ranges.emplace(std::string(symbol), TimeRange{time, time});
    else {
        found->second.min = std::min(found->second.min, time);
        found->second.max = std::max(found->second.max, time);
    }
}

#ifdef TICKSHELF_HAVE_ROCKSDB
// This is an index over the same TSG1 file, not a second payload store. The
// lifetime flock on that file serializes all writers/recovery. No Get+Put pair
// is presented as a transaction: all index changes use one synchronous batch.
class RocksSegmentIndex {
public:
    RocksSegmentIndex(const fs::path& path, bool has_data, Metrics& metrics)
        : path_(path), metrics_(metrics) {
        const bool existing = fs::exists(path_ / "CURRENT");
        if (!existing && has_data) fail("missing RocksDB segment index for existing data");
        options_.create_if_missing = !existing;
        options_.use_fsync = true;
        options_.compression = rocksdb::kNoCompression;
        options_.paranoid_checks = true;
        options_.wal_recovery_mode = rocksdb::WALRecoveryMode::kAbsoluteConsistency;
        check(rocksdb::DB::Open(options_, path_.string(), &db_), "open");
        if (!existing) {
            // Only genuinely empty new stores can acquire their initial stamp.
            std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_options()));
            it->SeekToFirst();
            if (it->Valid()) fail("unstamped nonempty RocksDB segment index");
            check(it->status(), "initial iterator");
            rocksdb::WriteBatch batch;
            check(batch.Put("F", stamp()), "format stamp");
            check(batch.Put("H", be(0)), "initial high water");
            check(db_->Write(write_options(), &batch), "initialize format");
        }
        if (get("F") != stamp()) fail("unsupported RocksDB segment index format stamp");
        const auto high = get("H");
        if (high.size() != 8) fail("invalid RocksDB committed high water");
        high_water_ = from_be(high);
        sync_directory(path_);
    }
    bool finished() const { return finished_; }
    std::uint64_t high_water() const { return high_water_; }
    void commit(const SegmentEntry& entry, const SymbolRanges& ranges, const StoreOptions& options,
                std::uint64_t canonical_bytes) {
        if (finished_) fail("RocksDB segment index is finished; reopen before writing");
        rocksdb::WriteBatch batch;
        std::uint64_t puts = 0, key_bytes = 0, value_bytes = 0;
        const auto put = [&](const std::string& key, const std::string& value) {
            check(batch.Put(key, value), "batch Put");
            ++puts; key_bytes += key.size(); value_bytes += value.size();
        };
        put("S" + be(entry.offset), be(entry.length) + be(entry.records) + be(entry.crc, 4));
        for (const auto& [symbol, range] : ranges)
            put(range_prefix(symbol) + be(range.min) + be(entry.offset), be(range.max));
        put("H", be(entry.offset + entry.length));
        if (options.transaction_model == "heavy") {
            put("M" + be(entry.offset), be(entry.records) + be(entry.records) + be(0) + be(canonical_bytes));
            put("C" + be(entry.crc, 4) + be(entry.records) + be(entry.offset), "");
        }
        {
            Timer timer(metrics_, "rocksdb_write");
            check(db_->Write(write_options(), &batch), "atomic segment index WriteBatch");
        }
        high_water_ = entry.offset + entry.length;
        metrics_.inc("rocksdb_write_batches");
        metrics_.inc("rocksdb_put_ops", puts);
        metrics_.inc("rocksdb_key_bytes", key_bytes);
        metrics_.inc("rocksdb_value_bytes", value_bytes);
        metrics_.inc("rocksdb_writebatch_bytes", batch.GetDataSize());
        if (options.transaction_model == "heavy") metrics_.inc("metadata_kv_rows");
    }
    // Scan every logical key on reopen. Missing/extra ranges, stale high water,
    // unknown keys and orphan heavy metadata are corruption, never repair work.
    void recover(const std::function<void(const SegmentEntry&, SymbolRanges)>& visit) {
        std::map<std::uint64_t, SegmentEntry> entries;
        std::map<std::uint64_t, SymbolRanges> ranges;
        std::map<std::uint64_t, std::string> metadata;
        std::vector<std::string> crc_keys;
        std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_options()));
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            const auto key = it->key().ToString();
            const auto value = it->value().ToString();
            if (key.empty()) fail("invalid RocksDB empty index key");
            if (key == "F") { if (value != stamp()) fail("invalid RocksDB format stamp"); }
            else if (key == "H") { if (value != be(high_water_)) fail("invalid RocksDB high water"); }
            else if (key[0] == 'S') {
                const auto entry = decode_entry(key, value);
                entries.emplace(entry.offset, entry);
            } else if (key[0] == 'R') {
                std::string symbol; std::uint64_t offset, min, max;
                decode_range(key, value, symbol, offset, min, max);
                if (!ranges[offset].emplace(symbol, TimeRange{min, max}).second)
                    fail("duplicate RocksDB indexed segment symbol/time range");
            } else if (key[0] == 'M' && key.size() == 9 && value.size() == 32) {
                metadata.emplace(from_be(std::string_view(key).substr(1)), value);
            } else if (key[0] == 'C' && key.size() == 21 && value.empty()) crc_keys.push_back(key);
            else fail("unknown/malformed RocksDB segment index key");
        }
        check(it->status(), "recover iterator");
        std::uint64_t end = 0;
        for (const auto& [offset, entry] : entries) {
            if (offset != end) fail("noncontiguous indexed segment");
            auto found = ranges.find(offset);
            if (found == ranges.end()) fail("indexed segment symbol/time range is missing");
            visit(entry, std::move(found->second));
            ranges.erase(found);
            end = entry.offset + entry.length;
        }
        if (!ranges.empty()) fail("indexed segment symbol/time range references an unknown segment");
        if (end != high_water_) fail("RocksDB committed high water does not match complete segment index");
        for (const auto& [offset, value] : metadata) {
            const auto found = entries.find(offset);
            if (found == entries.end() || from_be(std::string_view(value).substr(0, 8)) != found->second.records ||
                from_be(std::string_view(value).substr(8, 8)) != found->second.records ||
                from_be(std::string_view(value).substr(16, 8)) != 0 ||
                from_be(std::string_view(value).substr(24, 8)) != found->second.length - 32 - 4 * found->second.records)
                fail("invalid RocksDB heavy batch metadata");
            const auto crc_key = "C" + be(found->second.crc, 4) + be(found->second.records) + be(offset);
            if (!std::binary_search(crc_keys.begin(), crc_keys.end(), crc_key))
                fail("missing RocksDB heavy CRC index");
        }
        for (const auto& key : crc_keys) {
            const auto offset = from_be(std::string_view(key).substr(13));
            const auto found = entries.find(offset);
            if (found == entries.end() || !metadata.count(offset) ||
                from_be(std::string_view(key).substr(1, 4)) != found->second.crc ||
                from_be(std::string_view(key).substr(5, 8)) != found->second.records)
                fail("invalid RocksDB heavy CRC index");
        }
    }
    void scan(const std::function<void(const SegmentEntry&)>& visit) const {
        std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_options()));
        std::uint64_t end = 0;
        for (it->Seek("S"); it->Valid() && it->key().starts_with("S"); it->Next()) {
            const auto entry = decode_entry(it->key().ToString(), it->value().ToString());
            if (entry.offset != end) fail("noncontiguous indexed segment");
            visit(entry);
            end += entry.length;
        }
        check(it->status(), "scan iterator");
        if (end != high_water_) fail("RocksDB committed high water mismatch");
    }
    std::vector<SegmentEntry> range(const std::string& symbol, std::uint64_t from, std::uint64_t to) const {
        const auto prefix = range_prefix(symbol);
        std::vector<SegmentEntry> result;
        std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_options()));
        for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
            std::string stored_symbol; std::uint64_t offset, min, max;
            const auto key = it->key().ToString();
            const auto value = it->value().ToString();
            decode_range(key, value, stored_symbol, offset, min, max);
            metrics_.inc("rocksdb_range_iterator_steps");
            metrics_.inc("rocksdb_range_read_bytes", key.size() + value.size());
            if (min > to) break;
            if (max >= from) result.push_back(decode_entry("S" + be(offset), get("S" + be(offset))));
        }
        check(it->status(), "range iterator");
        std::sort(result.begin(), result.end(), [](const auto& a, const auto& b) { return a.offset < b.offset; });
        return result;
    }
    void finish() {
        if (finished_) return;
        Timer all(metrics_, "rocksdb_finish");
        rocksdb::FlushOptions flush; flush.wait = true;
        { Timer timer(metrics_, "rocksdb_flush"); check(db_->Flush(flush), "finish Flush(wait)"); }
        rocksdb::WaitForCompactOptions wait; wait.flush = true;
        { Timer timer(metrics_, "rocksdb_wait_compact"); check(db_->WaitForCompact(wait), "finish WaitForCompact"); }
        { Timer timer(metrics_, "rocksdb_sync_wal"); check(db_->SyncWAL(), "finish SyncWAL"); }
        finish_state_ = background_json();
        { Timer timer(metrics_, "rocksdb_close"); check(db_->Close(), "finish Close"); db_.reset(); }
        finished_ = true;
        // Export is read-only inspection, never the point that persists an index.
        // Reopen is also timed and preserves the append-file writer lock.
        options_.create_if_missing = false;
        check(rocksdb::DB::OpenForReadOnly(options_, path_.string(), &db_), "finish reopen read-only");
    }
    std::string info_json() const {
        return "{\"version\":" + json_quote(rocksdb::GetRocksVersionAsString()) +
            ",\"format_stamp\":" + json_quote(stamp()) +
            ",\"write_options\":{\"sync\":true,\"disableWAL\":false,\"use_fsync\":true},"
            "\"compression\":\"none\",\"wal_recovery_mode\":\"absolute_consistency\","
            "\"sync_source\":\"RocksDB POSIX use_fsync=true; on macOS a HAVE_FULLFSYNC build uses fcntl(F_FULLFSYNC); otherwise fsync; build capability must be disclosed, not a hardware PLP claim\","
            "\"finish_contract\":\"Flush(wait), WaitForCompact(flush), SyncWAL, Close, read-only reopen all inside finish timing\","
            "\"finish_called\":" + std::string(finished_ ? "true" : "false") +
            ",\"finish_background_state\":" + finish_state_ + ",\"background_state\":" + background_json() + "}";
    }
private:
    fs::path path_;
    Metrics& metrics_;
    rocksdb::Options options_;
    std::unique_ptr<rocksdb::DB> db_;
    std::uint64_t high_water_ = 0;
    bool finished_ = false;
    std::string finish_state_ = "null";
    static std::string stamp() { return "tickshelf-bench/segment-rocksdb/v1/TSG1"; }
    static void check(const rocksdb::Status& status, const std::string& operation) {
        if (!status.ok()) fail("RocksDB " + operation + ": " + status.ToString());
    }
    static rocksdb::ReadOptions read_options() { rocksdb::ReadOptions options; options.verify_checksums = true; return options; }
    static rocksdb::WriteOptions write_options() {
        rocksdb::WriteOptions options; options.sync = true; options.disableWAL = false; return options;
    }
    static std::string be(std::uint64_t value, unsigned count = 8) {
        std::string result(count, '\0');
        for (unsigned i = 0; i < count; ++i) result[i] = static_cast<char>(value >> (8 * (count - i - 1)));
        return result;
    }
    static std::uint64_t from_be(std::string_view value) {
        if (value.empty() || value.size() > 8) fail("invalid RocksDB unsigned integer");
        std::uint64_t result = 0;
        for (unsigned char c : value) result = (result << 8) | c;
        return result;
    }
    static std::string range_prefix(std::string_view symbol) {
        if (symbol.size() > 65535) fail("symbol exceeds canonical header bounds");
        return "R" + be(symbol.size(), 2) + std::string(symbol);
    }
    static SegmentEntry decode_entry(std::string_view key, std::string_view value) {
        if (key.size() != 9 || key[0] != 'S' || value.size() != 20) fail("invalid RocksDB segment entry");
        const SegmentEntry entry{from_be(key.substr(1)), from_be(value.substr(0, 8)),
                                 from_be(value.substr(8, 8)), static_cast<std::uint32_t>(from_be(value.substr(16, 4)))};
        if (entry.length < 32 || entry.length > 32 + kMaxSegmentPayloadBytes || !entry.records ||
            entry.records > (entry.length - 32) / 4 || entry.offset > std::uint64_t(std::numeric_limits<off_t>::max()) - entry.length)
            fail("invalid indexed segment bounds");
        return entry;
    }
    static void decode_range(std::string_view key, std::string_view value, std::string& symbol,
                             std::uint64_t& offset, std::uint64_t& min, std::uint64_t& max) {
        if (key.size() < 19 || key[0] != 'R' || value.size() != 8) fail("invalid indexed segment symbol/time range");
        const auto size = from_be(key.substr(1, 2));
        if (key.size() != 19 + size) fail("invalid indexed segment symbol/time range key length");
        symbol.assign(key.substr(3, size));
        min = from_be(key.substr(3 + size, 8)); offset = from_be(key.substr(11 + size, 8)); max = from_be(value);
        if (min > max) fail("invalid indexed segment symbol/time range bounds");
    }
    std::string get(const std::string& key) const {
        std::string value;
        metrics_.inc("rocksdb_get_calls");
        check(db_->Get(read_options(), key, &value), "Get required index key");
        return value;
    }
    std::string background_json() const {
        if (!db_) return "null";
        std::string out = "{";
        bool comma = false;
        for (const char* property : {"rocksdb.num-running-flushes", "rocksdb.num-running-compactions",
                "rocksdb.mem-table-flush-pending", "rocksdb.compaction-pending", "rocksdb.estimate-pending-compaction-bytes",
                "rocksdb.cur-size-all-mem-tables", "rocksdb.total-sst-files-size", "rocksdb.estimate-live-data-size"}) {
            std::uint64_t value = 0;
            const bool known = db_->GetIntProperty(property, &value);
            if (comma) out += ',';
            comma = true;
            out += json_quote(property) + ":" + (known ? std::to_string(value) : "null");
        }
        return out + "}";
    }
};
#endif

class SegmentStore final : public StoreBase {
public:
    SegmentStore(const StoreOptions& options, Metrics& metrics)
        : StoreBase(options, metrics), directory_(prepare_directory(options.backend == "segment_rocksdb" ?
              (fs::path(options.directory) / "segment-rocksdb-v1").string() : options.directory)),
          data_(::open((directory_ / "records.segments").c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600)) {
        const auto index_path = open_index(); // Takes lifetime append/recovery lock before either index is opened.
#ifdef TICKSHELF_HAVE_ROCKSDB
        if (options.backend == "segment_rocksdb") {
            rocks_ = std::make_unique<RocksSegmentIndex>(directory_ / "index.rocksdb", data_size() != 0, metrics_);
            sync_directory(directory_);
            recover();
            return;
        }
#endif
        if (data_size() != 0 && !fs::exists(index_path)) fail("missing SQLite segment index for existing data");
        db_ = std::make_unique<Database>(index_path, options, 2);
        if (data_size() != 0 && db_->scalar_int("SELECT count(*) FROM sqlite_master WHERE "
                "(type='table' AND name IN ('segments','segment_symbols')) OR "
                "(type='index' AND name='segment_symbols_lookup')") != 3)
            fail("indexed segment symbol/time range schema/index is missing");
        db_->exec("CREATE TABLE IF NOT EXISTS segments(offset INTEGER PRIMARY KEY CHECK(offset>=0),"
                 " length INTEGER NOT NULL CHECK(length>=32), records INTEGER NOT NULL CHECK(records>0),"
                 " crc INTEGER NOT NULL CHECK(crc>=0 AND crc<=4294967295)) WITHOUT ROWID");
        db_->exec("CREATE TABLE IF NOT EXISTS segment_symbols("
                 "segment_offset INTEGER NOT NULL, symbol TEXT NOT NULL,"
                 "min_time BLOB NOT NULL CHECK(typeof(min_time)='blob' AND length(min_time)=8),"
                 "max_time BLOB NOT NULL CHECK(typeof(max_time)='blob' AND length(max_time)=8),"
                 "PRIMARY KEY(segment_offset,symbol), CHECK(min_time<=max_time),"
                 "FOREIGN KEY(segment_offset) REFERENCES segments(offset)) WITHOUT ROWID");
        db_->exec("CREATE INDEX IF NOT EXISTS segment_symbols_lookup ON segment_symbols(symbol,min_time,max_time)");
        if (options_.transaction_model == "heavy") {
            db_->exec("CREATE INDEX IF NOT EXISTS segments_crc_records ON segments(crc,records)");
            db_->exec(kHeavySchema);
        }
        if (options_.reuse_statement) {
            db_->prepare(insert_, insert_sql(), &metrics_);
            db_->prepare(symbol_insert_, symbol_insert_sql(), &metrics_);
            if (options_.transaction_model == "heavy") db_->prepare(metadata_, kHeavyInsert, &metrics_);
        }
        sync_directory(directory_);
        recover();
    }
    bool write(const std::vector<EncodedRecord>& records, std::string& error) override {
        error.clear();
        attempt(records.size());
        bool transaction = false;
        try {
            if (poisoned_) fail("segment store requires reopen after a failed write");
#ifdef TICKSHELF_HAVE_ROCKSDB
            if (rocks_ && rocks_->finished()) fail("RocksDB segment index is finished; reopen before writing");
#endif
            const auto canonical_bytes = validate_batch(records, metrics_);
            if (records.empty()) return true;
            SymbolRanges ranges;
            {
                Timer timer(metrics_, "sparse_index_build");
                for (const auto& record : records) add_range(ranges, record.symbol, record.time);
            }
            const auto payload_size = canonical_bytes + records.size() * 4;
            if (end_ > std::uint64_t(std::numeric_limits<off_t>::max()) - payload_size - kSegmentHeaderBytes)
                fail("segment file offset overflow");
            // Reuse one allocation across batches. Reserve the header in-place,
            // append the canonical payload once, then backfill its final CRC.
            auto& frame = frame_;
            frame.clear();
            frame.reserve(payload_size + kSegmentHeaderBytes);
            frame.resize(kSegmentHeaderBytes, '\0');
            for (const auto& record : records) {
                append_le(frame, record.bytes.size(), 4);
                frame += record.bytes;
            }
            const SegmentEntry entry{end_, frame.size(), records.size(),
                                     crc32(std::string_view(frame).substr(kSegmentHeaderBytes))};
            std::memcpy(frame.data(), "TSG1", 4);
            const auto put = [&](std::size_t offset, std::uint64_t value, unsigned count) {
                for (unsigned i = 0; i < count; ++i) frame[offset + i] = static_cast<char>(value >> (8 * i));
            };
            put(4, 1, 4);
            put(8, payload_size, 8);
            put(16, records.size(), 8);
            put(24, entry.crc, 4);
            // Any error from here leaves a possibly written tail. Never erase it
            // against an uncertain commit result: recovery uses the durable index.
            poisoned_ = true;
            duplicates_.reset();
            {
                Timer timer(metrics_, "file_append");
                std::size_t done = 0;
                while (done < frame.size()) {
                    metrics_.inc("file_write_calls");
                    const auto n = ::pwrite(data_.fd, frame.data() + done, frame.size() - done,
                                            static_cast<off_t>(end_ + done));
                    if (n < 0) { if (errno == EINTR) continue; system_failure("append segment"); }
                    if (n == 0) fail("zero-byte segment append");
                    done += static_cast<std::size_t>(n);
                    metrics_.inc("file_write_bytes", static_cast<std::uint64_t>(n));
                }
            }
            crash_at("after_append");
            sync_file(data_.fd, metrics_);
            crash_at("after_sync");
            {
                Timer index_timer(metrics_, "index_commit");
#ifdef TICKSHELF_HAVE_ROCKSDB
                if (rocks_) rocks_->commit(entry, ranges, options_, canonical_bytes);
                else
#endif
                {
                { Timer timer(metrics_, "sqlite_begin"); metrics_.inc("sqlite_begin_calls"); metrics_.inc("sqlite_exec_calls"); db_->exec("BEGIN IMMEDIATE"); }
                transaction = true;
                Statement local;
                auto& statement = options_.reuse_statement ? insert_ : local;
                if (!statement.ptr) db_->prepare(statement, insert_sql(), &metrics_);
                bind_int(*db_, statement.ptr, 1, entry.offset, &metrics_);
                bind_int(*db_, statement.ptr, 2, entry.length, &metrics_);
                bind_int(*db_, statement.ptr, 3, entry.records, &metrics_);
                bind_int(*db_, statement.ptr, 4, entry.crc, &metrics_);
                step_done(*db_, statement.ptr, metrics_);
                reset_statement(*db_, statement.ptr, &metrics_);
                Statement local_symbols;
                auto& symbols = options_.reuse_statement ? symbol_insert_ : local_symbols;
                if (!symbols.ptr) db_->prepare(symbols, symbol_insert_sql(), &metrics_);
                for (const auto& [symbol, range] : ranges) {
                    bind_int(*db_, symbols.ptr, 1, entry.offset, &metrics_);
                    bind_symbol(*db_, symbols.ptr, 2, symbol, &metrics_);
                    bind_time(*db_, symbols.ptr, 3, range.min, &metrics_);
                    bind_time(*db_, symbols.ptr, 4, range.max, &metrics_);
                    step_done(*db_, symbols.ptr, metrics_);
                    reset_statement(*db_, symbols.ptr, &metrics_);
                }
                write_batch_metadata(*db_, metadata_, options_, metrics_, records.size(), records.size(), 0, canonical_bytes);
                { Timer timer(metrics_, "sqlite_commit"); metrics_.inc("sqlite_commit_calls"); metrics_.inc("sqlite_exec_calls"); db_->exec("COMMIT"); }
                transaction = false;
                }
            }
            crash_at("after_index");
            end_ += entry.length;
            ++segments_;
            symbol_rows_ += ranges.size();
            metrics_.inc("index_rows");
            metrics_.inc("symbol_index_rows", ranges.size());
            committed(records.size(), 0, canonical_bytes);
            poisoned_ = false;
            return true;
        } catch (const std::exception& e) {
            cleanup_statement(insert_.ptr, metrics_);
            cleanup_statement(symbol_insert_.ptr, metrics_);
            cleanup_statement(metadata_.ptr, metrics_);
            if (transaction) { metrics_.inc("sqlite_rollback_calls"); metrics_.inc("sqlite_exec_calls"); db_->rollback(); }
            error = e.what();
            return false;
        }
    }
    bool finish(std::string& error) override {
        error.clear();
        try {
            if (poisoned_) fail("segment store requires reopen after a failed write");
            // Every successful write already synced the file before its FULL
            // index commit. Checkpointing is separately timed, not the ack point.
#ifdef TICKSHELF_HAVE_ROCKSDB
            if (rocks_) rocks_->finish();
            else
#endif
                db_->checkpoint(metrics_);
            return true;
        } catch (const std::exception& e) { poisoned_ = true; error = e.what(); return false; }
    }
    bool export_rows(std::ostream& out, std::string& error) override {
        error.clear();
        try {
            if (poisoned_) fail("segment store requires reopen after a failed write");
            std::set<std::string> seen;
            std::uint64_t duplicates = 0, rows = 0;
            scan_index([&](const SegmentEntry& entry) {
                const auto payload = verified_payload(entry);
                visit_records(entry, payload, [&](const Record& record, std::string_view bytes) {
                    if (!seen.emplace(bytes).second) ++duplicates;
                    ++rows;
                    emit_row(out, record, bytes);
                });
            });
            if (!out) fail("failed to export store rows");
            duplicates_ = duplicates;
            metrics_.counters["segment_export_physical_rows"] = rows;
            metrics_.counters["segment_duplicate_physical_rows"] = duplicates;
            return true;
        } catch (const std::exception& e) { error = e.what(); return false; }
    }
    bool export_range(const std::string& symbol, std::uint64_t from, std::uint64_t to,
                      std::ostream& out, std::string& error) override {
        error.clear();
        try {
            Timer timer(metrics_, "export_range");
            if (poisoned_) fail("segment store requires reopen after a failed write");
            if (from > to) fail("range lower bound exceeds upper bound");
            if (!out) fail("failed to export store range");
            std::set<std::string> seen;
            const auto emit = [&](const SegmentEntry& entry) {
                metrics_.inc("range_candidate_segments");
                const auto payload = verified_payload(entry);
                visit_records(entry, payload, [&](const Record& record, std::string_view bytes) {
                    metrics_.inc("range_decoded_rows");
                    if (record.symbol == symbol && record.time >= from && record.time <= to &&
                        seen.emplace(bytes).second) {
                        emit_row(out, record, bytes);
                        metrics_.inc("range_emitted_rows");
                    }
                });
            };
#ifdef TICKSHELF_HAVE_ROCKSDB
            if (rocks_) {
                for (const auto& entry : rocks_->range(symbol, from, to)) emit(entry);
            } else
#endif
            {
                Statement stmt;
                db_->prepare(stmt, "SELECT s.offset,s.length,s.records,s.crc "
                                  "FROM segment_symbols AS r INDEXED BY segment_symbols_lookup "
                                  "JOIN segments AS s ON s.offset=r.segment_offset "
                                  "WHERE r.symbol=?1 AND r.min_time<=?3 AND r.max_time>=?2 ORDER BY s.offset");
                bind_symbol(*db_, stmt.ptr, 1, symbol);
                bind_time(*db_, stmt.ptr, 2, from);
                bind_time(*db_, stmt.ptr, 3, to);
                int rc;
                while ((rc = sqlite3_step(stmt.ptr)) == SQLITE_ROW) emit(entry_from_row(stmt.ptr));
                if (rc != SQLITE_DONE) db_->db_fail("select segment symbol/time ranges");
            }
            if (!out) fail("failed to export store range");
            return true;
        } catch (const std::exception& e) { error = e.what(); return false; }
    }
    std::string info_json() const override {
        const bool rocks = options_.backend == "segment_rocksdb";
        std::string index_info = db_ ? ",\"sqlite\":" + db_->info_json() : "";
#ifdef TICKSHELF_HAVE_ROCKSDB
        if (rocks_) index_info = ",\"rocksdb\":" + rocks_->info_json();
#endif
        return "{\"backend\":" + json_quote(options_.backend) +
            ",\"durable\":true,\"physical_power_loss_tested\":false,\"hardware_plp\":\"unknown\","
            "\"durability_scope\":" + json_quote(rocks ?
                "append then OS file sync then atomic RocksDB WriteBatch(sync=true,disableWAL=false,use_fsync=true); no physical PLP claim" :
                "append then OS file sync then SQLite FULL segment-index commit; no physical PLP claim") +
            ",\"sync_method\":" + json_quote(
#if defined(__linux__)
                "fdatasync"
#else
                "fsync"
#endif
            ) + ",\"directory_sync\":\"fsync on new directory/file creation\",\"data_file\":" +
            json_quote((directory_ / "records.segments").string()) + ",\"database\":" +
            json_quote((directory_ / (rocks ? "index.rocksdb" : "segment-index.sqlite3")).string()) +
            ",\"transaction_model\":" + json_quote(options_.transaction_model) +
            ",\"reuse_statement\":" + (options_.reuse_statement ? "true" : "false") +
            ",\"statement_reuse_scope\":" + json_quote(options_.reuse_statement ?
                "store lifetime, prepared at open; reset/rebind within each batch" :
                "one write transaction, prepared once per statement kind; reset/rebind within each batch") +
            ",\"prepare_metric_scope\":\"sqlite_prepare measures write-statement prepares only, including open-time prepares for lifetime reuse; query/recovery/schema prepares excluded\"" +
            ",\"sqlite_api_count_scope\":\"library API calls, NOT syscalls/kernel calls; begin/commit count sqlite3_exec calls in the same scope as corresponding Timer counts; prepare/step count their existing write-only Timer scope; bind/reset/clear_bindings/changes and error rollback/cleanup count workload API invocations; schema, recovery and export APIs excluded; no per-record JSON logging\"" +
            ",\"format\":\"TSG1; u32 version=1,u64 payload length,u64 record count,u32 CRC32(payload),u32 reserved=0; little endian; payload repeats u32 length + canonical RCD1\","
            "\"deduplication\":\"none at ingress; export retains physical duplicates for logical oracle\","
            "\"index_granularity\":\"one segment row plus one symbol/min-max row per distinct symbol per segment; not a per-record index\","
            "\"sparse_index\":" + json_quote(rocks ?
                "S+offsetBE64 metadata; R+symbolLengthBE16+symbol+minBE64+offsetBE64 maps to maxBE64; H committed endBE64; F format; logical ranges equal SQLite rows" :
                "segment_symbols(segment_offset,symbol,min_time,max_time) WITHOUT ROWID plus (symbol,min_time,max_time) lookup index; time is uint64 big-endian BLOB") +
            ",\"heavy_index_cost\":" + json_quote(rocks ?
                "one M+offset batch-metadata KV plus C+crc+recordcount+offset secondary KV per heavy batch; not physically equivalent to SQL heavy" :
                "segments_crc_records SQL secondary index plus write_batches metadata row") +
            ",\"statement_reuse_applicable\":" + (rocks ? "false" : "true") +
            ",\"range_export\":\"inclusive symbol/time sparse lookup, candidate segment CRC/decode/filter, exact canonical dedup per call\","
            "\"duplicate_count_scope\":\"exact canonical payload comparison during most recent full export_rows; invalidated by writes; range export does not set this total\","
            "\"physical_duplicate_rows\":" + (duplicates_ ? std::to_string(*duplicates_) : "null") +
            ",\"physical_rows\":" + std::to_string(physical_rows_) + ",\"segments\":" + std::to_string(segments_) +
            ",\"symbol_index_rows\":" + std::to_string(symbol_rows_) +
            ",\"requires_reopen\":" + (poisoned_ ? "true" : "false") +
            ",\"indexed_file_bytes\":" + std::to_string(end_) + ",\"recovered_tail_bytes\":" + std::to_string(recovered_tail_) +
            ",\"metric_scope\":\"file_write_calls/bytes are segment pwrite syscalls/results; SQLite steps/rows/transactions are API counts, not filesystem writes\","
            "\"kv_metric_scope\":\"rocksdb_put_ops/key_bytes/value_bytes/writebatch_bytes are successful API batch logical counts/bytes, not device writes; includes S/R/H and heavy M/C, excludes one-time F/H initialization; actual LSM costs in rocksdb.background_state\","
            "\"counters\":" + counts_json() + index_info + "}";
    }
private:
    fs::path directory_;
    File data_;
#ifdef TICKSHELF_HAVE_ROCKSDB
    std::unique_ptr<RocksSegmentIndex> rocks_;
#endif
    std::unique_ptr<Database> db_;
    Statement insert_, symbol_insert_, metadata_;
    std::uint64_t end_ = 0, segments_ = 0, symbol_rows_ = 0, recovered_tail_ = 0;
    bool poisoned_ = false;
    std::string frame_;
    std::optional<std::uint64_t> duplicates_;

    std::uint64_t data_size() const {
        struct stat status{};
        if (::fstat(data_.fd, &status) < 0) system_failure("stat segment file");
        if (!S_ISREG(status.st_mode) || status.st_size < 0) fail("segment data is not a regular file");
        return static_cast<std::uint64_t>(status.st_size);
    }
    fs::path open_index() {
        if (data_.fd < 0) system_failure("open segment file");
        if (::flock(data_.fd, LOCK_EX | LOCK_NB) < 0) system_failure("lock segment file (single writer required)");
        return directory_ / "segment-index.sqlite3";
    }
    static const char* insert_sql() {
        return "INSERT INTO segments(offset,length,records,crc) VALUES(?1,?2,?3,?4)";
    }
    static const char* symbol_insert_sql() {
        return "INSERT INTO segment_symbols(segment_offset,symbol,min_time,max_time) VALUES(?1,?2,?3,?4)";
    }
    void crash_at(std::string_view point) const {
        if (options_.failpoint == point) {
            // Only this process is targeted; a failpoint is never a parent or
            // shared-process kill. SIGKILL deliberately bypasses all destructors.
            ::raise(SIGKILL);
            ::_exit(137);
        }
    }
    static SegmentEntry entry_from_row(sqlite3_stmt* statement) {
        for (int i = 0; i < 4; ++i)
            if (sqlite3_column_type(statement, i) != SQLITE_INTEGER || sqlite3_column_int64(statement, i) < 0)
                fail("invalid indexed segment integer");
        const auto offset = static_cast<std::uint64_t>(sqlite3_column_int64(statement, 0));
        const auto length = static_cast<std::uint64_t>(sqlite3_column_int64(statement, 1));
        const auto records = static_cast<std::uint64_t>(sqlite3_column_int64(statement, 2));
        const auto crc = static_cast<std::uint64_t>(sqlite3_column_int64(statement, 3));
        if (length < kSegmentHeaderBytes || length > kSegmentHeaderBytes + kMaxSegmentPayloadBytes ||
            records == 0 || records > (length - kSegmentHeaderBytes) / 4 || crc > 0xffffffffULL ||
            offset > std::uint64_t(std::numeric_limits<off_t>::max()) - length)
            fail("invalid indexed segment bounds");
        return {offset, length, records, static_cast<std::uint32_t>(crc)};
    }
    template<class Visitor> void scan_index(Visitor visit) const {
#ifdef TICKSHELF_HAVE_ROCKSDB
        if (rocks_) { rocks_->scan(visit); return; }
#endif
        Statement stmt;
        db_->prepare(stmt, "SELECT offset,length,records,crc FROM segments ORDER BY offset");
        std::uint64_t expected_offset = 0;
        int rc;
        while ((rc = sqlite3_step(stmt.ptr)) == SQLITE_ROW) {
            const auto entry = entry_from_row(stmt.ptr);
            if (entry.offset != expected_offset) fail("noncontiguous indexed segment");
            visit(entry);
            expected_offset += entry.length;
        }
        if (rc != SQLITE_DONE) db_->db_fail("scan segment index");
    }
    std::string verified_payload(const SegmentEntry& entry) const {
        std::string header(kSegmentHeaderBytes, '\0');
        read_at(data_.fd, entry.offset, header);
        if (std::string_view(header).substr(0, 4) != "TSG1" || read_le(header, 4, 4) != 1 ||
            read_le(header, 8, 8) != entry.length - kSegmentHeaderBytes ||
            read_le(header, 16, 8) != entry.records || read_le(header, 24, 4) != entry.crc ||
            read_le(header, 28, 4) != 0)
            fail("indexed segment header/version/length/count/CRC metadata mismatch");
        std::string payload(entry.length - kSegmentHeaderBytes, '\0');
        read_at(data_.fd, entry.offset + kSegmentHeaderBytes, payload);
        if (crc32(payload) != entry.crc) fail("indexed segment CRC mismatch");
        return payload;
    }
    template<class Visitor> static void visit_records(const SegmentEntry& entry, std::string_view payload, Visitor visit) {
        std::size_t offset = 0;
        std::uint64_t rows = 0;
        while (offset < payload.size()) {
            if (payload.size() - offset < 4) fail("indexed segment truncated record length");
            const auto size = read_le(payload, offset, 4);
            offset += 4;
            if (size == 0 || size > kMaxRecordBytes || size > payload.size() - offset)
                fail("indexed segment invalid record length");
            const auto bytes = payload.substr(offset, size);
            Record record;
            std::string error;
            if (!decode_record(bytes, record, error)) fail("indexed segment record decode: " + error);
            visit(record, bytes);
            offset += size;
            ++rows;
        }
        if (rows != entry.records) fail("indexed segment record count mismatch");
    }
    std::uint64_t verify_ranges(const SegmentEntry& entry, SymbolRanges expected, Statement& statement) const {
        bind_int(*db_, statement.ptr, 1, entry.offset);
        std::uint64_t rows = 0;
        int rc;
        while ((rc = sqlite3_step(statement.ptr)) == SQLITE_ROW) {
            if (sqlite3_column_type(statement.ptr, 0) != SQLITE_TEXT ||
                sqlite3_column_type(statement.ptr, 1) != SQLITE_BLOB ||
                sqlite3_column_type(statement.ptr, 2) != SQLITE_BLOB)
                fail("indexed segment symbol/time range has invalid types");
            const auto* text = reinterpret_cast<const char*>(sqlite3_column_text(statement.ptr, 0));
            if (!text) fail("indexed segment symbol/time range has null symbol");
            const std::string_view symbol(text, sqlite3_column_bytes(statement.ptr, 0));
            const auto min = read_time_key(sqlite3_column_blob(statement.ptr, 1), sqlite3_column_bytes(statement.ptr, 1));
            const auto max = read_time_key(sqlite3_column_blob(statement.ptr, 2), sqlite3_column_bytes(statement.ptr, 2));
            const auto found = expected.find(symbol);
            if (found == expected.end() || found->second.min != min || found->second.max != max)
                fail("indexed segment symbol/time range does not match complete segment contents");
            expected.erase(found);
            ++rows;
        }
        if (rc != SQLITE_DONE) db_->db_fail("validate segment symbol/time ranges");
        reset_statement(*db_, statement.ptr);
        if (!expected.empty()) fail("indexed segment symbol/time range is missing");
        return rows;
    }
    void recover() {
        struct stat status{};
        if (::fstat(data_.fd, &status) < 0) system_failure("stat segment file");
        if (!S_ISREG(status.st_mode) || status.st_size < 0) fail("segment data is not a regular file");
#ifdef TICKSHELF_HAVE_ROCKSDB
        if (rocks_) {
            rocks_->recover([&](const SegmentEntry& entry, SymbolRanges stored) {
                const auto payload = verified_payload(entry);
                SymbolRanges expected;
                visit_records(entry, payload, [&](const Record& record, std::string_view) {
                    add_range(expected, record.symbol, record.time);
                });
                if (expected.size() != stored.size()) fail("indexed segment symbol/time range is missing or extra");
                for (const auto& [symbol, range] : expected) {
                    const auto found = stored.find(symbol);
                    if (found == stored.end() || found->second.min != range.min || found->second.max != range.max)
                        fail("indexed segment symbol/time range does not match complete segment contents");
                }
                symbol_rows_ += stored.size();
                end_ = entry.offset + entry.length;
                physical_rows_ += entry.records;
                ++segments_;
            });
        } else
#endif
        {
        if (db_->scalar_int("SELECT count(*) FROM segment_symbols AS r LEFT JOIN segments AS s "
                           "ON s.offset=r.segment_offset WHERE s.offset IS NULL") != 0)
            fail("indexed segment symbol/time range references an unknown segment");
        Statement range_statement;
        db_->prepare(range_statement, "SELECT symbol,min_time,max_time FROM segment_symbols "
                                     "WHERE segment_offset=?1 ORDER BY symbol");
        scan_index([&](const SegmentEntry& entry) {
            const auto payload = verified_payload(entry);
            SymbolRanges ranges;
            visit_records(entry, payload, [&](const Record& record, std::string_view) {
                add_range(ranges, record.symbol, record.time);
            });
            symbol_rows_ += verify_ranges(entry, std::move(ranges), range_statement);
            end_ = entry.offset + entry.length;
            physical_rows_ += entry.records;
            ++segments_;
        });
        }
        if (static_cast<std::uint64_t>(status.st_size) < end_) fail("indexed segment file is truncated");
        // Never truncate until ALL indexed segments have been validated. A bad
        // indexed segment is fatal, even if a later orphan would be disposable.
        recovered_tail_ = static_cast<std::uint64_t>(status.st_size) - end_;
        if (recovered_tail_) {
            if (::ftruncate(data_.fd, static_cast<off_t>(end_)) < 0) system_failure("truncate unindexed segment tail");
            sync_file(data_.fd, metrics_);
            metrics_.inc("recovered_tail_bytes", recovered_tail_);
        }
    }
};

// ---- Day-run store (TSD1 v2): raw-first per-symbol runs, chunked reservations ----
//
// 用户方向（2026-11-20）：最大程度减少数据库操作——数据按 (UTC 日, symbol) 成段
// 追加，数据库只保存每 run 的区间元数据，元数据惰性落盘。
//
// 布局（v2 修正）：当天一个数据文件；每个 (symbol,day) run 由**chunk 列表**组成，
// chunk 从文件尾顺序分配、定长保留 kDayRunChunkBytes（超大块按需超过）。run 的帧
// 永不跨 chunk；run 增长出 chunk 时追加新 chunk。为什么必须 chunk 化：相邻连续
// region + 各自增长必然互相覆盖（v1 实测 CRC mismatch 的根因）；chunk 化后写入
// 只发生在自己的保留区内，重叠不可能。
//
// 帧格式（每条记录）：[u32 len][canonical payload][u32 crc32(payload)]，小端；
//   逐帧自校验。与 batch-segment 的关键区别：segment 以索引提交为提交点、丢弃
//   索引外尾部；day-run 的提交点是元数据 flush，恢复会把已 fsync 的有效帧
//   **采纳**进 run——重试重写同一批产生物理重复帧，由导出端精确去重并单独报告
//   （与 segment 后端同一 oracle 口径）。
//
// 写路径顺序（与 delivery-contract §4.1 同构）：
//   1. 按 (day, symbol) 分组（保留到达顺序），块 = 逐条帧；
//   2. 每 run 在自己当前 chunk 内追加（放不下 → 从当日分配指针领新 chunk）；
//   3. fsync 触碰过的天文件（每天一个文件，每批一次 fsync）；
//   4. 每 kDayMetadataFlushBatches 批或 finish() 时，一个事务 upsert 脏 run/chunk
//      与 day_alloc。提交点 = 元数据 flush；崩溃丢失窗口 = 未 flush 的批。
//   新 run/chunk 的分配不需要专门的持久事务：分配指针 day_alloc 随 flush 持久化，
//   崩溃后未 flush 的区域无元数据（视为死区，后续分配可复用），与合同一致。
//
// 恢复：加载元数据 → 每个 run 从"当前 chunk 的已写字节末尾"逐帧扫描（len 边界 +
// CRC + 归属 symbol + chunk 容量边界）→ 有效帧采纳（脏，随后持久化）；撕裂帧或
// chunk 末尾停止（chunk 内残余由下次写覆盖）。文件短于持久 chunk 位置 = 致命。
constexpr unsigned kDayMetadataFlushBatches = 8;
constexpr std::uint64_t kDayRunChunkBytes = 65536;

class DayRunStore final : public StoreBase {
public:
    DayRunStore(const StoreOptions& options, Metrics& metrics)
        : StoreBase(options, metrics),
          directory_(prepare_directory((fs::path(options.directory) / "segment-day-v1").string())),
          db_(std::make_unique<Database>(directory_ / "day-index.sqlite3", options, 1)) {
        // 元数据缺失但数据文件存在 = 未提交的数据无归属，拒绝打开。
        const bool had_metadata = db_->scalar_int(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='symbol_runs'") == 1;
        if (!had_metadata && any_day_file_present())
            fail("day-run data files exist without metadata tables");
        db_->exec("CREATE TABLE IF NOT EXISTS symbol_runs("
                  "symbol TEXT NOT NULL, day INTEGER NOT NULL,"
                  "records INTEGER NOT NULL CHECK(records>=0),"
                  "chunk INTEGER NOT NULL CHECK(chunk>=0),"
                  "min_time BLOB NOT NULL CHECK(typeof(min_time)='blob' AND length(min_time)=8),"
                  "max_time BLOB NOT NULL CHECK(typeof(max_time)='blob' AND length(max_time)=8),"
                  "PRIMARY KEY(symbol,day)) WITHOUT ROWID");
        db_->exec("CREATE TABLE IF NOT EXISTS run_chunks("
                  "symbol TEXT NOT NULL, day INTEGER NOT NULL, seq INTEGER NOT NULL CHECK(seq>=0),"
                  "offset INTEGER NOT NULL CHECK(offset>=0),"
                  "bytes INTEGER NOT NULL CHECK(bytes>=0),"
                  "PRIMARY KEY(symbol,day,seq)) WITHOUT ROWID");
        db_->exec("CREATE TABLE IF NOT EXISTS day_alloc("
                  "day INTEGER PRIMARY KEY CHECK(day>=0),"
                  "allocated_end INTEGER NOT NULL CHECK(allocated_end>=0)) WITHOUT ROWID");
        db_->prepare(flush_run_, "INSERT OR REPLACE INTO symbol_runs"
                                 "(symbol,day,records,chunk,min_time,max_time) "
                                 "VALUES(?1,?2,?3,?4,?5,?6)", &metrics_);
        db_->prepare(flush_chunk_, "INSERT OR REPLACE INTO run_chunks"
                                   "(symbol,day,seq,offset,bytes) VALUES(?1,?2,?3,?4,?5)", &metrics_);
        db_->prepare(flush_alloc_, "INSERT OR REPLACE INTO day_alloc(day,allocated_end) VALUES(?1,?2)",
                     &metrics_);
        load_runs();
        sync_directory(directory_);
        recover();
    }

    bool write(const std::vector<EncodedRecord>& records, std::string& error) override {
        error.clear();
        attempt(records.size());
        bool transaction = false;
        try {
            if (poisoned_) fail("day-run store requires reopen after a failed write");
            const auto canonical_bytes = validate_batch(records, metrics_);
            if (records.empty()) return true;

            // 按 (day, symbol) 分组（保留到达顺序）；块 = 逐条 [len][payload][crc]。
            std::map<RunKey, std::string> blocks;
            std::map<RunKey, std::uint64_t> block_counts;
            std::map<RunKey, std::pair<std::uint64_t, std::uint64_t>> block_bounds;
            {
                Timer timer(metrics_, "day_run_group");
                for (const auto& record : records) {
                    RunKey key{record.time / kDayMs, record.symbol};
                    auto [bounds_it, inserted] = block_bounds.try_emplace(
                        key, std::numeric_limits<std::uint64_t>::max(), 0);
                    bounds_it->second.first = std::min(bounds_it->second.first, record.time);
                    bounds_it->second.second = std::max(bounds_it->second.second, record.time);
                    ++block_counts[key];
                    auto& block = blocks[key];
                    append_le(block, record.bytes.size(), 4);
                    block += record.bytes;
                    append_le(block, crc32(record.bytes), 4);
                }
            }

            // 新 run 的首次分配立即持久（run 行 + 空 chunk 行 + day_alloc）：保证
            // 任何已写/待写数据都有已知归属，恢复扫描才有起点。仅在有新 run 的批
            // 发生；常规批（无新 symbol/day）零额外事务。
            bool fresh = false;
            for (const auto& [key, block] : blocks) {
                if (runs_.count(key)) continue;
                Run& run = runs_[key];
                current_chunk(run, key);
                run.dirty = true;
                dirty_.insert(key);
                fresh = true;
            }
            if (fresh) {
                { Timer timer(metrics_, "index_commit"); metrics_.inc("sqlite_begin_calls"); metrics_.inc("sqlite_exec_calls"); db_->exec("BEGIN IMMEDIATE"); }
                transaction = true;
                flush_metadata();
                { metrics_.inc("sqlite_commit_calls"); metrics_.inc("sqlite_exec_calls"); db_->exec("COMMIT"); }
                transaction = false;
                batches_since_flush_ = 0;
                dirty_.clear();
            }

            poisoned_ = true;
            duplicates_.reset();
            std::set<std::uint64_t> touched;
            {
                Timer timer(metrics_, "file_append");
                for (const auto& [key, block] : blocks) {
                    Run& run = runs_.at(key);
                    Chunk& chunk = current_chunk(run, key);
                    if (chunk.bytes + block.size() > chunk.capacity) {
                        // 块永不跨 chunk：放不下就领新 chunk（分配只在内存，随 flush 持久）。
                        const std::uint64_t capacity =
                            std::max<std::uint64_t>(kDayRunChunkBytes, block.size());
                        run.chunks.push_back(Chunk{allocated_end_[key.first], 0, capacity});
                        run.current_chunk = static_cast<unsigned>(run.chunks.size() - 1);
                        allocated_end_[key.first] += capacity;
                    }
                    Chunk& target = run.chunks[run.current_chunk];
                    write_block(day_file(key.first).fd, target.offset + target.bytes, block);
                    target.bytes += block.size();
                    run.records += block_counts.at(key);
                    run.min_time = std::min(run.min_time, block_bounds.at(key).first);
                    run.max_time = std::max(run.max_time, block_bounds.at(key).second);
                    run.dirty = true;
                    dirty_.insert(key);
                    touched.insert(key.first);
                }
            }
            crash_at("after_append");
            for (const auto day : touched) sync_file(day_file(day).fd, metrics_);
            crash_at("after_sync");

            // 元数据惰性 flush：提交点。窗口内的批允许丢失；恢复采纳已 fsync 帧。
            if (++batches_since_flush_ >= kDayMetadataFlushBatches) {
                { Timer timer(metrics_, "index_commit"); metrics_.inc("sqlite_begin_calls"); metrics_.inc("sqlite_exec_calls"); db_->exec("BEGIN IMMEDIATE"); }
                transaction = true;
                flush_metadata();
                { metrics_.inc("sqlite_commit_calls"); metrics_.inc("sqlite_exec_calls"); db_->exec("COMMIT"); }
                transaction = false;
                batches_since_flush_ = 0;
                dirty_.clear();
                crash_at("after_metadata");
            }
            committed(records.size(), 0, canonical_bytes);
            poisoned_ = false;
            return true;
        } catch (const std::exception& e) {
            cleanup_statement(flush_run_.ptr, metrics_);
            cleanup_statement(flush_chunk_.ptr, metrics_);
            cleanup_statement(flush_alloc_.ptr, metrics_);
            if (transaction) { metrics_.inc("sqlite_rollback_calls"); metrics_.inc("sqlite_exec_calls"); db_->rollback(); }
            error = e.what();
            return false;
        }
    }

    bool finish(std::string& error) override {
        error.clear();
        try {
            if (poisoned_) fail("day-run store requires reopen after a failed write");
            // 全部脏 run/chunk 与分配指针落盘（数据早已逐批 fsync），再对天文件收尾同步。
            { Timer timer(metrics_, "index_commit"); metrics_.inc("sqlite_begin_calls"); metrics_.inc("sqlite_exec_calls"); db_->exec("BEGIN IMMEDIATE"); }
            flush_metadata();
            { metrics_.inc("sqlite_commit_calls"); metrics_.inc("sqlite_exec_calls"); db_->exec("COMMIT"); }
            batches_since_flush_ = 0;
            dirty_.clear();
            for (auto& [day, file] : day_files_) sync_file(file.fd, metrics_);
            db_->checkpoint(metrics_);
            return true;
        } catch (const std::exception& e) { poisoned_ = true; error = e.what(); return false; }
    }

    bool export_rows(std::ostream& out, std::string& error) override {
        error.clear();
        try {
            if (poisoned_) fail("day-run store requires reopen after a failed write");
            std::set<std::string> seen;
            std::uint64_t duplicates = 0, rows = 0;
            for (const auto& [key, run] : runs_) {
                if (!run.records) continue;
                visit_run(key, run, [&](const Record& record, std::string_view bytes) {
                    if (!seen.emplace(bytes).second) ++duplicates;
                    ++rows;
                    emit_row(out, record, bytes);
                });
            }
            if (!out) fail("failed to export store rows");
            duplicates_ = duplicates;
            metrics_.counters["segment_export_physical_rows"] = rows;
            metrics_.counters["segment_duplicate_physical_rows"] = duplicates;
            return true;
        } catch (const std::exception& e) { error = e.what(); return false; }
    }

    bool export_range(const std::string& symbol, std::uint64_t from, std::uint64_t to,
                      std::ostream& out, std::string& error) override {
        error.clear();
        try {
            Timer timer(metrics_, "export_range");
            if (poisoned_) fail("day-run store requires reopen after a failed write");
            if (symbol.size() > std::numeric_limits<std::uint16_t>::max())
                fail("symbol exceeds canonical header bounds");
            if (from > to) fail("range lower bound exceeds upper bound");
            if (!out) fail("failed to export store range");
            std::set<std::string> seen;
            // 直接定位该 symbol 的连续 run：读的是纯 symbol 数据，无跨 symbol 过滤浪费。
            for (const auto& [key, run] : runs_) {
                if (key.second != symbol || !run.records ||
                    run.min_time > to || run.max_time < from)
                    continue;
                metrics_.inc("range_candidate_segments");
                visit_run(key, run, [&](const Record& record, std::string_view bytes) {
                    metrics_.inc("range_decoded_rows");
                    if (record.time >= from && record.time <= to && seen.emplace(bytes).second) {
                        emit_row(out, record, bytes);
                        metrics_.inc("range_emitted_rows");
                    }
                });
            }
            if (!out) fail("failed to export store range");
            return true;
        } catch (const std::exception& e) { error = e.what(); return false; }
    }

    std::string info_json() const override {
        return "{\"backend\":" + json_quote(options_.backend) +
            ",\"durable\":true,\"physical_power_loss_tested\":false,\"hardware_plp\":\"unknown\","
            "\"durability_scope\":" + json_quote(
                "per-symbol raw frames appended then day-file fsync before deferred SQLite FULL metadata flush; "
                "commit point is the metadata flush; a crash may lose unflushed batches while valid fsynced frames "
                "are re-adopted on recovery; no physical PLP claim") +
            ",\"sync_method\":" + json_quote(
#if defined(__linux__)
                "fdatasync"
#else
                "fsync"
#endif
            ) + ",\"directory_sync\":\"fsync on new directory/file creation\",\"data_layout\":"
            + json_quote("one file per UTC day (day-<index>.runs); per-symbol runs grown inside sequentially "
                          "allocated " + std::to_string(kDayRunChunkBytes) + "-byte chunk reservations (frames never "
                          "straddle chunks; oversized blocks get a dedicated larger chunk); each record framed "
                          "[u32 len][canonical RCD1][u32 crc32] little-endian") +
            ",\"database\":" + json_quote((directory_ / "day-index.sqlite3").string()) +
            ",\"format\":\"TSD1\",\"metadata_flush_batches\":" + std::to_string(kDayMetadataFlushBatches) +
            ",\"run_chunk_bytes\":" + std::to_string(kDayRunChunkBytes) +
            ",\"index_granularity\":" + json_quote(
                "one metadata row per (symbol,day) run plus one row per run chunk (usually one) plus one "
                "day_alloc row per day, refreshed every " + std::to_string(kDayMetadataFlushBatches) +
                " batches; no per-record and no per-batch index rows") +
            ",\"loss_window\":" + json_quote("unflushed batches since the last metadata flush; valid fsynced frames "
                                              "beyond the durable position are adopted on recovery; a torn frame or chunk "
                                              "end stops the scan and residual bytes are overwritten by the next write") +
            ",\"write_pattern\":\"scattered pwrites, one per (symbol,day) block per batch — the honest cost of "
            "query-ordered layout versus one contiguous append per batch\","
            "\"deduplication\":\"none at ingress; export retains physical duplicates for logical oracle\","
            "\"range_export\":\"direct per-symbol run read (chunk list) then time filter; no cross-symbol scanning\","
            "\"duplicate_count_scope\":\"exact canonical payload comparison during most recent full export_rows; "
            "invalidated by writes; range export does not set this total\","
            "\"physical_duplicate_rows\":" + (duplicates_ ? std::to_string(*duplicates_) : "null") +
            ",\"physical_rows\":" + std::to_string(physical_rows_) +
            ",\"day_runs\":" + std::to_string(runs_.size()) +
            ",\"day_files\":" + std::to_string(day_files_.size()) +
            ",\"recovered_tail_records\":" + std::to_string(recovered_tail_records_) +
            ",\"recovered_tail_bytes\":" + std::to_string(recovered_tail_bytes_) +
            ",\"requires_reopen\":" + (poisoned_ ? "true" : "false") +
            ",\"metric_scope\":\"file_write_calls/bytes are per-symbol-block pwrite syscalls/results; SQLite "
            "steps/rows/transactions are API counts, not filesystem writes\"}";
    }

private:
    static constexpr std::uint64_t kDayMs = 86400000;
    struct Chunk {
        std::uint64_t offset = 0;     // chunk 起始（文件内）
        std::uint64_t bytes = 0;      // 已写帧字节
        std::uint64_t capacity = 0;   // 保留容量（≥kDayRunChunkBytes，超大块按需）
    };
    struct Run {
        std::vector<Chunk> chunks;    // 到达顺序；末尾是当前写 chunk
        unsigned current_chunk = 0;
        std::uint64_t records = 0;
        std::uint64_t min_time = std::numeric_limits<std::uint64_t>::max(), max_time = 0;
        bool dirty = false;
    };
    using RunKey = std::pair<std::uint64_t, std::string>;

    Chunk& current_chunk(Run& run, const RunKey& key) {
        if (run.chunks.empty()) {
            run.chunks.push_back(Chunk{allocated_end_[key.first], 0, kDayRunChunkBytes});
            run.current_chunk = 0;
            allocated_end_[key.first] += kDayRunChunkBytes;
        }
        return run.chunks[run.current_chunk];
    }

    File& day_file(std::uint64_t day) {
        // 先查后开：try_emplace 的参数总是被求值，热路径上会把打开的 fd 丢掉
        // （每批每 symbol 泄漏一个，大输入直接 EMFILE）。
        const auto found = day_files_.find(day);
        if (found != day_files_.end()) return found->second;
        const auto path = directory_ / ("day-" + std::to_string(day) + ".runs");
        auto [it, inserted] = day_files_.try_emplace(day, ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600));
        if (inserted && it->second.fd < 0) {
            day_files_.erase(it);
            system_failure("open day-run file " + path.string());
        }
        return it->second;
    }

    void write_block(int fd, std::uint64_t offset, const std::string& block) {
        std::size_t done = 0;
        while (done < block.size()) {
            metrics_.inc("file_write_calls");
            const auto n = ::pwrite(fd, block.data() + done, block.size() - done,
                                    static_cast<off_t>(offset + done));
            if (n < 0) { if (errno == EINTR) continue; system_failure("append day-run block"); }
            if (n == 0) fail("zero-byte day-run append");
            done += static_cast<std::size_t>(n);
            metrics_.inc("file_write_bytes", static_cast<std::uint64_t>(n));
        }
    }

    void flush_metadata() {
        for (const auto& key : dirty_) {
            const Run& run = runs_.at(key);
            bind_symbol(*db_, flush_run_.ptr, 1, key.second, &metrics_);
            bind_int(*db_, flush_run_.ptr, 2, key.first, &metrics_);
            bind_int(*db_, flush_run_.ptr, 3, run.records, &metrics_);
            bind_int(*db_, flush_run_.ptr, 4, run.current_chunk, &metrics_);
            bind_time(*db_, flush_run_.ptr, 5, run.min_time, &metrics_);
            bind_time(*db_, flush_run_.ptr, 6, run.max_time, &metrics_);
            step_done(*db_, flush_run_.ptr, metrics_);
            reset_statement(*db_, flush_run_.ptr, &metrics_);
            metrics_.inc("index_rows");
            for (unsigned seq = 0; seq < run.chunks.size(); ++seq) {
                const Chunk& chunk = run.chunks[seq];
                bind_symbol(*db_, flush_chunk_.ptr, 1, key.second, &metrics_);
                bind_int(*db_, flush_chunk_.ptr, 2, key.first, &metrics_);
                bind_int(*db_, flush_chunk_.ptr, 3, seq, &metrics_);
                bind_int(*db_, flush_chunk_.ptr, 4, chunk.offset, &metrics_);
                bind_int(*db_, flush_chunk_.ptr, 5, chunk.bytes, &metrics_);
                step_done(*db_, flush_chunk_.ptr, metrics_);
                reset_statement(*db_, flush_chunk_.ptr, &metrics_);
                metrics_.inc("symbol_index_rows");
            }
        }
        // 分配指针随每次 flush 推进：崩溃后恢复也能还原不重叠的分配视图（含未写满
        // 的 chunk 保留区——它们的容量已记账，后续分配不会重叠任何活数据）。
        for (const auto& [day, end] : allocated_end_) {
            bind_int(*db_, flush_alloc_.ptr, 1, day, &metrics_);
            bind_int(*db_, flush_alloc_.ptr, 2, end, &metrics_);
            step_done(*db_, flush_alloc_.ptr, metrics_);
            reset_statement(*db_, flush_alloc_.ptr, &metrics_);
        }
        metrics_.inc("metadata_sql_rows", allocated_end_.size() + dirty_.size());
    }

    bool any_day_file_present() const {
        std::error_code error;
        for (const auto& entry : fs::directory_iterator(directory_, error)) {
            const auto path = entry.path();
            if (path.filename().string().rfind("day-", 0) == 0 && path.extension() == ".runs" &&
                entry.file_size(error) > 0 && !error)
                return true;
        }
        return false;
    }

    void load_runs() {
        {
            Statement stmt;
            db_->prepare(stmt, "SELECT day,allocated_end FROM day_alloc");
            int rc;
            while ((rc = sqlite3_step(stmt.ptr)) == SQLITE_ROW)
                allocated_end_[static_cast<std::uint64_t>(sqlite3_column_int64(stmt.ptr, 0))] =
                    static_cast<std::uint64_t>(sqlite3_column_int64(stmt.ptr, 1));
            if (rc != SQLITE_DONE) db_->db_fail("load day allocation pointers");
            reset_statement(*db_, stmt.ptr);
        }
        {
            Statement stmt;
            db_->prepare(stmt, "SELECT symbol,day,seq,offset,bytes FROM run_chunks ORDER BY symbol,day,seq");
            int rc;
            while ((rc = sqlite3_step(stmt.ptr)) == SQLITE_ROW) {
                const auto* text = reinterpret_cast<const char*>(sqlite3_column_text(stmt.ptr, 0));
                if (!text || sqlite3_column_type(stmt.ptr, 0) != SQLITE_TEXT)
                    fail("invalid day-run chunk symbol");
                RunKey key{static_cast<std::uint64_t>(sqlite3_column_int64(stmt.ptr, 1)),
                           std::string(text, static_cast<std::size_t>(sqlite3_column_bytes(stmt.ptr, 0)))};
                Chunk chunk;
                chunk.offset = static_cast<std::uint64_t>(sqlite3_column_int64(stmt.ptr, 3));
                chunk.bytes = static_cast<std::uint64_t>(sqlite3_column_int64(stmt.ptr, 4));
                chunk.capacity = std::max(kDayRunChunkBytes, chunk.bytes);
                Run& run = runs_[key];
                run.chunks.push_back(chunk);
                run.current_chunk = static_cast<unsigned>(run.chunks.size() - 1);
            }
            if (rc != SQLITE_DONE) db_->db_fail("load day-run chunks");
            reset_statement(*db_, stmt.ptr);
        }
        Statement stmt;
        db_->prepare(stmt, "SELECT symbol,day,records,min_time,max_time FROM symbol_runs");
        int rc;
        while ((rc = sqlite3_step(stmt.ptr)) == SQLITE_ROW) {
            const auto* text = reinterpret_cast<const char*>(sqlite3_column_text(stmt.ptr, 0));
            if (!text || sqlite3_column_type(stmt.ptr, 0) != SQLITE_TEXT)
                fail("invalid day-run metadata symbol");
            RunKey key{static_cast<std::uint64_t>(sqlite3_column_int64(stmt.ptr, 1)),
                       std::string(text, static_cast<std::size_t>(sqlite3_column_bytes(stmt.ptr, 0)))};
            const auto found = runs_.find(key);
            if (found == runs_.end()) fail("day-run metadata run has no chunks");
            found->second.records = static_cast<std::uint64_t>(sqlite3_column_int64(stmt.ptr, 2));
            found->second.min_time = read_time_key(sqlite3_column_blob(stmt.ptr, 3), sqlite3_column_bytes(stmt.ptr, 3));
            found->second.max_time = read_time_key(sqlite3_column_blob(stmt.ptr, 4), sqlite3_column_bytes(stmt.ptr, 4));
            physical_rows_ += found->second.records;
        }
        if (rc != SQLITE_DONE) db_->db_fail("load day-run metadata");
        reset_statement(*db_, stmt.ptr);
    }

    // 恢复：每个 run 从"当前 chunk 已写字节末尾"扫描到 chunk 容量边界或文件尾，
    // 逐帧校验（len/CRC/归属 symbol）并采纳；撕裂帧或 chunk 末尾停止。chunk 之外
    // 未 flush 的新 chunk 区域无元数据（死区，后续分配可复用）。文件短于持久
    // chunk 位置 = 致命。
    void recover() {
        bool adopted_any = false;
        for (auto& [key, run] : runs_) {
            if (run.chunks.empty()) fail("day-run metadata run has no chunks");
            Chunk& chunk = run.chunks[run.current_chunk];
            // chunk.bytes==0 也可能"分配后已写、未 flush"——恢复扫描必须从
            // chunk.offset 起步，不能跳过。
            File& file = day_file(key.first);
            struct stat status{};
            if (::fstat(file.fd, &status) < 0) system_failure("stat day-run file");
            const std::uint64_t scan_end = std::min(chunk.offset + chunk.capacity,
                                                    static_cast<std::uint64_t>(std::max<off_t>(status.st_size, 0)));
            if (chunk.bytes && static_cast<std::uint64_t>(status.st_size) < chunk.offset + chunk.bytes)
                fail("day-run file is shorter than durable metadata: day " + std::to_string(key.first) +
                     " symbol " + key.second);
            std::uint64_t scan = chunk.offset + chunk.bytes;
            const std::uint64_t durable_bytes = chunk.bytes;
            const std::uint64_t durable_records = run.records;
            std::string length_bytes(4, '\0'), crc_bytes(4, '\0');
            for (;;) {
                if (scan + 8 > scan_end) break;  // chunk 保留边界：帧永不跨 chunk
                if (!pread_exact(file.fd, length_bytes.data(), 4, scan)) break;
                const auto length = static_cast<std::size_t>(read_le(length_bytes, 0, 4));
                if (length == 0 || length > kMaxRecordBytes) break;  // 撕裂/垃圾帧
                if (scan + 8 + length > scan_end) break;
                std::string payload(length, '\0');
                if (!pread_exact(file.fd, payload.data(), length, scan + 4)) break;
                if (!pread_exact(file.fd, crc_bytes.data(), 4, scan + 4 + length)) break;
                if (crc32(payload) != static_cast<std::uint32_t>(read_le(crc_bytes, 0, 4))) break;
                Record record;
                std::string decode_error;
                if (!decode_record(payload, record, decode_error) || record.symbol != key.second) break;
                scan += 4 + length + 4;
                chunk.bytes = scan - chunk.offset;
                run.records += 1;
                run.min_time = std::min(run.min_time, record.time);
                run.max_time = std::max(run.max_time, record.time);
            }
            if (run.records != durable_records) {
                run.dirty = true;
                dirty_.insert(key);
                metrics_.inc("recovered_tail_records", run.records - durable_records);
                metrics_.inc("recovered_tail_bytes", chunk.bytes - durable_bytes);
                recovered_tail_records_ += run.records - durable_records;
                recovered_tail_bytes_ += chunk.bytes - durable_bytes;
                adopted_any = true;
            }
            allocated_end_[key.first] = std::max(allocated_end_[key.first], chunk.offset + chunk.capacity);
        }
        if (adopted_any) {
            { metrics_.inc("sqlite_begin_calls"); metrics_.inc("sqlite_exec_calls"); db_->exec("BEGIN IMMEDIATE"); }
            flush_metadata();
            { metrics_.inc("sqlite_commit_calls"); metrics_.inc("sqlite_exec_calls"); db_->exec("COMMIT"); }
            dirty_.clear();
        }
    }

    bool pread_exact(int fd, void* out, std::size_t size, std::uint64_t offset) const {
        std::size_t done = 0;
        while (done < size) {
            const auto n = ::pread(fd, static_cast<char*>(out) + done, size - done,
                                   static_cast<off_t>(offset + done));
            if (n < 0) { if (errno == EINTR) continue; system_failure("read day-run frame"); }
            if (n == 0) return false;
            done += static_cast<std::size_t>(n);
        }
        return true;
    }

    void visit_run(const RunKey& key, const Run& run, const auto& visit) {
        File& file = day_file(key.first);
        for (const Chunk& chunk : run.chunks) {
            if (!chunk.bytes) continue;
            std::string buffer(static_cast<std::size_t>(chunk.bytes), '\0');
            if (!pread_exact(file.fd, buffer.data(), buffer.size(), chunk.offset))
                fail("day-run file truncated inside indexed chunk");
            std::size_t at = 0;
            while (at < buffer.size()) {
                const auto length = static_cast<std::size_t>(read_le(buffer, at, 4));
                if (length == 0 || length > kMaxRecordBytes || at + 8 + length > buffer.size())
                    fail("day-run indexed chunk contains a malformed frame");
                const auto expected_crc = static_cast<std::uint32_t>(read_le(buffer, at + 4 + length, 4));
                const std::string_view payload(buffer.data() + at + 4, length);
                if (crc32(payload) != expected_crc) fail("day-run frame CRC mismatch in indexed chunk");
                Record record;
                std::string error;
                if (!decode_record(payload, record, error)) fail("day-run frame decode failed: " + error);
                visit(record, payload);
                at += 8 + length;
            }
        }
    }

    void crash_at(std::string_view point) const {
        if (options_.failpoint == point) {
            // Only this process is targeted; a failpoint is never a parent or
            // shared-process kill. SIGKILL deliberately bypasses all destructors.
            ::raise(SIGKILL);
            ::_exit(137);
        }
    }

    const fs::path directory_;
    std::unique_ptr<Database> db_;
    std::map<std::uint64_t, File> day_files_;
    std::map<std::uint64_t, std::uint64_t> allocated_end_;
    std::map<RunKey, Run> runs_;
    std::set<RunKey> dirty_;
    Statement flush_run_, flush_chunk_, flush_alloc_;
    bool poisoned_ = false;
    unsigned batches_since_flush_ = 0;
    std::uint64_t recovered_tail_records_ = 0, recovered_tail_bytes_ = 0;
    std::optional<std::uint64_t> duplicates_;
};

} // namespace

namespace store_detail {
std::filesystem::path prepare_directory(const std::string& directory) {
    return ::tickshelf::bench::prepare_directory(directory);
}
std::uint64_t validate_batch(const std::vector<EncodedRecord>& records, Metrics& metrics) {
    return ::tickshelf::bench::validate_batch(records, metrics);
}
void emit_row(std::ostream& out, const Record& record, std::string_view bytes) {
    ::tickshelf::bench::emit_row(out, record, bytes);
}
std::array<unsigned char, 32> record_digest(const EncodedRecord& record, const StoreOptions& options, Metrics& metrics) {
    if (!options.test_digest && record.has_sha256) {
        metrics.inc("store_digest_cache_hits");
        metrics.inc("backend_digest_cached");
        return record.sha256;
    }
    Timer timer(metrics, "store_digest_compute");
    std::array<unsigned char, 32> result{};
    if (options.test_digest) {
        metrics.inc("store_digest_override_calls");
        std::string digest;
        if (!options.test_digest(record.bytes, digest) || digest.size() != result.size())
            fail("store digest override must produce exactly 32 bytes");
        std::memcpy(result.data(), digest.data(), result.size());
    } else {
        metrics.inc("store_digest_computed");
        metrics.inc("backend_digest_recomputed");
        unsigned size = 0;
        if (EVP_Digest(record.bytes.data(), record.bytes.size(), result.data(), &size, EVP_sha256(), nullptr) != 1 ||
            size != result.size()) fail("store SHA-256 computation failed");
        metrics.inc("store_digest_bytes", record.bytes.size());
    }
    return result;
}
} // namespace store_detail

std::unique_ptr<Store> open_store(const StoreOptions& options, Metrics& metrics, std::string& error) {
    error.clear();
    try {
        if (options.backend != "sqlite" && options.backend != "sqlite_compact" &&
            options.backend != "rocksdb" && options.backend != "segment" && options.backend != "segment_rocksdb" &&
            options.backend != "segment_day" && options.backend != "null")
            fail("unknown store backend: " + options.backend);
        if (options.synchronous != "FULL" && options.synchronous != "NORMAL")
            fail("synchronous must be explicitly FULL or NORMAL");
        if (options.transaction_model != "light" && options.transaction_model != "heavy")
            fail("transaction_model must be light or heavy");
        if (options.autocheckpoint < 0) fail("autocheckpoint must be nonnegative");
        if (options.fullfsync != "default" && options.fullfsync != "on" && options.fullfsync != "off")
            fail("fullfsync must be default, on or off");
        if (options.store_lookup != "exact" && options.store_lookup != "assume_unique")
            fail("store_lookup must be exact or assume_unique");
        if (options.store_lookup == "assume_unique" && options.backend != "sqlite_compact" && options.backend != "rocksdb")
            fail("store_lookup=assume_unique requires sqlite_compact or full-record rocksdb backend");
        if (options.sqlite_compact_lookup != "subquery" && options.sqlite_compact_lookup != "separate")
            fail("sqlite_compact_lookup must be subquery or separate");
        if (options.sqlite_compact_lookup == "separate" && options.backend != "sqlite_compact")
            fail("sqlite_compact_lookup=separate requires sqlite_compact backend");
        const bool segment = options.backend == "segment" || options.backend == "segment_rocksdb";
        const bool day_run = options.backend == "segment_day";
        const bool rocks = options.backend == "rocksdb";
        if (!options.failpoint.empty()) {
            if (segment) {
                if (options.failpoint != "after_append" && options.failpoint != "after_sync" && options.failpoint != "after_index")
                    fail("unknown segment store failpoint");
            } else if (day_run) {
                if (options.failpoint != "after_append" && options.failpoint != "after_sync" && options.failpoint != "after_metadata")
                    fail("unknown day-run store failpoint");
            } else if (rocks) {
                if (options.failpoint != "before_write" && options.failpoint != "after_write")
                    fail("unknown full-record RocksDB failpoint");
            } else fail("hard-crash failpoints require a segment, day-run or full-record RocksDB backend");
        }
        if ((segment || day_run || rocks) && options.synchronous != "FULL") fail("segment/RocksDB stores require synchronous=FULL");
        if (options.test_rocks_write_status_error && !rocks) fail("RocksDB write status injection requires rocksdb backend");
        if (options.backend != "null" && options.directory.empty()) fail("store directory must not be empty");
#ifndef TICKSHELF_HAVE_ROCKSDB
        if (options.backend == "segment_rocksdb" || rocks)
            fail("unsupported backend " + options.backend + ": built without TICKSHELF_HAVE_ROCKSDB");
#endif
        if (rocks) return open_rocks_record_store(options, metrics);
        if (options.backend == "null") return std::make_unique<NullStore>(options, metrics);
        if (options.backend == "sqlite" || options.backend == "sqlite_compact")
            return std::make_unique<SqliteStore>(options, metrics);
        if (options.backend == "segment_day") return std::make_unique<DayRunStore>(options, metrics);
        return std::make_unique<SegmentStore>(options, metrics);
    } catch (const std::exception& e) { error = e.what(); return nullptr; }
    catch (...) { error = "unexpected failure opening benchmark store"; return nullptr; }
}

} // namespace tickshelf::bench
