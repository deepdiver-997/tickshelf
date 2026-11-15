#include "rocks_record_store.hpp"

#include <stdexcept>

#ifdef TICKSHELF_HAVE_ROCKSDB
#include <rocksdb/advanced_cache.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/table.h>
#include <rocksdb/version.h>
#include <rocksdb/write_batch.h>

#include <algorithm>
#include <csignal>
#include <cstring>
#include <limits>
#include <map>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <unistd.h>
#endif

namespace tickshelf::bench {
namespace {
[[noreturn]] void fail(const std::string& message) { throw std::runtime_error(message); }
#ifdef TICKSHELF_HAVE_ROCKSDB
namespace fs = std::filesystem;
constexpr std::uint64_t kMaxStoreBytes = 64ULL * 1024 * 1024;
constexpr std::uint64_t kMinimumRecordBytes = 22;
void check(const rocksdb::Status& status, const std::string& operation) {
    if (!status.ok()) fail("RocksDB full-record " + operation + ": " + status.ToString());
}
std::uint64_t add(std::uint64_t a, std::uint64_t b) {
    if (b > std::numeric_limits<std::uint64_t>::max() - a) fail("RocksDB uint64 counter/record ID overflow");
    return a + b;
}
std::string be(std::uint64_t value, unsigned count = 8) {
    std::string out(count, '\0');
    for (unsigned i = 0; i < count; ++i) out[i] = static_cast<char>(value >> (8 * (count - i - 1)));
    return out;
}
std::uint64_t number(std::string_view value) {
    if (value.empty() || value.size() > 8) fail("invalid RocksDB uint64 encoding");
    std::uint64_t out = 0;
    for (unsigned char c : value) out = (out << 8) | c;
    return out;
}
std::string digest_bytes(const std::array<unsigned char, 32>& digest) {
    return {reinterpret_cast<const char*>(digest.data()), digest.size()};
}
std::string range_prefix(std::string_view symbol) {
    if (symbol.size() > 65535) fail("symbol exceeds canonical header bounds");
    return "R" + be(symbol.size(), 2) + std::string(symbol);
}
std::string range_key(const Record& record, std::uint64_t id) {
    return range_prefix(record.symbol) + be(record.time) + be(id);
}
std::string range_key(const EncodedRecord& record, std::uint64_t id) {
    return range_prefix(record.symbol) + be(record.time) + be(id);
}
std::string heavy_key(std::string_view symbol, std::uint64_t time, std::uint64_t id) {
    return "T" + be(time) + be(symbol.size(), 2) + std::string(symbol) + be(id);
}
struct Identity { std::string symbol; std::uint64_t time, id; };
Identity decode_index(std::string_view key, std::string_view value) {
    if (key.size() < 19 || value.size() != 32 || (key[0] != 'R' && key[0] != 'T'))
        fail("malformed RocksDB record secondary index");
    const bool heavy = key[0] == 'T';
    const auto length = number(key.substr(heavy ? 9 : 1, 2));
    if (key.size() != 19 + length) fail("malformed RocksDB secondary index symbol length");
    const auto time = number(key.substr(heavy ? 1 : 3 + length, 8));
    const auto id = number(key.substr(key.size() - 8));
    if (!id) fail("invalid RocksDB secondary record ID zero");
    return {std::string(key.substr(heavy ? 11 : 3, length)), time, id};
}
Record decode_payload(std::string_view bytes) {
    if (bytes.size() < kMinimumRecordBytes || bytes.size() > kMaxRecordBytes)
        fail("invalid RocksDB full payload length");
    Record record;
    std::string error;
    if (!decode_record(bytes, record, error)) fail("RocksDB stored record decode: " + error);
    return record;
}
rocksdb::ReadOptions reads(bool digest_prefix = false) {
    rocksdb::ReadOptions out;
    out.verify_checksums = true;
    // Only D+digest32 equality scans may use prefix Bloom filtering. All other
    // reads must retain global bytewise order, including short F/S/M keys and
    // binary symbol/time ranges that cross capped-prefix boundaries.
    out.total_order_seek = !digest_prefix;
    out.prefix_same_as_start = digest_prefix;
    return out;
}
rocksdb::WriteOptions writes() {
    rocksdb::WriteOptions out;
    out.sync = true;
    out.disableWAL = false;
    return out;
}
struct State {
    std::uint64_t last_id = 0, batches = 0, attempted = 0, ignored = 0, canonical_bytes = 0, payload_bytes = 0;
    std::string encode() const {
        return be(last_id) + be(batches) + be(attempted) + be(ignored) + be(canonical_bytes) + be(payload_bytes);
    }
    static State decode(std::string_view value) {
        if (value.size() != 48) fail("invalid RocksDB full-record ID/batch state");
        State state{number(value.substr(0, 8)), number(value.substr(8, 8)), number(value.substr(16, 8)),
                    number(value.substr(24, 8)), number(value.substr(32, 8)), number(value.substr(40, 8))};
        if (add(state.last_id, state.ignored) != state.attempted || state.batches > state.attempted ||
            (state.attempted == 0) != (state.batches == 0) ||
            (!state.last_id && (state.attempted || state.canonical_bytes || state.payload_bytes)) ||
            state.canonical_bytes < state.payload_bytes || state.last_id > state.payload_bytes / kMinimumRecordBytes ||
            state.attempted > state.canonical_bytes / kMinimumRecordBytes ||
            state.canonical_bytes / kMaxRecordBytes + (state.canonical_bytes % kMaxRecordBytes != 0) > state.attempted ||
            state.payload_bytes / kMaxRecordBytes + (state.payload_bytes % kMaxRecordBytes != 0) > state.last_id)
            fail("inconsistent RocksDB full-record ID/batch state");
        return state;
    }
};

// Deliberately one writer instance, not TransactionDB. Its mutex serializes
// candidate reads through sync WriteBatch. RocksDB's LOCK rejects another DB
// writer handle. No Get+Put sequence is claimed to be multi-writer transactional.
class RocksRecordStore final : public Store {
public:
    RocksRecordStore(StoreOptions options, Metrics& metrics) : options_(std::move(options)), metrics_(metrics) {
        for (const char* key : {"attempted", "inserted", "ignored", "transactions", "payload_bytes",
                 "store_digest_cache_hits", "store_digest_computed", "store_digest_bytes", "store_digest_override_calls",
                 "rocksdb_write_batches", "rocksdb_write_calls", "rocksdb_put_ops", "rocksdb_key_bytes",
                 "rocksdb_value_bytes", "rocksdb_writebatch_bytes", "rocksdb_payload_value_bytes",
                 "rocksdb_get_calls", "rocksdb_seek_calls", "rocksdb_iterator_creations", "rocksdb_dedup_candidate_rows",
                 "rocksdb_dedup_pending_hits", "rocksdb_dedup_iterator_steps", "rocksdb_range_iterator_steps",
                 "rocksdb_range_read_bytes", "range_candidate_segments", "range_decoded_rows", "range_emitted_rows", "index_rows",
                 "symbol_index_rows", "time_symbol_index_rows", "metadata_kv_rows", "rocksdb_state_rows",
                 "sqlite_prepare_calls", "sqlite_step_calls", "file_write_calls", "file_write_bytes", "file_sync_calls"})
            metrics_.counters.try_emplace(key, 0);
        const auto base = store_detail::prepare_directory(options_.directory);
        path_ = base / "full-record-rocksdb-v1";
        const bool existed = fs::exists(path_);
        const bool current = fs::exists(path_ / "CURRENT");
        if (existed && !fs::is_directory(path_)) fail("RocksDB full-record path is not a directory");
        // Never create a replacement manifest over a damaged or foreign directory.
        if (existed && !current && !fs::is_empty(path_)) fail("RocksDB full-record CURRENT missing in nonempty directory");
        store_detail::prepare_directory(path_.string());
        rocks_options_.create_if_missing = !current;
        rocks_options_.use_fsync = true;
        rocks_options_.compression = rocksdb::kNoCompression;
        rocks_options_.bottommost_compression = rocksdb::kNoCompression;
        rocks_options_.paranoid_checks = true;
        rocks_options_.wal_recovery_mode = rocksdb::WALRecoveryMode::kAbsoluteConsistency;
        rocks_options_.manual_wal_flush = false;
        rocks_options_.allow_mmap_writes = false;
        // Native Bloom prefix support avoids a deliberately unfiltered random
        // SHA-256 Seek baseline. Capped (not fixed) also defines F/S and other
        // short keys. D keys always have the exact stable 33-byte prefix.
        rocks_options_.prefix_extractor.reset(rocksdb::NewCappedPrefixTransform(33));
        rocksdb::BlockBasedTableOptions table;
        table.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
        table.whole_key_filtering = true; // Point Get and digest-prefix Bloom both remain available.
        rocks_options_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table));
        check(rocksdb::DB::Open(rocks_options_, path_.string(), &db_), "open");
        if (!current) {
            auto it = iterator(true);
            metrics_.inc("rocksdb_recovery_seek_calls");
            it->SeekToFirst();
            if (it->Valid()) fail("unstamped nonempty RocksDB full-record store");
            check(it->status(), "initial iterator");
            rocksdb::WriteBatch batch;
            check(batch.Put("F", stamp()), "initial stamp Put");
            check(batch.Put("S", state_.encode()), "initial state Put");
            check(db_->Write(writes(), &batch), "initialize stamp/state");
            metrics_.inc("rocksdb_initialization_write_batches");
        }
        if (get("F", true) != stamp()) fail("unsupported RocksDB full-record format stamp or transaction model");
        state_ = State::decode(get("S", true));
        recover();
        // Persist new directory entries only at startup, not once per record.
        store_detail::prepare_directory(path_.string());
        store_detail::prepare_directory(base.string());
    }
    bool write(const std::vector<EncodedRecord>& records, std::string& error) override {
        std::lock_guard<std::mutex> lock(mutex_);
        error.clear();
        try {
            available();
            if (finished_) fail("RocksDB full-record store is finished; reopen before writing");
            attempted_ = add(attempted_, records.size());
            metrics_.inc("attempted", records.size());
            const auto canonical_bytes = store_detail::validate_batch(records, metrics_);
            if (records.empty()) return true;
            State next = state_;
            next.batches = add(next.batches, 1);
            next.attempted = add(next.attempted, records.size());
            next.canonical_bytes = add(next.canonical_bytes, canonical_bytes);
            // All allocation, validation and digest failures happen before DB::Write.
            // One iterator per batch; one persisted candidate scan per digest prefix.
            struct Bucket { std::vector<std::string> stored; std::vector<std::string_view> pending; };
            std::unordered_map<std::string, Bucket> buckets;
            rocksdb::WriteBatch batch;
            auto it = iterator(false, true);
            std::uint64_t inserted = 0, payload = 0, put_ops = 0, key_bytes = 0, value_bytes = 0;
            const auto put = [&](const std::string& key, std::string_view value) {
                check(batch.Put(key, rocksdb::Slice(value.data(), value.size())), "batch Put");
                ++put_ops; key_bytes += key.size(); value_bytes += value.size();
            };
            {
                Timer timer(metrics_, "rocksdb_batch_prepare");
                for (const auto& record : records) {
                    const auto digest = digest_bytes(store_detail::record_digest(record, options_, metrics_));
                    auto [entry, first] = buckets.try_emplace(digest);
                    auto& bucket = entry->second;
                    const auto prefix = "D" + digest;
                    if (first) {
                        metrics_.inc("rocksdb_seek_calls");
                        for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
                            if (it->key().size() != 41) fail("malformed RocksDB digest/record ID key");
                            bucket.stored.push_back(it->value().ToString());
                            metrics_.inc("rocksdb_dedup_candidate_rows");
                            metrics_.inc("rocksdb_dedup_iterator_steps");
                        }
                        check(it->status(), "digest candidate iterator");
                    }
                    if (std::find(bucket.pending.begin(), bucket.pending.end(), record.bytes) != bucket.pending.end()) {
                        metrics_.inc("rocksdb_dedup_pending_hits");
                        continue;
                    }
                    if (std::find(bucket.stored.begin(), bucket.stored.end(), record.bytes) != bucket.stored.end()) continue;
                    next.last_id = add(next.last_id, 1);
                    payload = add(payload, record.bytes.size());
                    ++inserted;
                    bucket.pending.emplace_back(record.bytes);
                    put(prefix + be(next.last_id), record.bytes);
                    put(range_key(record, next.last_id), digest);
                    if (heavy()) put(heavy_key(record.symbol, record.time, next.last_id), digest);
                }
                next.ignored = add(next.ignored, records.size() - inserted);
                next.payload_bytes = add(next.payload_bytes, payload);
                if (heavy())
                    put("M" + be(next.batches), be(state_.last_id) + be(records.size()) + be(inserted) +
                        be(records.size() - inserted) + be(canonical_bytes) + be(payload));
                put("S", next.encode());
            }
            // No iterator may outlive Close(), including during finish.
            it.reset();
            crash_at("before_write");
            poisoned_ = true; // A non-OK status can have an uncertain commit outcome.
            {
                Timer timer(metrics_, "rocksdb_write");
                metrics_.inc("rocksdb_write_calls");
                check(db_->Write(writes(), &batch), "atomic full-record sync WriteBatch");
                if (options_.test_rocks_write_status_error) fail("test-injected uncertain RocksDB write status; reopen required");
            }
            crash_at("after_write");
            state_ = next;
            inserted_ += inserted;
            ignored_ += records.size() - inserted;
            ++transactions_;
            metrics_.inc("inserted", inserted);
            metrics_.inc("ignored", records.size() - inserted);
            metrics_.inc("transactions");
            metrics_.inc("payload_bytes", canonical_bytes);
            metrics_.inc("rocksdb_write_batches");
            metrics_.inc("rocksdb_put_ops", put_ops);
            metrics_.inc("rocksdb_key_bytes", key_bytes);
            metrics_.inc("rocksdb_value_bytes", value_bytes);
            metrics_.inc("rocksdb_writebatch_bytes", batch.GetDataSize());
            metrics_.inc("rocksdb_payload_value_bytes", payload);
            metrics_.inc("index_rows", inserted);
            metrics_.inc("symbol_index_rows", inserted);
            metrics_.inc("time_symbol_index_rows", heavy() ? inserted : 0);
            metrics_.inc("metadata_kv_rows", heavy() ? 1 : 0);
            metrics_.inc("rocksdb_state_rows");
            poisoned_ = false;
            return true;
        } catch (const std::exception& e) { error = e.what(); return false; }
    }
    bool finish(std::string& error) override {
        std::lock_guard<std::mutex> lock(mutex_);
        error.clear();
        try {
            available();
            if (finished_) return true;
            Timer all(metrics_, "rocksdb_finish");
            // Failure anywhere in finalization also makes this instance unusable.
            poisoned_ = true;
            debt_after_writes_ = background_json();
            rocksdb::FlushOptions flush; flush.wait = true;
            { Timer timer(metrics_, "rocksdb_flush"); check(db_->Flush(flush), "finish Flush(wait)"); }
            debt_after_flush_ = background_json();
            rocksdb::WaitForCompactOptions wait;
            wait.flush = true;
            wait.wait_for_purge = true;
            { Timer timer(metrics_, "rocksdb_wait_compact"); check(db_->WaitForCompact(wait), "finish WaitForCompact"); }
            debt_after_compact_ = background_json();
            { Timer timer(metrics_, "rocksdb_sync_wal"); check(db_->SyncWAL(), "finish SyncWAL"); }
            debt_before_close_ = background_json();
            { Timer timer(metrics_, "rocksdb_close"); check(db_->Close(), "finish Close"); db_.reset(); }
            rocks_options_.create_if_missing = false;
            {
                Timer timer(metrics_, "rocksdb_reopen_readonly");
                check(rocksdb::DB::OpenForReadOnly(rocks_options_, path_.string(), &db_), "finish reopen read-only");
            }
            finished_ = true;
            poisoned_ = false;
            return true;
        } catch (const std::exception& e) { error = e.what(); return false; }
    }
    bool export_rows(std::ostream& out, std::string& error) override {
        std::lock_guard<std::mutex> lock(mutex_);
        error.clear();
        try {
            available();
            if (!out) fail("failed to export RocksDB rows");
            auto it = iterator();
            std::uint64_t rows = 0;
            metrics_.inc("rocksdb_seek_calls");
            for (it->Seek("D"); it->Valid() && it->key().starts_with("D"); it->Next()) {
                const auto key = it->key().ToString();
                if (key.size() != 41) fail("malformed RocksDB full-record key");
                const std::string_view bytes(it->value().data(), it->value().size());
                const auto record = decode_payload(bytes);
                store_detail::emit_row(out, record, bytes);
                ++rows;
            }
            check(it->status(), "export iterator");
            if (rows != state_.last_id) fail("RocksDB export record count differs from validated ID state");
            return true;
        } catch (const std::exception& e) { error = e.what(); return false; }
    }
    bool export_range(const std::string& symbol, std::uint64_t from, std::uint64_t to,
                      std::ostream& out, std::string& error) override {
        std::lock_guard<std::mutex> lock(mutex_);
        error.clear();
        try {
            available();
            Timer timer(metrics_, "export_range");
            if (from > to) fail("range lower bound exceeds upper bound");
            if (!out) fail("failed to export RocksDB range");
            const auto prefix = range_prefix(symbol);
            auto it = iterator();
            metrics_.inc("rocksdb_seek_calls");
            for (it->Seek(prefix + be(from)); it->Valid() && it->key().starts_with(prefix); it->Next()) {
                const auto key = it->key().ToString(), digest = it->value().ToString();
                const auto identity = decode_index(key, digest);
                metrics_.inc("rocksdb_range_iterator_steps");
                metrics_.inc("rocksdb_range_read_bytes", key.size() + digest.size());
                if (identity.time > to) break;
                const auto bytes = get("D" + digest + be(identity.id));
                const auto record = decode_payload(bytes);
                if (record.symbol != symbol || record.time != identity.time) fail("RocksDB range index/payload identity mismatch");
                store_detail::emit_row(out, record, bytes);
                metrics_.inc("range_decoded_rows");
                metrics_.inc("range_emitted_rows");
            }
            check(it->status(), "export range iterator");
            return true;
        } catch (const std::exception& e) { error = e.what(); return false; }
    }
    std::string info_json() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto actual_number = [&](std::uint64_t n) { return poisoned_ ? std::string("null") : std::to_string(n); };
        const auto persisted = poisoned_ ? std::string("null") :
            "{\"attempted\":" + std::to_string(state_.attempted) + ",\"inserted\":" + std::to_string(state_.last_id) +
            ",\"ignored\":" + std::to_string(state_.ignored) + ",\"transactions\":" + std::to_string(state_.batches) +
            ",\"canonical_bytes\":" + std::to_string(state_.canonical_bytes) + ",\"unique_payload_bytes\":" + std::to_string(state_.payload_bytes) + "}";
        std::ostringstream out;
        out << "{\"backend\":\"rocksdb\",\"durable\":true,\"physical_power_loss_tested\":false,\"hardware_plp\":\"unknown\","
            << "\"database\":" << json_quote(path_.string()) << ",\"transaction_model\":" << json_quote(options_.transaction_model)
            << ",\"payload_location\":\"RocksDB D values contain complete canonical records; no external segment files\","
            << "\"deduplication\":\"nonunique fixed32 SHA-256 prefix candidates, exact full canonical byte equality only; pending batch cache\","
            << "\"digest_cache_contract\":\"shared store_digest; trusted EncodedRecord SHA256 only while bytes unchanged; test override takes precedence\","
            << "\"writer_model\":\"one DB writer handle; mutex covers dedup read through synchronous WriteBatch; DB LOCK rejects independent writers; not TransactionDB or multi-writer transactions\","
            << "\"schema_cost\":" << json_quote(heavy() ?
                "per unique record: D digest/id -> full payload, R symbol/time/id -> digest, T time/symbol/id -> digest; one M batch metadata and one S state replacement per nonempty batch" :
                "per unique record: D digest/id -> full payload and R symbol/time/id -> digest; one S state replacement per nonempty batch")
            << ",\"heavy_equivalence\":\"logical per-record time/symbol secondary index and attempted/inserted/ignored/bytes batch metadata; Rocks KV and SQLite B-tree layouts/physical costs are not identical; no segment CRC substitute\","
            << "\"time_encoding\":\"uint64 big-endian 8-byte; length-prefixed binary symbols\","
            << "\"input_validation\":\"same trusted-codec canonical header/64MiB batch boundary as SQLite; recovery fully decodes all payloads and validates both directions of every index\","
            << "\"sync_method\":\"RocksDB WAL sync=true,use_fsync=true\","
            << "\"durability_scope\":\"synchronous WAL atomic WriteBatch with OS sync; no physical power-loss or hardware PLP claim\","
            << "\"physical_rows\":" << actual_number(state_.last_id) << ",\"physical_duplicate_rows\":" << actual_number(0)
            << ",\"symbol_index_rows\":" << actual_number(state_.last_id)
            << ",\"time_symbol_index_rows\":" << actual_number(heavy() ? state_.last_id : 0)
            << ",\"batch_metadata_rows\":" << actual_number(heavy() ? state_.batches : 0)
            << ",\"physical_key_rows\":" << actual_number(key_rows_ + (state_.last_id - recovered_rows_) * (heavy() ? 3 : 2) +
                (heavy() ? state_.batches - recovered_batches_ : 0))
            << ",\"persisted_counts\":" << persisted
            << ",\"persisted_count_scope\":\"strictly verified recovery state plus acknowledged atomic batches; null when poisoned because write status may have an uncertain commit outcome\","
            << "\"counters\":{\"attempted\":" << attempted_ << ",\"inserted\":" << inserted_ << ",\"ignored\":" << ignored_
            << ",\"transactions\":" << transactions_ << "},\"rocksdb\":{\"version\":" << json_quote(rocksdb::GetRocksVersionAsString())
            << ",\"format_stamp\":" << json_quote(stamp()) << ",\"build_properties\":{";
        bool comma = false;
        for (const auto& [key, value] : std::map<std::string, std::string>(rocksdb::GetRocksBuildProperties().begin(), rocksdb::GetRocksBuildProperties().end())) {
            if (comma) out << ',';
            comma = true;
            out << json_quote(key) << ':' << json_quote(value);
        }
        out << "},\"write_options\":{\"sync\":true,\"disableWAL\":false,\"use_fsync\":true},"
            << "\"options\":" << options_json()
            << ",\"sync_source\":\"POSIX use_fsync=true; macOS HAVE_FULLFSYNC builds select fcntl(F_FULLFSYNC), otherwise fsync; build capability is not inferred from use_fsync\","
            << "\"finish_contract\":\"Flush(wait), WaitForCompact(flush,wait_for_purge), SyncWAL, Close/delete, read-only reopen all timed inside finish; export does not write or repair\","
            << "\"finish_called\":" << (finished_ ? "true" : "false") << ",\"poisoned\":" << (poisoned_ ? "true" : "false")
            << ",\"finish_background_state\":" << debt_before_close_ << ",\"debt_after_writes\":" << debt_after_writes_
            << ",\"debt_after_flush\":" << debt_after_flush_ << ",\"debt_after_wait_compact\":" << debt_after_compact_
            << ",\"background_state\":" << background_json() << ",\"logical_api_counts\":{";
        comma = false;
        for (const char* key : {"rocksdb_write_calls", "rocksdb_write_batches", "rocksdb_put_ops", "rocksdb_get_calls", "rocksdb_seek_calls", "rocksdb_iterator_creations"}) {
            if (comma) out << ',';
            comma = true;
            out << json_quote(key) << ':' << counter(key);
        }
        out << "},\"api_count_scope\":\"logical library operations, not syscalls; successful workload batch Put/key/value bytes exclude startup stamp/state; dedup and range iterators reused per operation; recovery has separate Get/iterator counters\"}}";
        return out.str();
    }
private:
    StoreOptions options_;
    Metrics& metrics_;
    fs::path path_;
    rocksdb::Options rocks_options_;
    std::unique_ptr<rocksdb::DB> db_;
    mutable std::mutex mutex_;
    State state_;
    std::uint64_t attempted_ = 0, inserted_ = 0, ignored_ = 0, transactions_ = 0;
    std::uint64_t key_rows_ = 0, recovered_rows_ = 0, recovered_batches_ = 0;
    bool poisoned_ = false, finished_ = false;
    std::string debt_after_writes_ = "null", debt_after_flush_ = "null", debt_after_compact_ = "null", debt_before_close_ = "null";
    bool heavy() const { return options_.transaction_model == "heavy"; }
    std::string stamp() const { return "tickshelf-bench/full-record-rocksdb/v1/RCD1/" + options_.transaction_model; }
    void available() const { if (poisoned_ || !db_) fail("RocksDB full-record store requires reopen after a failed write/finish"); }
    void crash_at(std::string_view site) const {
        if (options_.failpoint != site) return;
        ::kill(::getpid(), SIGKILL);
        ::_exit(99);
    }
    std::uint64_t counter(const char* name) const {
        const auto found = metrics_.counters.find(name);
        return found == metrics_.counters.end() ? 0 : found->second;
    }
    std::unique_ptr<rocksdb::Iterator> iterator(bool recovery = false, bool digest_prefix = false) const {
        metrics_.inc(recovery ? "rocksdb_recovery_iterator_creations" : "rocksdb_iterator_creations");
        return std::unique_ptr<rocksdb::Iterator>(db_->NewIterator(reads(digest_prefix)));
    }
    std::string get(const std::string& key, bool recovery = false) const {
        std::string value;
        metrics_.inc(recovery ? "rocksdb_recovery_get_calls" : "rocksdb_get_calls");
        check(db_->Get(reads(), key, &value), "Get required key");
        return value;
    }
    void recover() {
        Timer timer(metrics_, "rocksdb_recover_validate");
        std::map<std::uint64_t, std::uint64_t> sizes;
        std::map<std::uint64_t, std::string> metadata;
        std::unordered_set<std::string> bucket_payloads;
        std::string previous_digest;
        std::uint64_t range_rows = 0, heavy_rows = 0, payload_bytes = 0;
        bool format = false, state = false;
        Metrics digest_metrics;
        auto it = iterator(true);
        metrics_.inc("rocksdb_recovery_seek_calls");
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            ++key_rows_;
            const auto key = it->key().ToString(), value = it->value().ToString();
            if (key == "F") { format = value == stamp(); if (!format) fail("invalid RocksDB format stamp"); }
            else if (key == "S") { state = value == state_.encode(); if (!state) fail("invalid RocksDB ID state"); }
            else if (!key.empty() && key[0] == 'D') {
                if (key.size() != 41) fail("malformed RocksDB digest/record ID key");
                const auto id = number(std::string_view(key).substr(33));
                if (!id || id > state_.last_id || !sizes.emplace(id, value.size()).second)
                    fail("invalid/duplicate RocksDB full-record ID");
                const auto record = decode_payload(value);
                const EncodedRecord encoded{record.symbol, record.time, value};
                const auto digest = digest_bytes(store_detail::record_digest(encoded, options_, digest_metrics));
                if (std::string_view(key).substr(1, 32) != digest) fail("RocksDB full payload digest mismatch");
                if (digest != previous_digest) { bucket_payloads.clear(); previous_digest = digest; }
                if (!bucket_payloads.emplace(value).second) fail("duplicate exact RocksDB full payload");
                if (get(range_key(record, id), true) != digest) fail("missing/incorrect RocksDB record symbol/time index");
                if (heavy() && get(heavy_key(record.symbol, record.time, id), true) != digest)
                    fail("missing/incorrect RocksDB heavy time/symbol index");
                payload_bytes = add(payload_bytes, value.size());
            } else if (!key.empty() && (key[0] == 'R' || key[0] == 'T')) {
                if (key[0] == 'T' && !heavy()) fail("unexpected heavy RocksDB index in light schema");
                const auto identity = decode_index(key, value);
                const auto bytes = get("D" + value + be(identity.id), true);
                const auto record = decode_payload(bytes);
                if (record.symbol != identity.symbol || record.time != identity.time)
                    fail("RocksDB secondary index does not match full payload identity");
                if (key[0] == 'R') ++range_rows; else ++heavy_rows;
            } else if (!key.empty() && key[0] == 'M') {
                if (!heavy() || key.size() != 9 || value.size() != 48) fail("malformed RocksDB batch metadata");
                const auto id = number(std::string_view(key).substr(1));
                if (!id || id > state_.batches) fail("invalid RocksDB batch metadata ID");
                metadata.emplace(id, value);
            } else fail("unknown/malformed RocksDB full-record schema key");
        }
        check(it->status(), "full recovery iterator");
        if (!format || !state || sizes.size() != state_.last_id || range_rows != state_.last_id ||
            heavy_rows != (heavy() ? state_.last_id : 0) || payload_bytes != state_.payload_bytes ||
            metadata.size() != (heavy() ? state_.batches : 0))
            fail("incomplete RocksDB payload/index/batch state");
        if (heavy()) {
            State observed;
            auto size = sizes.begin();
            for (const auto& [id, value] : metadata) {
                const auto prior_id = number(std::string_view(value).substr(0, 8));
                const auto attempted = number(std::string_view(value).substr(8, 8));
                const auto inserted = number(std::string_view(value).substr(16, 8));
                const auto ignored = number(std::string_view(value).substr(24, 8));
                const auto bytes = number(std::string_view(value).substr(32, 8));
                const auto payload = number(std::string_view(value).substr(40, 8));
                if (id != add(observed.batches, 1) || prior_id != observed.last_id || !attempted ||
                    inserted > attempted || ignored != attempted - inserted || bytes > kMaxStoreBytes ||
                    attempted > bytes / kMinimumRecordBytes || payload > bytes)
                    fail("inconsistent RocksDB heavy batch metadata");
                const auto end = add(prior_id, inserted);
                std::uint64_t actual_payload = 0, n = 0;
                while (size != sizes.end() && size->first <= end) {
                    if (size->first != add(prior_id, ++n)) fail("noncontiguous RocksDB heavy batch record IDs");
                    actual_payload = add(actual_payload, size->second);
                    ++size;
                }
                if (n != inserted || actual_payload != payload) fail("RocksDB batch payload bytes/count mismatch");
                observed.last_id = end;
                observed.batches = id;
                observed.attempted = add(observed.attempted, attempted);
                observed.ignored = add(observed.ignored, ignored);
                observed.canonical_bytes = add(observed.canonical_bytes, bytes);
                observed.payload_bytes = add(observed.payload_bytes, payload);
            }
            if (observed.encode() != state_.encode()) fail("RocksDB heavy batch totals do not match ID state");
        }
        metrics_.inc("rocksdb_recovery_payload_rows", sizes.size());
        metrics_.inc("rocksdb_recovery_payload_bytes", payload_bytes);
        recovered_rows_ = state_.last_id;
        recovered_batches_ = state_.batches;
    }
    std::string options_json() const {
        if (!db_) return "null";
        const auto actual = db_->GetOptions();
        const auto* table = actual.table_factory ? actual.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>() : nullptr;
        std::ostringstream out;
        out << "{\"compression\":\"none\",\"bottommost_compression\":\"none\",\"write_buffer_size\":" << actual.write_buffer_size
            << ",\"max_write_buffer_number\":" << actual.max_write_buffer_number
            << ",\"min_write_buffer_number_to_merge\":" << actual.min_write_buffer_number_to_merge
            << ",\"max_background_jobs\":" << actual.max_background_jobs
            << ",\"disable_auto_compactions\":" << (actual.disable_auto_compactions ? "true" : "false")
            << ",\"manual_wal_flush\":" << (actual.manual_wal_flush ? "true" : "false")
            << ",\"allow_mmap_writes\":" << (actual.allow_mmap_writes ? "true" : "false")
            << ",\"paranoid_checks\":" << (actual.paranoid_checks ? "true" : "false")
            << ",\"wal_recovery_mode\":\"absolute_consistency\",\"table_factory\":"
            << json_quote(actual.table_factory ? actual.table_factory->Name() : "unknown")
            << ",\"prefix_extractor\":" << json_quote(actual.prefix_extractor ? actual.prefix_extractor->Name() : "none")
            << ",\"digest_prefix_bytes\":33,\"bloom_bits_per_key\":10,\"filter_policy\":"
            << json_quote(table && table->filter_policy ? table->filter_policy->Name() : "none")
            << ",\"whole_key_filtering\":" << (table ? (table->whole_key_filtering ? "true" : "false") : "null")
            << ",\"dedup_read_options\":{\"prefix_same_as_start\":true,\"total_order_seek\":false},"
            << "\"other_read_options\":{\"prefix_same_as_start\":false,\"total_order_seek\":true},"
            << "\"memtable_prefix_bloom_size_ratio\":" << actual.memtable_prefix_bloom_size_ratio
            << ",\"block_size\":" << (table ? std::to_string(table->block_size) : "null")
            << ",\"no_block_cache\":" << (table ? (table->no_block_cache ? "true" : "false") : "null")
            << ",\"block_cache_capacity\":" << (table && table->block_cache ? std::to_string(table->block_cache->GetCapacity()) : "null")
            << ",\"block_cache_usage\":" << (table && table->block_cache ? std::to_string(table->block_cache->GetUsage()) : "null")
            << ",\"block_cache_pinned_usage\":" << (table && table->block_cache ? std::to_string(table->block_cache->GetPinnedUsage()) : "null")
            << ",\"option_policy\":\"library cache/memtable defaults; explicit sync/checksum/recovery/no-compression plus capped33 prefix and 10-bit/key Bloom; actual memory including Bloom is not claimed equal to SQLite cache budget\"}";
        return out.str();
    }
    std::string background_json() const {
        if (!db_) return "null";
        std::string out = "{";
        bool comma = false;
        for (const char* property : {"rocksdb.num-immutable-mem-table", "rocksdb.num-running-flushes", "rocksdb.num-running-compactions",
                "rocksdb.mem-table-flush-pending", "rocksdb.compaction-pending", "rocksdb.estimate-pending-compaction-bytes",
                "rocksdb.cur-size-all-mem-tables", "rocksdb.total-sst-files-size", "rocksdb.live-sst-files-size",
                "rocksdb.estimate-live-data-size", "rocksdb.block-cache-capacity", "rocksdb.block-cache-usage",
                "rocksdb.block-cache-pinned-usage", "rocksdb.background-errors"}) {
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
} // namespace

std::unique_ptr<Store> open_rocks_record_store(const StoreOptions& options, Metrics& metrics) {
#ifdef TICKSHELF_HAVE_ROCKSDB
    return std::make_unique<RocksRecordStore>(options, metrics);
#else
    (void)options; (void)metrics;
    fail("unsupported backend rocksdb: built without TICKSHELF_HAVE_ROCKSDB");
#endif
}
} // namespace tickshelf::bench
