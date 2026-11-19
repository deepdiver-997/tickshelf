#pragma once
#include <filesystem>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "metrics.hpp"
#include "record.hpp"

namespace tickshelf::bench {
struct StoreOptions {
    std::string backend = "sqlite";  // sqlite | sqlite_compact | rocksdb | segment | segment_rocksdb | segment_day | null
    std::string directory;
    std::string synchronous = "FULL";
    std::string transaction_model = "light";  // light | heavy
    bool reuse_statement = true;
    int autocheckpoint = 1000;
    // Explicit test-only hard-crash sites: segment after_append/after_sync/after_index;
    // full-record RocksDB before_write/after_write.
    std::string failpoint;
    // Test-only: simulate an uncertain non-OK status after RocksDB accepted a batch.
    // Proves that failed writes poison the instance and require recovery; never serialized.
    bool test_rocks_write_status_error = false;
    // Test-only collision/error injection. Must return exactly 32 bytes on success.
    // Overrides even a trusted upstream SHA-256 cache; never serialized.
    bool (*test_digest)(std::string_view, std::string&) = nullptr;
    // Preserve SQLite's default unless explicitly selected for diagnostic runs.
    std::string fullfsync = "default";  // default | on | off
    // sqlite_compact only: preserve the original INSERT SELECT by default.
    // separate uses digest-candidate SELECT + exact byte comparison, then INSERT VALUES
    // in the same BEGIN IMMEDIATE transaction, with no schema/dedup change.
    std::string sqlite_compact_lookup = "subquery";  // subquery | separate
    // Synthetic zero-repeat diagnostic only; uses an isolated directory/format.
    // Appended to preserve existing positional aggregate initialization.
    std::string store_lookup = "exact";  // exact | assume_unique
};

class Store {
public:
    virtual ~Store() = default;
    virtual bool write(const std::vector<EncodedRecord>& records, std::string& error) = 0;
    virtual bool finish(std::string& error) = 0;
    // One JSON object per physical stored record: symbol, time, hex (canonical bytes).
    virtual bool export_rows(std::ostream& out, std::string& error) = 0;
    // Inclusive time bounds; exact mode emits canonical duplicates once.
    // assume_unique exports physical rows without masking precondition violations.
    virtual bool export_range(const std::string& symbol, std::uint64_t from, std::uint64_t to,
                              std::ostream& out, std::string& error) = 0;
    virtual std::string info_json() const = 0;
};
// Shared store implementation helpers. These do not change codec or digest-cache semantics.
namespace store_detail {
std::array<unsigned char, 32> record_digest(const EncodedRecord& record, const StoreOptions& options, Metrics& metrics);
std::uint64_t validate_batch(const std::vector<EncodedRecord>& records, Metrics& metrics);
std::filesystem::path prepare_directory(const std::string& directory);
void emit_row(std::ostream& out, const Record& record, std::string_view bytes);
}
std::unique_ptr<Store> open_store(const StoreOptions& options, Metrics& metrics, std::string& error);
}  // namespace tickshelf::bench
