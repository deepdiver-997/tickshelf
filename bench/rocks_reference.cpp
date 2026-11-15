// Native, library-level RocksDB reference. Not a network/pipeline benchmark.
#include "record.hpp"

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/version.h>
#include <rocksdb/write_batch.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <limits>
#include <locale>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <unistd.h>

namespace {
namespace fs = std::filesystem;
namespace bench = tickshelf::bench;
using Clock = std::chrono::steady_clock;
constexpr std::string_view kSourceSha = "17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa";
constexpr std::size_t kKeyBytes = 16;
constexpr std::size_t kValueBytes = 291;
constexpr bench::GeneratorOptions kGenerator{54, 5, 42, false};

struct Config {
    fs::path directory;
    fs::path json_file;
    fs::path export_file;
    std::uint64_t records = 0;
    std::uint64_t batch_rows = 1024;
};

[[noreturn]] void fail(const std::string& message) { throw std::runtime_error(message); }
void check(const rocksdb::Status& status, std::string_view operation) {
    if (!status.ok()) fail(std::string(operation) + ": " + status.ToString());
}
double elapsed(Clock::time_point start) {
    return std::chrono::duration<double>(Clock::now() - start).count();
}
std::uint64_t number(std::string_view text, std::string_view flag) {
    std::uint64_t value = 0;
    const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
    if (result.ec != std::errc{} || result.ptr != text.data() + text.size() || !value)
        fail(std::string(flag) + " requires a positive unsigned integer");
    return value;
}
fs::path normalize(const fs::path& path) { return fs::weakly_canonical(fs::absolute(path)); }
bool within(const fs::path& child, const fs::path& parent) {
    const auto relative = child.lexically_relative(parent);
    return !relative.empty() && *relative.begin() != "..";
}
Config parse(int argc, char** argv) {
    Config config;
    std::vector<std::string_view> seen;
    for (int i = 1; i < argc; ++i) {
        const std::string_view flag = argv[i];
        if (std::find(seen.begin(), seen.end(), flag) != seen.end())
            fail("duplicate option: " + std::string(flag));
        seen.push_back(flag);
        if (flag != "--directory" && flag != "--records" && flag != "--batch-rows" &&
            flag != "--json-file" && flag != "--export")
            fail("unknown option: " + std::string(flag));
        if (++i == argc) fail("missing value for " + std::string(flag));
        const std::string_view value = argv[i];
        if (value.empty()) fail("empty value for " + std::string(flag));
        if (flag == "--directory") config.directory = std::string(value);
        else if (flag == "--json-file") config.json_file = std::string(value);
        else if (flag == "--export") config.export_file = std::string(value);
        else if (flag == "--records") config.records = number(value, flag);
        else config.batch_rows = number(value, flag);
    }
    if (config.directory.empty() || !config.records)
        fail("--directory NEW and --records N are required");
    if (config.batch_rows > bench::kMaxBatchRecords)
        fail("--batch-rows must be at most 8192");
    if (config.records > std::numeric_limits<std::uint64_t>::max() / (kKeyBytes + kValueBytes))
        fail("record byte totals would overflow");
    // Reject unsupported generator ranges before creating any output.
    (void)bench::generate_record(config.records - 1, kGenerator);
    config.directory = normalize(config.directory);
    if (fs::exists(fs::symlink_status(config.directory)))
        fail("database directory must not already exist: " + config.directory.string());
    if (!fs::is_directory(config.directory.parent_path()))
        fail("database parent directory must exist");
    if (config.json_file.empty()) config.json_file = config.directory.string() + ".reference.json";
    config.json_file = normalize(config.json_file);
    if (within(config.json_file, config.directory)) fail("JSON file must be outside the database");
    if (!config.export_file.empty()) {
        config.export_file = normalize(config.export_file);
        if (within(config.export_file, config.directory)) fail("export must be outside the database");
        if (config.export_file == config.json_file) fail("JSON and export files must differ");
    }
    return config;
}

// O_EXCL prevents overwriting any previous result; failures leave artifacts for
// inspection. Output artifact I/O is outside the measured database workload.
class NewFile {
public:
    explicit NewFile(const fs::path& path) : path_(path.string()) {
        const int fd = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
        if (fd < 0) fail("create " + path_ + ": " + std::strerror(errno));
        file_ = ::fdopen(fd, "wb");
        if (!file_) {
            const int error = errno;
            ::close(fd);
            fail("fdopen " + path_ + ": " + std::strerror(error));
        }
    }
    ~NewFile() { if (file_) std::fclose(file_); }
    NewFile(const NewFile&) = delete;
    NewFile& operator=(const NewFile&) = delete;
    void write(std::string_view text) {
        if (std::fwrite(text.data(), 1, text.size(), file_) != text.size())
            fail("write failed: " + path_);
    }
    void close() {
        auto* file = std::exchange(file_, nullptr);
        if (std::fclose(file) != 0) fail("close failed: " + path_);
    }
private:
    std::string path_;
    std::FILE* file_ = nullptr;
};

// Exactly 16 ASCII hex bytes; bytewise order equals unsigned id order.
std::array<char, kKeyBytes> key_for(std::uint64_t id) {
    constexpr char digits[] = "0123456789abcdef";
    std::array<char, kKeyBytes> key{};
    for (std::size_t i = key.size(); i > 0; --i) {
        key[i - 1] = digits[id & 15];
        id >>= 4;
    }
    return key;
}
std::string hex(std::string_view bytes) {
    constexpr char digits[] = "0123456789abcdef";
    std::string result;
    result.reserve(bytes.size() * 2);
    for (unsigned char c : bytes) { result += digits[c >> 4]; result += digits[c & 15]; }
    return result;
}
using Properties = std::vector<std::pair<std::string, std::optional<std::uint64_t>>>;
Properties properties(rocksdb::DB& db) {
    using P = rocksdb::DB::Properties;
    const std::array names{P::kNumImmutableMemTable, P::kMemTableFlushPending,
        P::kNumRunningFlushes, P::kCompactionPending, P::kNumRunningCompactions,
        P::kEstimatePendingCompactionBytes, P::kCurSizeAllMemTables,
        P::kTotalSstFilesSize, P::kLiveSstFilesSize, P::kBackgroundErrors};
    Properties result;
    for (const auto& name : names) {
        std::uint64_t value = 0;
        result.emplace_back(name, db.GetIntProperty(name, &value)
            ? std::optional<std::uint64_t>(value) : std::nullopt);
    }
    return result;
}
void emit_properties(std::ostream& out, const Properties& values) {
    out << '{';
    bool first = true;
    for (const auto& [name, value] : values) {
        if (!first) out << ',';
        first = false;
        out << bench::json_quote(name) << ':';
        if (value) out << *value; else out << "null";
    }
    out << '}';
}
struct BatchTiming {
    std::uint64_t first_id;
    std::uint64_t rows;
    double prepare_seconds;
    double write_sync_seconds;
    double total_seconds;
};
struct Timing {
    double directory_and_open = 0;
    double write_phase = 0;
    double flush_wait = 0;
    double wait_for_compact = 0;
    double sync_wal = 0;
    double close = 0;
    double total = 0;
    double verify = 0;
};

std::string run(const Config& config) {
    const auto version = rocksdb::GetRocksVersionAsString();
    const auto& build = rocksdb::GetRocksBuildProperties();
    const auto sha = build.find("rocksdb_build_git_sha");
    if (version != "11.13.2" || sha == build.end() || sha->second != kSourceSha)
        fail("reference requires the audited RocksDB 11.13.2 build/commit");
    NewFile json_file(config.json_file);
    std::unique_ptr<NewFile> export_file;
    if (!config.export_file.empty()) export_file = std::make_unique<NewFile>(config.export_file);

    rocksdb::Options options;
    options.create_if_missing = true;
    options.error_if_exists = true;
    options.use_fsync = true;
    options.compression = rocksdb::kNoCompression;
    options.bottommost_compression = rocksdb::kNoCompression;
    options.disable_auto_compactions = false;
    options.manual_wal_flush = false;
    options.allow_mmap_writes = false;
    options.max_background_jobs = 2;
    rocksdb::WriteOptions write_options;
    write_options.sync = true;
    write_options.disableWAL = false;
    std::vector<BatchTiming> batches;
    Timing timing;
    const auto start = Clock::now();
    if (!fs::create_directory(config.directory)) fail("database directory was concurrently created");
    std::unique_ptr<rocksdb::DB> db;
    check(rocksdb::DB::Open(options, config.directory.string(), &db), "DB::Open");
    timing.directory_and_open = elapsed(start);
    const auto write_start = Clock::now();
    for (std::uint64_t first = 0; first < config.records;) {
        const auto batch_start = Clock::now();
        const auto rows = std::min(config.batch_rows, config.records - first);
        rocksdb::WriteBatch batch;
        for (std::uint64_t offset = 0; offset < rows; ++offset) {
            const auto key = key_for(first + offset);
            const auto value = bench::encode_record(bench::generate_record(first + offset, kGenerator));
            if (value.size() != kValueBytes) fail("generator no longer produces 291-byte values");
            check(batch.Put(rocksdb::Slice(key.data(), key.size()), value), "WriteBatch::Put");
        }
        const double prepare_seconds = elapsed(batch_start);
        const auto sync_start = Clock::now();
        check(db->Write(write_options, &batch), "DB::Write(sync=true,WAL=on)");
        const double sync_seconds = elapsed(sync_start);
        const double total_seconds = elapsed(batch_start);
        batches.push_back({first, rows, prepare_seconds, sync_seconds, total_seconds});
        first += rows;
    }
    timing.write_phase = elapsed(write_start);
    const auto after_writes = properties(*db);
    rocksdb::FlushOptions flush;
    flush.wait = true;
    flush.allow_write_stall = true;
    auto phase = Clock::now();
    check(db->Flush(flush), "DB::Flush(wait=true)");
    timing.flush_wait = elapsed(phase);
    const auto after_flush = properties(*db);
    rocksdb::WaitForCompactOptions wait;
    wait.wait_for_purge = true;
    phase = Clock::now();
    check(db->WaitForCompact(wait), "DB::WaitForCompact(wait_for_purge=true)");
    timing.wait_for_compact = elapsed(phase);
    const auto after_compact = properties(*db);
    phase = Clock::now();
    check(db->SyncWAL(), "DB::SyncWAL");
    timing.sync_wal = elapsed(phase);
    const auto before_close = properties(*db);
    phase = Clock::now();
    const auto close_status = db->Close();
    db.reset();
    check(close_status, "DB::Close");
    timing.close = elapsed(phase);
    timing.total = elapsed(start);

    // Reopen from files and verify every key/value AND the absence of extras.
    // No verification, export, or report formatting enters workload timings.
    const auto verify_start = Clock::now();
    options.create_if_missing = false;
    options.error_if_exists = false;
    check(rocksdb::DB::OpenForReadOnly(options, config.directory.string(), &db), "reopen read-only");
    {
        rocksdb::ReadOptions read;
        read.verify_checksums = true;
        std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(read));
        it->SeekToFirst();
        for (std::uint64_t id = 0; id < config.records; ++id) {
            if (!it->Valid()) fail("reopen verification: missing key at id " + std::to_string(id));
            const auto key = key_for(id);
            if (it->key().compare(rocksdb::Slice(key.data(), key.size())) != 0)
                fail("reopen verification: key/order mismatch at id " + std::to_string(id));
            const auto expected = bench::encode_record(bench::generate_record(id, kGenerator));
            if (it->value().compare(expected) != 0)
                fail("reopen verification: payload mismatch at id " + std::to_string(id));
            if (export_file) {
                const std::string_view stored(it->value().data(), it->value().size());
                bench::Record record;
                std::string error;
                if (!bench::decode_record(stored, record, error)) fail("stored record decode: " + error);
                export_file->write("{\"symbol\":" + bench::json_quote(record.symbol) +
                    ",\"time\":" + std::to_string(record.time) + ",\"hex\":\"" + hex(stored) + "\"}\n");
            }
            it->Next();
        }
        check(it->status(), "reopen verification iterator");
        if (it->Valid()) fail("reopen verification: unexpected extra keys");
    }
    const auto verify_close = db->Close();
    db.reset();
    check(verify_close, "read-only DB::Close");
    if (export_file) export_file->close();
    timing.verify = elapsed(verify_start);

    std::ostringstream out;
    out.imbue(std::locale::classic());
    out << std::setprecision(17)
        << "{\n  \"schema\":\"tickshelf.rocksdb-reference.v1\","
        << "\n  \"scope\":\"native_library_not_network_pipeline\","
        << "\n  \"rocksdb_version\":" << bench::json_quote(version)
        << ",\n  \"expected_source_sha\":" << bench::json_quote(kSourceSha)
        << ",\n  \"rocksdb_build_properties\":{";
    bool first_property = true;
    for (const auto& [name, value] : std::map<std::string, std::string>(build.begin(), build.end())) {
        if (!first_property) out << ',';
        first_property = false;
        out << bench::json_quote(name) << ':' << bench::json_quote(value);
    }
    out << "},\n  \"directory\":" << bench::json_quote(config.directory.string())
        << ",\n  \"json_file\":" << bench::json_quote(config.json_file.string())
        << ",\n  \"export_file\":";
    if (config.export_file.empty()) out << "null";
    else out << bench::json_quote(config.export_file.string());
    out << ",\n  \"records\":" << config.records
        << ",\n  \"batch_rows\":" << config.batch_rows
        << ",\n  \"batches\":" << batches.size()
        << ",\n  \"key_encoding\":\"16_ascii_lowercase_hex_unsigned_id\","
        << "\n  \"key_bytes_per_record\":" << kKeyBytes
        << ",\n  \"value_bytes_per_record\":" << kValueBytes
        << ",\n  \"payload_bytes\":" << config.records * kValueBytes
        << ",\n  \"logical_key_value_bytes\":" << config.records * (kKeyBytes + kValueBytes)
        << ",\n  \"generator\":{\"symbols\":54,\"depth\":5,\"seed\":42,\"optional_fields\":false},"
        << "\n  \"options\":{\"sync\":true,\"disableWAL\":false,\"use_fsync\":true,"
        << "\"compression\":\"none\",\"bottommost_compression\":\"none\","
        << "\"disable_auto_compactions\":false,\"manual_wal_flush\":false,"
        << "\"allow_mmap_writes\":false,\"max_background_jobs\":2},"
        << "\n  \"sync_source_evidence\":\"env/io_posix.cc:1840-1851: "
        << "HAVE_FULLFSYNC selects fcntl(F_FULLFSYNC); otherwise fsync\","
        << "\n  \"audited_local_build_evidence\":\"/tmp/tickshelf-oss-rocksdb-20261114/"
        << "build-rtti/CMakeCache.txt:HAVE_FULLFSYNC:INTERNAL=1,USE_RTTI:STRING=ON; "
        << "build-rtti/CMakeFiles/rocksdb.dir/flags.make:-DHAVE_FULLFSYNC,-DROCKSDB_USE_RTTI\","
        << "\n  \"hardware_power_loss_protection_verified\":false,"
        << "\n  \"timing_scope\":\"create_directory+open+generate_encode_batch+sync_writes+"
        << "debt_snapshots+flush_wait+wait_for_compact_and_purge+sync_wal+close_and_delete; "
        << "excludes CLI/options/output reservation and read-only reopen verification/export/report I/O\","
        << "\n  \"seconds\":{\"directory_and_open\":" << timing.directory_and_open
        << ",\"write_phase\":" << timing.write_phase
        << ",\"final_flush_wait\":" << timing.flush_wait
        << ",\"final_wait_for_compact_and_purge\":" << timing.wait_for_compact
        << ",\"final_sync_wal\":" << timing.sync_wal
        << ",\"close_and_delete\":" << timing.close
        << ",\"total\":" << timing.total
        << ",\"verification_and_export_outside_timing\":" << timing.verify << '}'
        << ",\n  \"records_per_second_including_finalization\":"
        << (static_cast<double>(config.records) / timing.total)
        << ",\n  \"payload_mib_per_second_including_finalization\":"
        << (static_cast<double>(config.records) * kValueBytes / 1048576.0 / timing.total)
        << ",\n  \"reopen_verified_records\":" << config.records
        << ",\n  \"reopen_verified_no_extra_keys\":true,"
        << "\n  \"debt_after_writes\":";
    emit_properties(out, after_writes);
    out << ",\n  \"debt_after_flush\":"; emit_properties(out, after_flush);
    out << ",\n  \"debt_after_wait_for_compact\":"; emit_properties(out, after_compact);
    out << ",\n  \"debt_before_close\":"; emit_properties(out, before_close);
    out << ",\n  \"batch_timings\":[";
    for (std::size_t i = 0; i < batches.size(); ++i) {
        const auto& batch = batches[i];
        if (i) out << ',';
        out << "\n    {\"first_id\":" << batch.first_id << ",\"rows\":" << batch.rows
            << ",\"prepare_seconds\":" << batch.prepare_seconds
            << ",\"write_sync_seconds\":" << batch.write_sync_seconds
            << ",\"total_seconds\":" << batch.total_seconds << '}';
    }
    out << "\n  ]\n}\n";
    auto result = out.str();
    json_file.write(result);
    json_file.close();
    return result;
}
}  // namespace

int main(int argc, char** argv) {
    if (argc == 2 && std::string_view(argv[1]) == "--help") {
        std::cout << "Usage: rocks_reference --directory NEW --records N [--batch-rows 1..8192] "
                     "[--json-file NEW.json] [--export NEW.jsonl]\n"
                     "Native library-only reference; WAL on, sync=true, use_fsync=true, no compression.\n"
                     "Default JSON output: NEW.reference.json; database and outputs must not exist.\n"
                     "Includes final Flush(wait), WaitForCompact, SyncWAL and Close in total time.\n";
        return 0;
    }
    try {
        std::cout << run(parse(argc, argv));
        if (!std::cout) fail("stdout write failed");
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "rocks_reference: " << e.what() << '\n';
        return 1;
    }
}
