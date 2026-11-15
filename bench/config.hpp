#pragma once
#include <cstdint>
#include <string>

namespace tickshelf::bench {
struct Config {
    std::string directory;
    std::string host = "127.0.0.1";
    std::uint32_t port = 19900;
    std::string redis_host = "127.0.0.1";
    std::uint32_t redis_port = 6379;
    std::string input = "json", input_file, input_manifest, output_file;
    std::uint64_t records = 54000;
    std::uint32_t symbols = 54, depth = 5;
    std::uint64_t seed = 42;
    std::uint32_t duplicate_every = 0;
    bool optional_fields = false;
    std::uint32_t batch_records = 256, hash_batch_records = 256, redis_batch_records = 256;
    std::uint64_t rate = 0;
    std::string parse = "real", hash = "dedup", hash_mode = "batch", hash_algorithm = "sha256";
    std::uint64_t dedup_max_entries = 1000000;
    std::string redis = "roundtrip", backend = "sqlite", synchronous = "FULL", transaction_model = "light";
    std::string sqlite_fullfsync = "default";  // default | on | off; report effective PRAGMA
    std::string sqlite_compact_lookup = "subquery";  // subquery | separate (sqlite_compact only)
    bool reuse_statement = true;
    std::uint32_t autocheckpoint = 1000, transaction_rows = 1000;
    std::uint64_t transaction_bytes = 1048576;
    std::uint32_t transaction_ms = 100, timeout_s = 120;
    std::string failpoint;
};
bool load_config(const std::string& path, Config& out, std::string& error);
bool validate_config(const Config& config, std::string& error);
std::string config_json(const Config& config);
}  // namespace tickshelf::bench
