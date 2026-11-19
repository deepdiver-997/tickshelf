#include "config.hpp"
#include "record.hpp"

#include <filesystem>
#include <fstream>
#include <limits>
#include <set>
#include <sstream>
#include <simdjson.h>

namespace tickshelf::bench {
namespace {
bool one_of(const std::string& value, std::initializer_list<const char*> values) {
    for (const auto* candidate : values) if (value == candidate) return true;
    return false;
}
}

bool validate_config(const Config& c, std::string& error) {
    auto check = [&](bool ok, const char* message) {
        if (!ok) error = message;
        return ok;
    };
    return check(!c.directory.empty(), "directory is required") &&
        check(c.directory.find('\0') == std::string::npos && c.input_file.find('\0') == std::string::npos &&
            c.output_file.find('\0') == std::string::npos && c.input_manifest.find('\0') == std::string::npos, "paths must not contain NUL") &&
        check(c.host == "127.0.0.1" && c.redis_host == "127.0.0.1", "benchmark only permits loopback hosts") &&
        check(c.port > 0 && c.port <= 65535 && c.redis_port > 0 && c.redis_port <= 65535 && c.port != c.redis_port, "invalid or overlapping ports") &&
        check(c.records > 0 && c.records <= 100000000, "records must be 1..100000000") &&
        check(c.symbols > 0 && c.symbols <= 50000 && c.depth <= 10, "symbols must be 1..50000; depth must be 0..10") &&
        check(c.seed <= 1000000000, "seed exceeds supported generator range") &&
        check(c.batch_records > 0 && c.batch_records <= kMaxBatchRecords &&
            c.hash_batch_records > 0 && c.hash_batch_records <= kMaxBatchRecords &&
            c.redis_batch_records > 0 && c.redis_batch_records <= kMaxBatchRecords, "invalid stage batch_records") &&
        check(c.duplicate_every != 1, "duplicate_every must be 0 or >=2") &&
        check(c.dedup_max_entries > 0 && c.dedup_max_entries <= 10000000, "invalid dedup_max_entries") &&
        check(c.transaction_rows > 0 && c.transaction_rows <= 1000000, "invalid transaction_rows") &&
        check(c.transaction_bytes >= 1024 && c.transaction_bytes <= 64 * 1024 * 1024, "transaction_bytes must be 1024..67108864") &&
        check(c.transaction_ms > 0 && c.transaction_ms <= 60000, "transaction_ms must be 1..60000") &&
        check(c.timeout_s > 0 && c.timeout_s <= 86400, "timeout_s must be 1..86400") &&
        check(c.rate <= 100000000, "invalid offered rate") &&
        check(c.autocheckpoint <= 10000000, "invalid autocheckpoint") &&
        check(one_of(c.input, {"json", "binary"}), "input must be json or binary") &&
        check(one_of(c.parse, {"real", "replay"}), "parse must be real or replay") &&
        check(one_of(c.hash, {"dedup", "only", "off"}), "hash must be dedup, only or off") &&
        check(one_of(c.hash_mode, {"batch", "record"}), "hash_mode must be batch or record") &&
        check(one_of(c.hash_algorithm, {"sha256", "fnv1a64"}), "hash_algorithm must be sha256 or fnv1a64") &&
        check(one_of(c.redis, {"roundtrip", "publish", "encode", "off"}), "invalid redis mode") &&
        check(one_of(c.backend, {"sqlite", "sqlite_compact", "segment", "segment_rocksdb", "segment_day", "rocksdb", "null"}), "invalid backend") &&
        check(one_of(c.synchronous, {"FULL", "NORMAL"}), "invalid synchronous level") &&
        check(one_of(c.sqlite_fullfsync, {"default", "on", "off"}), "invalid sqlite_fullfsync") &&
        check(one_of(c.store_lookup, {"exact", "assume_unique"}), "store_lookup must be exact or assume_unique") &&
        check(c.store_lookup == "exact" || one_of(c.backend, {"sqlite_compact", "rocksdb"}), "store_lookup=assume_unique requires backend=sqlite_compact or rocksdb") &&
        check(c.store_lookup == "exact" || c.duplicate_every == 0, "store_lookup=assume_unique requires duplicate_every=0 synthetic input") &&
        check(c.store_lookup == "exact" || c.input_manifest.empty(), "store_lookup=assume_unique does not support capture input_manifest") &&
        check(one_of(c.sqlite_compact_lookup, {"subquery", "separate"}), "invalid sqlite_compact_lookup") &&
        check(c.sqlite_compact_lookup != "separate" || c.backend == "sqlite_compact", "sqlite_compact_lookup=separate requires backend=sqlite_compact") &&
        check((c.backend != "segment" && c.backend != "segment_rocksdb" && c.backend != "segment_day" && c.backend != "rocksdb") || c.synchronous == "FULL", "segment/rocksdb backends require synchronous=FULL") &&
#ifndef TICKSHELF_HAVE_ROCKSDB
        check(c.backend != "segment_rocksdb" && c.backend != "rocksdb", "rocksdb backends require TICKSHELF_BENCH_ROCKSDB=ON") &&
#endif
        check(one_of(c.transaction_model, {"light", "heavy"}), "invalid transaction_model") &&
        check(one_of(c.failpoint, {"", "after_append", "after_sync", "after_index", "after_metadata", "before_write", "after_write"}), "invalid failpoint") &&
        check(c.failpoint.empty() ||
            (one_of(c.failpoint, {"after_append", "after_sync", "after_index"}) && one_of(c.backend, {"segment", "segment_rocksdb"})) ||
            (one_of(c.failpoint, {"after_append", "after_sync", "after_metadata"}) && c.backend == "segment_day") ||
            (one_of(c.failpoint, {"before_write", "after_write"}) && c.backend == "rocksdb"), "failpoint is not supported by this backend") &&
        check(c.parse != "replay" || c.input == "json", "parse replay requires JSON input") &&
        check(c.input_manifest.empty() || (!c.input_file.empty() && c.input == "json"), "input_manifest requires JSON input_file") &&
        check(c.input_manifest.empty() || c.parse == "real", "capture input_manifest does not support parse=replay");
}

bool load_config(const std::string& path, Config& out, std::string& error) {
    if (path.find('\0') != std::string::npos) { error = "config path must not contain NUL"; return false; }
    std::error_code filesystem_error;
    if (!std::filesystem::is_regular_file(path, filesystem_error)) {
        error = "config must be a regular file: " + path; return false;
    }
    std::ifstream file(path, std::ios::binary);
    if (!file) { error = "cannot open config: " + path; return false; }
    std::string text(65537, '\0');
    file.read(text.data(), static_cast<std::streamsize>(text.size()));
    text.resize(static_cast<std::size_t>(file.gcount()));
    if (file.bad()) { error = "config read failed"; return false; }
    if (text.size() > 65536) { error = "config exceeds 64KiB"; return false; }
    simdjson::dom::parser parser;
    simdjson::dom::element doc;
    if (parser.parse(text).get(doc)) { error = "invalid config JSON"; return false; }
    simdjson::dom::object object;
    if (doc.get_object().get(object)) { error = "config must be an object"; return false; }
    Config c;
    std::set<std::string> seen;
    for (auto field : object) {
        const std::string key(field.key);
        if (!seen.insert(key).second) { error = "duplicate config key: " + key; return false; }
        const auto value = field.value;
        bool known = false, ok = true;
#define STR(name) if (key == #name) { std::string_view v; ok = !value.get_string().get(v); if (ok) c.name = v; known = true; }
#define NUM(name) if (key == #name) { std::uint64_t v = 0; ok = !value.get_uint64().get(v) && v <= std::numeric_limits<decltype(c.name)>::max(); if (ok) c.name = static_cast<decltype(c.name)>(v); known = true; }
#define BOOL(name) if (key == #name) { bool v = false; ok = !value.get_bool().get(v); if (ok) c.name = v; known = true; }
        STR(directory) STR(host) STR(redis_host) STR(input) STR(input_file) STR(input_manifest) STR(output_file)
        STR(parse) STR(hash) STR(hash_mode) STR(hash_algorithm) STR(redis) STR(backend)
        STR(synchronous) STR(transaction_model) STR(failpoint) STR(sqlite_fullfsync) STR(sqlite_compact_lookup) STR(store_lookup)
        NUM(port) NUM(redis_port) NUM(records) NUM(symbols) NUM(depth) NUM(seed)
        NUM(duplicate_every) NUM(batch_records) NUM(hash_batch_records) NUM(redis_batch_records) NUM(rate) NUM(dedup_max_entries)
        NUM(autocheckpoint) NUM(transaction_rows) NUM(transaction_bytes) NUM(transaction_ms) NUM(timeout_s)
        BOOL(optional_fields) BOOL(reuse_statement)
#undef STR
#undef NUM
#undef BOOL
        if (!known || !ok) { error = (known ? "wrong type or overflow: " : "unknown config key: ") + key; return false; }
    }
    if (!validate_config(c, error)) return false;
    const auto parent = std::filesystem::absolute(path).parent_path();
    for (auto* p : {&c.directory, &c.input_file, &c.input_manifest, &c.output_file})
        if (!p->empty() && std::filesystem::path(*p).is_relative()) *p = (parent / *p).lexically_normal().string();
    out = std::move(c);
    return true;
}

std::string config_json(const Config& c) {
    std::ostringstream out;
    out << '{';
    bool comma = false;
    auto key = [&](const char* name) { if (comma) out << ','; comma = true; out << json_quote(name) << ':'; };
#define STR(name) key(#name); out << json_quote(c.name);
#define NUM(name) key(#name); out << c.name;
#define BOOL(name) key(#name); out << (c.name ? "true" : "false");
    STR(directory) STR(host) STR(redis_host) STR(input) STR(input_file) STR(input_manifest) STR(output_file)
    STR(parse) STR(hash) STR(hash_mode) STR(hash_algorithm) STR(redis) STR(backend)
    STR(synchronous) STR(transaction_model) STR(failpoint) STR(sqlite_fullfsync) STR(sqlite_compact_lookup) STR(store_lookup)
    NUM(port) NUM(redis_port) NUM(records) NUM(symbols) NUM(depth) NUM(seed)
    NUM(duplicate_every) NUM(batch_records) NUM(hash_batch_records) NUM(redis_batch_records) NUM(rate) NUM(dedup_max_entries)
    NUM(autocheckpoint) NUM(transaction_rows) NUM(transaction_bytes) NUM(transaction_ms) NUM(timeout_s)
    BOOL(optional_fields) BOOL(reuse_statement)
#undef STR
#undef NUM
#undef BOOL
    out << '}';
    return out.str();
}
}  // namespace tickshelf::bench
