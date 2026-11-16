#include "config.hpp"
#include "hash.hpp"
#include "metrics.hpp"
#include "pipeline.hpp"
#include "protocol.hpp"
#include "record.hpp"
#include "store.hpp"
#include "common/net.hpp"
#include "components/redis/resp_client.hpp"

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <poll.h>
#include <set>
#include <sstream>
#include <sys/stat.h>
#include <stdexcept>
#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>
#include <simdjson.h>

namespace tickshelf::bench {
namespace {
volatile std::sig_atomic_t stopping = 0;
void stop(int) { stopping = 1; }
void require(bool value, const std::string& message) { if (!value) throw std::runtime_error(message); }
std::string hex64(std::uint64_t n) {
    std::ostringstream out;
    out << std::hex << std::setw(16) << std::setfill('0') << n;
    return out.str();
}
void write_fd(int fd, std::string_view value) {
    while (!value.empty()) {
        const auto n = ::write(fd, value.data(), value.size());
        if (n < 0 && errno == EINTR && !stopping) continue;
        require(n > 0, "output write failed");
        value.remove_prefix(static_cast<std::size_t>(n));
    }
}
net::UniqueFd new_file(const std::filesystem::path& path) {
    const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    require(fd >= 0, "refuse existing/unwritable output: " + path.string());
    return net::UniqueFd(fd);
}
void write_json_file(const std::filesystem::path& path, const std::string& json) {
    auto fd = new_file(path);
    write_fd(fd.get(), json + "\n");
}
class Progress {
public:
    Progress(const Config& c, const std::string& role, Metrics& metrics)
        : fd_(new_file(std::filesystem::path(c.directory) / (role == "source" ? "source-progress.jsonl" : "progress.jsonl"))), metrics_(metrics) {}
    void emit(std::uint64_t start, bool force = false) {
        const auto now = clock_ns();
        if (!start || (!force && now - last_ < 1000000000ULL)) return;
        last_ = now;
        std::ostringstream line;
        line << std::setprecision(17) << "{\"elapsed_s\":" << (now - start) / 1e9
            << ",\"elapsed_ns\":" << now - start << ",\"monotonic_ns\":" << now << ",\"metrics\":" << metrics_.json() << "}\n";
        write_fd(fd_.get(), line.str());
    }
private:
    net::UniqueFd fd_;
    Metrics& metrics_;
    std::uint64_t last_ = 0;
};
void wait_socket(int fd, short events, std::uint64_t deadline, int max_ms = 100) {
    for (;;) {
        require(!stopping, "interrupted");
        require(clock_ns() < deadline, "socket/run deadline exceeded");
        pollfd pfd{fd, events, 0};
        const int rc = ::poll(&pfd, 1, max_ms);
        if (rc < 0 && errno == EINTR) continue;
        require(rc >= 0, "poll failed");
        if (rc == 0) return;
        require(!(pfd.revents & (POLLERR | POLLNVAL)), "socket error");
        return;
    }
}
void send_all(int fd, std::string_view bytes, std::uint64_t deadline) {
    while (!bytes.empty()) {
        require(!stopping && clock_ns() < deadline, "send interrupted or timed out");
        const auto n = ::send(fd, bytes.data(), bytes.size(), 0);
        if (n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) { wait_socket(fd, POLLOUT, deadline); continue; }
        require(n > 0, "send failed: " + std::string(std::strerror(errno)));
        bytes.remove_prefix(static_cast<std::size_t>(n));
    }
}
std::uint32_t payload_size(std::string_view header) {
    std::uint32_t size = 0;
    for (unsigned i = 0; i < 4; ++i) size |= static_cast<std::uint32_t>(static_cast<unsigned char>(header[16 + i])) << (8 * i);
    return size;
}
// Open first, then fstat the actual descriptor. O_NONBLOCK prevents FIFO open
// from hanging before a non-regular path can be rejected (including path races).
class RegularInput {
public:
    void open(const std::string& path) {
        require(path.find('\0') == std::string::npos, "input path contains NUL");
        fd_.reset(::open(path.c_str(), O_RDONLY | O_NONBLOCK | O_CLOEXEC));
        require(fd_.is_open(), "cannot open input: " + path);
        struct stat info{};
        require(::fstat(fd_.get(), &info) == 0 && S_ISREG(info.st_mode) && info.st_size >= 0,
            "input must be a regular file: " + path);
        size_ = static_cast<std::uint64_t>(info.st_size);
    }
    bool is_open() const { return fd_.is_open(); }
    std::uint64_t size() const { return size_; }
    void read_exact(char* target, std::size_t size, const char* error) {
        while (size) {
            require(!stopping, "input read interrupted");
            const auto n = ::read(fd_.get(), target, size);
            if (n < 0 && errno == EINTR) continue;
            require(n > 0, error);
            target += n;
            size -= static_cast<std::size_t>(n);
        }
    }
    bool eof() {
        char byte;
        for (;;) {
            const auto n = ::read(fd_.get(), &byte, 1);
            if (n < 0 && errno == EINTR && !stopping) continue;
            require(n >= 0, "input EOF check failed");
            return n == 0;
        }
    }
private:
    net::UniqueFd fd_;
    std::uint64_t size_ = 0;
};
Message file_message(RegularInput& input, std::string* raw = nullptr) {
    std::string wire(kWireHeaderBytes, '\0');
    input.read_exact(wire.data(), kWireHeaderBytes, "truncated input frame header");
    const auto size = payload_size(wire);
    require(size <= kMaxPayloadBytes, "input frame too large");
    wire.resize(kWireHeaderBytes + size);
    input.read_exact(wire.data() + kWireHeaderBytes, size, "truncated input payload");
    MessageParser parser;
    std::vector<Message> out;
    std::string error;
    require(parser.feed(wire, out, error) && out.size() == 1 && !parser.incomplete(), "invalid input frame: " + error);
    if (raw) *raw = std::move(wire);
    return std::move(out.front());
}

constexpr std::size_t kManifestMaxBytes = 16 * 1024 * 1024;
constexpr std::size_t kManifestMaxFrames = 100000;
struct CaptureFrame {
    std::uint64_t sequence = 0;
    std::uint32_t records = 0;
    std::array<unsigned char, 32> sha256{};
};
std::array<unsigned char, 32> manifest_digest(simdjson::dom::element value) {
    std::string_view text;
    require(!value.get_string().get(text) && text.size() == 64, "manifest SHA256 must be 64 hex characters");
    auto hex = [](char c) -> unsigned {
        if (c >= '0' && c <= '9') return static_cast<unsigned>(c - '0');
        if (c >= 'a' && c <= 'f') return static_cast<unsigned>(c - 'a' + 10);
        if (c >= 'A' && c <= 'F') return static_cast<unsigned>(c - 'A' + 10);
        throw std::runtime_error("manifest SHA256 contains non-hex character");
    };
    std::array<unsigned char, 32> digest{};
    for (std::size_t i = 0; i < digest.size(); ++i)
        digest[i] = static_cast<unsigned char>(hex(text[i * 2]) * 16 + hex(text[i * 2 + 1]));
    return digest;
}
std::uint64_t manifest_uint(simdjson::dom::element value) {
    std::uint64_t number = 0;
    require(!value.get_uint64().get(number), "manifest integer type/range error");
    return number;
}
std::vector<CaptureFrame> load_manifest(const Config& c, Metrics& metrics, const std::string& role = "source") {
    if (c.input_manifest.empty()) return {};
    Timer timer(metrics, role + "_manifest_validate"); // Preparation, before run clock starts.
    RegularInput input;
    input.open(c.input_manifest);
    require(input.size() > 0 && input.size() <= kManifestMaxBytes, "manifest exceeds 16MiB or is empty");
    std::string text(static_cast<std::size_t>(input.size()), '\0');
    input.read_exact(text.data(), text.size(), "truncated manifest");
    require(input.eof(), "manifest grew during read");
    simdjson::dom::parser parser;
    simdjson::dom::element doc;
    simdjson::dom::object root;
    require(!parser.parse(text).get(doc) && !doc.get_object().get(root), "manifest must be a JSON object");
    std::set<std::string> seen;
    std::uint64_t version = 0, records = 0, total_frames = 0;
    std::vector<CaptureFrame> frames;
    for (auto field : root) {
        const std::string key(field.key);
        require(seen.insert(key).second, "duplicate manifest key: " + key);
        if (key == "schema_version") version = manifest_uint(field.value);
        else if (key == "records") records = manifest_uint(field.value);
        else if (key == "total_frames") total_frames = manifest_uint(field.value);
        else if (key == "input_sha256" || key == "raw_input_sha256") (void)manifest_digest(field.value);
        else if (key == "frames") {
            simdjson::dom::array array;
            require(!field.value.get_array().get(array), "manifest frames must be an array");
            require(array.size() <= kManifestMaxFrames, "manifest exceeds 100000 frames");
            frames.reserve(array.size());
            for (auto element : array) {
                simdjson::dom::object object;
                require(!element.get_object().get(object), "manifest frame must be an object");
                CaptureFrame frame;
                std::set<std::string> keys;
                for (auto item : object) {
                    const std::string name(item.key);
                    require(keys.insert(name).second, "duplicate manifest frame key: " + name);
                    if (name == "sequence") frame.sequence = manifest_uint(item.value);
                    else if (name == "records") {
                        const auto n = manifest_uint(item.value);
                        require(n <= kMaxBatchRecords, "manifest frame records must be 0..8192 (capture preserves empty JSON frames)");
                        frame.records = static_cast<std::uint32_t>(n);
                    } else if (name == "payload_sha256") frame.sha256 = manifest_digest(item.value);
                    else throw std::runtime_error("unknown manifest frame key: " + name);
                }
                require(keys.size() == 3, "manifest frame is missing required fields");
                frames.push_back(frame);
            }
        } else if (key != "source" && key != "policy" && key != "coverage")
            throw std::runtime_error("unknown manifest key: " + key);
    }
    for (const auto* name : {"schema_version", "records", "total_frames", "frames", "input_sha256"})
        require(seen.count(name), std::string("manifest missing required key: ") + name);
    require(version == 1, "unsupported manifest schema_version");
    require(records == c.records, "manifest records differs from config.records");
    require(total_frames > 0 && total_frames <= kManifestMaxFrames && total_frames == frames.size(), "manifest total_frames mismatch or limit exceeded");
    std::uint64_t position = 0;
    for (const auto& frame : frames) {
        require(frame.sequence == position, "manifest sequence must be cumulative physical record offset");
        require(position <= records && frame.records <= records - position, "manifest record total exceeds declared records");
        position += frame.records;
    }
    require(position == records, "manifest frame record total mismatch");
    metrics.counters[role + "_manifest_frames"] = frames.size();
    metrics.counters[role + "_manifest_bytes"] = text.size();
    return frames;
}
void validate_capture_digest(const Message& message, const CaptureFrame& frame,
                             Metrics& metrics, const std::string& role = "source") {
    Timer timer(metrics, role + "_manifest_payload_validate");
    std::array<unsigned char, 32> digest{};
    unsigned length = 0;
    require(EVP_Digest(message.payload.data(), message.payload.size(), digest.data(), &length, EVP_sha256(), nullptr) == 1 &&
        length == digest.size() && digest == frame.sha256, "capture payload SHA256 mismatch");
}
void validate_capture_frame(const Message& message, const CaptureFrame& frame,
                            JsonDecoder& decoder, Metrics& metrics) {
    validate_capture_digest(message, frame, metrics);
    Timer timer(metrics, "source_count_validate");
    std::vector<Record> records;
    std::string error;
    require(decoder.parse(message.payload, records, error), "capture JSON validation failed: " + error);
    require(records.size() == frame.records, "capture payload record count differs from manifest");
}
GeneratorOptions generator(const Config& c) { return {c.symbols, c.depth, c.seed, c.optional_fields}; }
std::vector<Record> generated(const Config& c, std::uint64_t position, std::uint32_t count) {
    std::vector<Record> records;
    records.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i)
        records.push_back(generate_record(logical_id(position + i, c.duplicate_every), generator(c)));
    return records;
}
StoreOptions store_options(const Config& c) {
    StoreOptions options{c.backend, c.directory, c.synchronous, c.transaction_model, c.reuse_statement,
        static_cast<int>(c.autocheckpoint), c.failpoint};
    options.fullfsync = c.sqlite_fullfsync;
    options.sqlite_compact_lookup = c.sqlite_compact_lookup;
    return options;
}
std::string result_json(const Config& c, const std::string& role, const std::string& status,
                        const std::string& error, const Metrics& metrics, std::uint64_t started,
                        std::uint64_t finished, std::string_view extra = {}) {
    std::ostringstream out;
    out << std::setprecision(17) << "{\"role\":" << json_quote(role) << ",\"status\":" << json_quote(status)
        << ",\"error\":" << json_quote(error) << ",\"elapsed_s\":" << (started ? (finished - started) / 1e9 : 0)
        << ",\"elapsed_ns\":" << (started ? finished - started : 0)
        << ",\"started_ns\":" << started << ",\"finished_ns\":" << finished
        << ",\"config\":" << config_json(c) << ",\"metrics\":" << metrics.json()
        << ",\"dependencies\":{\"simdjson\":" << json_quote(SIMDJSON_VERSION)
        << ",\"simdjson_implementation\":" << json_quote(simdjson::get_active_implementation()->name())
        << ",\"openssl\":" << json_quote(OpenSSL_version(OPENSSL_VERSION))
        << ",\"compiler\":" << json_quote(__VERSION__) << '}';
    if (!extra.empty()) out << ',' << extra;
    out << '}';
    return out.str();
}

struct ReplayBatch { Message message; std::vector<Record> records; };
std::vector<ReplayBatch> prepare_replay(const Config& c) {
    std::vector<ReplayBatch> batches;
    if (c.parse != "replay") return batches;
    RegularInput input;
    if (!c.input_file.empty()) input.open(c.input_file);
    JsonDecoder decoder;
    std::size_t budget = 0;
    for (std::uint64_t pos = 0; pos < c.records;) {
        const auto count = static_cast<std::uint32_t>(std::min<std::uint64_t>(c.batch_records, c.records - pos));
        ReplayBatch batch;
        batch.message = input.is_open() ? file_message(input) : Message{MessageType::Json, pos, records_json(generated(c, pos, count))};
        require(batch.message.type == MessageType::Json && batch.message.sequence == pos, "replay fixture type/sequence mismatch");
        std::string error;
        require(decoder.parse(batch.message.payload, batch.records, error), "replay preparse failed: " + error);
        require(batch.records.size() == count, "replay fixture count mismatch");
        budget += batch.message.payload.size();
        for (const auto& r : batch.records) budget += sizeof(Record) + r.symbol.size() + encode_record(r).size();
        require(budget <= 128 * 1024 * 1024, "replay preloaded data exceeds 128MiB budget; use a smaller diagnostic case");
        batches.push_back(std::move(batch));
        pos += count;
    }
    if (input.is_open()) {
        const auto end = file_message(input);
        require(end.type == MessageType::End && end.sequence == c.records && input.eof(), "invalid replay END/trailing bytes");
    }
    return batches;
}

int receiver(const Config& c) {
    Metrics metrics;
    std::uint64_t start = 0;
    std::string error;
    std::unique_ptr<Store> store;
    try {
        auto replay = prepare_replay(c);
        const auto manifest = load_manifest(c, metrics, "receiver");
        store = open_store(store_options(c), metrics, error);
        require(static_cast<bool>(store), "open store failed: " + error);
        Pipeline pipeline(c, metrics, *store);
        Progress progress(c, "receiver", metrics);
        net::UniqueFd listener(net::tcp_listen(c.host, static_cast<std::uint16_t>(c.port)));
        require(listener.is_open(), "listen failed");
        net::set_nonblock(listener.get(), true);
        std::fprintf(stderr, "BENCH_READY pid=%ld\n", static_cast<long>(::getpid()));
        auto deadline = clock_ns() + static_cast<std::uint64_t>(c.timeout_s) * 1000000000;
        net::UniqueFd peer;
        while (!peer.is_open()) {
            wait_socket(listener.get(), POLLIN, deadline);
            const int fd = ::accept(listener.get(), nullptr, nullptr);
            if (fd < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) continue;
            require(fd >= 0, "accept failed");
            peer.reset(fd);
        }
        net::set_nonblock(peer.get(), true);
        MessageParser parser;
        JsonDecoder decoder;
        std::uint64_t position = 0;
        std::size_t replay_index = 0, frame_index = 0;
        bool ended = false;
        while (!ended) {
            pipeline.tick();
            progress.emit(start);
            wait_socket(peer.get(), POLLIN, deadline, pipeline.poll_timeout_ms());
            char buffer[65536];
            const auto n = ::recv(peer.get(), buffer, sizeof(buffer), 0);
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) continue;
            require(n > 0, "source disconnected before END");
            if (!start) start = clock_ns();
            metrics.inc("network_bytes", static_cast<std::uint64_t>(n));
            std::vector<Message> messages;
            {
                Timer timer(metrics, "deframe_crc");
                require(parser.feed(std::string_view(buffer, static_cast<std::size_t>(n)), messages, error), error);
            }
            for (const auto& message : messages) {
                require(!ended && message.sequence == position, "out-of-order frame or data after END");
                if (message.type == MessageType::End) {
                    require(position == c.records && (manifest.empty() || frame_index == manifest.size()) && !parser.incomplete() && &message == &messages.back(), "early END or data after END");
                    ended = true;
                    break;
                }
                require(message.type == (c.input == "json" ? MessageType::Json : MessageType::Binary), "unexpected input type");
                std::vector<Record> records;
                {
                    Timer timer(metrics, c.parse == "replay" ? "replay_copy" : (c.input == "json" ? "json_parse_normalize" : "binary_decode"));
                    if (c.parse == "replay") {
                        require(replay_index < replay.size() && replay[replay_index].message.sequence == message.sequence && replay[replay_index].message.payload == message.payload, "replay payload differs from prevalidated fixture");
                        records = replay[replay_index++].records;
                    } else {
                        const bool ok = c.input == "json" ? decoder.parse(message.payload, records, error) : decode_records(message.payload, records, error);
                        if (!ok) metrics.inc("rejected");
                        require(ok, "decode failed: " + error);
                    }
                }
                const auto frame_limit = c.input_manifest.empty() ? c.batch_records : kMaxBatchRecords;
                require((!records.empty() || !c.input_manifest.empty()) && records.size() <= frame_limit && records.size() <= c.records - position, "invalid frame record count");
                if (!manifest.empty())
                    require(frame_index < manifest.size() && manifest[frame_index].sequence == position &&
                        manifest[frame_index].records == records.size(), "received JSON count differs from capture manifest");
                ++frame_index;
                metrics.inc("received_frames");
                metrics.inc("received", records.size());
                metrics.inc("parsed", records.size());
                position += records.size();
                pipeline.add(records);
                pipeline.tick();
                progress.emit(start);
            }
        }
        // The source half-closes after END. Require EOF so delayed trailing data
        // cannot be accepted or rejected depending on TCP segmentation.
        for (;;) {
            wait_socket(peer.get(), POLLIN, deadline, pipeline.poll_timeout_ms());
            char trailing[4096];
            const auto n = ::recv(peer.get(), trailing, sizeof(trailing), 0);
            if (n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) { pipeline.tick(); continue; }
            require(n == 0, "data after END or terminal socket failure");
            break;
        }
        pipeline.flush();
        require(store->finish(error), "store finish failed: " + error);
        const auto finished = clock_ns();
        progress.emit(start, true);
        const bool durable = c.backend != "null" && c.synchronous == "FULL";
        std::ostringstream ack;
        ack << "{\"durable\":" << (durable ? "true" : "false") << ",\"received\":" << metrics.counters["received"]
            << ",\"forwarded\":" << metrics.counters["forwarded"] << ",\"committed_attempts\":" << metrics.counters["committed_attempts"]
            << ",\"checksum_fnv1a64\":" << json_quote(hex64(pipeline.checksum())) << '}';
        const auto json = result_json(c, "receiver", "PASS", "", metrics, start, finished,
            "\"store\":" + store->info_json() + ",\"ack\":" + ack.str());
        // ACK covers data commit. The supervisor independently requires both
        // successful process exits and result files; failed sends never leave PASS.
        send_all(peer.get(), encode_message({MessageType::Ack, c.records, ack.str()}), deadline);
        write_json_file(std::filesystem::path(c.directory) / "receiver.json", json);
        return 0;
    } catch (const std::exception& exc) {
        std::fprintf(stderr, "receiver: %s\n", exc.what());
        const auto path = std::filesystem::path(c.directory) / "receiver.json";
        if (!std::filesystem::exists(path)) write_json_file(path, result_json(c, "receiver", "FAIL", exc.what(), metrics, start, clock_ns()));
        return 1;
    }
}

std::string source_schedule_json(const Config& c) {
    std::ostringstream out;
    out << "\"source_schedule\":{\"mode\":" << json_quote(c.rate ? "fixed_rate" : "as_fast")
        << ",\"due_policy\":\"start_ns+floor(frame_physical_record_offset*1e9/rate); as_fast_due=start_ns\""
        << ",\"planned_policy\":\"min(records,floor(elapsed_ns*rate/1e9)); as_fast_planned=records\""
        << ",\"lag_is_observation_not_work\":true,\"lag_unit\":\"ns\""
        << ",\"sampling\":\"before_and_after_each_synchronous_frame_send_and_during_rate_wait; no_samples_inside_send; peak_is_sampled_lower_bound\""
        << ",\"manifest_preparation\":\"bounded_manifest_read_before_start; packed_input_streamed_during_run; packed_SHA256_verified_by_runner\""
        << ",\"capture_validation\":\"raw_payload_SHA256_and_real_JSON_count_checked_at_source; no_payload_reserialization\"}";
    return out.str();
}
class SourceSchedule {
public:
    SourceSchedule(const Config& c, Metrics& metrics, const std::uint64_t& start)
        : config_(c), metrics_(metrics), start_(start) {
        for (const auto* key : {"source_planned_records", "source_planned_minus_sent", "source_planned_minus_sent_peak",
                "source_schedule_samples", "source_send_start_lag_max_ns", "source_send_complete_lag_max_ns"})
            metrics_.counters[key] = 0;
    }
    std::uint64_t due(std::uint64_t position) const {
        // Config bounds keep position*1e9 below uint64 maximum. Integer arithmetic
        // makes the hard offered-rate floor independent of floating precision.
        return start_ + (config_.rate ? position * 1000000000ULL / config_.rate : 0);
    }
    void sample(std::uint64_t now) {
        if (!start_) return;
        const auto elapsed = now - start_;
        const auto planned = config_.rate ? std::min(config_.records,
            (elapsed / 1000000000ULL) * config_.rate + (elapsed % 1000000000ULL) * config_.rate / 1000000000ULL) : config_.records;
        const auto sent = metrics_.counters["source_sent"];
        const auto pending = planned > sent ? planned - sent : 0;
        metrics_.counters["source_planned_records"] = planned;
        metrics_.counters["source_planned_minus_sent"] = pending;
        auto& peak = metrics_.counters["source_planned_minus_sent_peak"];
        peak = std::max(peak, pending);
        metrics_.inc("source_schedule_samples");
    }
    void lag(const char* name, const char* maximum, std::uint64_t due, std::uint64_t now) {
        const auto ns = now > due ? now - due : 0;
        metrics_.time(name, ns);
        auto& peak = metrics_.counters[maximum];
        peak = std::max(peak, ns);
    }
private:
    const Config& config_;
    Metrics& metrics_;
    const std::uint64_t& start_;
};

int source(const Config& c, bool generate_only) {
    Metrics metrics;
    std::uint64_t start = 0, send_ended = 0;
    const std::string role = generate_only ? "generate" : "source";
    SourceSchedule schedule(c, metrics, start);
    try {
        const auto manifest = load_manifest(c, metrics);
        RegularInput input;
        if (!c.input_file.empty()) input.open(c.input_file);
        JsonDecoder capture_decoder;
        net::UniqueFd peer, output;
        const auto deadline = clock_ns() + static_cast<std::uint64_t>(c.timeout_s) * 1000000000;
        if (generate_only) {
            require(!c.output_file.empty(), "generate requires output_file");
            output = new_file(c.output_file);
        } else {
            peer.reset(net::tcp_connect(c.host, static_cast<std::uint16_t>(c.port)));
            require(peer.is_open(), "source connect failed");
            net::set_nonblock(peer.get(), true);
        }
        Progress progress(c, "source", metrics);
        start = clock_ns();
        auto transmit = [&](const std::string& bytes) {
            Timer timer(metrics, generate_only ? "source_file_write" : "source_send_wait");
            if (generate_only) write_fd(output.get(), bytes);
            else send_all(peer.get(), bytes, deadline);
            metrics.inc("source_bytes", bytes.size());
        };
        std::uint64_t position = 0;
        std::size_t frame_index = 0;
        schedule.sample(start);
        while (manifest.empty() ? position < c.records : frame_index < manifest.size()) {
            require(!stopping && clock_ns() < deadline, "source interrupted or timed out");
            const auto due = schedule.due(position);
            if (c.rate && !generate_only) {
                while (clock_ns() < due) {
                    require(!stopping && clock_ns() < deadline, "source rate wait interrupted");
                    const auto now = clock_ns();
                    schedule.sample(now);
                    progress.emit(start);
                    if (now >= due) break;
                    const auto delay = std::min<std::uint64_t>(due - now, 10000000);
                    if (delay > 1000) ::usleep(static_cast<useconds_t>(delay / 1000));
                }
            }
            const auto count = manifest.empty() ? static_cast<std::uint32_t>(std::min<std::uint64_t>(c.batch_records, c.records - position)) : manifest[frame_index].records;
            std::string wire;
            if (input.is_open()) {
                Message message;
                {
                    Timer timer(metrics, "source_file_read");
                    message = file_message(input, &wire);
                }
                require(message.type == (c.input == "json" ? MessageType::Json : MessageType::Binary) && message.sequence == position, "input_file frame type/sequence mismatch");
                if (!manifest.empty()) validate_capture_frame(message, manifest[frame_index], capture_decoder, metrics);
            } else {
                std::vector<Record> records;
                {
                    Timer timer(metrics, "source_generate");
                    records = generated(c, position, count);
                }
                std::string payload;
                {
                    Timer timer(metrics, c.input == "json" ? "source_json" : "source_binary");
                    payload = c.input == "json" ? records_json(records) : encode_records(records);
                }
                {
                    Timer timer(metrics, "source_frame_crc");
                    wire = encode_message({c.input == "json" ? MessageType::Json : MessageType::Binary, position, std::move(payload)});
                }
            }
            metrics.inc("source_generated", count);
            schedule.sample(clock_ns());
            progress.emit(start);
            const auto send_start = clock_ns();
            schedule.sample(send_start);
            schedule.lag("source_send_start_lag", "source_send_start_lag_max_ns", due, send_start);
            try {
                transmit(wire);
            } catch (...) {
                const auto failed_at = clock_ns();
                schedule.sample(failed_at); // Include blocked-send backlog even on failure.
                metrics.inc("source_incomplete_frame_sends");
                progress.emit(start, true);
                throw;
            }
            const auto send_complete = clock_ns();
            // Sample with the old sent count first: work due during a blocked
            // send must remain visible even if this completion catches it up.
            schedule.sample(send_complete);
            schedule.lag("source_send_complete_lag", "source_send_complete_lag_max_ns", due, send_complete);
            metrics.inc("source_sent", count);
            metrics.inc("source_frames_sent");
            position += count;
            ++frame_index;
            schedule.sample(send_complete);
            progress.emit(start);
        }
        std::string end_wire;
        if (input.is_open()) {
            const auto end = file_message(input, &end_wire);
            require(end.type == MessageType::End && end.sequence == c.records && input.eof(), "invalid input END/trailing bytes");
        } else end_wire = encode_message({MessageType::End, c.records, {}});
        transmit(end_wire);
        if (!generate_only) require(::shutdown(peer.get(), SHUT_WR) == 0, "source half-close failed");
        send_ended = clock_ns();
        schedule.sample(send_ended);
        progress.emit(start, true);
        std::string ack = "null";
        if (!generate_only) {
            MessageParser parser;
            bool got = false;
            while (!got) {
                wait_socket(peer.get(), POLLIN, deadline);
                char bytes[4096];
                const auto n = ::recv(peer.get(), bytes, sizeof(bytes), 0);
                if (n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) continue;
                require(n > 0, "receiver disconnected without completion ACK");
                std::vector<Message> messages;
                std::string error;
                require(parser.feed(std::string_view(bytes, static_cast<std::size_t>(n)), messages, error), error);
                if (!messages.empty()) {
                    require(messages.size() == 1 && !parser.incomplete() && messages[0].type == MessageType::Ack && messages[0].sequence == c.records, "invalid completion ACK");
                    ack = messages[0].payload;
                    simdjson::dom::parser json_parser;
                    simdjson::dom::element doc;
                    std::uint64_t received = 0;
                    bool durable = false;
                    require(!json_parser.parse(ack).get(doc) && !doc["received"].get_uint64().get(received) && received == c.records && !doc["durable"].get_bool().get(durable), "malformed completion counters");
                    require(durable == (c.backend != "null" && c.synchronous == "FULL"), "completion durability contract mismatch");
                    got = true;
                }
            }
        }
        const auto finished = clock_ns();
        schedule.sample(finished);
        progress.emit(start, true);
        std::ostringstream extra;
        extra << std::setprecision(17) << "\"send_elapsed_s\":" << (send_ended - start) / 1e9
            << ",\"send_elapsed_ns\":" << send_ended - start << ",\"send_ended_ns\":" << send_ended
            << ',' << source_schedule_json(c) << ",\"ack\":" << ack;
        write_json_file(std::filesystem::path(c.directory) / (role + ".json"), result_json(c, role, "PASS", "", metrics, start, finished, extra.str()));
        return 0;
    } catch (const std::exception& exc) {
        std::fprintf(stderr, "%s: %s\n", role.c_str(), exc.what());
        const auto failed_at = clock_ns();
        schedule.sample(failed_at);
        const auto path = std::filesystem::path(c.directory) / (role + ".json");
        if (!std::filesystem::exists(path)) write_json_file(path, result_json(c, role, "FAIL", exc.what(), metrics, start, failed_at, source_schedule_json(c)));
        return 1;
    }
}

// A single-process control for the reported file -> normalize -> hash ceiling.
// It shares exactly the same codec, hash, Redis and store stages, without a TCP source.
int local_file(const Config& c) {
    Metrics metrics;
    std::uint64_t start = 0;
    try {
        require(!c.input_file.empty() && c.rate == 0, "local requires input_file and rate=0");
        require(std::filesystem::is_regular_file(c.input_file), "local input must be a regular file");
        auto replay = prepare_replay(c);
        const auto manifest = load_manifest(c, metrics, "local");
        RegularInput input;
        input.open(c.input_file);
        std::string error;
        auto store = open_store(store_options(c), metrics, error);
        require(static_cast<bool>(store), "cannot open local store: " + error);
        Pipeline pipeline(c, metrics, *store);
        Progress progress(c, "local", metrics);
        JsonDecoder decoder;
        std::uint64_t position = 0;
        std::size_t replay_index = 0, frame_index = 0;
        start = clock_ns();
        const auto deadline = start + static_cast<std::uint64_t>(c.timeout_s) * 1000000000;
        for (;;) {
            require(!stopping && clock_ns() < deadline, "local run interrupted or timed out");
            Message message;
            {
                Timer timer(metrics, "file_read_deframe_crc");
                message = file_message(input);
            }
            require(message.sequence == position, "local fixture sequence mismatch");
            if (message.type == MessageType::End) {
                require(position == c.records && (manifest.empty() || frame_index == manifest.size()) && input.eof(), "local early END or trailing data");
                break;
            }
            require(message.type == (c.input == "json" ? MessageType::Json : MessageType::Binary), "local fixture input type mismatch");
            if (!manifest.empty()) {
                require(frame_index < manifest.size(), "local capture has extra data frames");
                validate_capture_digest(message, manifest[frame_index], metrics, "local");
            }
            std::vector<Record> records;
            {
                Timer timer(metrics, c.parse == "replay" ? "replay_copy" : (c.input == "json" ? "json_parse_normalize" : "binary_decode"));
                if (c.parse == "replay") {
                    require(replay_index < replay.size() && replay[replay_index].message.payload == message.payload, "local replay fixture differs");
                    records = replay[replay_index++].records;
                } else {
                    const bool ok = c.input == "json" ? decoder.parse(message.payload, records, error) : decode_records(message.payload, records, error);
                    require(ok, "local decode failed: " + error);
                }
            }
            const auto frame_limit = manifest.empty() ? c.batch_records : kMaxBatchRecords;
            require((!records.empty() || !manifest.empty()) && records.size() <= frame_limit && records.size() <= c.records - position, "local frame count mismatch");
            if (!manifest.empty())
                require(manifest[frame_index].sequence == position && manifest[frame_index].records == records.size(), "local JSON count differs from capture manifest");
            ++frame_index;
            metrics.inc("received_frames");
            position += records.size();
            metrics.inc("received", records.size());
            metrics.inc("parsed", records.size());
            pipeline.add(records);
            pipeline.tick();
            progress.emit(start);
        }
        pipeline.flush();
        require(store->finish(error), "local store finish failed: " + error);
        const auto finished = clock_ns();
        progress.emit(start, true);
        std::ostringstream extra;
        extra << "\"transport\":\"local_file\",\"store\":" << store->info_json()
            << ",\"checksum_fnv1a64\":" << json_quote(hex64(pipeline.checksum()));
        write_json_file(std::filesystem::path(c.directory) / "local.json", result_json(c, "local", "PASS", "", metrics, start, finished, extra.str()));
        return 0;
    } catch (const std::exception& exc) {
        std::fprintf(stderr, "local: %s\n", exc.what());
        const auto path = std::filesystem::path(c.directory) / "local.json";
        if (!std::filesystem::exists(path)) write_json_file(path, result_json(c, "local", "FAIL", exc.what(), metrics, start, clock_ns()));
        return 1;
    }
}

int export_data(const Config& c) {
    require(c.backend != "null", "null backend has no persisted rows");
    Metrics metrics;
    std::string error;
    auto options = store_options(c);
    options.failpoint.clear();
    auto store = open_store(options, metrics, error);
    require(static_cast<bool>(store), "cannot open export store: " + error);
    const auto path = c.output_file.empty() ? std::filesystem::path(c.directory) / "rows.jsonl" : std::filesystem::path(c.output_file);
    require(!std::filesystem::exists(path), "refuse existing export output");
    std::ofstream output(path, std::ios::binary);
    require(static_cast<bool>(output), "cannot create export output");
    require(store->export_rows(output, error), "export failed: " + error);
    output.flush();
    require(static_cast<bool>(output), "export output failed");
    return 0;
}
}  // namespace
}  // namespace tickshelf::bench

int main(int argc, char** argv) {
    using namespace tickshelf::bench;
    std::signal(SIGPIPE, SIG_IGN);
    std::signal(SIGINT, stop);
    std::signal(SIGTERM, stop);
    try {
        std::string role, path;
        bool print = false, check = false, dedup_policy = false;
        for (int i = 1; i < argc; ++i) {
            const std::string arg = argv[i];
            if (arg == "--config" || arg == "--role") {
                require(i + 1 < argc, "missing argument: " + arg);
                (arg == "--config" ? path : role) = argv[++i];
            } else if (arg == "--print-config") print = true;
            else if (arg == "--check-config") check = true;
            else if (arg == "--dedup-policy") dedup_policy = true;
            else if (arg == "--help") {
                std::cout << "pipeline_bench --config FILE --role receiver|source|generate|export|local [--check-config|--print-config]\n"
                    << "pipeline_bench --dedup-policy  (side-effect-free accounting contract; no config needed)\n";
                return 0;
            } else throw std::runtime_error("unknown argument: " + arg);
        }
        if (dedup_policy) {
            require(argc == 2, "--dedup-policy must be used alone");
            std::cout << "{\"schema_version\":1,\"policy\":\"fifo\",\"fifo_limit_bytes\":" << Deduper::fifo_limit_bytes()
                << ",\"fixed_charge_bytes\":" << Deduper::fixed_charge_bytes()
                << ",\"entry_charge\":\"canonical_bytes+fixed_charge_bytes\""
                << ",\"max_entry_policy\":\"evict_oldest_while_entries_exceed_config.dedup_max_entries_or_charged_bytes_exceed_fifo_limit_bytes\""
                << ",\"duplicate_policy\":\"exact_canonical_bytes; duplicate_does_not_refresh_fifo\""
                << ",\"accounting\":\"budget_charge_not_allocator_or_RSS\"}\n";
            return 0;
        }
        require(!path.empty(), "--config is required");
        Config config;
        std::string error;
        require(load_config(path, config, error), "config rejected: " + error);
        if (print) { std::cout << config_json(config) << '\n'; return 0; }
        if (check) return 0;
        require(role == "receiver" || role == "source" || role == "generate" || role == "export" || role == "local", "invalid --role");
        if (role != "export") {
            const auto root = std::filesystem::path(config.directory);
            require(!std::filesystem::exists(root / (role + ".json")), "refuse existing result file");
            require(!std::filesystem::exists(root / ((role == "source" || role == "generate") ? "source-progress.jsonl" : "progress.jsonl")), "refuse existing progress file");
        }
        std::filesystem::create_directories(config.directory);
        if (role == "receiver") return receiver(config);
        if (role == "local") return local_file(config);
        if (role == "export") return export_data(config);
        return source(config, role == "generate");
    } catch (const std::exception& exc) {
        std::fprintf(stderr, "pipeline_bench: %s\n", exc.what());
        return 1;
    }
}
