#include <doctest/doctest.h>

#include <array>
#include <filesystem>
#include <fstream>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/startup_config.hpp"

using namespace tickshelf::startup;
namespace fs = std::filesystem;

namespace {

constexpr std::array<Component, 4> kComponents = {
    Component::Tickshelfd, Component::Persister, Component::Evictor, Component::Queryd};

// 每次只创建/清理自己拥有的唯一目录；不连接 Redis，也不打开 SQLite。
struct TmpConfig {
    fs::path dir;
    fs::path file;

    explicit TmpConfig(const std::string& contents = "") {
        std::random_device random;
        for (int attempt = 0; attempt < 100; ++attempt) {
            const auto candidate = fs::temp_directory_path() /
                ("tickshelf_startup_" + std::to_string(random()) + "_" + std::to_string(random()));
            if (fs::create_directory(candidate)) {
                dir = candidate;
                break;
            }
        }
        if (dir.empty()) throw std::runtime_error("cannot create unique test directory");
        file = dir / "tickshelf.ini";
        write(contents);
    }
    ~TmpConfig() {
        std::error_code ec;
        fs::remove_all(dir, ec);
    }
    void write(const std::string& contents) const {
        std::ofstream out(file, std::ios::binary);
        out << contents;
        out.close();
        if (!out) throw std::runtime_error("cannot write test config");
    }
};

struct ScopedCwd {
    fs::path old = fs::current_path();
    explicit ScopedCwd(const fs::path& path) { fs::current_path(path); }
    ~ScopedCwd() {
        std::error_code ec;
        fs::current_path(old, ec);
    }
};

void rejects(Component component, const std::vector<std::string>& args,
             const std::string& diagnostic = "") {
    bool threw = false;
    try {
        parse(component, args);
    } catch (const std::runtime_error& error) {
        threw = true;
        CHECK(std::string(error.what()).find(diagnostic) != std::string::npos);
    }
    CHECK(threw);
}

const char* kComplete = R"ini(
[redis]
host = file-redis
port = 6388
[storage]
db = file.db
shards = 4
[tickshelfd]
bind = 0.0.0.0
port = 19900
flush_interval_ms = 250
persist = sqlite
[persister]
worker = file-worker
poll_ms = 200
once = true
[evictor]
soft_watermark_bytes = 100
emergency_watermark_bytes = 200
sample_ms = 300
bytes_per_tick = 640
growth_tolerance_pct = 25.5
inflight = true
[queryd]
bind = ::1
port = 19910
)ini";

}  // namespace

TEST_CASE("startup_config: existing defaults and required evictor soft watermark") {
    for (auto component : kComponents) {
        const std::vector<std::string> args = component == Component::Evictor
            ? std::vector<std::string>{"--soft-watermark-bytes", "1"} : std::vector<std::string>{};
        const auto parsed = parse(component, args);
        const auto& options = parsed.options;
        CHECK(parsed.action == Action::Run);
        CHECK(options.redis_host == "127.0.0.1");
        CHECK(options.redis_port == 6379);
        CHECK(options.db == "ticks.db");
        CHECK(options.shards == 1);
        CHECK(options.bind_host == "127.0.0.1");
        CHECK(options.port == (component == Component::Queryd ? 9910 : 9900));
        CHECK(options.flush_interval_ms == 500);
        CHECK(options.persist == "none");
        CHECK(options.worker == "persister");
        CHECK(options.poll_ms == 1000);
        CHECK_FALSE(options.once);
        CHECK(options.emergency_watermark_bytes == 0);
        CHECK(options.sample_ms == 1000);
        CHECK(options.bytes_per_tick == 0);
        CHECK(options.growth_tolerance_pct == 50.0);
        CHECK_FALSE(options.inflight);
    }
    rejects(Component::Evictor, {}, "soft_watermark_bytes is required");
}

TEST_CASE("startup_config: shipped complete example parses for all four components") {
    const auto file = fs::path(TICKSHELF_SOURCE_DIR) / "examples/tickshelf.ini";
    REQUIRE(fs::is_regular_file(file));
    for (auto component : kComponents) {
        const auto parsed = parse(component, {"--config", file.string(), "--print-config"});
        CHECK(parsed.action == Action::PrintConfig);
        CHECK(parsed.options.redis_host == "127.0.0.1");
        CHECK(parsed.options.redis_port == 6379);
        CHECK(parsed.options.shards == 1);
        if (component != Component::Evictor)
            CHECK(parsed.options.db == (file.parent_path() / "../ticks.db").lexically_normal().string());
        if (component == Component::Tickshelfd) CHECK(parsed.options.persist == "sqlite");
        if (component == Component::Evictor) {
            CHECK(parsed.options.soft_watermark_bytes == 268435456);
            CHECK(parsed.options.emergency_watermark_bytes == 0);
            CHECK_FALSE(parsed.options.inflight);
        }
    }
}

TEST_CASE("startup_config: all shared and component file fields are consumed") {
    TmpConfig tmp(kComplete);
    for (auto component : kComponents) {
        const auto o = parse(component, {"--config", tmp.file.string()}).options;
        CHECK(o.redis_host == "file-redis");
        CHECK(o.redis_port == 6388);
        if (component != Component::Evictor) {
            CHECK(o.db == (tmp.dir / "file.db").string());
            CHECK(o.shards == 4);
        }
        if (component == Component::Tickshelfd) {
            CHECK(o.bind_host == "0.0.0.0");
            CHECK(o.port == 19900);
            CHECK(o.flush_interval_ms == 250);
            CHECK(o.persist == "sqlite");
        } else if (component == Component::Persister) {
            CHECK(o.worker == "file-worker");
            CHECK(o.poll_ms == 200);
            CHECK(o.once);
        } else if (component == Component::Evictor) {
            CHECK(o.soft_watermark_bytes == 100);
            CHECK(o.emergency_watermark_bytes == 200);
            CHECK(o.sample_ms == 300);
            CHECK(o.bytes_per_tick == 640);
            CHECK(o.growth_tolerance_pct == 25.5);
            CHECK(o.inflight);
        } else {
            CHECK(o.bind_host == "::1");
            CHECK(o.port == 19910);
        }
    }
}

TEST_CASE("startup_config: CLI overrides every field independent of config position") {
    TmpConfig tmp(kComplete);
    const std::array<std::vector<std::string>, 4> arguments = {{
        {"--redis-host", "cli-redis", "--redis-port", "1234", "--db", "cli.db", "--shards", "3",
         "--bind", "localhost", "--port", "4321", "--flush-interval-ms", "12", "--persist", "none"},
        {"--redis-host", "cli-redis", "--redis-port", "1234", "--db", "cli.db", "--shards", "3",
         "--worker", "cli-worker", "--poll-ms", "12", "--no-once"},
        {"--redis-host", "cli-redis", "--redis-port", "1234", "--soft-watermark-bytes", "300",
         "--emergency-watermark-bytes", "400", "--sample-ms", "12", "--bytes-per-tick", "123",
         "--growth-tolerance-pct", "12.75", "--no-inflight"},
        {"--redis-host", "cli-redis", "--redis-port", "1234", "--db", "cli.db", "--shards", "3",
         "--bind", "localhost", "--port", "4321"}
    }};
    for (std::size_t i = 0; i < kComponents.size(); ++i) {
        const auto component = kComponents[i];
        const auto expected = to_json(component, parse(component, arguments[i]).options);
        for (const auto at : {std::size_t{0}, std::size_t{2}, arguments[i].size()}) {
            auto args = arguments[i];
            args.insert(args.begin() + at, {"--config", tmp.file.string()});
            CHECK(to_json(component, parse(component, args).options) == expected);
        }
    }
    CHECK(parse(Component::Queryd, {"--port", "100", "--port", "200"}).options.port == 200);
    rejects(Component::Queryd, {"--port", "bad", "--port", "200"}, "--port");
}

TEST_CASE("startup_config: positive and negative boolean flags override file and each other") {
    TmpConfig tmp("[persister]\nonce=true\n[evictor]\nsoft_watermark_bytes=1\ninflight=true\n");
    CHECK_FALSE(parse(Component::Persister, {"--no-once", "--config", tmp.file.string()}).options.once);
    CHECK_FALSE(parse(Component::Evictor, {"--no-inflight", "--config", tmp.file.string()}).options.inflight);
    CHECK(parse(Component::Persister, {"--config", tmp.file.string(), "--no-once", "--once"}).options.once);
    CHECK(parse(Component::Evictor, {"--config", tmp.file.string(), "--no-inflight", "--inflight"}).options.inflight);
    CHECK_FALSE(parse(Component::Persister, {"--config", tmp.file.string(), "--once", "--no-once"}).options.once);
    CHECK_FALSE(parse(Component::Evictor, {"--config", tmp.file.string(), "--inflight", "--no-inflight"}).options.inflight);
    tmp.write("[persister]\nonce=false\n[evictor]\nsoft_watermark_bytes=1\ninflight=false\n");
    CHECK(parse(Component::Persister, {"--once", "--config", tmp.file.string()}).options.once);
    CHECK(parse(Component::Evictor, {"--inflight", "--config", tmp.file.string()}).options.inflight);
}

TEST_CASE("startup_config: db origins differ and paths are not expanded or auto-searched") {
    TmpConfig tmp("[storage]\ndb = sub/../data/ticks.db\n");
    for (auto component : {Component::Tickshelfd, Component::Persister, Component::Queryd}) {
        const auto file = parse(component, {"--config", tmp.file.string()}).options;
        CHECK(file.db == (tmp.dir / "data/ticks.db").string());
        const auto cli = parse(component, {"--db", "data/ticks.db", "--config", tmp.file.string()}).options;
        CHECK(cli.db == "data/ticks.db");
        CHECK(fs::absolute(cli.db) != fs::path(file.db));
    }
    tmp.write("[storage]\ndb = ~/literal/$HOME/${DATA}.db\n");
    CHECK(parse(Component::Queryd, {"--config", tmp.file.string()}).options.db ==
          (tmp.dir / "~/literal/$HOME/${DATA}.db").string());
    CHECK(parse(Component::Queryd, {"--db", "~/$HOME.db"}).options.db == "~/$HOME.db");
    tmp.write("[storage]\ndb = " + (tmp.dir / "absolute.db").string() + "\n");
    CHECK(parse(Component::Queryd, {"--config", tmp.file.string()}).options.db ==
          (tmp.dir / "absolute.db").string());
    tmp.write("[redis]\nport=6380\n");
    CHECK(parse(Component::Queryd, {"--config", tmp.file.string()}).options.db == "ticks.db");
    {
        ScopedCwd cwd(tmp.dir);
        CHECK(parse(Component::Queryd, {"--config", "tickshelf.ini"}).options.redis_port == 6380);
        tmp.write("not even INI");
        CHECK(parse(Component::Queryd, {}).options.redis_port == 6379);  // 不自动搜索同名文件
    }
    CHECK_FALSE(fs::exists(tmp.dir / "data"));  // 配置解析不创建数据库/父目录
}

TEST_CASE("startup_config: numeric boundaries and decimal fractions") {
    CHECK(parse(Component::Tickshelfd, {"--redis-port", "1", "--port", "65535"}).options.port == 65535);
    CHECK(parse(Component::Queryd, {"--shards", std::to_string(std::numeric_limits<int>::max())}).options.shards ==
          std::numeric_limits<int>::max());
    const auto max_interval = std::to_string(std::numeric_limits<std::int64_t>::max() / 1000000);
    CHECK(parse(Component::Tickshelfd, {"--flush-interval-ms", max_interval}).options.flush_interval_ms ==
          static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max() / 1000000));
    CHECK(parse(Component::Persister, {"--poll-ms", "1"}).options.poll_ms == 1);
    const auto max_bytes = std::to_string(std::numeric_limits<std::uint64_t>::max());
    const auto ev = parse(Component::Evictor, {"--soft-watermark-bytes", max_bytes,
        "--bytes-per-tick", max_bytes, "--sample-ms", max_interval}).options;
    CHECK(ev.soft_watermark_bytes == std::numeric_limits<std::uint64_t>::max());
    CHECK(ev.bytes_per_tick == std::numeric_limits<std::uint64_t>::max());
    for (const std::string value : {"0", "0.5", ".5", "1e2", "1.7976931348623157e308"}) {
        CAPTURE(value);
        CHECK_NOTHROW(parse(Component::Evictor, {"--soft-watermark-bytes", "1", "--growth-tolerance-pct", value}));
    }
}

TEST_CASE("startup_config: malformed numeric CLI values never coerce or wrap") {
    struct Numeric { Component component; const char* flag; };
    const Numeric integer_options[] = {
        {Component::Tickshelfd, "--redis-port"}, {Component::Tickshelfd, "--port"},
        {Component::Tickshelfd, "--shards"}, {Component::Tickshelfd, "--flush-interval-ms"},
        {Component::Persister, "--poll-ms"}, {Component::Evictor, "--sample-ms"},
        {Component::Evictor, "--soft-watermark-bytes"},
        {Component::Evictor, "--emergency-watermark-bytes"}, {Component::Evictor, "--bytes-per-tick"}
    };
    for (const auto& option : integer_options) {
        for (const std::string value : {"-1", "+1", "1.5", "NaN", "inf", "1e2", "1_000", "12junk",
                                       " 12", "12 ", "18446744073709551616", "99999999999999999999999999"}) {
            CAPTURE(option.flag);
            CAPTURE(value);
            rejects(option.component, {option.flag, value}, option.flag);
        }
    }
    for (const auto& option : integer_options) {
        if (std::string(option.flag) == "--emergency-watermark-bytes" ||
            std::string(option.flag) == "--bytes-per-tick") continue;
        rejects(option.component, {option.flag, "0"}, option.flag);
    }
    rejects(Component::Queryd, {"--port", "65536"}, "--port");
    rejects(Component::Queryd, {"--redis-port", "65536"}, "--redis-port");
    rejects(Component::Queryd, {"--shards", "2147483648"}, "--shards");
    rejects(Component::Tickshelfd, {"--flush-interval-ms", "9223372036855"}, "--flush-interval-ms");
    rejects(Component::Persister, {"--poll-ms", "9223372036855"}, "--poll-ms");
    rejects(Component::Evictor, {"--sample-ms", "9223372036855"}, "--sample-ms");
    for (const std::string value : {"NaN", "nan", "inf", "-inf", "+inf", "1e999", "1e-999", "-1",
                                   "-0", "+1", "1.2.3", "0x10", "0x1p2", ".", "1e", "1e+", "12%", " 12", "12 "}) {
        CAPTURE(value);
        rejects(Component::Evictor, {"--growth-tolerance-pct", value}, "--growth-tolerance-pct");
    }
}

TEST_CASE("startup_config: every file section validates types even for other executables") {
    TmpConfig tmp;
    const std::vector<std::string> invalid_files = {
        "[redis]\nport=0\n", "[storage]\nshards=-1\n", "[storage]\nshards=2147483648\n",
        "[storage]\ndb=''\n", "[redis]\nhost=''\n", "[tickshelfd]\nport=65536\n",
        "[tickshelfd]\nflush_interval_ms=0\n", "[tickshelfd]\npersist=other\n",
        "[persister]\npoll_ms=18446744073709551616\n", "[persister]\nonce=1\n",
        "[persister]\nonce=yes\n", "[persister]\nonce=TRUE\n", "[persister]\nworker=''\n",
        "[evictor]\nsoft_watermark_bytes=0\n", "[evictor]\nbytes_per_tick=-1\n",
        "[evictor]\nsample_ms=1.5\n", "[evictor]\ngrowth_tolerance_pct=NaN\n",
        "[evictor]\ngrowth_tolerance_pct=1e999\n", "[evictor]\ninflight=on\n",
        "[queryd]\nbind=''\n", "[queryd]\nport=12junk\n"
    };
    for (const auto& contents : invalid_files) {
        CAPTURE(contents);
        tmp.write(contents);
        for (auto component : kComponents)
            rejects(component, {"--config", tmp.file.string()}, tmp.file.string() + ":2");
    }
    tmp.write("[redis]\nport=not-a-port\n");
    rejects(Component::Queryd, {"--config", tmp.file.string(), "--redis-port", "6380"}, "redis.port");
}

TEST_CASE("startup_config: evictor invariants use effective CLI values and validate present sections") {
    TmpConfig tmp("[evictor]\nsoft_watermark_bytes=100\nemergency_watermark_bytes=100\n");
    for (auto component : kComponents)
        rejects(component, {"--config", tmp.file.string()}, "greater than soft");
    CHECK(parse(Component::Evictor, {"--emergency-watermark-bytes", "0", "--config", tmp.file.string()})
              .options.emergency_watermark_bytes == 0);
    CHECK(parse(Component::Evictor, {"--config", tmp.file.string(), "--soft-watermark-bytes", "99"})
              .options.soft_watermark_bytes == 99);
    rejects(Component::Evictor, {"--soft-watermark-bytes", "100", "--emergency-watermark-bytes", "99"},
            "greater than soft");
    tmp.write("[evictor]\nsample_ms=100\n");
    rejects(Component::Queryd, {"--config", tmp.file.string()}, "soft_watermark_bytes is required");
    CHECK(parse(Component::Evictor, {"--config", tmp.file.string(), "--soft-watermark-bytes", "1"})
              .options.sample_ms == 100);
}

TEST_CASE("startup_config: missing values files and unknown CLI flags are explicit errors") {
    const std::array<std::vector<std::string>, 4> valued_flags = {{
        {"--config", "--redis-host", "--redis-port", "--db", "--shards", "--bind", "--port",
         "--flush-interval-ms", "--persist"},
        {"--config", "--redis-host", "--redis-port", "--db", "--shards", "--worker", "--poll-ms"},
        {"--config", "--redis-host", "--redis-port", "--soft-watermark-bytes", "--emergency-watermark-bytes",
         "--sample-ms", "--bytes-per-tick", "--growth-tolerance-pct"},
        {"--config", "--redis-host", "--redis-port", "--db", "--shards", "--bind", "--port"}
    }};
    TmpConfig tmp;
    for (std::size_t i = 0; i < kComponents.size(); ++i) {
        for (const auto& flag : valued_flags[i]) {
            rejects(kComponents[i], {flag}, "missing value");
            rejects(kComponents[i], {flag, "--print-config"}, "missing value");
            rejects(kComponents[i], {flag, ""}, "missing value");
        }
        rejects(kComponents[i], {"--config", (tmp.dir / "missing.ini").string()}, "cannot read config file");
        rejects(kComponents[i], {"--config", tmp.dir.string()}, "cannot read config file");
        rejects(kComponents[i], {"--config", tmp.file.string(), "--config", tmp.file.string()}, "only be specified once");
        for (const std::string flag : {"--typo", "--", "positional", "--redis-port=6379", "--no-redis-host"})
            rejects(kComponents[i], {flag}, "unknown option");
    }
    rejects(Component::Queryd, {"--once"}, "unknown option");
    rejects(Component::Persister, {"--inflight"}, "unknown option");
    rejects(Component::Evictor, {"--db", "x"}, "unknown option");
    rejects(Component::Persister, {"--once", "false"}, "unknown option");
    rejects(Component::Tickshelfd, {"--persist", "memory"}, "none|sqlite");
}

TEST_CASE("startup_config: unknown duplicate and misplaced INI fields are rejected globally") {
    TmpConfig tmp;
    const std::vector<std::string> invalid_files = {
        "[unknown]\nx=1\n", "[Redis]\nhost=localhost\n", "[redis]\nredis_host=localhost\n",
        "[redis]\nport=1\nport=2\n", "[redis]\n[redis]\n", "[queryd]\nport=1\nport=2\n",
        "[queryd]\n[queryd]\n", "[storage]\nshards=1\nshards=2\n",
        "[tickshelfd]\nredis_host=localhost\n", "[tickshelfd]\ndb=ticks.db\n",
        "[tickshelfd]\nflush-interval-ms=1\n", "[persister]\nshards=1\n",
        "[evictor]\nredis_port=6379\n", "[queryd]\ndb=ticks.db\n"
    };
    for (const auto& contents : invalid_files) {
        CAPTURE(contents);
        tmp.write(contents);
        for (auto component : kComponents) rejects(component, {"--config", tmp.file.string()});
    }
}

TEST_CASE("startup_config: whitespace comments quotes and escape rules are consistent") {
    TmpConfig tmp("\xef\xbb\xbf  # UTF-8 BOM and CRLF\r\n"
                  " [ redis ] ; comment\r\n"
                  " host = 'redis#;host' # outside comment\r\n"
                  " port = \"6380\";comment\r\n"
                  " [storage]\r\n"
                  " db = 'dir #;/ticks.db'\r\n"
                  " [persister]\r\n"
                  " worker = \" worker\\t\\n\\r\\\"\\'\\\\#;中文 \" # comment\r\n"
                  " once = 'true'\r\n");
    const auto options = parse(Component::Persister, {"--config", tmp.file.string()}).options;
    CHECK(options.redis_host == "redis#;host");
    CHECK(options.redis_port == 6380);
    CHECK(options.db == (tmp.dir / "dir #;/ticks.db").string());
    CHECK(options.worker == " worker\t\n\r\"'\\#;中文 ");
    CHECK(options.once);
    tmp.write("[persister]\nworker = bare value#comment\nonce=false;comment\n");
    CHECK(parse(Component::Persister, {"--config", tmp.file.string()}).options.worker == "bare value");
    tmp.write("[persister]\nworker = 'a\\t\\n\\r\\\"\\'\\\\#;'\n");
    CHECK(parse(Component::Persister, {"--config", tmp.file.string()}).options.worker == "a\t\n\r\"'\\#;");
}

TEST_CASE("startup_config: malformed syntax strings and unsupported escapes fail with line diagnostics") {
    TmpConfig tmp;
    const std::vector<std::string> invalid_files = {
        "host=localhost\n", "[]\n", "[redis\n", "[redis] trailing\n", "[redis]]\n",
        "[redis]\nhost\n", "[redis]\n=abc\n", "[redis]\nhost=\n", "[redis]\nhost=#only comment\n",
        "[redis]\nhost='unterminated\n", "[redis]\nhost=\"a\" tail\n", "[redis]\nhost=a\"b\"\n",
        "[redis]\nhost=\"a\" \"b\"\n", "[redis]\nhost=\"bad\\q\"\n", "[redis]\nhost='bad\\u1234'\n",
        "[redis]\nhost=\"line1\nline2\"\n", "[redis]\nhost=\"\"\n",
        std::string("[redis]\nhost=a") + '\0' + "b\n", "[redis]\nhost=\xff\n"
    };
    for (const auto& contents : invalid_files) {
        CAPTURE(contents);
        tmp.write(contents);
        rejects(Component::Queryd, {"--config", tmp.file.string()}, tmp.file.string() + ":");
    }
    rejects(Component::Queryd, {"--redis-host", std::string("a\0b", 3)}, "without NUL");
    rejects(Component::Queryd, {"--db", "\xff"}, "UTF-8");
}

TEST_CASE("startup_config: exact JSON contracts use native numbers booleans and escaped strings") {
    CHECK(to_json(Component::Tickshelfd, parse(Component::Tickshelfd, {}).options) ==
          R"({"redis_host":"127.0.0.1","redis_port":6379,"db":"ticks.db","shards":1,"bind":"127.0.0.1","port":9900,"flush_interval_ms":500,"persist":"none"})");
    CHECK(to_json(Component::Persister, parse(Component::Persister, {}).options) ==
          R"({"redis_host":"127.0.0.1","redis_port":6379,"db":"ticks.db","shards":1,"worker":"persister","poll_ms":1000,"once":false})");
    CHECK(to_json(Component::Evictor, parse(Component::Evictor, {"--soft-watermark-bytes", "1"}).options) ==
          R"({"redis_host":"127.0.0.1","redis_port":6379,"soft_watermark_bytes":1,"emergency_watermark_bytes":0,"sample_ms":1000,"bytes_per_tick":0,"growth_tolerance_pct":50,"inflight":false})");
    CHECK(to_json(Component::Queryd, parse(Component::Queryd, {}).options) ==
          R"({"redis_host":"127.0.0.1","redis_port":6379,"db":"ticks.db","shards":1,"bind":"127.0.0.1","port":9910})");
    const auto options = parse(Component::Persister, {"--worker", "a\"\\\n\t\r\b\f中文", "--once"}).options;
    const auto json = to_json(Component::Persister, options);
    CHECK(json.find(R"("worker":"a\"\\\u000a\u0009\u000d\u0008\u000c中文")") != std::string::npos);
    CHECK(json.find("\n") == std::string::npos);
    CHECK(json.find("\"once\":true") != std::string::npos);
    CHECK(options.worker == "a\"\\\n\t\r\b\f中文");  // JSON 不追加运行时 -PID
}

TEST_CASE("startup_config: check print and help actions share validation and advertise usage") {
    TmpConfig tmp(kComplete);
    for (auto component : kComponents) {
        CHECK(parse(component, {"--help"}).action == Action::Help);
        CHECK(usage(component).find("--config PATH") != std::string::npos);
        CHECK(usage(component).find("--check-config") != std::string::npos);
        CHECK(usage(component).find("--print-config") != std::string::npos);
        CHECK(parse(component, {"--check-config", "--config", tmp.file.string()}).action == Action::CheckConfig);
        CHECK(parse(component, {"--print-config", "--config", tmp.file.string()}).action == Action::PrintConfig);
        CHECK(parse(component, {"--check-config", "--print-config", "--config", tmp.file.string()}).action ==
              Action::PrintConfig);
        rejects(component, {"--help", "--unknown"}, "unknown option");
        rejects(component, {"--print-config", "--redis-port", "0"}, "--redis-port");
        rejects(component, {"--check-config", "--redis-port", "0"}, "--redis-port");
    }
    rejects(Component::Evictor, {"--print-config"}, "soft_watermark_bytes is required");
    rejects(Component::Evictor, {"--check-config"}, "soft_watermark_bytes is required");
    rejects(Component::Evictor, {"--help", "--print-config"}, "soft_watermark_bytes is required");
    rejects(Component::Evictor, {"--help", "--check-config"}, "soft_watermark_bytes is required");
}
