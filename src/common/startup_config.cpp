#include "common/startup_config.hpp"

#include <array>
#include <charconv>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <locale>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <variant>

namespace tickshelf::startup {
namespace {

constexpr unsigned mask(Component c) { return 1u << static_cast<unsigned>(c); }
constexpr unsigned kDaemon = mask(Component::Tickshelfd);
constexpr unsigned kPersister = mask(Component::Persister);
constexpr unsigned kEvictor = mask(Component::Evictor);
constexpr unsigned kQuery = mask(Component::Queryd);
constexpr unsigned kAll = kDaemon | kPersister | kEvictor | kQuery;
constexpr unsigned kStorage = kDaemon | kPersister | kQuery;
constexpr auto kMaxU64 = std::numeric_limits<std::uint64_t>::max();
// sleep_for / steady_clock 最终使用有符号纳秒；拒绝转换会溢出的毫秒值。
constexpr std::uint64_t kMaxIntervalMs = std::numeric_limits<std::int64_t>::max() / 1000000;

using Member = std::variant<std::string Options::*, std::uint16_t Options::*, int Options::*,
                            std::uint64_t Options::*, double Options::*, bool Options::*>;
struct Spec {
    std::string_view section;
    std::string_view key;
    std::string_view flag;
    std::string_view json_key;
    unsigned consumers;
    Member member;
    const char* metavar;  // nullptr = 布尔开关，同时支持 --no-xxx
    std::uint64_t min = 0;
    std::uint64_t max = kMaxU64;
};

// 文件、CLI、JSON 和 usage 使用同一张表；共享选项只能出现在共享 section 中。
const Spec kSchema[] = {
    {"redis", "host", "--redis-host", "redis_host", kAll, &Options::redis_host, "H"},
    {"redis", "port", "--redis-port", "redis_port", kAll, &Options::redis_port, "P", 1, 65535},
    {"storage", "db", "--db", "db", kStorage, &Options::db, "PATH"},
    {"storage", "shards", "--shards", "shards", kStorage, &Options::shards, "N", 1,
     static_cast<std::uint64_t>(std::numeric_limits<int>::max())},
    {"tickshelfd", "bind", "--bind", "bind", kDaemon, &Options::bind_host, "H"},
    {"tickshelfd", "port", "--port", "port", kDaemon, &Options::port, "P", 1, 65535},
    {"tickshelfd", "flush_interval_ms", "--flush-interval-ms", "flush_interval_ms", kDaemon,
     &Options::flush_interval_ms, "MS", 1, kMaxIntervalMs},
    {"tickshelfd", "persist", "--persist", "persist", kDaemon, &Options::persist, "none|sqlite"},
    {"persister", "worker", "--worker", "worker", kPersister, &Options::worker, "ID"},
    {"persister", "poll_ms", "--poll-ms", "poll_ms", kPersister, &Options::poll_ms, "MS", 1,
     kMaxIntervalMs},
    {"persister", "once", "--once", "once", kPersister, &Options::once, nullptr},
    {"evictor", "soft_watermark_bytes", "--soft-watermark-bytes", "soft_watermark_bytes", kEvictor,
     &Options::soft_watermark_bytes, "N", 1},
    {"evictor", "emergency_watermark_bytes", "--emergency-watermark-bytes", "emergency_watermark_bytes",
     kEvictor, &Options::emergency_watermark_bytes, "N"},
    {"evictor", "sample_ms", "--sample-ms", "sample_ms", kEvictor, &Options::sample_ms, "MS", 1,
     kMaxIntervalMs},
    {"evictor", "bytes_per_tick", "--bytes-per-tick", "bytes_per_tick", kEvictor,
     &Options::bytes_per_tick, "N"},
    {"evictor", "growth_tolerance_pct", "--growth-tolerance-pct", "growth_tolerance_pct", kEvictor,
     &Options::growth_tolerance_pct, "PCT"},
    {"evictor", "inflight", "--inflight", "inflight", kEvictor, &Options::inflight, nullptr},
    {"queryd", "bind", "--bind", "bind", kQuery, &Options::bind_host, "H"},
    {"queryd", "port", "--port", "port", kQuery, &Options::port, "P", 1, 65535},
};

const char* name(Component c) {
    switch (c) {
        case Component::Tickshelfd: return "tickshelfd";
        case Component::Persister: return "persister";
        case Component::Evictor: return "evictor";
        case Component::Queryd: return "queryd";
    }
    return "tickshelf";
}

[[noreturn]] void fail(const std::string& where, const std::string& message) {
    throw std::runtime_error(where + ": " + message);
}

std::string_view trim(std::string_view s) {
    const auto first = s.find_first_not_of(" \t\r");
    if (first == std::string_view::npos) return {};
    return s.substr(first, s.find_last_not_of(" \t\r") - first + 1);
}

// 引号内的 #/; 为内容，引号外均为注释；两种引号遵循相同的转义规则。
std::string_view without_comment(std::string_view s, const std::string& where) {
    char quote = 0;
    bool escaped = false;
    for (std::size_t i = 0; i < s.size(); ++i) {
        const char c = s[i];
        if (static_cast<unsigned char>(c) < 0x20 && c != '\t' && c != '\r')
            fail(where, "unexpected control character");
        if (quote) {
            if (escaped) escaped = false;
            else if (c == '\\') escaped = true;
            else if (c == quote) quote = 0;
        } else if (c == '#' || c == ';') {
            return trim(s.substr(0, i));
        } else if (c == '\'' || c == '"') {
            quote = c;
        }
    }
    if (quote) fail(where, "unterminated quoted value");
    return trim(s);
}

std::string value_text(std::string_view s, const std::string& where) {
    s = trim(s);
    if (s.empty()) fail(where, "missing value");
    if (s.front() != '\'' && s.front() != '"') {
        if (s.find_first_of("\"'") != std::string_view::npos)
            fail(where, "quotes must surround the whole value");
        return std::string(s);
    }
    const char quote = s.front();
    std::string out;
    for (std::size_t i = 1; i < s.size(); ++i) {
        char c = s[i];
        if (c == quote) {
            if (i + 1 != s.size()) fail(where, "unexpected text after quoted value");
            return out;
        }
        if (c == '\\') {
            if (++i == s.size()) fail(where, "incomplete escape");
            switch (s[i]) {
                case '\\': c = '\\'; break;
                case '\'': c = '\''; break;
                case '"': c = '"'; break;
                case 'n': c = '\n'; break;
                case 'r': c = '\r'; break;
                case 't': c = '\t'; break;
                default: fail(where, "unknown escape in quoted value");
            }
        }
        out += c;
    }
    fail(where, "unterminated quoted value");
}

// 保证 --print-config 的字符串始终是合法 UTF-8 JSON；不替换或扩展输入内容。
bool valid_utf8(std::string_view s) {
    for (std::size_t i = 0; i < s.size();) {
        const auto c = static_cast<unsigned char>(s[i++]);
        if (c == 0) return false;
        if (c < 0x80) continue;
        unsigned count = 0;
        std::uint32_t cp = 0, minimum = 0;
        if (c >= 0xc2 && c <= 0xdf) { count = 1; cp = c & 0x1f; minimum = 0x80; }
        else if (c >= 0xe0 && c <= 0xef) { count = 2; cp = c & 0x0f; minimum = 0x800; }
        else if (c >= 0xf0 && c <= 0xf4) { count = 3; cp = c & 0x07; minimum = 0x10000; }
        else return false;
        if (s.size() - i < count) return false;
        while (count--) {
            const auto next = static_cast<unsigned char>(s[i++]);
            if ((next & 0xc0) != 0x80) return false;
            cp = (cp << 6) | (next & 0x3f);
        }
        if (cp < minimum || cp > 0x10ffff || (cp >= 0xd800 && cp <= 0xdfff)) return false;
    }
    return true;
}

bool decimal_number(std::string_view text) {
    std::size_t i = 0;
    auto digits = [&]() {
        const auto begin = i;
        while (i < text.size() && text[i] >= '0' && text[i] <= '9') ++i;
        return i != begin;
    };
    bool mantissa = digits();
    if (i < text.size() && text[i] == '.') {
        ++i;
        mantissa = digits() || mantissa;
    }
    if (!mantissa) return false;
    if (i < text.size() && (text[i] == 'e' || text[i] == 'E')) {
        ++i;
        if (i < text.size() && (text[i] == '+' || text[i] == '-')) ++i;
        if (!digits()) return false;
    }
    return i == text.size();
}

void assign(Options& options, const Spec& spec, const std::string& text, const std::string& where) {
    std::visit([&](auto member) {
        using T = std::remove_reference_t<decltype(options.*member)>;
        if constexpr (std::is_same_v<T, std::string>) {
            if (text.empty() || !valid_utf8(text)) fail(where, "expected a nonempty UTF-8 string without NUL");
            if (member == &Options::persist && text != "none" && text != "sqlite")
                fail(where, "persist must be none|sqlite");
            options.*member = text;
        } else if constexpr (std::is_same_v<T, bool>) {
            if (text != "true" && text != "false") fail(where, "expected true or false");
            options.*member = text == "true";
        } else if constexpr (std::is_same_v<T, double>) {
            // 浮点 from_chars 在较旧 macOS 不可用；经典 locale 避免小数点随环境变化。
            std::istringstream input(text);
            input.imbue(std::locale::classic());
            double value = 0;
            input >> std::noskipws >> value;
            const auto mantissa = text.substr(0, text.find_first_of("eE"));
            const bool underflow = value == 0 && mantissa.find_first_of("123456789") != std::string::npos;
            if (!decimal_number(text) || input.fail() || !input.eof() || !std::isfinite(value) ||
                std::signbit(value) || underflow)
                fail(where, "expected a finite nonnegative decimal number without overflow/underflow");
            options.*member = value;
        } else {
            std::uint64_t value = 0;
            const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value, 10);
            if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() ||
                value < spec.min || value > spec.max)
                fail(where, "expected a decimal integer in [" + std::to_string(spec.min) + ", " +
                            std::to_string(spec.max) + "]");
            options.*member = static_cast<T>(value);
        }
    }, spec.member);
}

using Configs = std::array<Options, 4>;
Options& for_component(Configs& configs, Component component) {
    return configs[static_cast<unsigned>(component)];
}

// 返回是否出现 [evictor]；缺席时不强迫其他服务配置无关的必填水位。
bool read_ini(const std::string& filename, Configs& configs) {
    if (!valid_utf8(filename)) fail("--config", "expected a UTF-8 path without NUL");
    const auto path = std::filesystem::absolute(filename);
    std::error_code ec;
    if (!std::filesystem::is_regular_file(path, ec)) fail(filename, "cannot read config file");
    std::ifstream input(path, std::ios::binary);
    if (!input) fail(filename, "cannot open config file");
    std::set<std::string> sections, keys;
    std::string section, line;
    std::size_t line_no = 0;
    while (std::getline(input, line)) {
        ++line_no;
        if (line_no == 1 && line.compare(0, 3, "\xef\xbb\xbf") == 0) line.erase(0, 3);
        const std::string where = filename + ":" + std::to_string(line_no);
        const auto text = without_comment(line, where);
        if (text.empty()) continue;
        if (text.front() == '[') {
            if (text.back() != ']') fail(where, "malformed section header");
            section = trim(text.substr(1, text.size() - 2));
            bool known = false;
            for (const auto& spec : kSchema) known = known || spec.section == section;
            if (!known) fail(where, "unknown section [" + section + "]");
            if (!sections.insert(section).second) fail(where, "duplicate section [" + section + "]");
            continue;
        }
        const auto equal = text.find('=');
        if (equal == std::string_view::npos) fail(where, "expected key = value");
        if (section.empty()) fail(where, "key must appear inside a section");
        const auto key = trim(text.substr(0, equal));
        const Spec* found = nullptr;
        for (const auto& spec : kSchema)
            if (spec.section == section && spec.key == key) found = &spec;
        if (!found) fail(where, "unknown key " + section + "." + std::string(key));
        if (!keys.insert(section + "." + std::string(key)).second)
            fail(where, "duplicate key " + section + "." + std::string(key));
        auto value = value_text(text.substr(equal + 1), where);
        const auto location = where + " (" + section + "." + std::string(key) + ")";
        if (section == "storage" && key == "db") {
            // 先验证原始值，避免空字符串被路径拼接悄悄变成配置目录。
            Options checked;
            assign(checked, *found, value, location);
            value = (path.parent_path() / value).lexically_normal().string();
        }
        for (unsigned i = 0; i < configs.size(); ++i)
            if (found->consumers & (1u << i)) assign(configs[i], *found, value, location);
    }
    if (!input.eof()) fail(filename, "error reading config file");
    return sections.count("evictor") != 0;
}

void validate_evictor(const Options& options) {
    if (options.soft_watermark_bytes == 0)
        fail("evictor", "soft_watermark_bytes is required (or --soft-watermark-bytes)");
    if (options.emergency_watermark_bytes != 0 &&
        options.emergency_watermark_bytes <= options.soft_watermark_bytes)
        fail("evictor", "emergency_watermark_bytes must be 0 (disabled) or greater than soft_watermark_bytes");
}

std::string json_string(std::string_view text) {
    std::string out = "\"";
    constexpr char hex[] = "0123456789abcdef";
    for (const unsigned char c : text) {
        if (c == '"' || c == '\\') {
            out += '\\';
            out += static_cast<char>(c);
        } else if (c < 0x20) {
            out += "\\u00";
            out += hex[c >> 4];
            out += hex[c & 15];
        } else {
            out += static_cast<char>(c);
        }
    }
    return out + '"';
}

}  // namespace

ParsedConfig parse(Component component, const std::vector<std::string>& args) {
    struct Override { const Spec* spec; std::string value; };
    std::vector<Override> overrides;
    std::string filename;
    bool has_config = false, help = false, print = false, check = false;
    for (std::size_t i = 0; i < args.size(); ++i) {
        const auto& arg = args[i];
        auto next = [&]() -> const std::string& {
            if (i + 1 == args.size() || args[i + 1].starts_with("--") || args[i + 1].empty())
                fail(arg, "missing value");
            return args[++i];
        };
        if (arg == "--config") {
            if (has_config) fail(arg, "may only be specified once");
            has_config = true;
            filename = next();
        } else if (arg == "--help") {
            help = true;
        } else if (arg == "--print-config") {
            print = true;
        } else if (arg == "--check-config") {
            check = true;
        } else {
            const bool negative = arg.starts_with("--no-");
            const std::string flag = negative ? "--" + arg.substr(5) : arg;
            const Spec* found = nullptr;
            for (const auto& spec : kSchema)
                if (spec.flag == flag && (spec.consumers & mask(component)) &&
                    (!negative || spec.metavar == nullptr)) found = &spec;
            if (!found) fail(arg, "unknown option for " + std::string(name(component)));
            overrides.push_back({found, found->metavar ? next() : (negative ? "false" : "true")});
        }
    }
    Configs configs;
    for_component(configs, Component::Queryd).port = 9910;
    // 帮助无需配置/必填水位；print/check 始终执行完整校验，即使同时传入 --help。
    if (help && !print && !check) return {for_component(configs, component), Action::Help};
    const bool has_evictor = has_config && read_ini(filename, configs);
    auto& options = for_component(configs, component);
    for (const auto& override : overrides)
        assign(options, *override.spec, override.value, std::string(override.spec->flag));
    if (component == Component::Evictor || has_evictor)
        validate_evictor(for_component(configs, Component::Evictor));
    return {options, print ? Action::PrintConfig : (check ? Action::CheckConfig : Action::Run)};
}

std::string to_json(Component component, const Options& options) {
    std::ostringstream out;
    out.imbue(std::locale::classic());
    out << std::setprecision(std::numeric_limits<double>::max_digits10) << '{';
    bool first = true;
    for (const auto& spec : kSchema) {
        if (!(spec.consumers & mask(component))) continue;
        if (!first) out << ',';
        first = false;
        out << json_string(spec.json_key) << ':';
        std::visit([&](auto member) {
            using T = std::remove_cv_t<std::remove_reference_t<decltype(options.*member)>>;
            if constexpr (std::is_same_v<T, std::string>) out << json_string(options.*member);
            else if constexpr (std::is_same_v<T, bool>) out << (options.*member ? "true" : "false");
            else out << options.*member;
        }, spec.member);
    }
    return out.str() + '}';
}

std::string usage(Component component) {
    std::ostringstream out;
    out << "usage: " << name(component) << " [--config PATH] [--check-config] [--print-config] [--help]";
    for (const auto& spec : kSchema) {
        if (!(spec.consumers & mask(component))) continue;
        out << " [" << spec.flag;
        if (spec.metavar) out << ' ' << spec.metavar;
        else out << "|--no-" << spec.flag.substr(2);
        out << ']';
    }
    out << "\nCLI > INI file > defaults; --check-config validates without starting; "
           "--print-config validates and prints one JSON object without starting.\n";
    if (component == Component::Evictor)
        out << "evictor requires a positive soft watermark; emergency must be 0 or greater than soft.\n";
    return out.str();
}

Status configure(Component component, int argc, const char* const* argv, Options& options) {
    try {
        const auto parsed = parse(component, std::vector<std::string>(argv + 1, argv + argc));
        options = parsed.options;
        switch (parsed.action) {
            case Action::Run: return Status::Run;
            case Action::Help: std::printf("%s", usage(component).c_str()); break;
            case Action::PrintConfig: std::printf("%s\n", to_json(component, options).c_str()); break;
            case Action::CheckConfig: break;
        }
        return Status::ExitSuccess;
    } catch (const std::exception& error) {
        std::fprintf(stderr, "%s: %s\n%s", name(component), error.what(), usage(component).c_str());
        return Status::ExitFailure;
    }
}

}  // namespace tickshelf::startup
