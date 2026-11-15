#include "record.hpp"

#include <simdjson.h>

#include <bit>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace tickshelf::bench {
namespace {

constexpr std::uint32_t kKnownMask = (1U << (kScalarCount + kBookNames.size())) - 1;
constexpr auto kI64Max = std::numeric_limits<std::int64_t>::max();
constexpr auto kU64Max = std::numeric_limits<std::uint64_t>::max();

[[noreturn]] void invalid(std::string_view message) {
    throw std::invalid_argument(std::string(message));
}

bool whitespace(char c) { return c == ' ' || c == '\t' || c == '\r' || c == '\n'; }

// Reject invalid UTF-8 rather than replacing bytes and silently changing a symbol.
void validate_utf8(std::string_view value) {
    for (std::size_t i = 0; i < value.size();) {
        const auto c = static_cast<unsigned char>(value[i]);
        if (c < 0x80) { ++i; continue; }
        std::size_t width;
        std::uint32_t cp;
        if (c >= 0xc2 && c <= 0xdf) { width = 2; cp = c & 0x1f; }
        else if (c >= 0xe0 && c <= 0xef) { width = 3; cp = c & 0x0f; }
        else if (c >= 0xf0 && c <= 0xf4) { width = 4; cp = c & 0x07; }
        else invalid("symbol contains invalid UTF-8");
        if (width > value.size() - i) invalid("symbol contains truncated UTF-8");
        for (std::size_t n = 1; n < width; ++n) {
            const auto next = static_cast<unsigned char>(value[i + n]);
            if ((next & 0xc0) != 0x80) invalid("symbol contains invalid UTF-8 continuation");
            cp = (cp << 6) | (next & 0x3f);
        }
        if ((width == 3 && cp < 0x800) || (width == 4 && cp < 0x10000) ||
            (cp >= 0xd800 && cp <= 0xdfff) || cp > 0x10ffff)
            invalid("symbol contains non-scalar or overlong UTF-8");
        i += width;
    }
}

void validate_masks(std::uint32_t present, std::uint32_t nulls) {
    if ((present & ~kKnownMask) || (nulls & ~present)) invalid("invalid present/null masks");
}

void validate_depth(std::size_t count, std::size_t& depth, bool& have_depth) {
    if (count > 10) invalid("book depth exceeds 10");
    if (have_depth && depth != count) invalid("non-null book depths differ");
    depth = count;
    have_depth = true;
}

// Also enforce a canonical in-memory representation: absent/null fields cannot
// hide values that the binary or JSON encoder would otherwise discard.
std::size_t validate_record(const Record& record) {
    validate_masks(record.present, record.nulls);
    if (record.symbol.size() > std::numeric_limits<std::uint16_t>::max())
        invalid("symbol exceeds u16 byte length");
    validate_utf8(record.symbol);
    std::size_t size = 4 + 2 + record.symbol.size() + 8 + 4 + 4;
    const auto active = record.present & ~record.nulls;
    for (std::size_t i = 0; i < kScalarCount; ++i) {
        if (active & (1U << i)) size += 8;
        else if (record.values[i] != 0) invalid("absent/null scalar hides a value");
    }
    std::size_t depth = 0;
    bool have_depth = false;
    for (std::size_t j = 0; j < record.book.size(); ++j) {
        if (active & (1U << (kScalarCount + j))) {
            const auto count = record.book[j].size();
            validate_depth(count, depth, have_depth);
            size += 1 + count * 8;
        } else if (!record.book[j].empty()) invalid("absent/null book hides levels");
    }
    if (size > kMaxRecordBytes) invalid("record exceeds maximum byte length");
    return size;
}

void append_le(std::string& out, std::uint64_t value, unsigned width) {
    for (unsigned i = 0; i < width; ++i) out.push_back(static_cast<char>((value >> (i * 8)) & 0xff));
}

class Cursor {
public:
    explicit Cursor(std::string_view input) : input_(input) {}
    std::string_view take(std::size_t size) {
        if (size > input_.size()) invalid("truncated binary record or length prefix");
        auto result = input_.substr(0, size);
        input_.remove_prefix(size);
        return result;
    }
    std::uint64_t number(unsigned width) {
        const auto bytes = take(width);
        std::uint64_t result = 0;
        for (unsigned i = 0; i < width; ++i)
            result |= std::uint64_t(static_cast<unsigned char>(bytes[i])) << (i * 8);
        return result;
    }
    bool empty() const { return input_.empty(); }
private:
    std::string_view input_;
};

struct RecordHeader {
    std::string_view symbol;
    std::uint64_t time;
};

// One canonical cursor/validator serves both decoders. All scalar bit patterns
// are legal i64 values, so the encoded-only path can validate their extent
// without constructing any book vectors or re-encoding the record.
RecordHeader read_record(std::string_view bytes, Record* record) {
    if (bytes.size() > kMaxRecordBytes) invalid("record exceeds maximum byte length");
    Cursor cursor(bytes);
    if (cursor.take(4) != "RCD1") invalid("invalid binary record magic/version");
    const auto symbol_size = cursor.number(2);
    const auto symbol = cursor.take(static_cast<std::size_t>(symbol_size));
    validate_utf8(symbol);
    const auto time = cursor.number(8);
    const auto present = static_cast<std::uint32_t>(cursor.number(4));
    const auto nulls = static_cast<std::uint32_t>(cursor.number(4));
    validate_masks(present, nulls);
    const auto active = present & ~nulls;
    if (record) {
        record->symbol = symbol;
        record->time = time;
        record->present = present;
        record->nulls = nulls;
        for (std::size_t i = 0; i < kScalarCount; ++i)
            if (active & (1U << i)) record->values[i] = std::bit_cast<std::int64_t>(cursor.number(8));
    } else {
        cursor.take(8 * std::popcount(active & ((1U << kScalarCount) - 1)));
    }
    std::size_t depth = 0;
    bool have_depth = false;
    for (std::size_t j = 0; j < kBookNames.size(); ++j) {
        if (!(active & (1U << (kScalarCount + j)))) continue;
        const auto count = static_cast<std::size_t>(cursor.number(1));
        validate_depth(count, depth, have_depth);
        if (record) {
            record->book[j].reserve(count);
            for (std::size_t l = 0; l < count; ++l)
                record->book[j].push_back(std::bit_cast<std::int64_t>(cursor.number(8)));
        } else cursor.take(count * 8);
    }
    if (!cursor.empty()) invalid("trailing bytes after binary record");
    return {symbol, time};
}

template <typename T>
bool read_records(std::string_view payload, std::vector<T>& out, std::string& error) {
    out.clear();
    error.clear();
    try {
        if (payload.size() > kMaxPayloadBytes) invalid("batch exceeds maximum byte length");
        Cursor cursor(payload);
        while (!cursor.empty()) {
            if (out.size() == kMaxBatchRecords) invalid("batch exceeds maximum record count");
            const auto length = cursor.number(4);
            if (length > kMaxRecordBytes) invalid("record length exceeds maximum byte length");
            const auto bytes = cursor.take(static_cast<std::size_t>(length));
            T record;
            if constexpr (std::is_same_v<T, Record>) read_record(bytes, &record);
            else {
                const auto header = read_record(bytes, nullptr);
                record.symbol = header.symbol;
                record.time = header.time;
                record.bytes = bytes;
            }
            out.push_back(std::move(record));
        }
        return true;
    } catch (const std::exception& e) { out.clear(); error = e.what(); return false; }
}

struct ExactNumber {
    bool negative;
    std::uint64_t magnitude;
};

// JSON's full numeric grammar, evaluated in base ten, never through double.
// A scaled value must be an EXACT multiple of 1e-4; integer fields must be
// exact integers. Additional zero decimal places are harmless and accepted.
ExactNumber exact_number(std::string_view token, unsigned scale, std::uint64_t positive_limit,
                         std::uint64_t negative_limit) {
    while (!token.empty() && whitespace(token.back())) token.remove_suffix(1);
    std::size_t pos = 0;
    const bool negative = !token.empty() && token[0] == '-';
    if (negative) ++pos;
    const auto digit = [&](std::size_t i) { return i < token.size() && token[i] >= '0' && token[i] <= '9'; };
    std::string digits;
    if (!digit(pos)) invalid("expected JSON number");
    if (token[pos] == '0') digits.push_back(token[pos++]);
    else while (digit(pos)) digits.push_back(token[pos++]);
    std::int64_t fraction = 0;
    if (pos < token.size() && token[pos] == '.') {
        ++pos;
        if (!digit(pos)) invalid("fraction requires decimal digits");
        while (digit(pos)) { digits.push_back(token[pos++]); ++fraction; }
    }
    std::int64_t exponent = 0;
    if (pos < token.size() && (token[pos] == 'e' || token[pos] == 'E')) {
        ++pos;
        bool exponent_negative = false;
        if (pos < token.size() && (token[pos] == '+' || token[pos] == '-'))
            exponent_negative = token[pos++] == '-';
        if (!digit(pos)) invalid("exponent requires decimal digits");
        // Input is bounded by kMaxPayloadBytes, so an exponent larger than
        // this cannot be canceled by its mantissa's decimal places.
        constexpr auto cap = static_cast<std::int64_t>(kMaxPayloadBytes) + 32;
        while (digit(pos)) {
            const auto n = token[pos++] - '0';
            exponent = exponent > (cap - n) / 10 ? cap : exponent * 10 + n;
        }
        if (exponent_negative) exponent = -exponent;
    }
    if (pos != token.size()) invalid("invalid JSON numeric token");
    const auto first = digits.find_first_not_of('0');
    if (first == std::string::npos) return {negative, 0};
    digits.erase(0, first);
    auto shift = exponent - fraction + scale;
    while (digits.size() > 1 && digits.back() == '0') { digits.pop_back(); ++shift; }
    if (shift < 0) invalid(scale ? "precision loss: value is not an exact multiple of 1e-4"
                               : "precision loss: integer field is fractional");
    if (shift > 20 || digits.size() > 20 || digits.size() + static_cast<std::size_t>(shift) > 20)
        invalid("numeric overflow");
    const auto limit = negative ? negative_limit : positive_limit;
    std::uint64_t magnitude = 0;
    const auto add_digit = [&](unsigned n) {
        if (n > limit || magnitude > (limit - n) / 10) invalid("numeric overflow");
        magnitude = magnitude * 10 + n;
    };
    for (char c : digits) add_digit(static_cast<unsigned>(c - '0'));
    for (std::int64_t i = 0; i < shift; ++i) add_digit(0);
    return {negative, magnitude};
}

std::int64_t signed_number(simdjson::ondemand::value value, bool scaled) {
    if (value.type() != simdjson::ondemand::json_type::number) invalid("quoter value must be numeric or null");
    const auto n = exact_number(value.raw_json_token(), scaled ? 4 : 0,
                                static_cast<std::uint64_t>(kI64Max), std::uint64_t(kI64Max) + 1);
    if (!n.negative) return static_cast<std::int64_t>(n.magnitude);
    if (n.magnitude == std::uint64_t(kI64Max) + 1) return std::numeric_limits<std::int64_t>::min();
    return -static_cast<std::int64_t>(n.magnitude);
}

void read_quoter(simdjson::ondemand::value value, Record& record) {
    for (auto field : value.get_object()) {
        const std::string_view key = field.unescaped_key();
        std::size_t index = 0;
        for (; index < kScalarCount; ++index)
            if (key == kScalarNames[index] || (index == 3 && key == "pvolume")) break;
        if (index == kScalarCount) {
            std::size_t j = 0;
            for (; j < kBookNames.size(); ++j) if (key == kBookNames[j]) break;
            if (j == kBookNames.size()) invalid("unknown quoter field");
            index += j;
        }
        const auto bit = 1U << index;
        if (record.present & bit) invalid("duplicate quoter field or pvolume/pvolumn alias collision");
        record.present |= bit;
        auto entry = field.value().value();
        if (entry.is_null()) { record.nulls |= bit; continue; }
        if (index < kScalarCount) record.values[index] = signed_number(entry, kScaled[index]);
        else {
            const auto j = index - kScalarCount;
            for (auto level : entry.get_array()) {
                if (record.book[j].size() == 10) invalid("book depth exceeds 10");
                // At most ten levels: avoid 1/2/4/8 growth, but keep [] allocation-free.
                if (record.book[j].empty()) record.book[j].reserve(10);
                record.book[j].push_back(signed_number(level.value(), j < 2));
            }
        }
    }
}

Record read_tick(simdjson::ondemand::value value) {
    Record record;
    unsigned seen = 0;
    for (auto field : value.get_object()) {
        const std::string_view key = field.unescaped_key();
        const unsigned bit = key == "symbol" ? 1 : key == "time" ? 2 : key == "quoter" ? 4 : 0;
        if (!bit) invalid("unknown tick wrapper field");
        if (seen & bit) invalid("duplicate tick wrapper field");
        seen |= bit;
        auto entry = field.value().value();
        if (bit == 1) record.symbol = std::string(std::string_view(entry.get_string()));
        else if (bit == 2) {
            if (entry.type() != simdjson::ondemand::json_type::number) invalid("time must be an unsigned integer");
            const auto n = exact_number(entry.raw_json_token(), 0, kU64Max, 0);
            if (n.negative) invalid("time must not be negative");
            record.time = n.magnitude;
        } else read_quoter(entry, record);
    }
    if (seen != 7) invalid("tick requires symbol, time and quoter");
    validate_record(record);
    return record;
}

std::string decimal(std::int64_t value, bool scaled) {
    if (!scaled) return std::to_string(value);
    const auto magnitude = value < 0 ? std::uint64_t(-(value + 1)) + 1 : std::uint64_t(value);
    std::string result = value < 0 ? "-" : "";
    result += std::to_string(magnitude / 10000);
    result.push_back('.');
    const auto fraction = std::to_string(10000 + magnitude % 10000);
    result.append(fraction, 1, 4);
    return result;
}

}  // namespace

namespace {
void append_record(std::string& bytes, const Record& record) {
    bytes += "RCD1";
    append_le(bytes, record.symbol.size(), 2);
    bytes += record.symbol;
    append_le(bytes, record.time, 8);
    append_le(bytes, record.present, 4);
    append_le(bytes, record.nulls, 4);
    const auto active = record.present & ~record.nulls;
    for (std::size_t i = 0; i < kScalarCount; ++i)
        if (active & (1U << i)) append_le(bytes, std::bit_cast<std::uint64_t>(record.values[i]), 8);
    for (std::size_t j = 0; j < record.book.size(); ++j) {
        if (!(active & (1U << (kScalarCount + j)))) continue;
        append_le(bytes, record.book[j].size(), 1);
        for (auto value : record.book[j]) append_le(bytes, std::bit_cast<std::uint64_t>(value), 8);
    }
}
}  // namespace

std::string encode_record(const Record& record) {
    std::string bytes;
    bytes.reserve(validate_record(record));
    append_record(bytes, record);
    return bytes;
}

bool decode_record(std::string_view bytes, Record& out, std::string& error) {
    error.clear();
    try {
        Record record;
        read_record(bytes, &record);
        out = std::move(record);
        return true;
    } catch (const std::exception& e) { error = e.what(); return false; }
}

std::string encode_records(const std::vector<Record>& records) {
    if (records.size() > kMaxBatchRecords) invalid("batch exceeds maximum record count");
    std::size_t size = 0;
    for (const auto& record : records) {
        size += 4 + validate_record(record);
        if (size > kMaxPayloadBytes) invalid("batch exceeds maximum byte length");
    }
    std::string payload;
    payload.reserve(size); // One encoded-byte allocation, no per-record temporary.
    for (const auto& record : records) {
        const auto prefix = payload.size();
        append_le(payload, 0, 4);
        append_record(payload, record);
        const auto length = payload.size() - prefix - 4;
        for (unsigned i = 0; i < 4; ++i)
            payload[prefix + i] = static_cast<char>((length >> (i * 8)) & 0xff);
    }
    return payload;
}

bool decode_records(std::string_view payload, std::vector<Record>& out, std::string& error) {
    return read_records(payload, out, error);
}

bool decode_encoded_records(std::string_view payload, std::vector<EncodedRecord>& out,
                            std::string& error) {
    return read_records(payload, out, error);
}

struct JsonDecoder::Impl {
    simdjson::ondemand::parser parser;
    // Both stage-one parser storage and the padded input allocation are reused.
    std::string input;
};

JsonDecoder::JsonDecoder() : impl_(std::make_unique<Impl>()) {}
JsonDecoder::~JsonDecoder() = default;

bool JsonDecoder::parse(std::string_view json, std::vector<Record>& out, std::string& error) {
    out.clear();
    error.clear();
    try {
        if (json.size() > kMaxPayloadBytes) invalid("JSON batch exceeds maximum byte length");
        impl_->input.assign(json.data(), json.size());
        const auto length = impl_->input.size();
        impl_->input.resize(length + simdjson::SIMDJSON_PADDING);
        simdjson::ondemand::document document = impl_->parser.iterate(impl_->input.data(), length, impl_->input.size());
        // Reuse the caller's vector capacity; clear it again on every failure.
        bool seen_ticks = false;
        std::size_t binary_size = 0;
        for (auto field : document.get_object()) {
            const std::string_view key = field.unescaped_key();
            if (key != "ticks") invalid("unknown root field");
            if (seen_ticks) invalid("duplicate ticks root field");
            seen_ticks = true;
            for (auto tick_result : field.value().get_array()) {
                if (out.size() == kMaxBatchRecords) invalid("batch exceeds maximum record count");
                auto tick = tick_result.value();
                const auto* begin = tick.current_location().value();
                auto record = read_tick(tick);
                const auto* end = document.current_location().value();
                while (end > begin && whitespace(end[-1])) --end;
                if (static_cast<std::size_t>(end - begin) > kMaxRecordBytes)
                    invalid("JSON record exceeds maximum byte length");
                binary_size += 4 + validate_record(record);
                if (binary_size > kMaxPayloadBytes) invalid("binary batch exceeds maximum byte length");
                out.push_back(std::move(record));
            }
        }
        if (!seen_ticks) invalid("root requires ticks array");
        if (!document.at_end()) invalid("trailing content after JSON root");
        return true;
    } catch (const std::exception& e) { out.clear(); error = e.what(); return false; }
}

std::uint64_t logical_id(std::uint64_t sequence, std::uint32_t duplicate_every) {
    if (duplicate_every == 1) invalid("duplicate_every must be zero or at least two");
    if (!duplicate_every) return sequence;
    // Equivalent to (sequence+1)/duplicate_every without overflowing at UINT64_MAX.
    return sequence - (sequence / duplicate_every + (sequence % duplicate_every == duplicate_every - 1));
}

Record generate_record(std::uint64_t id, const GeneratorOptions& options) {
    if (!options.symbols) invalid("generator requires at least one symbol");
    if (options.depth > 10) invalid("generator depth exceeds 10");
    if (id > kU64Max - options.seed) invalid("generator id + seed overflows");
    const auto s = id % options.symbols;
    const auto c = id / options.symbols;
    if (c > (kU64Max - kTimeBase) / 1000) invalid("generator timestamp overflows");
    const auto x = id + options.seed;
    // c + seed cannot exceed id + seed for symbols >= 1.
    const auto last = static_cast<std::int64_t>(100000 + s * 17 + (c + options.seed) % 10000);
    const auto volume = static_cast<std::int64_t>(100 + x % 1000);
    Record record;
    record.symbol = std::to_string(600000 + s) + ".SH";
    record.time = kTimeBase + c * 1000;
    record.present = kKnownMask & ~(1U << 12);
    record.values = {last, last * volume, volume, static_cast<std::int64_t>(99 + x % 1000),
                     static_cast<std::int64_t>(x % 100), static_cast<std::int64_t>(x % 3),
                     last - 2, last - 10, last + 20, last - 20, last - 3, last - 1, 0};
    if (options.optional_fields) {
        record.present |= 1U << 12;
        record.values[12] = static_cast<std::int64_t>(x % 10000);
        if (id % 2) { record.present &= ~(1U << 4); record.values[4] = 0; }
        else if (id % 3 == 0) { record.nulls |= 1U << 4; record.values[4] = 0; }
    }
    for (auto& levels : record.book) levels.reserve(options.depth);
    for (std::uint32_t l = 0; l < options.depth; ++l) {
        record.book[0].push_back(last + l + 1);
        record.book[1].push_back(last - l - 1);
        record.book[2].push_back(static_cast<std::int64_t>(1000 + x % 97 + l));
        record.book[3].push_back(static_cast<std::int64_t>(2000 + x % 89 + l));
    }
    return record;
}

std::string json_quote(std::string_view value) {
    validate_utf8(value);
    constexpr char hex[] = "0123456789abcdef";
    std::string result;
    result.reserve(value.size() + 2);
    result.push_back('"');
    for (char byte : value) {
        const auto c = static_cast<unsigned char>(byte);
        switch (c) {
            case '"': result += "\\\""; break;
            case '\\': result += "\\\\"; break;
            case '\b': result += "\\b"; break;
            case '\f': result += "\\f"; break;
            case '\n': result += "\\n"; break;
            case '\r': result += "\\r"; break;
            case '\t': result += "\\t"; break;
            default:
                if (c < 0x20) { result += "\\u00"; result += hex[c >> 4]; result += hex[c & 15]; }
                else result.push_back(static_cast<char>(c));
        }
    }
    result.push_back('"');
    return result;
}

std::string records_json(const std::vector<Record>& records) {
    if (records.size() > kMaxBatchRecords) invalid("batch exceeds maximum record count");
    std::string json = "{\"ticks\":[";
    std::size_t binary_size = 0;
    bool first_record = true;
    for (const auto& record : records) {
        binary_size += 4 + validate_record(record);
        if (binary_size > kMaxPayloadBytes) invalid("binary batch exceeds maximum byte length");
        if (!first_record) json += ',';
        first_record = false;
        const auto start = json.size();
        json += "{\"symbol\":" + json_quote(record.symbol) + ",\"time\":" + std::to_string(record.time) + ",\"quoter\":{";
        bool first_field = true;
        for (std::size_t index = 0; index < kScalarCount + kBookNames.size(); ++index) {
            const auto bit = 1U << index;
            if (!(record.present & bit)) continue;
            if (!first_field) json += ',';
            first_field = false;
            json += json_quote(index < kScalarCount ? kScalarNames[index] : kBookNames[index - kScalarCount]);
            json += ':';
            if (record.nulls & bit) json += "null";
            else if (index < kScalarCount) json += decimal(record.values[index], kScaled[index]);
            else {
                const auto j = index - kScalarCount;
                json += '[';
                for (std::size_t l = 0; l < record.book[j].size(); ++l) {
                    if (l) json += ',';
                    json += decimal(record.book[j][l], j < 2);
                }
                json += ']';
            }
        }
        json += "}}";
        if (json.size() - start > kMaxRecordBytes) invalid("JSON record exceeds maximum byte length");
        if (json.size() + 2 > kMaxPayloadBytes) invalid("JSON batch exceeds maximum byte length");
    }
    json += "]}";
    return json;
}

}  // namespace tickshelf::bench
