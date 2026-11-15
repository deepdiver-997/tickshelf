#include "../bench/record.hpp"

#include <doctest/doctest.h>

#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
using namespace tickshelf::bench;
constexpr std::uint32_t all_fields = (1U << 17) - 1;
constexpr auto i64_min = std::numeric_limits<std::int64_t>::min();
constexpr auto i64_max = std::numeric_limits<std::int64_t>::max();
constexpr auto u64_max = std::numeric_limits<std::uint64_t>::max();

std::string fixture(const char* name) {
    const auto path = std::filesystem::path(__FILE__).parent_path() / "fixtures" / "iquant" / name;
    std::ifstream stream(path, std::ios::binary);
    if (!stream) throw std::runtime_error("cannot open fixture: " + path.string());
    return {std::istreambuf_iterator<char>(stream), std::istreambuf_iterator<char>()};
}

std::string wrap(std::string_view quoter) {
    return "{\"ticks\":[{\"symbol\":\"600000.SH\",\"time\":1700000000000,\"quoter\":" + std::string(quoter) + "}]}";
}

void check_batch_decoders(std::string_view payload) {
    std::vector<Record> records = {generate_record(7, {})};
    std::vector<EncodedRecord> encoded = {{"stale", 8, "old"}};
    encoded[0].has_sha256 = true;
    encoded[0].ingress_ns = 123;
    std::string old_error = "stale", new_error = "stale";
    const bool old_ok = decode_records(payload, records, old_error);
    const bool new_ok = decode_encoded_records(payload, encoded, new_error);
    REQUIRE(old_ok == new_ok);
    CHECK(old_error == new_error);
    REQUIRE(records.size() == encoded.size());
    if (!old_ok) {
        CHECK(records.empty());
        CHECK(encoded.empty());
        CHECK_FALSE(new_error.empty());
        return;
    }
    CHECK(new_error.empty());
    for (std::size_t i = 0; i < records.size(); ++i) {
        CHECK(encoded[i].symbol == records[i].symbol);
        CHECK(encoded[i].time == records[i].time);
        CHECK(encoded[i].bytes == encode_record(records[i]));
        CHECK_FALSE(encoded[i].has_sha256);
        CHECK(encoded[i].sha256 == std::array<std::uint8_t, 32>{});
        CHECK(encoded[i].ingress_ns == 0);
    }
}

std::vector<Record> parse_ok(JsonDecoder& decoder, const std::string& json) {
    std::vector<Record> records;
    std::string error = "old error";
    const auto ok = decoder.parse(json, records, error);
    INFO("JSON: ", json.substr(0, 2048), "; error: ", error);
    REQUIRE(ok);
    CHECK(error.empty());
    // Every existing valid JSON boundary also exercises the strict zero-book-
    // allocation canonical decoder (including NUL symbols and UINT64_MAX).
    check_batch_decoders(encode_records(records));
    return records;
}

void parse_bad(JsonDecoder& decoder, const std::string& json) {
    std::vector<Record> records = {generate_record(7, {})};
    std::string error;
    INFO("invalid JSON: ", json.substr(0, 2048));
    CHECK_FALSE(decoder.parse(json, records, error));
    CHECK(records.empty());
    CHECK_FALSE(error.empty());
    // The same On-Demand parser must recover after an aborted document.
    const auto valid = parse_ok(decoder, wrap("{}"));
    REQUIRE(valid.size() == 1);
    CHECK(valid[0].present == 0);
}

void set_le(std::string& bytes, std::size_t offset, std::uint64_t value, unsigned width) {
    for (unsigned i = 0; i < width; ++i) bytes.at(offset + i) = static_cast<char>((value >> (8 * i)) & 0xff);
}

void binary_bad(const std::string& bytes) {
    auto record = generate_record(9, {});
    const auto before = record;
    std::string error;
    CHECK_FALSE(decode_record(bytes, record, error));
    CHECK(record == before);
    CHECK_FALSE(error.empty());
    std::string framed(4, '\0');
    set_le(framed, 0, bytes.size(), 4);
    framed += bytes;
    check_batch_decoders(framed);
    std::vector<EncodedRecord> encoded;
    CHECK_FALSE(decode_encoded_records(framed, encoded, error));
    CHECK(encoded.empty());
}

TEST_CASE("bench record official-field fixture preserves all five book levels") {
    JsonDecoder decoder;
    const auto records = parse_ok(decoder, fixture("full-five-level.json"));
    REQUIRE(records.size() == 1);
    const auto& record = records[0];
    CHECK(record.symbol == "600000.SH");
    CHECK(record.time == kTimeBase);
    CHECK(record.present == all_fields);
    CHECK(record.nulls == 0);
    const std::array<std::int64_t, kScalarCount> expected = {
        101234, 1250005678, 12345, 12200, 100, 2, 98765, 100000,
        105000, 95000, 100100, 99900, 456};
    CHECK(record.values == expected);
    CHECK(record.book[0] == std::vector<std::int64_t>{101235, 101236, 101237, 101238, 101239});
    CHECK(record.book[1] == std::vector<std::int64_t>{101233, 101232, 101231, 101230, 101229});
    CHECK(record.book[2] == std::vector<std::int64_t>{11, 22, 33, 44, 55});
    CHECK(record.book[3] == std::vector<std::int64_t>{66, 77, 88, 99, 110});
    Record decoded;
    std::string error;
    REQUIRE(decode_record(encode_record(record), decoded, error));
    CHECK(decoded == record);
    CHECK(parse_ok(decoder, records_json(records)) == records);
}

TEST_CASE("bench record accepts order alias Unicode escapes empty missing and null books") {
    JsonDecoder decoder;
    const auto records = parse_ok(decoder, fixture("optional-alias-unicode.json"));
    REQUIRE(records.size() == 3);
    const auto& record = records[0];
    CHECK(record.symbol == std::string("浦发\0\"\\\n🚀.SH", 17));
    CHECK(record.time == 1700000000001ULL);
    CHECK(record.present == ((1U << 0) | (1U << 1) | (1U << 3) | (1U << 4) | (15U << 13)));
    CHECK(record.nulls == ((1U << 1) | (1U << 4)));
    CHECK(record.values[0] == 101234);
    CHECK(record.values[3] == 12200);
    CHECK(record.values[1] == 0);
    CHECK(record.book[0] == std::vector<std::int64_t>{101235, 101236});
    CHECK(records[1].present == (15U << 13));
    CHECK(records[1].nulls == 0);
    for (const auto& levels : records[1].book) CHECK(levels.empty());
    CHECK(records[2].present == ((1U << 5) | (1U << 13) | (1U << 16)));
    CHECK(records[2].nulls == records[2].present);
    CHECK(records[2].time == u64_max);
    const auto json = records_json(records);
    CHECK(json.find("pvolume") == std::string::npos);
    CHECK(json.find("\"pvolumn\":12200") != std::string::npos);
    CHECK(json.find("\"volume\"") == std::string::npos);
    CHECK(parse_ok(decoder, json) == records);
}

TEST_CASE("bench record every quoter field distinguishes absent zero and null") {
    JsonDecoder decoder;
    for (std::size_t i = 0; i < 17; ++i) {
        const auto name = i < kScalarCount ? kScalarNames[i] : kBookNames[i - kScalarCount];
        for (bool null : {false, true}) {
            const auto value = null ? "null" : i < kScalarCount ? "0" : "[]";
            const auto records = parse_ok(decoder, wrap("{" + json_quote(name) + ":" + value + "}"));
            REQUIRE(records.size() == 1);
            CHECK(records[0].present == (1U << i));
            CHECK(records[0].nulls == (null ? 1U << i : 0));
            CHECK(parse_ok(decoder, records_json(records)) == records);
        }
    }
    CHECK(parse_ok(decoder, wrap("{}"))[0].present == 0);
}

TEST_CASE("bench record exact decimal and scientific values never round through double") {
    JsonDecoder decoder;
    const std::vector<std::pair<std::string, std::int64_t>> scaled = {
        {"0", 0}, {"-0", 0}, {"-0.0000", 0}, {"0.0001", 1}, {"-0.0001", -1},
        {"10.12340000000000000000", 101234}, {"101234e-4", 101234},
        {"1E+2", 1000000}, {"1000000000000000000000000000000e-30", 10000},
        {"900719925474.0993", 9007199254740993LL},
        {"922337203685477.5807", i64_max}, {"-922337203685477.5808", i64_min},
        {"9.223372036854775807e14", i64_max}, {"-9.223372036854775808E14", i64_min},
        {"0e999999999999999999999", 0}, {"0e-999999999999999999999", 0}};
    for (const auto& [token, expected] : scaled) {
        const auto records = parse_ok(decoder, wrap("{\"lastPrice\":" + token + ",\"askPrice\":[" + token + "]}"));
        CHECK(records[0].values[0] == expected);
        CHECK(records[0].book[0] == std::vector<std::int64_t>{expected});
        CHECK(parse_ok(decoder, records_json(records)) == records);
    }
    for (const auto& [token, expected] : std::vector<std::pair<std::string, std::int64_t>>{
             {"1.000", 1}, {"1e3", 1000}, {"100e-2", 1}, {"9007199254740993", 9007199254740993LL},
             {"9223372036854775807", i64_max}, {"-9223372036854775808", i64_min},
             {"9.223372036854775807e18", i64_max}}) {
        const auto records = parse_ok(decoder, wrap("{\"volume\":" + token + ",\"bidVol\":[" + token + "]}"));
        CHECK(records[0].values[2] == expected);
        CHECK(records[0].book[3] == std::vector<std::int64_t>{expected});
    }
    const auto times = parse_ok(decoder, "{\"ticks\":[{\"time\":184467440737095516150e-1,\"quoter\":{},\"symbol\":\"x\"}]}");
    CHECK(times[0].time == u64_max);
}

TEST_CASE("bench record precision overflow and malformed numeric grammar reject loudly") {
    JsonDecoder decoder;
    for (const auto& token : {
             "0.00001", "1e-5", "-1.00001", "10.123400000000000001", "922337203685477.5808",
             "-922337203685477.5809", "1e99999999999999999999", "1e-9999999999999999999",
             "+1", "01", "-01", ".1", "1.", "1e", "1e+", "--1", "1e1.0", "NaN", "Infinity", "1x",
             "true", "false", "nullx", "nul", "\"1.0000\"", "{}", "[]"})
        parse_bad(decoder, wrap("{\"amount\":" + std::string(token) + "}"));
    for (const auto& token : {"1.1", "1e-1", "9223372036854775808", "-9223372036854775809"})
        parse_bad(decoder, wrap("{\"volume\":" + std::string(token) + "}"));
    for (const auto& token : {"-1", "-0", "1.5", "18446744073709551616", "1e1000", "null", "\"123\""})
        parse_bad(decoder, "{\"ticks\":[{\"symbol\":\"x\",\"quoter\":{},\"time\":" + std::string(token) + "}]}");
}

TEST_CASE("bench record rejects unknown duplicate and alias collision fields at every level") {
    JsonDecoder decoder;
    for (const auto& json : {
             "{\"ticks\":[],\"other\":0}", "{\"ticks\":[],\"ticks\":[]}",
             "{\"ticks\":[],\"t\\u0069cks\":[]}",
             "{\"ticks\":[{\"symbol\":\"x\",\"symbol\":\"y\",\"time\":1,\"quoter\":{}}]}",
             "{\"ticks\":[{\"symbol\":\"x\",\"time\":1,\"time\":2,\"quoter\":{}}]}",
             "{\"ticks\":[{\"symbol\":\"x\",\"time\":1,\"quoter\":{},\"quoter\":{}}]}",
             "{\"ticks\":[{\"symbol\":\"x\",\"time\":1,\"quoter\":{},\"extra\":0}]}"}) parse_bad(decoder, json);
    for (const auto& quoter : {
             "{\"lastPrice\":1,\"lastPrice\":2}", "{\"volume\":null,\"vol\\u0075me\":3}",
             "{\"pvolumn\":1,\"pvolume\":1}", "{\"pvolume\":null,\"pvolumn\":null}",
             "{\"pvolume\":1,\"pvolume\":2}", "{\"askPrice\":[],\"askPrice\":[]}",
             "{\"unknown\":1}", "{\"close\":1}", "{\"\":null}"}) parse_bad(decoder, wrap(quoter));
    const auto alias = parse_ok(decoder, wrap("{\"pvolume\":null}"));
    CHECK(alias[0].present == (1U << 3));
    CHECK(alias[0].nulls == (1U << 3));
}

TEST_CASE("bench record malformed structures trailing input and partial batches are atomic") {
    JsonDecoder decoder;
    for (const auto& json : {
             "", "{}", "[]", "null", "{\"ticks\":null}", "{\"ticks\":{}}", "{\"ticks\":[null]}",
             "{\"ticks\":[{}]}", "{\"ticks\":[{\"symbol\":\"x\",\"time\":1}]}",
             "{\"ticks\":[{\"symbol\":null,\"time\":1,\"quoter\":{}}]}",
             "{\"ticks\":[{\"symbol\":1,\"time\":1,\"quoter\":{}}]}",
             "{\"ticks\":[{\"symbol\":\"x\",\"time\":1,\"quoter\":null}]}",
             "{\"ticks\":[{\"symbol\":\"x\",\"time\":1,\"quoter\":[]}]}",
             "{\"ticks\":[]} {}", "{\"ticks\":[]} 0", "{\"ticks\":[]} garbage", "{\"ticks\":[],}",
             "{\"ticks\":[,]}", "{\"ticks\":[{\"symbol\":\"x\",\"time\":1,\"quoter\":{}},]}",
             "{\"ticks\":[{\"symbol\":\"x\" \"time\":1,\"quoter\":{}}]}",
             "{\"ticks\":[{\"symbol\":\"x\",\"time\":1,\"quoter\":{\"volume\":1 \"amount\":2}}]}",
             "{\"ticks\":[{\"symbol\":\"x\",\"time\":1,\"quoter\":{}},{}]}",
             "{\"ticks\":[{\"symbol\":\"x\",\"time\":1,\"quoter\":{}},",
             "{\"ticks\":[{\"symbol\":\"\\ud800\",\"time\":1,\"quoter\":{}}]}"}) parse_bad(decoder, json);
    const auto valid = fixture("full-five-level.json");
    for (std::size_t n = 0; n < valid.size(); ++n) {
        if (n > valid.find_last_of('}')) continue; // Only trailing whitespace may be removed.
        std::vector<Record> out;
        std::string error;
        CHECK_FALSE(decoder.parse(std::string_view(valid).substr(0, n), out, error));
        CHECK(out.empty());
        CHECK_FALSE(error.empty());
    }
    CHECK(parse_ok(decoder, " \n{\"ticks\":[]}\t").empty());
}

TEST_CASE("bench record variable depth is lossless and mismatched or oversized books fail") {
    JsonDecoder decoder;
    for (std::uint32_t depth = 0; depth <= 10; ++depth) {
        const std::vector<Record> expected = {generate_record(123, {54, depth, 42, false})};
        CHECK(parse_ok(decoder, records_json(expected)) == expected);
        std::vector<Record> decoded;
        std::string error;
        REQUIRE(decode_records(encode_records(expected), decoded, error));
        CHECK(decoded == expected);
    }
    for (const auto& quoter : {
             "{\"askPrice\":[1],\"bidPrice\":[2,3]}", "{\"askPrice\":[],\"askVol\":[1]}",
             "{\"bidVol\":[1,2,3,4,5,6,7,8,9,10,11]}", "{\"askPrice\":[null]}",
             "{\"askVol\":[1.1]}", "{\"bidPrice\":[0.00001]}", "{\"askPrice\":1}",
             "{\"askVol\":[\"1\"]}", "{\"bidVol\":[[]]}"}) parse_bad(decoder, wrap(quoter));
    const auto partial = parse_ok(decoder, wrap("{\"askPrice\":[1],\"bidPrice\":null}"));
    CHECK(partial[0].book[0] == std::vector<std::int64_t>{10000});
    CHECK(partial[0].nulls == (1U << 14));
}

TEST_CASE("bench record binary layout is independently specified little endian RCD1") {
    Record record;
    record.symbol = "A";
    record.time = 0x0807060504030201ULL;
    record.present = (1U << 0) | (1U << 2) | (1U << 13);
    record.nulls = 1U << 2;
    record.values[0] = -2;
    record.book[0] = {0x0102030405060708LL};
    const unsigned char oracle[] = {
        'R', 'C', 'D', '1', 1, 0, 'A', 1, 2, 3, 4, 5, 6, 7, 8,
        5, 0x20, 0, 0, 4, 0, 0, 0,
        0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        1, 8, 7, 6, 5, 4, 3, 2, 1};
    const std::string expected(reinterpret_cast<const char*>(oracle), sizeof(oracle));
    CHECK(encode_record(record) == expected);
    Record decoded;
    std::string error = "stale";
    REQUIRE(decode_record(expected, decoded, error));
    CHECK(error.empty());
    CHECK(decoded == record);
    const auto batch = encode_records({record, record});
    CHECK(batch.size() == 2 * (expected.size() + 4));
    CHECK(static_cast<unsigned char>(batch[0]) == expected.size());
    CHECK(batch.substr(4, expected.size()) == expected);
}

TEST_CASE("bench record binary rejects every truncation corrupt masks hidden bytes and lengths") {
    const auto record = generate_record(42, {54, 5, 42, true});
    const auto valid = encode_record(record);
    for (std::size_t n = 0; n < valid.size(); ++n) binary_bad(valid.substr(0, n));
    binary_bad(valid + std::string(1, '\0'));
    auto bad = valid;
    bad[3] = '2';
    binary_bad(bad);
    bad = valid;
    set_le(bad, 4, 65535, 2);
    binary_bad(bad);
    const auto present_offset = 6 + record.symbol.size() + 8;
    bad = valid;
    set_le(bad, present_offset, all_fields | (1U << 17), 4);
    binary_bad(bad);
    bad = valid;
    set_le(bad, present_offset + 4, 1U << 31, 4);
    binary_bad(bad);
    Record empty;
    bad = encode_record(empty);
    set_le(bad, 18, 1, 4); // null without present
    binary_bad(bad);
    Record book;
    book.present = 1U << 13;
    bad = encode_record(book);
    bad[22] = 11;
    binary_bad(bad);
    book.book[0] = {1};
    book.present |= 1U << 14;
    bad = encode_record(Record{});
    set_le(bad, 14, book.present, 4);
    bad += '\1';
    bad.append(8, '\0');
    bad += '\0';
    binary_bad(bad); // one level vs zero levels
    bad = encode_record(Record{});
    bad.insert(6, 1, static_cast<char>(0xff));
    set_le(bad, 4, 1, 2);
    binary_bad(bad);
}

TEST_CASE("bench record encoder rejects noncanonical masks and data rather than hiding loss") {
    auto expect_bad = [](const Record& record) {
        CHECK_THROWS_AS(encode_record(record), std::invalid_argument);
        CHECK_THROWS_AS(records_json({record}), std::invalid_argument);
    };
    Record record;
    record.present = 1U << 17;
    expect_bad(record);
    record = {};
    record.nulls = 1;
    expect_bad(record);
    record = {};
    record.values[0] = 1;
    expect_bad(record);
    record.present = record.nulls = 1;
    expect_bad(record);
    record = {};
    record.book[0] = {1};
    expect_bad(record);
    record.present = record.nulls = 1U << 13;
    expect_bad(record);
    record.nulls = 0;
    record.book[0].resize(11);
    expect_bad(record);
    record.book[0].resize(1);
    record.present |= 1U << 14;
    expect_bad(record);
    record = {};
    record.symbol.assign(65536, 'x');
    expect_bad(record);
    record.symbol.assign(kMaxRecordBytes - 21, 'x'); // u16 fits, complete binary does not.
    expect_bad(record);
}

TEST_CASE("bench record binary batch validates framing limits and publishes no partial batch") {
    const std::vector<Record> records = {generate_record(1, {}), generate_record(2, {})};
    const auto payload = encode_records(records);
    std::vector<Record> out;
    std::string error;
    REQUIRE(decode_records(payload, out, error));
    CHECK(out == records);
    auto reject = [&](const std::string& bytes) {
        check_batch_decoders(bytes);
        out = records;
        CHECK_FALSE(decode_records(bytes, out, error));
        CHECK(out.empty());
        CHECK_FALSE(error.empty());
    };
    for (std::size_t n = 1; n < 4; ++n) reject(payload.substr(0, n));
    reject(payload.substr(0, payload.size() - 1));
    reject(payload + "x");
    auto bad = payload;
    set_le(bad, 0, 0, 4);
    reject(bad);
    set_le(bad, 0, kMaxRecordBytes + 1, 4);
    reject(bad);
    reject(std::string(kMaxPayloadBytes + 1, '\0'));
    const auto one = encode_records({Record{}});
    std::string too_many;
    for (std::size_t n = 0; n <= kMaxBatchRecords; ++n) too_many += one;
    reject(too_many);
    CHECK_THROWS_AS(encode_records(std::vector<Record>(kMaxBatchRecords + 1)), std::invalid_argument);
    REQUIRE(decode_records("", out, error));
    CHECK(out.empty());
    CHECK(error.empty());
    CHECK(encode_records({}).empty());
}

TEST_CASE("bench record exact binary size limits are inclusive") {
    Record maximum;
    maximum.symbol.assign(kMaxRecordBytes - 22, 'x');
    const auto bytes = encode_record(maximum);
    REQUIRE(bytes.size() == kMaxRecordBytes);
    Record out;
    std::string error;
    REQUIRE(decode_record(bytes, out, error));
    CHECK(out == maximum);
    binary_bad(bytes + "x");
    std::vector<Record> records(64, maximum);
    CHECK_THROWS_AS(encode_records(records), std::invalid_argument);
    records.back().symbol.resize(records.back().symbol.size() - 256);
    const auto payload = encode_records(records);
    REQUIRE(payload.size() == kMaxPayloadBytes);
    check_batch_decoders(payload);
    std::vector<Record> decoded;
    REQUIRE(decode_records(payload, decoded, error));
    CHECK(decoded == records);
}

TEST_CASE("bench record JSON input output and count bounds are enforced") {
    JsonDecoder decoder;
    parse_bad(decoder, std::string(kMaxPayloadBytes + 1, ' '));
    std::string exact_payload = "{\"ticks\":[]}";
    exact_payload.resize(kMaxPayloadBytes, ' ');
    CHECK(parse_ok(decoder, exact_payload).empty());
    const std::string empty_tick = "{\"symbol\":\"\",\"time\":0,\"quoter\":{}}";
    const std::string tick_prefix = "{\"symbol\":\"";
    const std::string tick_suffix = "\",\"time\":0,\"quoter\":{}}";
    const auto exact_tick = tick_prefix + std::string(kMaxRecordBytes - empty_tick.size(), 'x') + tick_suffix;
    REQUIRE(exact_tick.size() == kMaxRecordBytes);
    CHECK(parse_ok(decoder, "{\"ticks\":[" + exact_tick + " \n," + empty_tick + "]}").size() == 2);
    parse_bad(decoder, "{\"ticks\":[" + tick_prefix + std::string(kMaxRecordBytes - empty_tick.size() + 1, 'x') + tick_suffix + "]}");
    parse_bad(decoder, "{\"ticks\":[{\"symbol\":\"" + std::string(kMaxRecordBytes, 'x') + "\",\"time\":0,\"quoter\":{}}]}");
    // Small binary record, excessive raw JSON due to insignificant decimal zeros.
    parse_bad(decoder, wrap("{\"lastPrice\":1." + std::string(kMaxRecordBytes, '0') + "}"));
    std::vector<Record> records(kMaxBatchRecords);
    const auto json = records_json(records);
    CHECK(parse_ok(decoder, json) == records);
    auto excessive = json;
    excessive.insert(excessive.size() - 2, ",{\"symbol\":\"\",\"time\":0,\"quoter\":{}}");
    parse_bad(decoder, excessive);
    records.emplace_back();
    CHECK_THROWS_AS(records_json(records), std::invalid_argument);
    Record oversized_json;
    oversized_json.symbol.assign(12000, '\0');
    CHECK(encode_record(oversized_json).size() < kMaxRecordBytes);
    CHECK_THROWS_AS(records_json({oversized_json}), std::invalid_argument);
    Record long_symbol;
    long_symbol.symbol.assign(33000, 'x');
    CHECK_THROWS_AS(records_json(std::vector<Record>(128, long_symbol)), std::invalid_argument);
}

TEST_CASE("bench record JSON quote escapes every control and preserves scalar UTF8") {
    JsonDecoder decoder;
    std::string symbol;
    for (unsigned n = 0; n < 32; ++n) symbol += static_cast<char>(n);
    symbol += "\"\\/浦发\x7f\xc2\x80\xe0\xa0\x80\xf4\x8f\xbf\xbf";
    const auto quoted = json_quote(symbol);
    CHECK(quoted.find("\\u0000") != std::string::npos);
    CHECK(quoted.find("\\u001f") != std::string::npos);
    CHECK(quoted.find("\\\"") != std::string::npos);
    CHECK(quoted.find("\\\\") != std::string::npos);
    CHECK(quoted.find("\\n") != std::string::npos);
    const auto records = parse_ok(decoder, "{\"ticks\":[{\"symbol\":" + quoted + ",\"time\":0,\"quoter\":{}}]}");
    CHECK(records[0].symbol == symbol);
    Record out;
    std::string error;
    REQUIRE(decode_record(encode_record(records[0]), out, error));
    CHECK(out.symbol == symbol);
    for (const std::string invalid_utf8 : {
             "\x80", "\xc0\x80", "\xc1\xbf", "\xe0\x80\x80", "\xed\xa0\x80", "\xf0\x80\x80\x80",
             "\xf4\x90\x80\x80", "\xf5\x80\x80\x80", "\xe4\xb8", "\xe4\x41\x80", "\xff"}) {
        CHECK_THROWS_AS(json_quote(invalid_utf8), std::invalid_argument);
        Record invalid_record;
        invalid_record.symbol = invalid_utf8;
        CHECK_THROWS_AS(encode_record(invalid_record), std::invalid_argument);
        parse_bad(decoder, "{\"ticks\":[{\"symbol\":\"" + invalid_utf8 + "\",\"time\":0,\"quoter\":{}}]}");
    }
}

TEST_CASE("bench record generator matches frozen independent formula samples") {
    const auto record = generate_record(123, {54, 5, 42, false});
    CHECK(record.symbol == "600015.SH");
    CHECK(record.time == 1700000002000ULL);
    CHECK(record.present == (all_fields & ~(1U << 12)));
    CHECK(record.nulls == 0);
    const std::array<std::int64_t, kScalarCount> expected = {
        100299, 26579235, 265, 264, 65, 0, 100297, 100289, 100319,
        100279, 100296, 100298, 0};
    CHECK(record.values == expected);
    CHECK(record.book[0] == std::vector<std::int64_t>{100300, 100301, 100302, 100303, 100304});
    CHECK(record.book[1] == std::vector<std::int64_t>{100298, 100297, 100296, 100295, 100294});
    CHECK(record.book[2] == std::vector<std::int64_t>{1068, 1069, 1070, 1071, 1072});
    CHECK(record.book[3] == std::vector<std::int64_t>{2076, 2077, 2078, 2079, 2080});
    JsonDecoder decoder;
    for (std::uint64_t id = 0; id < 120; ++id) {
        const auto generated = generate_record(id, {7, 10, 99, true});
        CHECK((generated.present & (1U << 12)) != 0);
        CHECK(generated.values[12] == static_cast<std::int64_t>((id + 99) % 10000));
        if (id % 2) {
            CHECK((generated.present & (1U << 4)) == 0);
            CHECK((generated.nulls & (1U << 4)) == 0);
            CHECK(generated.values[4] == 0);
        } else if (id % 3 == 0) {
            CHECK((generated.present & (1U << 4)) != 0);
            CHECK((generated.nulls & (1U << 4)) != 0);
            CHECK(generated.values[4] == 0);
        } else {
            CHECK((generated.nulls & (1U << 4)) == 0);
            CHECK(generated.values[4] == static_cast<std::int64_t>((id + 99) % 100));
        }
        CHECK(parse_ok(decoder, records_json({generated}))[0] == generated);
    }
    CHECK(generate_record(0, {}).values[0] == 100042);
    CHECK(generate_record(0, {}).values[1] == 14205964);
    CHECK_THROWS_AS(generate_record(0, {0, 5, 42, false}), std::invalid_argument);
    CHECK_THROWS_AS(generate_record(0, {54, 11, 42, false}), std::invalid_argument);
    CHECK_THROWS_AS(generate_record(u64_max, {}), std::invalid_argument);
    CHECK_THROWS_AS(generate_record(u64_max, {1, 0, 0, false}), std::invalid_argument);
}

TEST_CASE("bench record duplicate sequence mapping is exact without UINT64 overflow") {
    const std::vector<std::uint64_t> expected = {0, 1, 1, 2, 3, 3, 4, 5, 5};
    for (std::uint64_t s = 0; s < expected.size(); ++s) {
        CHECK(logical_id(s, 0) == s);
        CHECK(logical_id(s, 3) == expected[static_cast<std::size_t>(s)]);
    }
    CHECK_THROWS_AS(logical_id(0, 1), std::invalid_argument);
    CHECK(logical_id(u64_max, 0) == u64_max);
    CHECK(logical_id(u64_max, 2) == 9223372036854775807ULL);
    CHECK(logical_id(u64_max, 3) == 12297829382473034410ULL);
}

TEST_CASE("bench record encoded decoder shares strict boundaries and owns canonical bytes") {
    check_batch_decoders("");
    std::vector<Record> records;
    for (std::uint32_t depth = 0; depth <= 10; ++depth) {
        auto record = generate_record(depth, {54, depth, 42, true});
        record.symbol = std::string("A\0浦发", 8);
        record.time = depth % 2 ? u64_max : 0;
        record.values[0] = i64_min;
        record.values[1] = i64_max;
        records.push_back(std::move(record));
    }
    auto payload = encode_records(records);
    check_batch_decoders(payload);
    std::vector<EncodedRecord> encoded;
    std::string error;
    REQUIRE(decode_encoded_records(payload, encoded, error));
    const auto capacity = encoded.capacity();
    const auto expected = encoded[0].bytes;
    payload.assign(payload.size(), 'x'); // Neither symbol nor bytes can be views.
    CHECK(encoded[0].symbol == records[0].symbol);
    CHECK(encoded[0].bytes == expected);
    encoded[0].has_sha256 = true;
    encoded[0].sha256.fill(0xff);
    encoded[0].ingress_ns = 999;
    REQUIRE(decode_encoded_records(encode_records({Record{}}), encoded, error));
    CHECK(encoded.capacity() == capacity);
    REQUIRE(encoded.size() == 1);
    CHECK_FALSE(encoded[0].has_sha256);
    CHECK(encoded[0].sha256 == std::array<std::uint8_t, 32>{});
    CHECK(encoded[0].ingress_ns == 0);
    CHECK_FALSE(decode_encoded_records(encode_records(records) + "x", encoded, error));
    CHECK(encoded.empty());
    REQUIRE(decode_encoded_records("", encoded, error));
    CHECK(encoded.empty());
    CHECK(error.empty());
}

TEST_CASE("bench record canonical decoders agree on deterministic mutations and random bytes") {
    std::uint64_t random = 0x4f7d903a20b18e65ULL;
    const auto next = [&] {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        return random;
    };
    // Differential checks include every truncation, every byte position, and
    // arbitrary masks, UTF8, uint64/i64 bit patterns, lengths and book counts.
    for (std::uint32_t depth : {0U, 1U, 5U, 10U}) {
        auto record = generate_record(18 + depth, {54, depth, 42, true});
        record.symbol = std::string("\0A🚀", 6);
        record.time = u64_max;
        const auto valid = encode_records({record});
        check_batch_decoders(valid);
        for (std::size_t n = 0; n < valid.size(); ++n) {
            check_batch_decoders(std::string_view(valid).substr(0, n));
            auto mutated = valid;
            mutated[n] ^= static_cast<char>(0xff);
            check_batch_decoders(mutated);
        }
        for (unsigned iteration = 0; iteration < 400; ++iteration) {
            auto mutated = valid;
            for (unsigned changes = 0; changes < 1 + iteration % 4; ++changes)
                mutated[next() % mutated.size()] = static_cast<char>(next());
            check_batch_decoders(mutated);
        }
    }
    for (unsigned iteration = 0; iteration < 400; ++iteration) {
        std::string bytes(next() % 256, '\0');
        for (auto& byte : bytes) byte = static_cast<char>(next());
        std::string framed(4, '\0');
        set_le(framed, 0, bytes.size(), 4);
        framed += bytes;
        check_batch_decoders(framed);
    }
}

}  // namespace
