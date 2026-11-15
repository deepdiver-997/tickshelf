#pragma once
// Clean-room benchmark record; not TTAP and not a claimed iQuant wire format.
#include <array>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace tickshelf::bench {

inline constexpr std::uint64_t kTimeBase = 1700000000000ULL;
inline constexpr std::size_t kMaxRecordBytes = 65536;
inline constexpr std::size_t kMaxPayloadBytes = 4 * 1024 * 1024;
inline constexpr std::size_t kMaxBatchRecords = 8192;
inline constexpr std::size_t kScalarCount = 13;
// Prices and amount use 1e-4; the other scalars are integers. pvolumn is
// the spelling in the Guosen manual; pvolume is an accepted, exclusive alias.
inline constexpr std::array<std::string_view, kScalarCount> kScalarNames = {
    "lastPrice", "amount", "volume", "pvolumn", "openInt", "stockStatus",
    "lastSettlementPrice", "open", "high", "low", "settlementPrice", "lastClose", "transactionNum"};
inline constexpr std::array<bool, kScalarCount> kScaled = {
    true, true, false, false, false, false, true, true, true, true, true, true, false};
inline constexpr std::array<std::string_view, 4> kBookNames = {"askPrice", "bidPrice", "askVol", "bidVol"};

struct Record {
    std::string symbol;
    std::uint64_t time = 0;
    // Bits 0..12: scalars; 13..16: book arrays. null is distinct from absent.
    std::uint32_t present = 0;
    std::uint32_t nulls = 0;
    std::array<std::int64_t, kScalarCount> values{};
    std::array<std::vector<std::int64_t>, 4> book;
    bool operator==(const Record&) const = default;
};

struct EncodedRecord {
    std::string symbol;
    std::uint64_t time = 0;
    std::string bytes;
    // In-process metadata only, never serialized or hashed as business content.
    // sha256 is valid only while bytes are unchanged; private pipeline producers
    // set this flag after hashing or exact Redis payload verification.
    std::array<std::uint8_t, 32> sha256{};
    bool has_sha256 = false;
    std::uint64_t ingress_ns = 0;
};

std::string encode_record(const Record& record);
bool decode_record(std::string_view bytes, Record& out, std::string& error);
std::string encode_records(const std::vector<Record>& records);
bool decode_records(std::string_view payload, std::vector<Record>& out, std::string& error);
// Strictly validate the complete canonical record while retaining its bytes.
// In-process metadata is left at its default (not yet hashed / no ingress time).
bool decode_encoded_records(std::string_view payload, std::vector<EncodedRecord>& out,
                            std::string& error);

class JsonDecoder {
public:
    JsonDecoder();
    ~JsonDecoder();
    JsonDecoder(const JsonDecoder&) = delete;
    JsonDecoder& operator=(const JsonDecoder&) = delete;
    // {"ticks":[{"symbol":"600000.SH","time":...,"quoter":{...}}]}
    // Entire batch validation is all-or-nothing. Unknown fields are rejected.
    bool parse(std::string_view json, std::vector<Record>& out, std::string& error);
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

struct GeneratorOptions {
    std::uint32_t symbols = 54;
    std::uint32_t depth = 5;
    std::uint64_t seed = 42;
    bool optional_fields = false;
};
// Physical sequence s maps to s-(s+1)/duplicate_every (0 disables, 1 invalid).
std::uint64_t logical_id(std::uint64_t sequence, std::uint32_t duplicate_every);
// Formula: s=id%symbols, c=id/symbols, x=id+seed; symbol=600000+s + ".SH";
// time=kTimeBase+c*1000; last=100000+s*17+(c+seed)%10000 (1e-4).
// Scalars in order: last,last*(100+x%1000),100+x%1000,99+x%1000,
// x%100,x%3,last-2,last-10,last+20,last-20,last-3,last-1,x%10000.
// Book[j][l]: last+l+1,last-l-1,1000+x%97+l,2000+x%89+l.
// Default present: all except transactionNum; optional_fields: transactionNum
// present, and scalar openInt absent for odd ids, null for ids divisible by 3
// (absence wins). All other fields present, all four book arrays length depth.
// Binary v1: magic RCD1; u16 symbol size; symbol; u64 time; u32 present;
// u32 nulls; i64 value for each present non-null scalar in order; for each
// present non-null book: u8 count + count*i64. All numbers little-endian.
// Batch binary: repeated u32 record length + canonical record, no count prefix.
Record generate_record(std::uint64_t id, const GeneratorOptions& options);
std::string records_json(const std::vector<Record>& records);
std::string json_quote(std::string_view value);

}  // namespace tickshelf::bench
