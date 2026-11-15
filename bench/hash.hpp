#pragma once
#include <array>
#include <cstdint>
#include <list>
#include <string>
#include <unordered_map>
#include <vector>
#include <openssl/evp.h>

#include "config.hpp"
#include "metrics.hpp"

namespace tickshelf::bench {
inline std::uint64_t fnv_extend(std::uint64_t state, std::string_view bytes) {
    for (unsigned char byte : bytes) state = (state ^ byte) * 1099511628211ULL;
    return state;
}
inline constexpr std::uint64_t kFnvOffset = 14695981039346656037ULL;

class Deduper {
public:
    using DigestOverride = bool (*)(std::string_view, std::string&);
    // Public accounting contract, shared by the runtime and independent oracle.
    // These charges bound the FIFO budget; they do not estimate allocator/RSS.
    static constexpr std::size_t fifo_limit_bytes() { return 256ULL * 1024 * 1024; }
    static std::size_t fixed_charge_bytes();
    // The fourth argument permits small byte-budget tests only.
    Deduper(const Config& config, Metrics& metrics, DigestOverride test_digest = nullptr,
            std::size_t test_cache_max_bytes = fifo_limit_bytes());
    ~Deduper() { EVP_MD_CTX_free(context_); }
    Deduper(const Deduper&) = delete;
    Deduper& operator=(const Deduper&) = delete;
    bool apply(std::vector<EncodedRecord>& records, std::string& error);
private:
    struct Digest {
        std::array<std::uint8_t, 32> bytes{};
        // 8 for FNV, 32 for SHA; 33 tags a normalized long test override.
        std::uint8_t size = 0;
        bool operator==(const Digest&) const = default;
    };
    struct DigestHash {
        std::size_t operator()(const Digest& digest) const;
    };
    struct Entry {
        Digest digest;
        std::string bytes;
        Entry* previous = nullptr;
        Entry* next = nullptr;
    };
    bool digest(std::string_view bytes, Digest& result, std::uint64_t* checksum,
                std::uint64_t& context_allocations);
    static std::size_t entry_bytes(const Entry& entry);
    void evict();
    const Config& config_;
    Metrics& metrics_;
    EVP_MD_CTX* context_ = nullptr;
    DigestOverride test_digest_;
    std::size_t max_bytes_;
    // list nodes are stable through map rehash and vector compaction. The FIFO
    // owns each canonical string once; digest buckets only link those nodes.
    std::list<Entry> fifo_;
    std::unordered_map<Digest, Entry*, DigestHash> seen_;
    std::vector<Digest> digests_; // Reused across apply calls, no N heap strings.
    std::string override_scratch_;
    std::size_t payload_bytes_ = 0;
    std::size_t cache_bytes_ = 0;
};
}  // namespace tickshelf::bench
