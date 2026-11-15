#include "hash.hpp"
#include <algorithm>

namespace tickshelf::bench {
Deduper::Deduper(const Config& config, Metrics& metrics, DigestOverride test_digest,
                 std::size_t test_cache_max_bytes)
    : config_(config), metrics_(metrics), test_digest_(test_digest), max_bytes_(test_cache_max_bytes) {
    if (!test_digest_ && config.hash != "off" && config.hash_algorithm == "sha256" &&
        config.hash_mode == "batch") {
        context_ = EVP_MD_CTX_new();
        if (context_) metrics_.inc("hash_context_allocations");
    }
}

std::size_t Deduper::DigestHash::operator()(const Digest& digest) const {
    const auto bytes = std::string_view(reinterpret_cast<const char*>(digest.bytes.data()), digest.bytes.size());
    return static_cast<std::size_t>((fnv_extend(kFnvOffset, bytes) ^ digest.size) * 1099511628211ULL);
}

bool Deduper::digest(std::string_view bytes, Digest& result, std::uint64_t* checksum,
                     std::uint64_t& context_allocations) {
    result = {};
    if (test_digest_) {
        override_scratch_.clear();
        if (!test_digest_(bytes, override_scratch_)) return false;
        if (checksum) *checksum = fnv_extend(*checksum, override_scratch_);
        if (override_scratch_.size() <= result.bytes.size()) {
            result.size = static_cast<std::uint8_t>(override_scratch_.size());
            std::copy(override_scratch_.begin(), override_scratch_.end(), result.bytes.begin());
            return true;
        }
        // Keep the pre-existing opaque override interface, including arbitrary
        // length outputs, without allocating a string for each cache key.
        // These are never marked as a real SHA of the canonical record.
        if (!context_) {
            context_ = EVP_MD_CTX_new();
            if (context_) ++context_allocations;
        }
        unsigned length = 0;
        if (!context_ || EVP_DigestInit_ex(context_, EVP_sha256(), nullptr) != 1 ||
            EVP_DigestUpdate(context_, override_scratch_.data(), override_scratch_.size()) != 1 ||
            EVP_DigestFinal_ex(context_, result.bytes.data(), &length) != 1 ||
            length != result.bytes.size()) return false;
        result.size = 33;
        return true;
    }
    if (config_.hash_algorithm == "fnv1a64") {
        const auto value = fnv_extend(kFnvOffset, bytes);
        result.size = 8;
        for (unsigned i = 0; i < 8; ++i) result.bytes[i] = static_cast<std::uint8_t>(value >> (i * 8));
    } else {
        EVP_MD_CTX* ctx = context_;
        if (config_.hash_mode == "record") {
            ctx = EVP_MD_CTX_new();
            if (ctx) ++context_allocations;
        }
        if (!ctx) return false;
        unsigned length = 0;
        const bool ok = EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) == 1 &&
            EVP_DigestUpdate(ctx, bytes.data(), bytes.size()) == 1 &&
            EVP_DigestFinal_ex(ctx, result.bytes.data(), &length) == 1 && length == result.bytes.size();
        if (config_.hash_mode == "record") EVP_MD_CTX_free(ctx);
        if (!ok) return false;
        result.size = 32;
    }
    if (checksum)
        *checksum = fnv_extend(*checksum, std::string_view(reinterpret_cast<const char*>(result.bytes.data()), result.size));
    return true;
}

std::size_t Deduper::fixed_charge_bytes() {
    // Charge an index key per entry even when collision chains share a map key.
    return sizeof(Entry) + sizeof(Digest) + 4 * sizeof(void*);
}

std::size_t Deduper::entry_bytes(const Entry& entry) {
    return entry.bytes.size() + fixed_charge_bytes();
}

void Deduper::evict() {
    while (fifo_.size() > config_.dedup_max_entries || cache_bytes_ > max_bytes_) {
        auto& oldest = fifo_.front();
        // Never retain unordered_map iterators across insertion/rehash.
        const auto bucket = seen_.find(oldest.digest);
        if (oldest.previous) oldest.previous->next = oldest.next;
        else if (oldest.next) bucket->second = oldest.next;
        else seen_.erase(bucket);
        if (oldest.next) oldest.next->previous = oldest.previous;
        payload_bytes_ -= oldest.bytes.size();
        cache_bytes_ -= entry_bytes(oldest);
        fifo_.pop_front(); // Destroy in place, never copy the long payload.
        metrics_.inc("dedup_evictions");
    }
}

bool Deduper::apply(std::vector<EncodedRecord>& records, std::string& error) {
    error.clear();
    if (config_.hash == "off") {
        for (auto& record : records) record.has_sha256 = false;
        return true;
    }
    digests_.resize(records.size());
    std::uint64_t checksum = kFnvOffset;
    const bool checksum_only = config_.hash == "only";
    {
        Timer timer(metrics_, "hash_compute");
        std::uint64_t bytes = 0, computed = 0, allocations = 0;
        bool ok = true;
        for (std::size_t i = 0; i < records.size(); ++i) {
            if (!digest(records[i].bytes, digests_[i], checksum_only ? &checksum : nullptr, allocations)) {
                ok = false;
                break;
            }
            bytes += records[i].bytes.size();
            ++computed;
        }
        // Count completed work even when a later digest fails. No records,
        // metadata or cache entries have been changed at this point.
        metrics_.inc("hash_input_bytes", bytes);
        metrics_.inc("hash_context_allocations", allocations);
        metrics_.inc("hash_invocations", computed);
        metrics_.inc("hash_api_calls", computed);
        if (!ok) { error = "hash computation failed"; return false; }
        metrics_.inc("hash_compute_batches");
    }
    const bool real_sha256 = !test_digest_ && config_.hash_algorithm == "sha256";
    const auto cache_sha256 = [&](EncodedRecord& record, const Digest& value) {
        record.has_sha256 = real_sha256;
        if (real_sha256) record.sha256 = value.bytes;
    };
    if (checksum_only) {
        for (std::size_t i = 0; i < records.size(); ++i) cache_sha256(records[i], digests_[i]);
        metrics_.counters["hash_digest_checksum"] ^= checksum;
        return true;
    }
    Timer timer(metrics_, "dedup_lookup");
    std::size_t output = 0;
    for (std::size_t i = 0; i < records.size(); ++i) {
        auto found = seen_.find(digests_[i]);
        bool duplicate = false;
        if (found != seen_.end()) {
            for (auto* entry = found->second; entry; entry = entry->next) {
                if (entry->bytes == records[i].bytes) { duplicate = true; break; }
            }
        }
        if (duplicate) { metrics_.inc("dedup_dropped"); continue; }
        fifo_.push_back({digests_[i], records[i].bytes});
        auto* entry = &fifo_.back();
        // try_emplace can rehash; only its newly returned iterator is used.
        const auto bucket = seen_.try_emplace(entry->digest, nullptr).first;
        entry->next = bucket->second;
        if (entry->next) entry->next->previous = entry;
        bucket->second = entry;
        payload_bytes_ += entry->bytes.size();
        cache_bytes_ += entry_bytes(*entry);
        evict();
        cache_sha256(records[i], digests_[i]);
        if (output != i) records[output] = std::move(records[i]);
        ++output;
    }
    records.resize(output); // Stable in-place compaction; retain vector capacity.
    metrics_.counters["dedup_entries"] = fifo_.size();
    // The old implementation counted two canonical copies here. There is now
    // exactly one cache-owned canonical string; fixed overhead is separate.
    metrics_.counters["dedup_payload_bytes"] = payload_bytes_;
    metrics_.counters["dedup_cache_bytes"] = cache_bytes_;
    return true;
}
}  // namespace tickshelf::bench
