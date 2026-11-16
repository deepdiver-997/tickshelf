#include "dedupe.hpp"

#include <algorithm>
#include <cstring>
#include <limits>

namespace tickshelf {
namespace {

constexpr std::uint64_t kFnvOffset = 14695981039346656037ull;
constexpr std::uint64_t kFnvPrime = 1099511628211ull;

std::size_t next_power_of_two(std::size_t value) {
    std::size_t n = 8;
    while (n < value) n <<= 1;
    return n;
}

}  // namespace

DedupeCache::DedupeCache(std::size_t max_bytes) {
    if (max_bytes < sizeof(Entry)) return;
    std::size_t capacity = max_bytes / sizeof(Entry);
    capacity = std::min<std::size_t>(capacity, 1u << 30);  // table 用 int32 下标
    if (capacity == 0) return;
    capacity_ = capacity;
    ring_.resize(capacity_);
    table_.assign(next_power_of_two(capacity_ * 2), -1);
    mask_ = table_.size() - 1;
}

std::size_t DedupeCache::entry_size() { return sizeof(Entry); }

std::uint64_t DedupeCache::hash_tick(const Tick& tick) {
    const auto* bytes = reinterpret_cast<const unsigned char*>(&tick);
    std::uint64_t h = kFnvOffset;
    for (std::size_t i = 0; i < sizeof(Tick); ++i) {
        h ^= bytes[i];
        h *= kFnvPrime;
    }
    return h;
}

bool DedupeCache::check_and_insert(const Tick& tick) {
    if (capacity_ == 0) return false;
    const std::uint64_t hash = hash_tick(tick);
    std::size_t slot = static_cast<std::size_t>(hash) & mask_;
    std::size_t insert_at = table_.size();  // 未找到空位/墓碑
    for (;;) {
        const std::int32_t value = table_[slot];
        if (value == -1) {
            if (insert_at == table_.size()) insert_at = slot;
            break;
        }
        if (value == -2) {
            if (insert_at == table_.size()) insert_at = slot;
        } else {
            const Entry& entry = ring_[static_cast<std::size_t>(value)];
            if (entry.hash == hash && std::memcmp(&entry.tick, &tick, sizeof(Tick)) == 0) {
                ++dropped_;
                return true;
            }
        }
        slot = (slot + 1) & mask_;
    }

    if (insert_at == table_.size()) {  // 理论上不可达：表容量 ≥ 2×entries 且 tombstone 受控
        rebuild();
        return check_and_insert(tick);
    }
    if (size_ == capacity_) evict_oldest();

    const bool replacing_tombstone = table_[insert_at] == -2;
    const std::size_t ring_index = (head_ + size_) % capacity_;
    ring_[ring_index] = Entry{tick, hash};
    table_[insert_at] = static_cast<std::int32_t>(ring_index);
    if (replacing_tombstone) --tombs_;
    ++size_;

    // tombstone 过多会拖慢探测；按容量比例重建一次，摊薄成本。
    if (tombs_ > capacity_ / 2 + 1) rebuild();
    return false;
}

void DedupeCache::evict_oldest() {
    if (size_ == 0) return;
    const Entry& entry = ring_[head_];
    std::size_t slot = static_cast<std::size_t>(entry.hash) & mask_;
    bool found = false;
    for (;;) {
        const std::int32_t value = table_[slot];
        if (value == -1) break;
        if (value == static_cast<std::int32_t>(head_)) {
            table_[slot] = -2;
            ++tombs_;
            found = true;
            break;
        }
        slot = (slot + 1) & mask_;
    }
    if (!found) {
        // 表与 ring 由本类独占维护；走到这里说明内部不变量已被破坏。
        // 先把被淘汰项从 ring 逻辑上移除，再从剩余有效项重建表，避免留下悬空下标。
        head_ = (head_ + 1) % capacity_;
        --size_;
        rebuild();
        return;
    }
    head_ = (head_ + 1) % capacity_;
    --size_;
}

void DedupeCache::rebuild() {
    std::fill(table_.begin(), table_.end(), -1);
    tombs_ = 0;
    for (std::size_t i = 0; i < size_; ++i) {
        const std::size_t ring_index = (head_ + i) % capacity_;
        std::size_t slot = static_cast<std::size_t>(ring_[ring_index].hash) & mask_;
        while (table_[slot] != -1) slot = (slot + 1) & mask_;
        table_[slot] = static_cast<std::int32_t>(ring_index);
    }
}

}  // namespace tickshelf
