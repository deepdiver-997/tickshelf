#pragma once
// 可选的有界 FIFO 去重缓存（默认关闭；每分片一个，仅源线程访问，无锁）。
//
// 键 = 完整 64B Tick（含 ts_receive_ms；_pad 在 encode/decode 中恒 0）。
// 哈希只用于定位，命中后必须 memcmp 全 64B——碰撞不会误判重复。
// 重复不刷新 FIFO 位置；驱逐后再次到达会被当作新记录（与唯一键兜底一致）。
//
// 注意：这不是生产唯一键的替代。SQLite PRIMARY KEY(symbol, ts_exchange_ms)
// 仍是最终收敛点；本缓存只是减少重复经过 Redis/存储的带宽。
#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/tick.hpp"

namespace tickshelf {

class DedupeCache {
public:
    explicit DedupeCache(std::size_t max_bytes);
    DedupeCache(const DedupeCache&) = delete;
    DedupeCache& operator=(const DedupeCache&) = delete;

    bool enabled() const { return capacity_ != 0; }
    // true = 已在窗口内（重复，未插入）；false = 新记录，已插入。
    bool check_and_insert(const Tick& tick);
    // 单条 Entry 的字节数（预算换算；测试用来构造精确容量）。
    static std::size_t entry_size();

    std::size_t entries() const { return size_; }
    std::size_t capacity() const { return capacity_; }
    std::uint64_t dropped() const { return dropped_; }

private:
    struct Entry {
        Tick tick;
        std::uint64_t hash;
    };

    static std::uint64_t hash_tick(const Tick& tick);
    void evict_oldest();
    void rebuild();

    std::vector<Entry> ring_;
    std::vector<std::int32_t> table_;  // ring 下标；-1 空，-2 tombstone
    std::size_t mask_ = 0;
    std::size_t capacity_ = 0;
    std::size_t head_ = 0;
    std::size_t size_ = 0;
    std::size_t tombs_ = 0;
    std::uint64_t dropped_ = 0;
};

}  // namespace tickshelf
