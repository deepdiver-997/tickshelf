#include <doctest/doctest.h>

#include <cstring>
#include <string>

#include "components/engine/dedupe.hpp"

using namespace tickshelf;

namespace {

Tick make_tick(const char* symbol, std::uint64_t ts, std::int64_t bid = 10005000) {
    Tick tick{};
    const std::size_t n = std::strlen(symbol);
    std::memcpy(tick.symbol, symbol, n < sizeof(tick.symbol) ? n : sizeof(tick.symbol));
    tick.ts_exchange_ms = ts;
    tick.ts_receive_ms = ts + 1;
    tick.bid_price = bid;
    return tick;
}

}  // namespace

TEST_CASE("DedupeCache: max_bytes=0 时完全关闭") {
    DedupeCache cache(0);
    CHECK_FALSE(cache.enabled());
    CHECK_FALSE(cache.check_and_insert(make_tick("A", 1)));
    CHECK(cache.entries() == 0);
    CHECK(cache.dropped() == 0);
}

TEST_CASE("DedupeCache: 完整 64B 比较，同 symbol/time 不同内容不算重复") {
    DedupeCache cache(2 * DedupeCache::entry_size());
    REQUIRE(cache.capacity() == 2);

    const Tick first = make_tick("A", 1, 100);
    const Tick same = first;
    const Tick changed = make_tick("A", 1, 200);

    CHECK_FALSE(cache.check_and_insert(first));
    CHECK(cache.check_and_insert(same));
    CHECK(cache.dropped() == 1);
    CHECK_FALSE(cache.check_and_insert(changed));  // 全字节不同：保留
    CHECK(cache.entries() == 2);
}

TEST_CASE("DedupeCache: FIFO 驱逐后再次到达按新记录处理") {
    DedupeCache cache(2 * DedupeCache::entry_size());
    REQUIRE(cache.capacity() == 2);

    const Tick a = make_tick("A", 1);
    const Tick b = make_tick("B", 2);
    const Tick c = make_tick("C", 3);

    CHECK_FALSE(cache.check_and_insert(a));
    CHECK_FALSE(cache.check_and_insert(b));
    CHECK(cache.check_and_insert(a));  // 命中不刷新位置
    CHECK_FALSE(cache.check_and_insert(c));  // 驱逐 a
    CHECK(cache.entries() == 2);
    CHECK_FALSE(cache.check_and_insert(a));  // 已驱逐：重新插入，驱逐 b
    CHECK(cache.check_and_insert(c));        // c 仍在窗口
    CHECK_FALSE(cache.check_and_insert(b));  // b 已被驱逐
}

TEST_CASE("DedupeCache: 大量条目下窗口内命中、窗口外重收") {
    constexpr std::size_t kCapacity = 64;
    DedupeCache cache(kCapacity * DedupeCache::entry_size());
    REQUIRE(cache.capacity() == kCapacity);

    for (std::size_t i = 0; i < 1000; ++i) {
        const std::string symbol = "S" + std::to_string(i);
        CHECK_FALSE(cache.check_and_insert(make_tick(symbol.c_str(), i + 1)));
    }
    CHECK(cache.entries() == kCapacity);

    // 最近 kCapacity 条仍在窗口内。
    for (std::size_t i = 1000 - kCapacity; i < 1000; ++i) {
        const std::string symbol = "S" + std::to_string(i);
        CHECK(cache.check_and_insert(make_tick(symbol.c_str(), i + 1)));
    }

    // 最早的条目已被驱逐，按新记录收下（会反过来驱逐当前窗口内的条目）。
    for (std::size_t i = 0; i < 100; ++i) {
        const std::string symbol = "S" + std::to_string(i);
        CHECK_FALSE(cache.check_and_insert(make_tick(symbol.c_str(), i + 1)));
    }
}
