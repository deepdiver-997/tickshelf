#include <doctest/doctest.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <string>
#include <vector>

#include "components/storage/shard_route.hpp"
#include "components/storage/sharded_store.hpp"
#include "components/storage/sqlite_store.hpp"

using namespace tickshelf;

namespace {

Tick make_tick(const char* symbol, std::uint64_t ts) {
    Tick tick{};
    const std::size_t n = std::strlen(symbol);
    std::memcpy(tick.symbol, symbol, n < sizeof(tick.symbol) ? n : sizeof(tick.symbol));
    tick.ts_exchange_ms = ts;
    tick.ts_receive_ms = ts + 1;
    tick.bid_price = 10005000;
    return tick;
}

struct TmpDir {
    std::string path;
    explicit TmpDir(const char* name)
        : path((std::filesystem::temp_directory_path() / name).string()) {
        std::filesystem::remove_all(path);
        std::filesystem::create_directories(path);
    }
    ~TmpDir() { std::filesystem::remove_all(path); }
};

}  // namespace

TEST_CASE("shard_route: 固定向量钉住非标准 FNV 变体") {
    // 这些常量由独立 Python 按 1469598103934665603 复算；任何"修正"都会改变磁盘布局。
    CHECK(symbol_hash("600000") == 5502609113162351133ull);
    CHECK(symbol_hash("000001") == 6011807077863851320ull);
    CHECK(symbol_hash("A") == 4953232625885864614ull);
    CHECK(symbol_hash("") == 1469598103934665603ull);
    CHECK(symbol_hash("600000.SH") == 4402894444697614258ull);

    CHECK(shard_of("600000", 4) == 1);
    CHECK(shard_of("000001", 4) == 0);
    CHECK(shard_of("A", 4) == 2);
    CHECK(shard_of("600000", 1) == 0);
    CHECK(shard_path("ticks.db", 0, 1) == "ticks.db");
    CHECK(shard_path("ticks.db", 2, 4) == "ticks.db.2");
}

TEST_CASE("shard_route: 与 ShardedTickStore 实际落盘分片逐符号一致") {
    TmpDir tmp("tickshelf_route_parity");
    const std::string base = tmp.path + "/ticks.db";
    constexpr std::size_t kShards = 4;

    ShardedTickStore sharded;
    sharded.set_shards(static_cast<int>(kShards));
    REQUIRE(sharded.open(base));

    std::vector<Tick> ticks = {make_tick("600000", 1000), make_tick("000001", 1001),
                               make_tick("A", 1002), make_tick("60000.SH", 1003)};
    REQUIRE(sharded.upsert_batch(ticks) == 4);

    for (const Tick& tick : ticks) {
        const std::string symbol(tick_symbol(tick));
        const std::size_t expected = shard_of(tick, kShards);
        for (std::size_t shard = 0; shard < kShards; ++shard) {
            SqliteTickStore reader;
            REQUIRE(reader.open(shard_path(base, shard, kShards)));
            const auto rows = reader.query_range(
                symbol, 0,
                static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()));
            INFO("symbol=" << symbol << " expected=" << expected << " shard=" << shard
                           << " rows=" << rows.size() << " count=" << reader.count()
                           << " failed=" << reader.query_failed());
            CHECK_FALSE(reader.query_failed());
            if (shard == expected)
                CHECK(rows.size() == 1);
            else
                CHECK(rows.empty());
        }
    }
}
