#include <doctest/doctest.h>

#include <cstring>
#include <string>
#include <vector>

#include "common/civil.hpp"
#include "libtickshelf/ingestor.hpp"
#include "libtickshelf/reader.hpp"
#include "libtickshelf/sqlite_store.hpp"

using namespace tickshelf;

namespace {

Tick make_tick(const char* sym, std::uint64_t ts) {
    Tick t{};
    const std::size_t n = std::strlen(sym);
    std::memcpy(t.symbol, sym, n < 8 ? n : 8);
    t.ts_exchange_ms = ts;
    t.ts_receive_ms = ts + 1;
    t.bid_price = 10005000;
    return t;
}

bool require_redis(RespClient& rc) {
    if (!rc.connect("127.0.0.1", 6379)) {
        MESSAGE("SKIP: redis-server 不可达，跳过集成用例");
        return false;
    }
    std::vector<RespValue> r;
    if (!rc.exec({{"PING"}}, r) || r.empty() || r[0].str != "PONG") {
        MESSAGE("SKIP: redis-server 不响应 PING，跳过集成用例");
        return false;
    }
    return true;
}

void seed_hot(RespClient& rc, const char* sym, const std::vector<std::uint64_t>& tss) {
    Ingestor ing;
    for (std::uint64_t ts : tss) ing.ingest(make_tick(sym, ts));
    auto batches = ing.drain();
    for (const Batch& b : batches) {
        std::vector<Batch> one;
        one.push_back(b);
        REQUIRE(flush_batches(rc, one));
    }
}

bool key_exists(RespClient& rc, const std::string& key) {
    std::vector<RespValue> r;
    REQUIRE(rc.exec({{"EXISTS", key}}, r));
    return r[0].integer > 0;
}

Tick decode_at(const QueryResult& res, std::size_t i) {
    return decode_tick(reinterpret_cast<const std::uint8_t*>(res.payload.data()) +
                       i * kTickBytes);
}

}  // namespace

TEST_CASE("RangeReader 集成: 段式合并读/缺口检测/回补触发（需 redis-server）") {
    RespClient rc;
    if (!require_redis(rc)) return;
    std::vector<RespValue> sink;
    rc.exec({{"SELECT", "15"}}, sink);
    rc.exec({{"FLUSHDB"}}, sink);

    SqliteTickStore store;
    REQUIRE(store.open(":memory:"));

    SUBCASE("热+耐久合并去重：同 tick 两边同字节只算一份") {
        // ts=100,101 两边都有；102 只在 SQLite；103 只在热侧（段）
        seed_hot(rc, "600000", {100, 101, 103});
        std::vector<Tick> durable;
        durable.push_back(make_tick("600000", 100));
        durable.push_back(make_tick("600000", 101));
        durable.push_back(make_tick("600000", 102));
        REQUIRE(store.upsert_batch(durable) == 3);

        RangeReader reader(rc, store);
        QueryResult res;
        REQUIRE(reader.read("600000", 100, 103, res));
        REQUIRE(res.tick_count() == 4);  // 100..103 无缺口
        CHECK(res.missing.empty());
        CHECK(res.ts[2] == 102);  // 102 来自耐久侧
        CHECK(res.ts[3] == 103);  // 103 来自热侧（段字节 verbatim）
        const Tick t103 = decode_at(res, 3);
        CHECK(t103.bid_price == 10005000);  // 段字节完整可解码
    }

    SUBCASE("缺口检测 + 头尾半开区间 + 回补键触发") {
        seed_hot(rc, "600000", {100, 101, 104, 107});  // 缺 102-103 和 105-106

        RangeReader reader(rc, store);
        QueryResult res;
        REQUIRE(reader.read("600000", 100, 110, res));
        REQUIRE(res.missing.size() == 3);
        CHECK(res.missing[0].first == 102);
        CHECK(res.missing[0].second == 103);
        CHECK(res.missing[1].first == 105);
        CHECK(res.missing[1].second == 106);
        CHECK(res.missing[2].first == 108);
        CHECK(res.missing[2].second == 110);
        REQUIRE(res.backfill_requested.size() == 3);
        CHECK(key_exists(rc, "backfill:600000:102-103"));

        // 重复查询：requested 已存在 → 不覆盖、不重复计入 requested
        QueryResult res2;
        REQUIRE(reader.read("600000", 102, 103, res2));
        CHECK(res2.backfill_requested.empty());
        std::vector<RespValue> r;
        REQUIRE(rc.exec({{"GET", "backfill:600000:102-103"}}, r));
        CHECK(r[0].str == "requested");
    }

    SUBCASE("空区间：整体即缺口") {
        RangeReader reader(rc, store);
        QueryResult res;
        REQUIRE(reader.read("600000", 500, 510, res));
        REQUIRE(res.missing.size() == 1);
        CHECK(res.missing[0].first == 500);
        CHECK(res.missing[0].second == 510);
        CHECK(res.tick_count() == 0);
    }

    SUBCASE("跨日界：两个日期的段联合读") {
        const std::uint64_t d2 = static_cast<std::uint64_t>(days_from_civil(2026, 11, 10)) * kMsPerDay;
        seed_hot(rc, "600001", {d2 - 1, d2});  // 日界两侧各一 tick（各成一段）
        RangeReader reader(rc, store);
        QueryResult res;
        REQUIRE(reader.read("600001", d2 - 1, d2, res));
        REQUIRE(res.tick_count() == 2);
        CHECK(res.missing.empty());
        CHECK(res.ts[0] == d2 - 1);
        CHECK(res.ts[1] == d2);
    }

    SUBCASE("read_latest: HGET 投影快速路径") {
        bool found = false;
        Tick t{};
        REQUIRE(read_latest(rc, "600000", found, t));
        CHECK_FALSE(found);  // 尚无投影

        Ingestor ing;
        ing.ingest(make_tick("600000", 1000));
        ing.ingest(make_tick("600000", 1002));
        REQUIRE(flush_batches(rc, ing.drain()));
        REQUIRE(read_latest(rc, "600000", found, t));
        CHECK(found);
        CHECK(t.ts_exchange_ms == 1002);  // 窗口内最新

        REQUIRE(read_latest(rc, "999999", found, t));
        CHECK_FALSE(found);  // 不存在的 symbol：found=false 而非连接错误
    }

    rc.exec({{"SELECT", "0"}}, sink);
}
