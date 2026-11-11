#include <doctest/doctest.h>

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "common/civil.hpp"
#include "libtickshelf/ingestor.hpp"
#include "libtickshelf/lease_persister.hpp"
#include "libtickshelf/resp_client.hpp"
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
    t.ask_price = 10008000;
    t.last_price = 10006000;
    t.bid_size = 10;
    t.ask_size = 20;
    t.last_size = 3;
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

// 端到端垫场：ingest → flush_batches 落 Redis（模拟 tickshelfd 侧产物）
void seed_batch(RespClient& rc, const char* sym, std::uint64_t ts_a, std::uint64_t ts_b) {
    Ingestor ing;
    ing.ingest(make_tick(sym, ts_a));
    if (ts_b != ts_a) ing.ingest(make_tick(sym, ts_b));
    auto batches = ing.drain();
    REQUIRE(batches.size() == 1);
    REQUIRE(flush_batches(rc, batches));
}

std::string state_value(RespClient& rc, const std::string& field) {
    std::vector<RespValue> r;
    REQUIRE(rc.exec({{"HGET", state_key(), field}}, r));
    return r[0].type == RespValue::Type::Bulk ? r[0].str : "<nil>";
}

LeasePersister::Options worker_opts(const char* name) {
    LeasePersister::Options o;
    o.worker_id = name;
    return o;
}

}  // namespace

TEST_CASE("批id解析: 四段结构") {
    std::string sym, date;
    std::uint64_t a = 0, b = 0;
    REQUIRE(LeasePersister::parse_batch_id("600000:20261108:1000-2000", sym, date, a, b));
    CHECK(sym == "600000");
    CHECK(date == "20261108");
    CHECK(a == 1000);
    CHECK(b == 2000);
    CHECK_FALSE(LeasePersister::parse_batch_id("600000:20261108", sym, date, a, b));
    CHECK_FALSE(LeasePersister::parse_batch_id(":20261108:1-2", sym, date, a, b));
}

TEST_CASE("SQLite 格式版本戳: 新库盖戳、不匹配拒绝打开（ADR-018）") {
    const char* path = "tickshelf_uv_probe.db";
    const std::string wal = std::string(path) + "-wal";
    const std::string shm = std::string(path) + "-shm";
    std::remove(path);
    std::remove(wal.c_str());
    std::remove(shm.c_str());

    {  // 首开：新库盖 user_version = kStoreFormatVersion
        SqliteTickStore store;
        REQUIRE(store.open(path));
    }

    // 裸 sqlite3 读回戳值
    sqlite3* raw = nullptr;
    REQUIRE(sqlite3_open(path, &raw) == SQLITE_OK);
    sqlite3_stmt* stmt = nullptr;
    REQUIRE(sqlite3_prepare_v2(raw, "PRAGMA user_version", -1, &stmt, nullptr) == SQLITE_OK);
    REQUIRE(sqlite3_step(stmt) == SQLITE_ROW);
    CHECK(sqlite3_column_int(stmt, 0) == kStoreFormatVersion);
    sqlite3_finalize(stmt);
    // 伪造"未来格式"的库
    REQUIRE(sqlite3_exec(raw, "PRAGMA user_version = 99", nullptr, nullptr, nullptr) == SQLITE_OK);
    sqlite3_close(raw);

    {  // 旧二进制遇新格式库：响亮失败，不静默拿旧字段偏移误解码
        SqliteTickStore store;
        CHECK_FALSE(store.open(path));
    }

    std::remove(path);
    std::remove(wal.c_str());
    std::remove(shm.c_str());
}

TEST_CASE("租约落盘集成: 幂等/抢占/重做（需 redis-server）") {
    RespClient rc;
    if (!require_redis(rc)) return;
    std::vector<RespValue> sink;
    rc.exec({{"SELECT", "15"}}, sink);
    rc.exec({{"FLUSHDB"}}, sink);

    SqliteTickStore store;
    REQUIRE(store.open(":memory:"));

    const std::uint64_t d1 = static_cast<std::uint64_t>(days_from_civil(2026, 11, 8)) * kMsPerDay;
    seed_batch(rc, "600000", d1 + 100, d1 + 101);

    // 批id 由 flush 路径生成——直接按公式推
    const std::string id = "600000:20261108:" + std::to_string(d1 + 100) + "-" + std::to_string(d1 + 101);

    SUBCASE("claim → 落库 → done → 重复处理不再插行") {
        LeasePersister lp(rc, store, worker_opts("w1"));
        CHECK(lp.poll_once() == 1);
        CHECK(store.count() == 2);
        CHECK(state_value(rc, id) == "done");

        // state 重置回 pending 重跑：租约释放 + 幂等 upsert → 行数不变
        REQUIRE(rc.exec({{"HSET", state_key(), id, "pending"}}, sink));
        CHECK(lp.poll_once() == 1);
        CHECK(store.count() == 2);
        CHECK(lp.stats().ticks_upserted == 2);  // 第二轮插入 0 行
        // 租约已释放
        REQUIRE(rc.exec({{"EXISTS", lease_key_of(id)}}, sink));
        CHECK(sink[0].integer == 0);
    }

    SUBCASE("租约被他占 → 跳过不处理") {
        REQUIRE(rc.exec({{"SET", lease_key_of(id), "other-worker", "EX", "60"}}, sink));
        LeasePersister lp(rc, store, worker_opts("w2"));
        CHECK(lp.poll_once() == 0);  // 全部跳过
        CHECK(lp.stats().skipped == 1);
        CHECK(store.count() == 0);
        CHECK(state_value(rc, id) == "pending");
    }

    SUBCASE("空批（区间无数据）→ 标 done 不崩") {
        // 只写 state 标记、无 ZSET 数据：模拟"标记落了、数据被淘汰/未写"的残局
        const std::string ghost = "ghost:20261108:5-10";
        REQUIRE(rc.exec({{"HSET", state_key(), ghost, "pending"}}, sink));
        LeasePersister lp(rc, store, worker_opts("w3"));
        CHECK(lp.poll_once() >= 0);
        CHECK(lp.stats().empty_batches == 1);
        CHECK(state_value(rc, ghost) == "done");
    }

    SUBCASE("多实例并发视角: 两 worker 轮询不重复插行") {
        LeasePersister w1(rc, store, worker_opts("w1"));
        LeasePersister w2(rc, store, worker_opts("w2"));
        w1.poll_once();
        w2.poll_once();  // state 已 done，w2 无活可干
        CHECK(store.count() == 2);

        // 再造一批 pending 且预先把租约给 w1 —— w2 跳过
        seed_batch(rc, "600001", d1 + 200, d1 + 200);
        const std::string id2 = "600001:20261108:" + std::to_string(d1 + 200) + "-" + std::to_string(d1 + 200);
        REQUIRE(rc.exec({{"SET", lease_key_of(id2), "w1", "EX", "60"}}, sink));
        CHECK(w2.poll_once() == 0);
        CHECK(store.count() == 2);  // 600001 未落库
        REQUIRE(rc.exec({{"DEL", lease_key_of(id2)}}, sink));
        CHECK(w2.poll_once() >= 1);
        CHECK(store.count() == 3);
    }

    rc.exec({{"SELECT", "0"}}, sink);
}
