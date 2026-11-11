#include <doctest/doctest.h>

#include <cstring>
#include <string>
#include <vector>

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

std::string get_str(RespClient& rc, const std::string& key) {
    std::vector<RespValue> r;
    REQUIRE(rc.exec({{"GET", key}}, r));
    return r[0].type == RespValue::Type::Bulk ? r[0].str : "<nil>";
}

std::string hget_state(RespClient& rc, const std::string& field) {
    std::vector<RespValue> r;
    REQUIRE(rc.exec({{"HGET", state_key(), field}}, r));
    return r[0].type == RespValue::Type::Bulk ? r[0].str : "<nil>";
}

std::int64_t exists(RespClient& rc, const std::string& key) {
    std::vector<RespValue> r;
    REQUIRE(rc.exec({{"EXISTS", key}}, r));
    return r[0].integer;
}

LeasePersister::Options worker_opts(const char* name) {
    LeasePersister::Options o;
    o.worker_id = name;
    return o;
}

// 写穿 flush 一批并返回批id
std::string wt_flush(RespClient& rc, const char* sym, std::uint64_t ts_a, std::uint64_t ts_b,
                     std::vector<Batch>& batches, SqliteTickStore& store) {
    Ingestor ing;
    ing.ingest(make_tick(sym, ts_a));
    if (ts_b != ts_a) ing.ingest(make_tick(sym, ts_b));
    batches = ing.drain();
    REQUIRE(batches.size() == 1);
    FlushOptions fo;
    fo.claim_lease = true;
    fo.worker_id = "daemon-test";
    std::vector<char> claimed;
    REQUIRE(flush_batches(rc, batches, fo, &claimed));
    REQUIRE(claimed.size() == 1);
    CHECK(claimed[0] == 1);  // 新批必抢到
    (void)store;
    return batch_id_of(batches[0]);
}

}  // namespace

TEST_CASE("upsert_flush: flush 级单事务——多批合计/跨批去重/幂等重放") {
    SqliteTickStore store;
    REQUIRE(store.open(":memory:"));

    std::vector<Tick> a = {make_tick("600000", 1000), make_tick("600000", 1001)};
    std::vector<Tick> b = {make_tick("600001", 1000), make_tick("600000", 1001)};  // 跨批重复
    std::vector<const std::vector<Tick>*> gs = {&a, &b};
    CHECK(store.upsert_flush(gs) == 3);  // 跨批重复 (600000,1001) 被唯一键吃掉
    CHECK(store.count() == 3);

    CHECK(store.upsert_flush(gs) == 0);  // 幂等重放：零新行
    CHECK(store.count() == 3);

    std::vector<Tick> empty;
    std::vector<const std::vector<Tick>*> mixed = {&empty, &a};
    CHECK(store.upsert_flush(mixed) == 0);   // 已存在的行不再计
    CHECK(store.count() == 3);
    CHECK(store.upsert_flush({}) == 0);      // 空 flush 短路

    std::vector<Tick> c = {make_tick("600002", 1000)};
    std::vector<const std::vector<Tick>*> one = {&c};
    CHECK(store.upsert_flush(one) == 1);
    CHECK(store.count() == 4);
}

TEST_CASE("写穿集成: flush 认领 → 落库 → done（需 redis-server）") {
    RespClient rc;
    if (!require_redis(rc)) return;
    std::vector<RespValue> sink;
    rc.exec({{"SELECT", "15"}}, sink);
    rc.exec({{"FLUSHDB"}}, sink);

    SqliteTickStore store;
    REQUIRE(store.open(":memory:"));

    const std::uint64_t d1 = static_cast<std::uint64_t>(days_from_civil(2026, 11, 8)) * kMsPerDay;
    std::vector<Batch> batches;

    SUBCASE("认领挡住 persister：租约在飞时不重复劳动") {
        const std::string id = wt_flush(rc, "600000", d1 + 100, d1 + 101, batches, store);
        CHECK(exists(rc, lease_key_of(id)) == 1);
        CHECK(hget_state(rc, id) == "pending");

        LeasePersister lp(rc, store, worker_opts("w1"));
        CHECK(lp.poll_once() == 0);  // 租约被 daemon 占 → 全部跳过
        CHECK(lp.stats().skipped == 1);
        CHECK(store.count() == 0);

        // daemon 落库（flush 级单事务）+ 批量收尾 → done 且租约释放
        std::vector<const std::vector<Tick>*> mine = {&batches[0].ticks};
        REQUIRE(store.upsert_flush(mine) == 2);
        REQUIRE(finish_written_batches(rc, batches));
        CHECK(hget_state(rc, id) == "done");
        CHECK(exists(rc, lease_key_of(id)) == 0);
        CHECK(lp.poll_once() == 0);  // done 批不再碰
        CHECK(lp.stats().claimed == 0);
    }

    SUBCASE("release_claims：SQLite 故障降级 → persister 读回接手") {
        const std::string id = wt_flush(rc, "600001", d1 + 200, d1 + 201, batches, store);
        CHECK(exists(rc, lease_key_of(id)) == 1);

        // 模拟 daemon 侧 SQLite 故障：不落库直接放弃认领
        REQUIRE(release_claims(rc, batches));
        CHECK(exists(rc, lease_key_of(id)) == 0);
        CHECK(hget_state(rc, id) == "pending");  // 数据已在 Redis，等接手

        LeasePersister lp(rc, store, worker_opts("w2"));
        CHECK(lp.poll_once() == 1);  // 读回路径落库
        CHECK(store.count() == 2);
        CHECK(hget_state(rc, id) == "done");
    }

    SUBCASE("daemon 崩溃窗口：租约 TTL 过期后 persister 接手（语义演示，不等 60s）") {
        const std::string id = wt_flush(rc, "600002", d1 + 300, d1 + 301, batches, store);
        // 模拟 daemon 死亡：手动删租约（等价 TTL 过期后的键空间状态）
        REQUIRE(rc.exec({{"DEL", lease_key_of(id)}}, sink));
        LeasePersister lp(rc, store, worker_opts("w3"));
        CHECK(lp.poll_once() == 1);  // pending + 无租约 → 认领 → 读回落库
        CHECK(store.count() == 2);
        CHECK(hget_state(rc, id) == "done");
    }

    rc.exec({{"SELECT", "0"}}, sink);
}
