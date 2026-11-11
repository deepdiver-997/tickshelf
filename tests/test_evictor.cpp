#include <doctest/doctest.h>

#include <cstring>
#include <string>
#include <vector>

#include "common/civil.hpp"
#include "libtickshelf/evictor.hpp"
#include "libtickshelf/ingestor.hpp"
#include "libtickshelf/resp_client.hpp"

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

void flush_classic(RespClient& rc, const char* sym, std::uint64_t ts_a, std::uint64_t ts_b) {
    Ingestor ing;
    for (std::uint64_t ts = ts_a; ts <= ts_b; ++ts) ing.ingest(make_tick(sym, ts));
    auto batches = ing.drain();
    REQUIRE(batches.size() == 1);
    REQUIRE(batches[0].first_ts == ts_a);
    REQUIRE(batches[0].last_ts == ts_b);
    REQUIRE(flush_batches(rc, batches));
}

void claim(RespClient& rc, const std::string& id) {
    std::vector<RespValue> r;
    REQUIRE(rc.exec({{"SET", lease_key_of(id), "worker", "NX", "EX", "60"}}, r));
}

void mark_done(RespClient& rc, const std::string& id) {
    std::vector<RespValue> r;
    REQUIRE(rc.exec({{"HSET", state_key(), id, "done"}}, r));
}

std::int64_t exists(RespClient& rc, const std::string& key) {
    std::vector<RespValue> r;
    REQUIRE(rc.exec({{"EXISTS", key}}, r));
    return r[0].integer;
}

std::int64_t hexists(RespClient& rc, const std::string& field) {
    std::vector<RespValue> r;
    REQUIRE(rc.exec({{"HEXISTS", state_key(), field}}, r));
    return r[0].integer;
}

std::int64_t idx_len(RespClient& rc, const char* sym) {
    std::vector<RespValue> r;
    REQUIRE(rc.exec({{"ZCARD", idx_key(sym)}}, r));
    return r[0].integer;
}

std::string id_of(const char* sym, std::uint64_t a, std::uint64_t b) {
    return std::string(sym) + ":" + std::to_string(yyyymmdd_from_ms(a)) + ":" +
           std::to_string(a) + "-" + std::to_string(b);
}

}  // namespace

TEST_CASE("Evictor v2 段语义: done 整删 / 在飞 / 紧急（需 redis-server）") {
    RespClient rc;
    if (!require_redis(rc)) return;
    std::vector<RespValue> sink;
    rc.exec({{"SELECT", "15"}}, sink);
    rc.exec({{"FLUSHDB"}}, sink);

    const std::uint64_t d1 = static_cast<std::uint64_t>(days_from_civil(2026, 11, 10)) * kMsPerDay;
    const std::string seg_a = seg_key_of(id_of("600000", d1 + 100, d1 + 103));
    const std::string id_a = id_of("600000", d1 + 100, d1 + 103);
    const std::string id_b = id_of("600000", d1 + 200, d1 + 203);
    const std::string seg_b = seg_key_of(id_b);

    SUBCASE("done 段整删：seg + idx + state 全清（已持久化先淘汰由构造成立）") {
        flush_classic(rc, "600000", d1 + 100, d1 + 103);
        REQUIRE(exists(rc, seg_a) == 1);
        CHECK(idx_len(rc, "600000") == 1);
        mark_done(rc, id_a);

        Evictor ev(rc);
        REQUIRE(ev.run_once());
        CHECK(exists(rc, seg_a) == 0);
        CHECK(hexists(rc, id_a) == 0);
        CHECK(idx_len(rc, "600000") == 0);  // 悬挂索引必须同删
        CHECK(ev.stats().segs_deleted == 1);
        CHECK(ev.stats().idx_removed == 1);
        CHECK(ev.stats().states_deleted == 1);
    }

    SUBCASE("pending 段保留（淘汰只认 done）") {
        flush_classic(rc, "600000", d1 + 100, d1 + 103);
        Evictor ev(rc);
        REQUIRE(ev.run_once());
        CHECK(exists(rc, seg_a) == 1);
        CHECK(idx_len(rc, "600000") == 1);
        CHECK(hexists(rc, id_a) == 1);
    }

    SUBCASE("tier2 显式开：在飞段可删，state/lease 留给 worker 收尾") {
        flush_classic(rc, "600000", d1 + 100, d1 + 103);
        claim(rc, id_a);

        SUBCASE("默认关：在飞段保留") {
            Evictor ev0(rc);
            REQUIRE(ev0.run_once());
            CHECK(exists(rc, seg_a) == 1);
            CHECK(idx_len(rc, "600000") == 1);
        }

        SUBCASE("开启：段+索引删，state pending 保留") {
            Evictor::Options opt;
            opt.allow_inflight_evict = true;
            Evictor ev(rc, opt);
            REQUIRE(ev.run_once());
            CHECK(exists(rc, seg_a) == 0);
            CHECK(idx_len(rc, "600000") == 0);
            CHECK(hexists(rc, id_a) == 1);  // worker 收尾后自清
            CHECK(ev.stats().segs_deleted == 1);
        }
    }

    SUBCASE("tier3 紧急：未认领 pending 段真丢（整批抹除）") {
        flush_classic(rc, "600000", d1 + 100, d1 + 103);
        Evictor ev(rc);
        REQUIRE(ev.run_once(false));
        CHECK(exists(rc, seg_a) == 1);  // 非紧急不动

        REQUIRE(ev.run_once(true));
        CHECK(exists(rc, seg_a) == 0);
        CHECK(hexists(rc, id_a) == 0);
        CHECK(idx_len(rc, "600000") == 0);
        CHECK(ev.stats().emergency_segs == 1);
    }

    SUBCASE("多 symbol 独立：只删 done 段所在 symbol") {
        flush_classic(rc, "600000", d1 + 100, d1 + 103);
        mark_done(rc, id_of("600000", d1 + 100, d1 + 103));
        flush_classic(rc, "600001", d1 + 100, d1 + 103);  // pending

        Evictor ev(rc);
        REQUIRE(ev.run_once());
        CHECK(exists(rc, seg_key_of(id_of("600000", d1 + 100, d1 + 103))) == 0);
        CHECK(exists(rc, seg_key_of(id_of("600001", d1 + 100, d1 + 103))) == 1);
        CHECK(idx_len(rc, "600000") == 0);
        CHECK(idx_len(rc, "600001") == 1);
    }

    (void)seg_b; (void)id_b;
    rc.exec({{"SELECT", "0"}}, sink);
}
