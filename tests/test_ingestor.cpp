#include <doctest/doctest.h>

#include <cstring>
#include <string>
#include <vector>

#include "common/civil.hpp"
#include "common/tick.hpp"
#include "libtickshelf/ingestor.hpp"

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

const Batch* find_batch(const std::vector<Batch>& bs, const char* sym, std::uint32_t date) {
    for (const Batch& b : bs)
        if (b.symbol == sym && b.yyyymmdd == date) return &b;
    return nullptr;
}

}  // namespace

TEST_CASE("build_flush_commands: 段式 v2 + 状态 HASH——SET seg + ZADD idx + 单条 HSET（纯函数）") {
    // 3 tick 分布在 2 symbol → classic 应为 2×(SET seg+ZADD idx) + HSET state + HSET latest = 6 条
    Ingestor ing;
    ing.ingest(make_tick("600000", 1000));
    ing.ingest(make_tick("600000", 1002));
    ing.ingest(make_tick("000001", 1001));
    const auto batches = ing.drain();
    REQUIRE(batches.size() == 2);

    FlushOptions classic;
    std::vector<std::vector<std::string>> cmds;
    std::vector<std::size_t> claimed;
    build_flush_commands(batches, classic, cmds, claimed);
    REQUIRE(cmds.size() == 2 * 2 + 2);  // ADR-019：state 单条 HSET，命令数与符号数解耦
    CHECK(claimed.empty());

    // 段命令：值 = 批内 tick 打包（2 tick = 128B / 1 tick = 64B），段键 = "seg:" + 批id
    std::size_t segs = 0, idxs = 0;
    const std::vector<std::string>* state_hset = nullptr;
    for (const auto& c : cmds) {
        if (c[0] == "SET" && c[1].rfind("seg:", 0) == 0) {
            ++segs;
            const std::size_t nticks = c[2].size() / kTickBytes;
            CHECK((nticks == 2 || nticks == 1));
            if (nticks == 2) {
                const Tick t0 = decode_tick(reinterpret_cast<const std::uint8_t*>(c[2].data()));
                CHECK((t0.ts_exchange_ms == 1000 || t0.ts_exchange_ms == 1002));
            }
        }
        if (c[0] == "ZADD") {
            ++idxs;
            CHECK((c[1] == idx_key("600000") || c[1] == idx_key("000001")));
            CHECK(c.size() == 4);  // ZADD idx first member
        }
        if (c[0] == "HSET" && c[1] == state_key()) state_hset = &c;
    }
    CHECK(segs == 2);
    CHECK(idxs == 2);
    REQUIRE(state_hset != nullptr);
    CHECK(state_hset->size() == 2 + 2 * 2);  // 2 批 × (field=批id, value=pending)
    for (std::size_t i = 2; i + 1 < state_hset->size(); i += 2)
        CHECK(state_hset->at(i + 1) == "pending");

    CHECK(cmds.back()[0] == "HSET");
    CHECK(cmds.back()[1] == latest_key());
    CHECK(cmds.back().size() == 2 + 2 * 2);  // 2 symbol × (field, value)
    for (std::size_t i = 2; i + 1 < cmds.back().size(); i += 2) {
        const Tick lt = decode_tick(reinterpret_cast<const std::uint8_t*>(cmds.back()[i + 1].data()));
        if (cmds.back()[i] == "600000")
            CHECK(lt.ts_exchange_ms == 1002);  // 取最新不取先到
        else if (cmds.back()[i] == "000001")
            CHECK(lt.ts_exchange_ms == 1001);
    }

    SUBCASE("写穿：认领块前置（claimed 步长 1），state 单条 HSET 在段/索引块之后") {
        FlushOptions wt;
        wt.claim_lease = true;
        wt.worker_id = "daemon";
        build_flush_commands(batches, wt, cmds, claimed);
        REQUIRE(cmds.size() == 2 * 3 + 2);  // 2 lease + 2 seg + 2 zadd + state + latest
        REQUIRE(claimed.size() == 2);
        CHECK(claimed[0] == 0);
        CHECK(claimed[1] == 1);  // 认领块连续，步长 1
        CHECK(cmds[claimed[0]][0] == "SET");
        CHECK(cmds[claimed[0]][3] == "NX");
        CHECK((cmds[2][1].rfind("seg:", 0) == 0));  // 认领块后是段块
        CHECK(cmds[6][0] == "HSET");
        CHECK(cmds[6][1] == state_key());
        CHECK(cmds[7][1] == latest_key());
    }

    SUBCASE("单 tick 批：classic 4 条 / 写穿 5 条，结构不变") {
        Ingestor ing1;
        ing1.ingest(make_tick("600000", 2000));
        auto one = ing1.drain();
        build_flush_commands(one, classic, cmds, claimed);
        REQUIRE(cmds.size() == 4);  // SET seg + ZADD idx + HSET state + HSET latest
        CHECK((cmds[0][1].rfind("seg:", 0) == 0));
        CHECK(cmds[0][2].size() == kTickBytes);

        FlushOptions wt;
        wt.claim_lease = true;
        wt.worker_id = "d";
        build_flush_commands(one, wt, cmds, claimed);
        REQUIRE(cmds.size() == 5);
        CHECK(cmds[0][3] == "NX");
        CHECK(cmds[3][1] == state_key());
    }
}

TEST_CASE("Ingestor: 交织喂入按 symbol 分桶，drain 取空") {
    Ingestor ing;
    ing.ingest(make_tick("600000", 1000));
    ing.ingest(make_tick("000001", 1001));
    ing.ingest(make_tick("600000", 1002));
    CHECK(ing.pending_ticks() == 3);
    CHECK(ing.pending_symbols() == 2);

    const auto batches = ing.drain();
    CHECK(ing.pending_ticks() == 0);
    REQUIRE(batches.size() == 2);
    const Batch* a = find_batch(batches, "600000", yyyymmdd_from_ms(1000));
    const Batch* b = find_batch(batches, "000001", yyyymmdd_from_ms(1001));
    REQUIRE(a != nullptr);
    REQUIRE(b != nullptr);
    CHECK(a->ticks.size() == 2);
    CHECK(a->first_ts == 1000);
    CHECK(a->last_ts == 1002);
    CHECK(b->ticks.size() == 1);
    const Tick expect = make_tick("600000", 1000);
    CHECK(std::memcmp(&a->ticks[0], &expect, kTickBytes) == 0);
}

TEST_CASE("Ingestor: 跨日界拆桶（批次粒度 = symbol × 交易日）") {
    const std::uint64_t d1 = static_cast<std::uint64_t>(days_from_civil(2026, 11, 10)) * kMsPerDay;
    const std::uint64_t d2 = d1 + kMsPerDay;
    Ingestor ing;
    ing.ingest(make_tick("600000", d1 - 1));
    ing.ingest(make_tick("600000", d1));
    ing.ingest(make_tick("600000", d2 + 500));
    CHECK(ing.pending_ticks() == 3);
    const auto batches = ing.drain();
    REQUIRE(batches.size() == 3);
    CHECK(find_batch(batches, "600000", yyyymmdd_from_ms(d1 - 1)) != nullptr);
    CHECK(find_batch(batches, "600000", yyyymmdd_from_ms(d1)) != nullptr);
    CHECK(find_batch(batches, "600000", yyyymmdd_from_ms(d2 + 500)) != nullptr);
}

TEST_CASE("Ingestor: 乱序 ts 取 min/max（批 id 区间语义）") {
    Ingestor ing;
    ing.ingest(make_tick("600000", 5000));
    ing.ingest(make_tick("600000", 1000));
    ing.ingest(make_tick("600000", 3000));
    const auto batches = ing.drain();
    REQUIRE(batches.size() == 1);
    CHECK(batches[0].first_ts == 1000);
    CHECK(batches[0].last_ts == 5000);
    CHECK(batch_id_of(batches[0]) ==
          "600000:" + std::to_string(yyyymmdd_from_ms(1000)) + ":1000-5000");
}

TEST_CASE("build_flush_commands: 段键与批 id 1:1") {
    Ingestor ing;
    ing.ingest(make_tick("600000", 1000));
    ing.ingest(make_tick("600000", 1002));
    const auto batches = ing.drain();
    REQUIRE(batches.size() == 1);
    FlushOptions classic;
    std::vector<std::vector<std::string>> cmds;
    std::vector<std::size_t> claimed;
    build_flush_commands(batches, classic, cmds, claimed);
    REQUIRE(cmds.size() == 4);
    const std::string id = batch_id_of(batches[0]);
    CHECK((cmds[0][1] == seg_key_of(id)));  // 段键 = "seg:" + 批id（persister 读回同款推导）
}

TEST_CASE("flush_batches 集成: 段 + idx + state + latest 落 Redis（需 redis-server）") {
    RespClient rc;
    if (!require_redis(rc)) return;
    std::vector<RespValue> sink;
    rc.exec({{"SELECT", "15"}}, sink);
    rc.exec({{"FLUSHDB"}}, sink);

    Ingestor ing;
    ing.ingest(make_tick("utone", 1000));
    ing.ingest(make_tick("utsym", 1001));
    ing.ingest(make_tick("utsym", 1002));
    auto batches = ing.drain();
    REQUIRE(batches.size() == 2);
    REQUIRE(flush_batches(rc, batches));

    std::vector<RespValue> r;
    // 段存在且字节可逆解码；索引成员存在（按 member 查，ZSCORE 返回 score）
    for (const Batch& b : batches) {
        const std::string sk = seg_key_of(batch_id_of(b));
        REQUIRE(rc.exec({{"GET", sk}}, r));
        REQUIRE(r[0].type == RespValue::Type::Bulk);
        REQUIRE(r[0].str.size() == b.ticks.size() * kTickBytes);
        const Tick back =
            decode_tick(reinterpret_cast<const std::uint8_t*>(r[0].str.data()));
        CHECK(back.ts_exchange_ms == b.first_ts);
        REQUIRE(rc.exec({{"ZSCORE", idx_key(b.symbol),
                          idx_member(b.yyyymmdd, b.first_ts, b.last_ts)}},
                        r));
        REQUIRE(r[0].type == RespValue::Type::Bulk);
        CHECK(r[0].str == std::to_string(b.first_ts));
    }

    // state 标记 pending
    const std::string id = "utsym:" + std::to_string(yyyymmdd_from_ms(1001)) + ":1001-1002";
    REQUIRE(rc.exec({{"HGET", state_key(), id}}, r));
    REQUIRE(r[0].type == RespValue::Type::Bulk);
    CHECK(r[0].str == "pending");

    // latest 投影：窗口内最新
    REQUIRE(rc.exec({{"HGET", "latest", "utsym"}}, r));
    REQUIRE(r[0].type == RespValue::Type::Bulk);
    const Tick lt = decode_tick(reinterpret_cast<const std::uint8_t*>(r[0].str.data()));
    CHECK(lt.ts_exchange_ms == 1002);

    // 第二次 flush（更新 ts）→ 投影覆盖 + 索引追加
    Ingestor ing2;
    ing2.ingest(make_tick("utsym", 2000));
    REQUIRE(flush_batches(rc, ing2.drain()));
    REQUIRE(rc.exec({{"HGET", "latest", "utsym"}}, r));
    const Tick t3 = decode_tick(reinterpret_cast<const std::uint8_t*>(r[0].str.data()));
    CHECK(t3.ts_exchange_ms == 2000);
    REQUIRE(rc.exec({{"ZCARD", idx_key("utsym")}}, r));
    CHECK(r[0].integer == 2);  // 两个批两个索引成员

    rc.exec({{"SELECT", "0"}}, sink);
}
