#include <doctest/doctest.h>

#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/tick.hpp"
#include "components/ingest/ingestor.hpp"
#include "components/storage/sharded_store.hpp"
#include "components/storage/sqlite_store.hpp"

using namespace tickshelf;

// 行身份与冲突规则（delivery-contract §4.2）：TTAP ingest → 落库 → 查询端的
// 三组判定数据。主键 (symbol, ts_exchange_ms) 是行情身份；同键的语义由合同定义，
// 不由主键静默决定。

namespace {

// 临时目录：分片数 >1 时 ":memory:" 不再是内存库（分片路径带后缀），走真实文件。
struct Directory {
    std::filesystem::path path;
    Directory() {
        const auto pattern =
            (std::filesystem::temp_directory_path() / "tickshelf_identity_XXXXXX").string();
        std::vector<char> name(pattern.begin(), pattern.end());
        name.push_back('\0');
        const auto* made = ::mkdtemp(name.data());
        if (!made) throw std::runtime_error("mkdtemp failed");
        path = made;
    }
    ~Directory() {
        std::error_code error;
        std::filesystem::remove_all(path, error);
    }
};

Tick make_tick(const char* sym, std::uint64_t ts) {
    Tick t{};
    const std::size_t n = std::strlen(sym);
    std::memcpy(t.symbol, sym, n < 8 ? n : 8);
    t.ts_exchange_ms = ts;
    t.ts_receive_ms = ts + 1;
    t.bid_price = 10005000;
    t.ask_price = 10007000;
    t.last_price = 10006000;
    t.bid_size = 10;
    t.ask_size = 20;
    t.last_size = 5;
    return t;
}

// 经 Ingestor（TTAP 进程内摄入路径）写入一批并 flush。
std::int64_t ingest_flush(SqliteTickStore& store, Ingestor& ing, const Tick& t) {
    ing.ingest(t);
    const auto batches = ing.drain();
    REQUIRE(batches.size() == 1);
    REQUIRE(batches[0].ticks.size() == 1);
    return store.upsert_flush({&batches[0].ticks});
}

void check_identical(const Tick& expected, const Tick& got) {
    CHECK(std::string(tick_symbol(got)) == std::string(tick_symbol(expected)));
    CHECK(got.ts_exchange_ms == expected.ts_exchange_ms);
    CHECK(got.ts_receive_ms == expected.ts_receive_ms);
    CHECK(got.bid_price == expected.bid_price);
    CHECK(got.ask_price == expected.ask_price);
    CHECK(got.last_price == expected.last_price);
    CHECK(got.bid_size == expected.bid_size);
    CHECK(got.ask_size == expected.ask_size);
    CHECK(got.last_size == expected.last_size);
    CHECK(got.flags == expected.flags);
}

}  // namespace

TEST_CASE("身份: 同键异内容——first-accepted-wins，冲突可计数") {
    SqliteTickStore store;
    REQUIRE(store.open(":memory:"));
    Ingestor ing;

    const Tick first = make_tick("600000", 1700000000000);
    CHECK(ingest_flush(store, ing, first) == 1);

    // 同 (symbol, ts_exchange_ms)，内容不同：后到者被丢弃，但绝不是静默的。
    Tick second = first;
    second.bid_price = 10006000;
    second.ask_price = 10008000;
    CHECK(ingest_flush(store, ing, second) == 0);

    const auto stats = store.identity_stats();
    CHECK(stats.content_conflicts == 1);
    CHECK(stats.exact_duplicates == 0);

    // 查询端拿到的身份内容 = 先到者。
    const auto rows = store.query_range("600000", 0, 4102444800000ull);
    REQUIRE(rows.size() == 1);
    check_identical(first, rows[0]);
}

TEST_CASE("身份: 同事件重投——字节级相同收敛，按重复计数不计冲突") {
    SqliteTickStore store;
    REQUIRE(store.open(":memory:"));
    Ingestor ing;

    const Tick event = make_tick("600001", 1700000000001);
    CHECK(ingest_flush(store, ing, event) == 1);
    // 上游 at-least-once 重投同一事件：幂等收敛，语义是重投不是冲突。
    CHECK(ingest_flush(store, ing, event) == 0);

    const auto stats = store.identity_stats();
    CHECK(stats.exact_duplicates == 1);
    CHECK(stats.content_conflicts == 0);

    const auto rows = store.query_range("600001", 0, 4102444800000ull);
    REQUIRE(rows.size() == 1);  // 效果上 exactly-once：查询端只见一行
    check_identical(event, rows[0]);
}

TEST_CASE("身份: 乱序到达——不同键全保留，查询按 ts 升序") {
    SqliteTickStore store;
    REQUIRE(store.open(":memory:"));
    Ingestor ing;

    const Tick late = make_tick("600002", 1700000000002);
    const Tick early = make_tick("600002", 1700000000000);
    const Tick mid = make_tick("600002", 1700000000001);
    // 到达顺序故意乱：不同的键互不冲突，全部落库。
    CHECK(ingest_flush(store, ing, late) == 1);
    CHECK(ingest_flush(store, ing, early) == 1);
    CHECK(ingest_flush(store, ing, mid) == 1);

    const auto stats = store.identity_stats();
    CHECK(stats.exact_duplicates == 0);
    CHECK(stats.content_conflicts == 0);

    const auto rows = store.query_range("600002", 0, 4102444800000ull);
    REQUIRE(rows.size() == 3);
    CHECK(rows[0].ts_exchange_ms == early.ts_exchange_ms);
    CHECK(rows[1].ts_exchange_ms == mid.ts_exchange_ms);
    CHECK(rows[2].ts_exchange_ms == late.ts_exchange_ms);
}

TEST_CASE("身份: 分片聚合——同键跨 flush 落同分片，计数聚合一致") {
    Directory dir;
    ShardedTickStore store;
    store.set_shards(2);
    REQUIRE(store.open((dir.path / "ticks.db").string()));

    const Tick first = make_tick("600003", 1700000000000);
    CHECK(store.upsert_batch({first}) == 1);  // 路由纯函数：同 symbol 同分片
    Tick second = first;
    second.last_price = 10007000;
    CHECK(store.upsert_batch({second}) == 0);  // 同键异内容，同分片拒绝
    CHECK(store.upsert_batch({first}) == 0);   // 同事件重投

    const auto stats = store.identity_stats();
    CHECK(stats.content_conflicts == 1);
    CHECK(stats.exact_duplicates == 1);

    const auto rows = store.query_range("600003", 0, 4102444800000ull);
    REQUIRE(rows.size() == 1);
    check_identical(first, rows[0]);
}
