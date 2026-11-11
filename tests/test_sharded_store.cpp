#include <doctest/doctest.h>

#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include "libtickshelf/sharded_store.hpp"

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

// 临时分片目录（每用例独立，RAII 清理含 -wal/-shm）
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

TEST_CASE("ShardedTickStore: 纯函数路由 + demux + 查询一致性（4 分片）") {
    TmpDir tmp("tickshelf_sharded_test");
    ShardedTickStore store;
    store.set_shards(4);
    REQUIRE(store.open(tmp.path + "/ticks.db"));

    // 路由确定性：同 symbol 永远同分片（写读两侧同一纯函数的前提）
    std::vector<Tick> a = {make_tick("600000", 1000), make_tick("600000", 1002)};
    std::vector<Tick> b = {make_tick("000001", 1001), make_tick("600001", 1001)};
    std::vector<Tick> c = {make_tick("600000", 1003)};  // 跨 flush 同 symbol

    std::vector<const std::vector<Tick>*> g1 = {&a, &b};
    std::vector<const std::vector<Tick>*> g2 = {&c};
    CHECK(store.upsert_flush(g1) == 4);
    CHECK(store.upsert_flush(g2) == 1);
    CHECK(store.count() == 5);

    // 跨 flush 幂等重放：同 (symbol, ts) 不产生第二行
    CHECK(store.upsert_flush(g1) == 0);
    CHECK(store.count() == 5);

    // 跨 flush 同 symbol 落同分片：幂等去重对分片透明
    CHECK(store.upsert_batch(a) == 0);
    CHECK(store.count() == 5);

    // 查询：单 symbol（可能落任意分片）读回，与写入顺序无关
    const auto ra = store.query_range("600000", 900, 2000);
    REQUIRE(ra.size() == 3);
    CHECK(ra[0].ts_exchange_ms == 1000);  // 升序
    CHECK(ra[1].ts_exchange_ms == 1002);
    CHECK(ra[2].ts_exchange_ms == 1003);
    const auto rb = store.query_range("000001", 900, 2000);
    REQUIRE(rb.size() == 1);
    CHECK(store.query_range("999999", 0, 9999999).empty());  // 路由到的分片无数据
    CHECK_FALSE(store.query_failed());

    // upsert_batch 与 upsert_flush 语义一致
    std::vector<Tick> d = {make_tick("600002", 500)};
    CHECK(store.upsert_batch(d) == 1);
    CHECK(store.count() == 6);
}

TEST_CASE("ShardedTickStore: 单分片退化与文件布局（ADR-020）") {
    TmpDir tmp("tickshelf_sharded_single");
    ShardedTickStore one;
    one.set_shards(1);
    REQUIRE(one.open(tmp.path + "/ticks.db"));  // N=1：路径原样，无后缀
    CHECK(std::filesystem::exists(tmp.path + "/ticks.db"));

    std::vector<Tick> a = {make_tick("600000", 100)};
    CHECK(one.upsert_batch(a) == 1);
    CHECK(one.count() == 1);
    CHECK(one.query_range("600000", 0, 1000).size() == 1);
}

TEST_CASE("make_tick_store 工厂: shards<=1 → 单文件，>1 → 分片") {
    auto s1 = make_tick_store(1);
    CHECK(s1 != nullptr);
    auto s4 = make_tick_store(4);
    CHECK(s4 != nullptr);
    auto s0 = make_tick_store(0);  // 归一为单文件
    CHECK(s0 != nullptr);
}
