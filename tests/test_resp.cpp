#include <doctest/doctest.h>

#include <cstring>
#include <string>
#include <vector>

#include "common/tick.hpp"
#include "libtickshelf/resp_client.hpp"

using namespace tickshelf;

namespace {

std::vector<RespValue> parse_all(RespParser& p, const std::string& bytes) {
    std::vector<RespValue> out;
    p.feed(reinterpret_cast<const std::uint8_t*>(bytes.data()), bytes.size(), out);
    return out;
}

// Redis 探活：不可达则跳过本用例（ctest 保持绿，联调阶段自然覆盖）
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

}  // namespace

TEST_CASE("RESP: 命令编码为 bulk string 数组") {
    std::string out;
    encode_command({"ZADD", "ts:X", "123", std::string("a\0b", 3)}, out);
    // 期望串含 \0，不能直接比字面量（strlen 会截断），分段拼
    std::string expect = "*4\r\n$4\r\nZADD\r\n$4\r\nts:X\r\n$3\r\n123\r\n$3\r\na";
    expect += '\0';
    expect += "b\r\n";
    CHECK(out == expect);
}

TEST_CASE("RESP: 解析器各类回复") {
    RespParser p;
    auto out = parse_all(p, "+OK\r\n-ERR bad\r\n:42\r\n");
    REQUIRE(out.size() == 3);
    CHECK(out[0].type == RespValue::Type::Simple);
    CHECK(out[0].str == "OK");
    CHECK(out[1].type == RespValue::Type::Error);
    CHECK(out[1].str == "ERR bad");
    CHECK(out[2].type == RespValue::Type::Integer);
    CHECK(out[2].integer == 42);
    CHECK_FALSE(p.failed());
}

TEST_CASE("RESP: bulk 含二进制（\\r\\n 与 \\0 都不破坏边界）") {
    RespParser p;
    // 内容 = "a\r\n\0b"（5 字节），尾部再跟下一条命令的回复
    auto out = parse_all(p, std::string("$5\r\na\r\n\0b\r\n:7\r\n", 17));
    REQUIRE(out.size() == 2);
    CHECK(out[0].type == RespValue::Type::Bulk);
    REQUIRE(out[0].str.size() == 5);
    CHECK(std::memcmp(out[0].str.data(), "a\r\n\0b", 5) == 0);
    CHECK(out[1].integer == 7);
}

TEST_CASE("RESP: null bulk / null array") {
    RespParser p;
    auto out = parse_all(p, "$-1\r\n*-1\r\n");
    REQUIRE(out.size() == 2);
    CHECK(out[0].type == RespValue::Type::Null);
    CHECK(out[1].type == RespValue::Type::Null);
}

TEST_CASE("RESP: 半包任意切分与嵌套数组") {
    const std::string stream = "*2\r\n$3\r\nfoo\r\n$-1\r\n*2\r\n:1\r\n$2\r\nok\r\n:9\r\n";
    RespParser p;
    std::vector<RespValue> out;
    for (std::size_t i = 0; i < stream.size(); i += 3) {  // 3 字节一喂，覆盖所有边界
        const std::size_t end = std::min(stream.size(), i + 3);
        p.feed(reinterpret_cast<const std::uint8_t*>(stream.data()) + i, end - i, out);
        REQUIRE_FALSE(p.failed());
    }
    REQUIRE(out.size() == 3);
    CHECK(out[0].type == RespValue::Type::Array);
    REQUIRE(out[0].array.size() == 2);
    CHECK(out[0].array[0].str == "foo");
    CHECK(out[0].array[1].type == RespValue::Type::Null);
    REQUIRE(out[1].array.size() == 2);
    CHECK(out[1].array[0].integer == 1);
    CHECK(out[1].array[1].str == "ok");
    CHECK(out[2].integer == 9);
}

TEST_CASE("RESP: bulk 长度不足时整体等待") {
    RespParser p;
    auto out = parse_all(p, "*1\r\n$10\r\n012345678");  // 少 1 字节
    CHECK(out.empty());
    out = parse_all(p, "9\r\n");
    REQUIRE(out.size() == 1);
    REQUIRE(out[0].type == RespValue::Type::Array);  // 外层是 *1 数组，bulk 在子元素
    REQUIRE(out[0].array.size() == 1);
    CHECK(out[0].array[0].str == "0123456789");
}

TEST_CASE("RESP: 垃圾字节置 failed 且不再产出") {
    RespParser p;
    auto out = parse_all(p, "garbage\r\n");
    CHECK(p.failed());
    out = parse_all(p, "+OK\r\n");
    CHECK(out.empty());
}

TEST_CASE("RESP 集成: PING / SETGET 二进制值（需 redis-server）") {
    RespClient rc;
    if (!require_redis(rc)) return;
    const std::string val("v\0\r\nx", 5);
    std::vector<RespValue> r;
    REQUIRE(rc.exec({{"SET", "ut:bin", val}}, r));
    REQUIRE(rc.exec({{"GET", "ut:bin"}}, r));
    REQUIRE(r.size() == 1);
    CHECK(r[0].type == RespValue::Type::Bulk);
    CHECK(r[0].str == val);
    rc.exec({{"DEL", "ut:bin"}}, r);
}

TEST_CASE("RESP 集成: SET NX 语义（需 redis-server）") {
    RespClient rc;
    if (!require_redis(rc)) return;
    std::vector<RespValue> r;
    REQUIRE(rc.exec({{"DEL", "ut:nx"}, {"SET", "ut:nx", "w1", "NX"}, {"SET", "ut:nx", "w2", "NX"}}, r));
    REQUIRE(r.size() == 3);
    CHECK(r[1].type == RespValue::Type::Simple);  // 抢占成功 +OK
    CHECK(r[2].type == RespValue::Type::Null);    // 已被占 → nil
    REQUIRE(rc.exec({{"GET", "ut:nx"}}, r));
    CHECK(r[0].str == "w1");
    rc.exec({{"DEL", "ut:nx"}}, r);
}

TEST_CASE("RESP 集成: ZSET 64B member 原始字节往返（需 redis-server）") {
    RespClient rc;
    if (!require_redis(rc)) return;
    std::vector<RespValue> sink;
    rc.exec({{"SELECT", "15"}}, sink);
    rc.exec({{"FLUSHDB"}}, sink);

    // member 是 64B Tick 原始字节（含 \0 / \r\n 模式字节）
    Tick t{};
    std::memcpy(t.symbol, "ut:\r\n\0\0\0", 8);
    t.ts_exchange_ms = 1000000;
    std::string member;
    member.resize(kTickBytes);
    encode_tick(t, reinterpret_cast<std::uint8_t*>(member.data()));

    std::vector<RespValue> r;
    REQUIRE(rc.exec({{"ZADD", "ut:z", "1000000", member}}, r));
    REQUIRE(rc.exec({{"ZRANGEBYSCORE", "ut:z", "1000000", "1000000"}}, r));
    REQUIRE(r.size() == 1);
    REQUIRE(r[0].type == RespValue::Type::Array);
    REQUIRE(r[0].array.size() == 1);
    CHECK(r[0].array[0].str == member);  // 二进制无损

    // SCAN 游标形状：[bulk cursor, array keys]
    REQUIRE(rc.exec({{"SCAN", "0", "MATCH", "ut:*"}}, r));
    REQUIRE(r.size() == 1);
    REQUIRE(r[0].type == RespValue::Type::Array);
    CHECK(r[0].array.size() == 2);
    CHECK(r[0].array[0].type == RespValue::Type::Bulk);  // cursor
    rc.exec({{"SELECT", "0"}}, sink);
}
