#include <doctest/doctest.h>

#include <netinet/in.h>
#include <sys/socket.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "common/net.hpp"
#include "common/tick.hpp"
#include "components/redis/resp_client.hpp"

using namespace tickshelf;
using namespace std::chrono_literals;

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

// 离线假 RESP 服务器：只在 loopback 监听，回什么、何时回由测试控制。
// 用于不依赖 redis-server 的客户端 I/O 行为测试（中断 / 截止 / 跨调用留存）。
class FakeRespServer {
public:
    bool start() {
        listener_ = net::tcp_listen("127.0.0.1", 0, 4);
        if (listener_ < 0) return false;
        sockaddr_in addr{};
        socklen_t len = sizeof(addr);
        if (::getsockname(listener_, reinterpret_cast<sockaddr*>(&addr), &len) != 0) return false;
        port_ = ntohs(addr.sin_port);  // macOS 上 ntohs 是宏，不能加 ::
        return port_ != 0;
    }
    std::uint16_t port() const { return port_; }
    net::UniqueFd accept_one() { return net::UniqueFd(net::tcp_accept(listener_)); }

private:
    int listener_ = -1;
    std::uint16_t port_ = 0;
};

// 读掉一次请求字节（不解析），让"服务端保持沉默"的时序确定。
void drain_request(int fd) {
    std::uint8_t tmp[4096];
    (void)::recv(fd, tmp, sizeof(tmp), 0);
}

bool write_all_to(int fd, const std::string& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        const ssize_t n = ::send(fd, data.data() + sent, data.size() - sent, 0);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            return false;
        }
        sent += static_cast<std::size_t>(n);
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
    auto out = parse_all(p, std::string("$5\r\na\r\n\0b\r\n:7\r\n", 15));
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

TEST_CASE("RESP: drain 只解析缓冲已有的字节，跨半包收割") {
    RespParser p;
    std::vector<RespValue> out;
    p.feed(reinterpret_cast<const std::uint8_t*>("+OK\r"), 4, out);
    p.drain(out);  // 只有半行：什么也解不出
    CHECK(out.empty());
    p.feed(reinterpret_cast<const std::uint8_t*>("\n:7\r\n"), 5, out);
    p.drain(out);  // feed 尾部已解析；显式 drain 是幂等收割
    REQUIRE(out.size() == 2);
    CHECK(out[0].str == "OK");
    CHECK(out[1].integer == 7);
    CHECK_FALSE(p.failed());
}

TEST_CASE("RESP 客户端: send/collect 解耦——无数据按截止失败，回复跨调用收齐") {
    FakeRespServer server;
    REQUIRE(server.start());
    RespClient rc;
    REQUIRE(rc.connect("127.0.0.1", server.port()));
    auto conn = server.accept_one();
    REQUIRE(conn.is_open());

    // 发送不等回复：字节交给内核即成功返回。
    REQUIRE(rc.send_pipeline({{"PING"}, {"PING"}, {"PING"}}, RespDeadline::after_ms(1000)));

    // 服务端保持沉默：短截止 → 失败且零产出，不算中断。
    std::vector<RespValue> out;
    CHECK_FALSE(rc.collect_replies(1, RespDeadline::after_ms(5), out));
    CHECK(out.empty());
    CHECK_FALSE(rc.interrupted());

    drain_request(conn.get());  // 收掉请求字节；服务端仍不回复
    const std::string replies = "+OK\r\n+OK\r\n+OK\r\n";
    REQUIRE(write_all_to(conn.get(), replies));

    // 就绪驱动：回复到达即被唤醒。一次 recv 可能拿到 1..3 条（TCP 无边界），
    // 收不满的部分由解析缓冲跨调用补齐，最终恰好 3 条。
    REQUIRE(rc.collect_replies(1, RespDeadline::after_ms(1000), out));
    REQUIRE(!out.empty());
    REQUIRE(out.size() <= 3);
    const auto remaining = 3 - out.size();
    if (remaining > 0) REQUIRE(rc.collect_replies(remaining, RespDeadline::after_ms(1000), out));
    REQUIRE(out.size() == 3);
    for (const auto& r : out) {
        CHECK(r.type == RespValue::Type::Simple);
        CHECK(r.str == "OK");
    }
    CHECK_FALSE(rc.interrupted());
}

TEST_CASE("RESP 客户端: interrupt 从其他线程立即唤醒阻塞的 collect") {
    FakeRespServer server;
    REQUIRE(server.start());
    RespClient rc;
    REQUIRE(rc.connect("127.0.0.1", server.port()));
    auto conn = server.accept_one();
    REQUIRE(conn.is_open());
    REQUIRE(rc.send_pipeline({{"PING"}}, RespDeadline::after_ms(1000)));
    drain_request(conn.get());  // 服务端此后保持沉默

    std::atomic<bool> done{false};
    bool collect_result = true;
    std::thread waiter([&] {
        std::vector<RespValue> out;
        collect_result = rc.collect_replies(1, RespDeadline::after_ms(10000), out);
        done.store(true, std::memory_order_release);
    });
    std::this_thread::sleep_for(150ms);
    CHECK_FALSE(done.load(std::memory_order_acquire));  // 仍阻塞（截止还有 ~10s）
    rc.interrupt();
    waiter.join();  // 若 interrupt 失效，这里会等满 10s
    CHECK(done.load());
    CHECK_FALSE(collect_result);  // 被打断 = 连接层失败
    CHECK(rc.interrupted());

    std::vector<RespValue> r;
    CHECK_FALSE(rc.exec({{"PING"}}, r));  // 被中断的连接不可信
    rc.close();
    CHECK_FALSE(rc.connected());
    CHECK_FALSE(rc.interrupted());  // close 复位，可重连
}

TEST_CASE("RESP 客户端: 截止时间到期返回失败而非永久阻塞") {
    FakeRespServer server;
    REQUIRE(server.start());
    RespClient rc;
    REQUIRE(rc.connect("127.0.0.1", server.port()));
    auto conn = server.accept_one();
    REQUIRE(conn.is_open());
    REQUIRE(rc.send_pipeline({{"PING"}}, RespDeadline::after_ms(1000)));
    drain_request(conn.get());

    const auto start = std::chrono::steady_clock::now();
    std::vector<RespValue> out;
    CHECK_FALSE(rc.collect_replies(1, RespDeadline::after_ms(150), out));
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    CHECK(elapsed >= 100ms);   // 确实等过截止区间
    CHECK(elapsed < 5000ms);   // 真正永久阻塞会卡死框架；这里是秒级内失败
    CHECK_FALSE(rc.interrupted());
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
