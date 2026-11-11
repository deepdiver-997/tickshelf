#include <doctest/doctest.h>

#include <cstring>

#include "common/fixed.hpp"
#include "common/tick.hpp"

using namespace tickshelf;

namespace {
Tick sample_tick() {
    Tick t{};
    std::memcpy(t.symbol, "600000\0\0", 8);
    t.ts_exchange_ms = 0x0102030405060708ull;
    t.ts_receive_ms = 1760000000123ull;
    t.bid_price = 12345000;
    t.ask_price = 12346000;
    t.last_price = 12345500;
    t.bid_size = 100;
    t.ask_size = 200;
    t.last_size = 15;
    t.flags = kTickFlagNoLast;
    return t;
}
}  // namespace

TEST_CASE("Tick: 编解码往返保留全部字段") {
    const Tick t = sample_tick();
    std::uint8_t buf[kTickBytes];
    encode_tick(t, buf);
    const Tick back = decode_tick(buf);
    CHECK(std::memcmp(back.symbol, t.symbol, 8) == 0);
    CHECK(back.ts_exchange_ms == t.ts_exchange_ms);
    CHECK(back.ts_receive_ms == t.ts_receive_ms);
    CHECK(back.bid_price == t.bid_price);
    CHECK(back.ask_price == t.ask_price);
    CHECK(back.last_price == t.last_price);
    CHECK(back.bid_size == t.bid_size);
    CHECK(back.ask_size == t.ask_size);
    CHECK(back.last_size == t.last_size);
    CHECK(back.flags == t.flags);

    SUBCASE("极值字段往返无损") {
        Tick mx{};
        std::memset(mx.symbol, 0xAB, 8);
        mx.ts_exchange_ms = 0xFFFFFFFFFFFFFFFFull;
        mx.ts_receive_ms = 0xFFFFFFFFFFFFFFFFull;
        mx.bid_price = -12345000;
        mx.ask_price = 999999999999;
        mx.last_price = 0;
        mx.bid_size = 0xFFFFFFFFu;
        mx.ask_size = 0xFFFFFFFFu;
        mx.last_size = 0xFFFFFFFFu;
        mx.flags = 0xFF;
        std::uint8_t b2[kTickBytes];
        encode_tick(mx, b2);
        const Tick back2 = decode_tick(b2);
        CHECK(std::memcmp(&back2, &mx, kTickBytes) == 0);
    }
}

TEST_CASE("Tick: 线格式小端字节序") {
    const Tick t = sample_tick();
    std::uint8_t buf[kTickBytes];
    encode_tick(t, buf);
    CHECK(buf[8] == 0x08);   // ts_exchange 最低字节在前
    CHECK(buf[15] == 0x01);
    CHECK(buf[24] == 0xA8);  // 12345000 = 0x00BC5EA8 → LE 首 byte 0xA8
    CHECK(buf[25] == 0x5E);
    CHECK(buf[26] == 0xBC);
    CHECK(buf[27] == 0x00);
    CHECK(buf[61] == 0);     // pad 恒 0
    CHECK(buf[62] == 0);
    CHECK(buf[63] == 0);
}

TEST_CASE("symbol: u64 打包互异，视图去 NUL 填充") {
    const Tick t = sample_tick();
    CHECK(pack_symbol(t.symbol) != pack_symbol("600001\0\0"));
    CHECK(pack_symbol("600000\0\0") == pack_symbol("600000\0\0"));
    CHECK(tick_symbol(t) == "600000");

    Tick full{};
    std::memcpy(full.symbol, "399NNNNN", 8);  // 8 字节无 NUL：整段视图
    CHECK(tick_symbol(full) == "399NNNNN");
}

TEST_CASE("定点价: 1e-4 比例") {
    CHECK(price_from_double(1234.5) == 12345000);
    CHECK(price_from_double(-0.5) == -5000);
    CHECK(price_to_string(12345000) == "1234.5000");
    CHECK(price_to_string(-5000) == "-0.5000");
    CHECK(price_to_string(12340001) == "1234.0001");
    CHECK(price_to_string(0) == "0.0000");
    CHECK(price_to_double(12345000) == doctest::Approx(1234.5));
}
