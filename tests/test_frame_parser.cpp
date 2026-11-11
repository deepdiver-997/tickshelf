#include <doctest/doctest.h>

#include <algorithm>
#include <cstring>
#include <random>
#include <vector>

#include "libtickshelf/frame_parser.hpp"

using namespace tickshelf;

namespace {

Tick make_tick(std::uint64_t ts, const char* sym = "600000") {
    Tick t{};
    const std::size_t n = std::strlen(sym);
    std::memcpy(t.symbol, sym, n < 8 ? n : 8);  // Tick{} 已清零，剩余位即 NUL 填充
    t.ts_exchange_ms = ts;
    t.ts_receive_ms = ts + 7;
    t.bid_price = 10005000;
    t.ask_price = 10008000;
    t.last_price = 10006000;
    t.bid_size = 10;
    t.ask_size = 20;
    t.last_size = 3;
    return t;
}

// 按 protocol.md 编一帧完整字节流（帧头 + 载荷 + 正确 CRC）
std::vector<std::uint8_t> build_frame(std::uint8_t type, const std::vector<Tick>& ticks,
                                      std::uint16_t flags = 0) {
    std::vector<std::uint8_t> payload(ticks.size() * kTickBytes);
    for (std::size_t i = 0; i < ticks.size(); ++i)
        encode_tick(ticks[i], payload.data() + i * kTickBytes);
    FrameHeader h{};
    h.version = kFrameVersion;
    h.type = type;
    h.count = static_cast<std::uint16_t>(ticks.size());
    h.flags = flags;
    h.payload_bytes = static_cast<std::uint32_t>(payload.size());
    h.crc32 = crc32(payload.data(), payload.size());
    std::vector<std::uint8_t> buf(kHeaderBytes + payload.size());
    encode_frame_header(h, buf.data());
    std::copy(payload.begin(), payload.end(),
              buf.begin() + static_cast<std::ptrdiff_t>(kHeaderBytes));
    return buf;
}

void flip_bit(std::vector<std::uint8_t>& buf, std::size_t byte_idx, unsigned bit) {
    buf[byte_idx] ^= static_cast<std::uint8_t>(1u << bit);
}

std::vector<Tick> feed_all(FrameParser& p, const std::vector<std::uint8_t>& bytes) {
    std::vector<Tick> out;
    REQUIRE(p.on_bytes(bytes.data(), bytes.size(), out));
    return out;
}

}  // namespace

TEST_CASE("解析器: 完整帧一次喂入与逐字节喂入等价") {
    const std::vector<Tick> ticks = {make_tick(1000), make_tick(1001), make_tick(1002)};
    const auto frame = build_frame(kFrameTypeTickBatch, ticks);

    FrameParser whole;
    auto out1 = feed_all(whole, frame);
    REQUIRE(out1.size() == 3);
    CHECK(std::memcmp(&out1[0], &ticks[0], kTickBytes) == 0);
    CHECK(whole.stats().frames_ok == 1);
    CHECK(whole.stats().ticks_ok == 3);

    FrameParser bytewise;
    std::vector<Tick> out2;
    for (std::size_t i = 0; i < frame.size(); ++i)
        REQUIRE(bytewise.on_bytes(frame.data() + i, 1, out2));
    REQUIRE(out2.size() == 3);
    CHECK(std::memcmp(&out2[2], &ticks[2], kTickBytes) == 0);
}

TEST_CASE("解析器: 半包/粘包任意切分，多帧混心跳") {
    std::mt19937 rng(123);
    std::vector<Tick> expect;
    std::vector<std::uint8_t> stream;
    for (int i = 0; i < 50; ++i) {
        const int n = i % 3;  // 0 → 心跳
        std::vector<Tick> ticks;
        for (int k = 0; k < n; ++k) ticks.push_back(make_tick(1000 + i * 10 + static_cast<unsigned>(k)));
        const auto type = n ? kFrameTypeTickBatch : kFrameTypeHeartbeat;
        expect.insert(expect.end(), ticks.begin(), ticks.end());
        const auto f = build_frame(type, ticks);
        stream.insert(stream.end(), f.begin(), f.end());
    }

    FrameParser p;
    std::vector<Tick> out;
    std::size_t pos = 0;
    while (pos < stream.size()) {
        const std::size_t step = 1 + rng() % 37;  // 含 1 字节极端半包
        const std::size_t end = std::min(stream.size(), pos + step);
        REQUIRE(p.on_bytes(stream.data() + pos, end - pos, out));
        pos = end;
    }
    REQUIRE(out.size() == expect.size());
    for (std::size_t i = 0; i < expect.size(); ++i)
        CHECK(std::memcmp(&out[i], &expect[i], kTickBytes) == 0);
    CHECK(p.stats().frames_ok == 50);
    CHECK(p.stats().heartbeats >= 1);
    CHECK(p.stats().crc_errors == 0);
    CHECK(p.stats().resync_events == 0);
    CHECK(p.stats().resync_bytes == 0);
}

TEST_CASE("解析器: 单字段翻转被对应防线拦截") {
    std::vector<Tick> ticks = {make_tick(2000)};
    std::vector<Tick> next = {make_tick(3000, "000001")};
    auto good_after = build_frame(kFrameTypeTickBatch, next);

    SUBCASE("crc32 翻转 → CRC 拒帧，边界对齐，下一帧恢复") {
        auto f = build_frame(kFrameTypeTickBatch, ticks);
        flip_bit(f, 16, 3);  // 帧头 crc32 字段（offset 16）翻一位
        f.insert(f.end(), good_after.begin(), good_after.end());
        FrameParser p;
        auto out = feed_all(p, f);
        CHECK(out.size() == 1);  // 只有下一帧的 tick
        CHECK(tick_symbol(out[0]) == "000001");
        CHECK(p.stats().crc_errors == 1);
        CHECK(p.stats().frames_ok == 1);
        CHECK(p.stats().resync_events == 0);
    }

    SUBCASE("载荷内容翻转 → 同走 CRC 拒帧") {
        auto f = build_frame(kFrameTypeTickBatch, ticks);
        flip_bit(f, kHeaderBytes + 10, 5);  // 载荷里翻一位
        f.insert(f.end(), good_after.begin(), good_after.end());
        FrameParser p;
        auto out = feed_all(p, f);
        CHECK(out.size() == 1);
        CHECK(p.stats().crc_errors == 1);
    }

    SUBCASE("count 翻转 → 长度互证失败 → 失步重同步") {
        auto f = build_frame(kFrameTypeTickBatch, ticks);
        flip_bit(f, 6, 0);  // count 字段（offset 6）翻一位：1→2，互证必破
        f.insert(f.end(), good_after.begin(), good_after.end());
        FrameParser p;
        auto out = feed_all(p, f);
        CHECK(out.size() == 1);
        CHECK(tick_symbol(out[0]) == "000001");
        CHECK(p.stats().resync_events >= 1);
        CHECK(p.stats().crc_errors == 0);
    }

    SUBCASE("payload_bytes 翻转 → 同走互证失败") {
        auto f = build_frame(kFrameTypeTickBatch, ticks);
        flip_bit(f, 12, 0);  // payload_bytes（offset 12）翻一位
        f.insert(f.end(), good_after.begin(), good_after.end());
        FrameParser p;
        auto out = feed_all(p, f);
        CHECK(out.size() == 1);
        CHECK(p.stats().resync_events >= 1);
    }

    SUBCASE("flags 翻转不参与校验 → 帧照常通过") {
        auto f = build_frame(kFrameTypeTickBatch, ticks, 0x0001);
        FrameParser p;
        auto out = feed_all(p, f);
        CHECK(out.size() == 1);
        CHECK(p.stats().frames_ok == 1);
        CHECK(p.stats().resync_events == 0);
    }

    SUBCASE("version 翻转 → 协议不匹配，致命断连") {
        auto f = build_frame(kFrameTypeTickBatch, ticks);
        flip_bit(f, 4, 0);  // version 1→0，长度互证仍通过
        FrameParser p;
        std::vector<Tick> out;
        CHECK_FALSE(p.on_bytes(f.data(), f.size(), out));
        CHECK(out.empty());
        CHECK(p.stats().protocol_errors == 1);
        // 致命后拒绝继续喂入
        CHECK_FALSE(p.on_bytes(good_after.data(), good_after.size(), out));
    }
}

TEST_CASE("解析器: 帧长上限边界（互证先于状态机移动）") {
    // 恰好 4096 ticks = 256KB：互证通过，正常解出
    std::vector<Tick> max_ticks;
    max_ticks.reserve(kMaxFrameTicks);
    for (std::uint32_t i = 0; i < kMaxFrameTicks; ++i) max_ticks.push_back(make_tick(i));
    const auto f = build_frame(kFrameTypeTickBatch, max_ticks);
    FrameParser p;
    auto out = feed_all(p, f);
    CHECK(out.size() == kMaxFrameTicks);

    // 4097 ticks（0x1001）× 64B = 262208B > 上限：互证拒绝，不触发巨大分配
    // 载荷填 0xFF：不构成任何互证通过的候选头（那会按 version 判致命断连）
    std::vector<std::uint8_t> over(kHeaderBytes + (kMaxFrameTicks + 1) * kTickBytes, 0xFF);
    FrameHeader h{};
    h.version = kFrameVersion;
    h.type = kFrameTypeTickBatch;
    h.count = static_cast<std::uint16_t>(kMaxFrameTicks + 1);
    h.payload_bytes = (kMaxFrameTicks + 1) * kTickBytes;
    h.crc32 = 0;
    encode_frame_header(h, over.data());

    FrameParser p2;
    std::vector<Tick> out2;
    REQUIRE(p2.on_bytes(over.data(), over.size(), out2));
    CHECK(out2.empty());
    CHECK(p2.stats().resync_events >= 1);
}

TEST_CASE("解析器: 载荷内假锚点 → 扫描跳过，在真帧头恢复") {
    // 构造载荷内嵌 "TTAP" + 不一致长度的假锚点：
    // symbol = TTAP + 控制字节，使假头 version=1、count=0x0010，payload_bytes 取自
    // ts_exchange 低 32 位（0x11223344，>256KB）→ 互证必拒，且不会误判 version 致命。
    Tick trap{};
    const std::uint8_t sym[8] = {'T', 'T', 'A', 'P', 0x01, 0x00, 0x10, 0x00};
    std::memcpy(trap.symbol, sym, 8);
    trap.ts_exchange_ms = 0x11223344ull;
    const auto bait = build_frame(kFrameTypeTickBatch, {trap});

    // 弄坏外层 magic（'T'→'X'）让解析器进入扫描，撞上载荷里的假锚点
    auto corrupted = bait;
    corrupted[0] = 'X';
    const std::vector<Tick> real = {make_tick(9000, "000002")};
    const auto good = build_frame(kFrameTypeTickBatch, real);

    std::vector<std::uint8_t> stream = corrupted;
    stream.insert(stream.end(), good.begin(), good.end());

    FrameParser p;
    auto out = feed_all(p, stream);
    REQUIRE(out.size() == 1);
    CHECK(tick_symbol(out[0]) == "000002");
    CHECK(out[0].ts_exchange_ms == 9000);
    CHECK(p.stats().resync_events >= 1);  // 假锚点被互证拒绝
    CHECK(p.stats().resync_bytes >= 1);   // 坏外层帧的字节按垃圾计
    CHECK(p.stats().crc_errors == 0);
}

TEST_CASE("解析器: 心跳帧") {
    const auto hb = build_frame(kFrameTypeHeartbeat, {});
    FrameParser p;
    auto out = feed_all(p, hb);
    CHECK(out.empty());
    CHECK(p.stats().frames_ok == 1);
    CHECK(p.stats().heartbeats == 1);
}

TEST_CASE("解析器: 控制型帧（type=2 查询请求，v1.1 互证分型）") {
    // 请求帧：count=0、payload 24B（非 64B 整数倍）——控制型互证只查上限
    std::string payload(24, '\0');
    payload[0] = '6';
    payload[1] = '0';
    FrameHeader h{};
    h.version = kFrameVersion;
    h.type = kFrameTypeQuery;
    h.count = 0;
    h.payload_bytes = static_cast<std::uint32_t>(payload.size());
    h.crc32 = crc32(reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size());
    std::vector<std::uint8_t> frame(kHeaderBytes + payload.size());
    encode_frame_header(h, frame.data());
    std::copy(payload.begin(), payload.end(),
              frame.begin() + static_cast<std::ptrdiff_t>(kHeaderBytes));

    FrameParser p;
    std::vector<Tick> out;
    std::vector<ControlFrame> controls;
    REQUIRE(p.on_bytes(frame.data(), frame.size(), out, &controls));
    CHECK(out.empty());
    REQUIRE(controls.size() == 1);
    CHECK(controls[0].type == kFrameTypeQuery);
    CHECK(controls[0].payload.size() == 24);
    CHECK(controls[0].payload[0] == '6');

    // 响应形态（count=N + 缺失块）：count 与 payload 不再互证，CRC 与上限把门
    std::string resp_payload = payload;  // 24B 假 ticks 区 + 4B + 16B 缺失块
    resp_payload += std::string(20, '\0');
    FrameHeader rh{};
    rh.version = kFrameVersion;
    rh.type = kFrameTypeQuery;
    rh.count = 1;
    rh.flags = 0x0002;
    rh.payload_bytes = static_cast<std::uint32_t>(resp_payload.size());
    rh.crc32 = crc32(reinterpret_cast<const std::uint8_t*>(resp_payload.data()),
                     resp_payload.size());
    std::vector<std::uint8_t> rframe(kHeaderBytes + resp_payload.size());
    encode_frame_header(rh, rframe.data());
    std::copy(resp_payload.begin(), resp_payload.end(),
              rframe.begin() + static_cast<std::ptrdiff_t>(kHeaderBytes));

    FrameParser p2;
    std::vector<ControlFrame> controls2;
    REQUIRE(p2.on_bytes(rframe.data(), rframe.size(), out, &controls2));
    REQUIRE(controls2.size() == 1);
    CHECK(controls2[0].flags == 0x0002);
    CHECK(controls2[0].payload.size() == 44);

    // 超限控制帧：上限防炸弹仍然生效（互证分型不豁免 cap）
    FrameHeader bh{};
    bh.version = kFrameVersion;
    bh.type = kFrameTypeQuery;
    bh.count = 0;
    bh.payload_bytes = kMaxFramePayloadBytes + 1;
    std::vector<std::uint8_t> bad(kHeaderBytes);
    encode_frame_header(bh, bad.data());
    std::memcpy(bad.data() + kHeaderBytes, kMagic, 4);  // 后随假锚点诱导扩容路径

    FrameParser p3;
    std::vector<ControlFrame> controls3;
    REQUIRE(p3.on_bytes(bad.data(), bad.size(), out, &controls3));
    CHECK(controls3.empty());
    CHECK(p3.stats().resync_events >= 1);
}

TEST_CASE("解析器: fuzz——随机流不错误行为，其后干净帧完整恢复") {
    std::mt19937 rng(42);
    FrameParser p;
    std::vector<Tick> out;
    constexpr int kRounds = 300;
    int recovered = 0;
    std::vector<std::uint64_t> recovered_ts;
    for (int round = 0; round < kRounds; ++round) {
        // 乱垃圾 + 密集假 magic（提高假锚点密度）
        const std::size_t garbage_len = rng() % 4096;
        std::vector<std::uint8_t> garbage(garbage_len);
        for (auto& b : garbage) b = static_cast<std::uint8_t>(rng());
        for (std::size_t i = 0; i + 4 < garbage_len; i += 997)
            std::memcpy(garbage.data() + i, kMagic, 4);
        // 其中 1/5 直接从完整帧打碎一个字段后混入（加重互证压力）
        if (round % 5 == 0 && garbage_len > kHeaderBytes) flip_bit(garbage, kHeaderBytes, 0);

        // v1.1 互证分型后，控制型帧防线较弱：随机流可能拼出"看似合法但 version≠1"
        // 的控制帧候选 → 按协议致命断连是正确行为（连接必须重开）。
        if (!p.on_bytes(garbage.data(), garbage.size(), out)) p = FrameParser{};

        // 恢复性：干净帧最多重试 3 次（fatal 可能来自垃圾半头+真帧字节的幽灵候选）
        const auto frame = build_frame(kFrameTypeTickBatch, {make_tick(50000u + round)});
        bool fed = false;
        for (int attempt = 0; attempt < 3 && !fed; ++attempt) {
            std::size_t pos = 0;
            bool fatal = false;
            while (pos < frame.size()) {
                const std::size_t step = 1 + rng() % 17;
                const std::size_t end = std::min(frame.size(), pos + step);
                if (!p.on_bytes(frame.data() + pos, end - pos, out)) {
                    fatal = true;
                    break;
                }
                pos = end;
            }
            if (fatal) {
                p = FrameParser{};  // 新连接重试
                continue;
            }
            fed = true;
        }
        if (fed) {
            ++recovered;
            recovered_ts.push_back(50000u + round);
        }
    }
    REQUIRE(recovered >= kRounds - 3);  // 垃圾半头吞真帧的极端情形允许少量重连重试
    REQUIRE(out.size() == recovered_ts.size());  // 垃圾永远不能造出 tick
    for (std::size_t i = 0; i < recovered_ts.size(); ++i)
        CHECK(out[i].ts_exchange_ms == recovered_ts[i]);
}
