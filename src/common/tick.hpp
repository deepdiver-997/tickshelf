#pragma once
// 线格式定义（docs/protocol.md）：Tick 64B + 帧头 20B，全部小端。
// 编解码逐字段手工 LE 读写，不依赖宿主端序与结构体内存布局（offsetof 断言钉住字段位置）。
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "fixed.hpp"

namespace tickshelf {

// ---- Tick（64B，唯一自然键 = (symbol, ts_exchange_ms)）----
struct Tick {
    char symbol[8];                // ASCII，NUL 填充，如 "600000\0\0"
    std::uint64_t ts_exchange_ms;  // 交易所时间戳（幂等唯一键的一部分）
    std::uint64_t ts_receive_ms;   // 采集端接收时间（链路延迟观测；不参与唯一键）
    price_t bid_price;
    price_t ask_price;
    price_t last_price;
    std::uint32_t bid_size;
    std::uint32_t ask_size;
    std::uint32_t last_size;
    std::uint8_t flags;  // bit0：last 无成交
    std::uint8_t _pad[3] = {};
};

static_assert(sizeof(Tick) == 64);
static_assert(offsetof(Tick, symbol) == 0);
static_assert(offsetof(Tick, ts_exchange_ms) == 8);
static_assert(offsetof(Tick, ts_receive_ms) == 16);
static_assert(offsetof(Tick, bid_price) == 24);
static_assert(offsetof(Tick, ask_price) == 32);
static_assert(offsetof(Tick, last_price) == 40);
static_assert(offsetof(Tick, bid_size) == 48);
static_assert(offsetof(Tick, ask_size) == 52);
static_assert(offsetof(Tick, last_size) == 56);
static_assert(offsetof(Tick, flags) == 60);

inline constexpr std::uint8_t kTickFlagNoLast = 0x01;  // flags bit0：last 无成交

void encode_tick(const Tick& t, std::uint8_t* out /*[64]*/);
Tick decode_tick(const std::uint8_t* in /*[64]*/);

// symbol 打包 u64 做 Ingestor map key（ADR-011：零分配；仅本进程内使用，不落线不跨进程）。
std::uint64_t pack_symbol(const char sym[8]);

// 去 NUL 填充的 symbol 视图（日志/落库用）。
std::string_view tick_symbol(const Tick& t);

// ---- 帧头（20B）----
inline constexpr std::uint8_t kMagic[4] = {'T', 'T', 'A', 'P'};
inline constexpr std::uint8_t kFrameVersion = 1;
inline constexpr std::uint8_t kFrameTypeTickBatch = 0;
inline constexpr std::uint8_t kFrameTypeHeartbeat = 1;
inline constexpr std::uint8_t kFrameTypeQuery = 2;  // v1.1：查询请求/响应（双向）
inline constexpr std::uint8_t kFrameTypeLatest = 3;  // v1.2：最新价请求/响应（双向）
inline constexpr std::size_t kHeaderBytes = 20;
inline constexpr std::size_t kTickBytes = 64;
inline constexpr std::uint32_t kMaxFrameTicks = 4096;
inline constexpr std::uint32_t kMaxFramePayloadBytes = kMaxFrameTicks * kTickBytes;  // 256KB

struct FrameHeader {
    std::uint8_t version;
    std::uint8_t type;
    std::uint16_t count;          // 本帧 tick 数（heartbeat 为 0）
    std::uint16_t flags;          // bit0：混沌帧（含乱序/重复，M4 测试用）
    std::uint32_t payload_bytes;  // = count × 64
    std::uint32_t crc32;          // payload 的 CRC-32（IEEE 802.3），帧头本身不校验
};

// 防线 1（长度交叉互证）：按 type 分型——
//   tick 批/心跳：count × 64 == payload_bytes（冗余字段表达同一信息，单字段翻转
//                无法同时骗过两者）；
//   控制型（type ≥ 2，查询请求/响应）：载荷结构不是 64B 整数倍（请求 24B、响应含
//                缺失块），count 对其无意义——互证退化为上限检查（防炸弹分配），
//                结构合法性由消费方（queryd / 客户端）校验。
// 上限检查必须发生在移动状态机/扩缓冲之前，两种 type 一致。
constexpr bool header_lengths_valid(const FrameHeader& h) {
    if (h.payload_bytes > kMaxFramePayloadBytes) return false;
    if (h.type == kFrameTypeTickBatch || h.type == kFrameTypeHeartbeat)
        return static_cast<std::uint64_t>(h.count) * kTickBytes == h.payload_bytes;
    return true;
}

inline bool magic_matches(const std::uint8_t* p /*[4]*/) { return std::memcmp(p, kMagic, 4) == 0; }

void encode_frame_header(const FrameHeader& h, std::uint8_t* out /*[20]*/);
FrameHeader decode_frame_header(const std::uint8_t* in /*[20]*/);

}  // namespace tickshelf
