#include "tick.hpp"

#include "endian.hpp"

namespace tickshelf {

void encode_tick(const Tick& t, std::uint8_t* out) {
    std::memcpy(out, t.symbol, 8);  // ASCII 按字节序写入，不做整数解释（回避端序歧义）
    put_u64(out + 8, t.ts_exchange_ms);
    put_u64(out + 16, t.ts_receive_ms);
    put_u64(out + 24, static_cast<std::uint64_t>(t.bid_price));
    put_u64(out + 32, static_cast<std::uint64_t>(t.ask_price));
    put_u64(out + 40, static_cast<std::uint64_t>(t.last_price));
    put_u32(out + 48, t.bid_size);
    put_u32(out + 52, t.ask_size);
    put_u32(out + 56, t.last_size);
    out[60] = t.flags;
    out[61] = out[62] = out[63] = 0;
}

Tick decode_tick(const std::uint8_t* in) {
    Tick t;
    std::memcpy(t.symbol, in, 8);
    t.ts_exchange_ms = get_u64(in + 8);
    t.ts_receive_ms = get_u64(in + 16);
    t.bid_price = static_cast<price_t>(get_u64(in + 24));
    t.ask_price = static_cast<price_t>(get_u64(in + 32));
    t.last_price = static_cast<price_t>(get_u64(in + 40));
    t.bid_size = get_u32(in + 48);
    t.ask_size = get_u32(in + 52);
    t.last_size = get_u32(in + 56);
    t.flags = in[60];
    t._pad[0] = t._pad[1] = t._pad[2] = 0;
    return t;
}

std::uint64_t pack_symbol(const char sym[8]) {
    std::uint64_t key;
    std::memcpy(&key, sym, 8);  // 宿主序即可：键只在进程内 map 用，永不落线
    return key;
}

std::string_view tick_symbol(const Tick& t) {
    const auto* end = static_cast<const char*>(std::memchr(t.symbol, '\0', 8));
    return end ? std::string_view(t.symbol, static_cast<std::size_t>(end - t.symbol))
               : std::string_view(t.symbol, 8);
}

void encode_frame_header(const FrameHeader& h, std::uint8_t* out) {
    std::memcpy(out, kMagic, 4);
    out[4] = h.version;
    out[5] = h.type;
    put_u16(out + 6, h.count);
    put_u16(out + 8, h.flags);
    put_u16(out + 10, 0);  // reserved
    put_u32(out + 12, h.payload_bytes);
    put_u32(out + 16, h.crc32);
}

FrameHeader decode_frame_header(const std::uint8_t* in) {
    FrameHeader h;
    // magic 由调用方（状态机）先行匹配，这里只解字段
    h.version = in[4];
    h.type = in[5];
    h.count = get_u16(in + 6);
    h.flags = get_u16(in + 8);
    h.payload_bytes = get_u32(in + 12);
    h.crc32 = get_u32(in + 16);
    return h;
}

}  // namespace tickshelf
