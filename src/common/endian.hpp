#pragma once
// 小端读写原语——线格式全部按 LE 逐字段编解码，不依赖宿主端序与结构体内存布局。
#include <cstdint>

namespace tickshelf {

inline void put_u16(std::uint8_t* p, std::uint16_t v) {
    p[0] = static_cast<std::uint8_t>(v);
    p[1] = static_cast<std::uint8_t>(v >> 8);
}

inline void put_u32(std::uint8_t* p, std::uint32_t v) {
    p[0] = static_cast<std::uint8_t>(v);
    p[1] = static_cast<std::uint8_t>(v >> 8);
    p[2] = static_cast<std::uint8_t>(v >> 16);
    p[3] = static_cast<std::uint8_t>(v >> 24);
}

inline void put_u64(std::uint8_t* p, std::uint64_t v) {
    put_u32(p, static_cast<std::uint32_t>(v));
    put_u32(p + 4, static_cast<std::uint32_t>(v >> 32));
}

inline std::uint16_t get_u16(const std::uint8_t* p) {
    return static_cast<std::uint16_t>(p[0]) | (static_cast<std::uint16_t>(p[1]) << 8);
}

inline std::uint32_t get_u32(const std::uint8_t* p) {
    return static_cast<std::uint32_t>(p[0]) | (static_cast<std::uint32_t>(p[1]) << 8) |
           (static_cast<std::uint32_t>(p[2]) << 16) | (static_cast<std::uint32_t>(p[3]) << 24);
}

inline std::uint64_t get_u64(const std::uint8_t* p) {
    return static_cast<std::uint64_t>(get_u32(p)) | (static_cast<std::uint64_t>(get_u32(p + 4)) << 32);
}

}  // namespace tickshelf
