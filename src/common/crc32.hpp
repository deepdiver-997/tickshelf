#pragma once
// CRC-32（IEEE 802.3：反射多项式 0xEDB88320，init 0xFFFFFFFF，final xor 0xFFFFFFFF）。
// 流式接口供帧解析器边收载荷边算：init → extend×N → finish（与一步到位的 crc32() 等价）。
#include <cstddef>
#include <cstdint>

namespace tickshelf {

constexpr std::uint32_t crc32_init() { return 0xFFFFFFFFu; }

std::uint32_t crc32_extend(std::uint32_t run, const std::uint8_t* data, std::size_t n);

constexpr std::uint32_t crc32_finish(std::uint32_t run) { return run ^ 0xFFFFFFFFu; }

std::uint32_t crc32(const std::uint8_t* data, std::size_t n);

}  // namespace tickshelf
