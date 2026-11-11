#include "crc32.hpp"

#include <array>

namespace tickshelf {
namespace {

constexpr std::uint32_t kPoly = 0xEDB88320;  // IEEE 802.3 反射多项式

std::array<std::uint32_t, 256> make_table() {
    std::array<std::uint32_t, 256> t{};
    for (std::uint32_t i = 0; i < 256; ++i) {
        std::uint32_t c = i;
        for (int k = 0; k < 8; ++k) c = (c & 1) ? (c >> 1) ^ kPoly : c >> 1;
        t[i] = c;
    }
    return t;
}

const std::array<std::uint32_t, 256>& table() {
    static const std::array<std::uint32_t, 256> t = make_table();
    return t;
}

}  // namespace

std::uint32_t crc32_extend(std::uint32_t run, const std::uint8_t* data, std::size_t n) {
    const auto& t = table();
    for (std::size_t i = 0; i < n; ++i) run = t[(run ^ data[i]) & 0xFFu] ^ (run >> 8);
    return run;
}

std::uint32_t crc32(const std::uint8_t* data, std::size_t n) {
    return crc32_finish(crc32_extend(crc32_init(), data, n));
}

}  // namespace tickshelf
