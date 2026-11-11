#include <doctest/doctest.h>

#include <cstring>
#include <string>

#include "common/crc32.hpp"

namespace {
std::uint32_t crc_of(const char* s) {
    return tickshelf::crc32(reinterpret_cast<const std::uint8_t*>(s), std::strlen(s));
}
}  // namespace

TEST_CASE("CRC32: IEEE 802.3 标准向量") {
    CHECK(crc_of("123456789") == 0xCBF43926u);
    CHECK(crc_of("The quick brown fox jumps over the lazy dog") == 0x414FA339u);
    CHECK(crc_of("") == 0x00000000u);  // 空载荷 CRC == 0，与心跳帧约定一致
}

TEST_CASE("CRC32: 流式分块 == 一步到位") {
    const std::string s = "tickshelf line format: 64 bytes per tick, 20 bytes per header";
    const auto* p = reinterpret_cast<const std::uint8_t*>(s.data());
    auto run = tickshelf::crc32_init();
    run = tickshelf::crc32_extend(run, p, 7);
    run = tickshelf::crc32_extend(run, p + 7, 1);
    run = tickshelf::crc32_extend(run, p + 8, s.size() - 8);
    CHECK(tickshelf::crc32_finish(run) == tickshelf::crc32(p, s.size()));
}
