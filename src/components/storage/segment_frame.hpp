#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace tickshelf::storage {

// TSG1 frame header. The body is opaque here: callers own the record codec,
// size policy, index and commit protocol.
inline constexpr std::size_t kSegmentHeaderBytes = 32;

struct SegmentHeader {
    std::uint64_t payload_bytes;
    std::uint64_t records;
    std::uint32_t crc32;

    bool operator==(const SegmentHeader&) const = default;
};

std::array<char, kSegmentHeaderBytes> encode_segment_header(SegmentHeader header);
std::optional<SegmentHeader> decode_segment_header(std::string_view bytes);

}  // namespace tickshelf::storage
