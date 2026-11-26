#include "segment_frame.hpp"

namespace tickshelf::storage {

std::array<char, kSegmentHeaderBytes> encode_segment_header(SegmentHeader header) {
    std::array<char, kSegmentHeaderBytes> bytes{'T', 'S', 'G', '1'};
    const auto put = [&](std::size_t offset, std::uint64_t value, unsigned count) {
        for (unsigned i = 0; i < count; ++i)
            bytes[offset + i] = static_cast<char>(value >> (8 * i));
    };
    put(4, 1, 4);
    put(8, header.payload_bytes, 8);
    put(16, header.records, 8);
    put(24, header.crc32, 4);
    return bytes;
}

std::optional<SegmentHeader> decode_segment_header(std::string_view bytes) {
    if (bytes.size() != kSegmentHeaderBytes || bytes.substr(0, 4) != "TSG1")
        return std::nullopt;
    const auto get = [&](std::size_t offset, unsigned count) {
        std::uint64_t value = 0;
        for (unsigned i = 0; i < count; ++i)
            value |= std::uint64_t(static_cast<unsigned char>(bytes[offset + i])) << (8 * i);
        return value;
    };
    if (get(4, 4) != 1 || get(28, 4) != 0) return std::nullopt;
    return SegmentHeader{get(8, 8), get(16, 8), static_cast<std::uint32_t>(get(24, 4))};
}

}  // namespace tickshelf::storage
