#include "protocol.hpp"
#include "record.hpp"
#include "common/crc32.hpp"
#include <cstring>
#include <stdexcept>

namespace tickshelf::bench {
namespace {
std::uint64_t read_le(const char* p, unsigned count) {
    std::uint64_t value = 0;
    for (unsigned i = 0; i < count; ++i) value |= static_cast<std::uint64_t>(static_cast<unsigned char>(p[i])) << (8 * i);
    return value;
}
void put_le(std::string& out, std::uint64_t value, unsigned count) {
    for (unsigned i = 0; i < count; ++i) out.push_back(static_cast<char>(value >> (8 * i)));
}
}
std::string encode_message(const Message& m) {
    const auto type = static_cast<unsigned>(m.type);
    if (m.payload.size() > kMaxPayloadBytes || type < 1 || type > 4 || (m.type == MessageType::End && !m.payload.empty()))
        throw std::invalid_argument("invalid experimental frame");
    std::string out = "IQB1";
    out.push_back(1);
    out.push_back(static_cast<char>(type));
    put_le(out, 0, 2);
    put_le(out, m.sequence, 8);
    put_le(out, m.payload.size(), 4);
    put_le(out, tickshelf::crc32(reinterpret_cast<const std::uint8_t*>(m.payload.data()), m.payload.size()), 4);
    out += m.payload;
    return out;
}

bool MessageParser::feed(std::string_view bytes, std::vector<Message>& out, std::string& error) {
    // Caller feeds socket-sized chunks, not an unbounded concatenation of frames.
    if (bytes.size() > kMaxPayloadBytes + kWireHeaderBytes || buffer_.size() + bytes.size() > 2 * (kMaxPayloadBytes + kWireHeaderBytes)) {
        error = "frame buffer limit exceeded"; return false;
    }
    buffer_.append(bytes);
    std::size_t pos = 0;
    while (buffer_.size() - pos >= kWireHeaderBytes) {
        const char* h = buffer_.data() + pos;
        const unsigned type = static_cast<unsigned char>(h[5]);
        const auto length = read_le(h + 16, 4);
        if (std::memcmp(h, "IQB1", 4) || h[4] != 1 || type < 1 || type > 4 || read_le(h + 6, 2) != 0 || length > kMaxPayloadBytes || (type == 3 && length != 0)) {
            error = "invalid experimental frame header"; return false;
        }
        if (buffer_.size() - pos < kWireHeaderBytes + length) break;
        const std::string_view payload(h + kWireHeaderBytes, static_cast<std::size_t>(length));
        if (tickshelf::crc32(reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size()) != read_le(h + 20, 4)) {
            error = "frame CRC mismatch"; return false;
        }
        out.push_back({static_cast<MessageType>(type), read_le(h + 8, 8), std::string(payload)});
        pos += kWireHeaderBytes + length;
    }
    buffer_.erase(0, pos);
    return true;
}
}  // namespace tickshelf::bench
