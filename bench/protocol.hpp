#pragma once
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace tickshelf::bench {
inline constexpr std::size_t kWireHeaderBytes = 24;
enum class MessageType : std::uint8_t { Json = 1, Binary = 2, End = 3, Ack = 4 };
struct Message {
    MessageType type = MessageType::End;
    std::uint64_t sequence = 0;
    std::string payload;
};
// IQB1, version u8=1, type u8, reserved u16=0, physical record index u64,
// payload length u32, IEEE CRC32 u32. Little endian; no claimed vendor framing.
std::string encode_message(const Message& message);
class MessageParser {
public:
    bool feed(std::string_view bytes, std::vector<Message>& out, std::string& error);
    bool incomplete() const { return !buffer_.empty(); }
private:
    std::string buffer_;
};
}  // namespace tickshelf::bench
