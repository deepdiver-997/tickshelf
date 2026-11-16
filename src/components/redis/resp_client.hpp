#pragma once
// RESP2 客户端，手写不用 hiredis（README：协议层手写是学习目标）。
//
// 两层结构：
//   RespParser  纯增量回复解析器——与网络无关，可直连单测（喂任意切分字节）。
//   RespClient  阻塞 socket + 请求编码 + pipeline（一次写入全部命令、按序收全部回复）。
//
// 请求是 bulk string 数组（binary-safe：64B Tick 字节直接做 member/参数）。
#include <cstdint>
#include <string>
#include <vector>

#include "common/net.hpp"

namespace tickshelf {

struct RespValue {
    enum class Type : std::uint8_t { Simple, Error, Integer, Bulk, Null, Array };
    Type type = Type::Null;
    std::string str;               // Simple / Error / Bulk 内容（Bulk 为原始字节）
    std::int64_t integer = 0;      // Integer
    std::vector<RespValue> array;  // Array
};

// RESP2 回复增量解析器。feed() 喂任意切分，解出的完整回复追加到 out。
class RespParser {
public:
    void feed(const std::uint8_t* data, std::size_t n, std::vector<RespValue>& out);
    bool failed() const { return failed_; }

private:
    // 1 = 解出一条；0 = 数据不足；-1 = 协议错误。从 buf_[pos_] 开始。
    int try_parse(RespValue& out);
    // 读一行（不含 CRLF）。incomplete 时返回 false 且不动 pos_。
    bool read_line(std::string& line);

    std::string buf_;
    std::size_t pos_ = 0;
    bool failed_ = false;
};

// 编码一条命令（bulk string 数组）追加到 out。
void encode_command(const std::vector<std::string>& args, std::string& out);

class RespClient {
public:
    // 阻塞连接；false = 连不上（errno 保留在日志侧）。
    bool connect(const std::string& host, std::uint16_t port);
    void close() { fd_.reset(); parser_ = RespParser{}; }
    bool connected() const { return fd_.is_open(); }

    // pipeline：一次发送全部命令，按序收齐全部回复。
    // false = 连接层失败（写/读/超时），此时连接不可信，调用方应 close 后重连。
    // 注意：回复里的 Error 类型（如 -ERR）不是连接失败，由调用方按需检查。
    bool exec(const std::vector<std::vector<std::string>>& commands,
              std::vector<RespValue>& replies);

private:
    bool write_all(const std::string& data);

    net::UniqueFd fd_;
    RespParser parser_;
};

}  // namespace tickshelf
