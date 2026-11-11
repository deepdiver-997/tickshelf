#include "resp_client.hpp"

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>

namespace tickshelf {
namespace {

inline bool is_resp_type_char(std::uint8_t c) {
    return c == '+' || c == '-' || c == ':' || c == '$' || c == '*';
}

}  // namespace

// ---- 编码 ----

void encode_command(const std::vector<std::string>& args, std::string& out) {
    char head[32];
    std::snprintf(head, sizeof(head), "*%zu\r\n", args.size());
    out.append(head);
    for (const auto& a : args) {
        std::snprintf(head, sizeof(head), "$%zu\r\n", a.size());
        out.append(head);
        out.append(a);
        out.append("\r\n");
    }
}

// ---- 增量解析 ----

bool RespParser::read_line(std::string& line) {
    const std::size_t nl = buf_.find("\r\n", pos_);
    if (nl == std::string::npos || nl < pos_) return false;
    line.assign(buf_, pos_, nl - pos_);
    pos_ = nl + 2;
    return true;
}

int RespParser::try_parse(RespValue& out) {
    const std::size_t start = pos_;  // 不完整时的整体回退点：重入必须从 tag 字节重新开始
    if (pos_ >= buf_.size()) return 0;
    const std::uint8_t tag = static_cast<std::uint8_t>(buf_[pos_]);
    if (!is_resp_type_char(tag)) {
        failed_ = true;
        return -1;
    }
    ++pos_;

    switch (tag) {
    case '+':
    case '-': {
        std::string line;
        if (!read_line(line)) {
            pos_ = start;
            return 0;
        }
        out.type = (tag == '+') ? RespValue::Type::Simple : RespValue::Type::Error;
        out.str = std::move(line);
        out.integer = 0;
        out.array.clear();
        return 1;
    }
    case ':': {
        std::string line;
        if (!read_line(line)) {
            pos_ = start;
            return 0;
        }
        out.type = RespValue::Type::Integer;
        out.integer = std::strtoll(line.c_str(), nullptr, 10);
        out.str.clear();
        out.array.clear();
        return 1;
    }
    case '$': {
        std::string line;
        if (!read_line(line)) {
            pos_ = start;
            return 0;
        }
        const std::int64_t len = std::strtoll(line.c_str(), nullptr, 10);
        if (len < 0) {  // $-1 空体 bulk（RESP2 的 null）
            out.type = RespValue::Type::Null;
            out.str.clear();
            out.integer = 0;
            out.array.clear();
            return 1;
        }
        if (len > (64 << 20)) {  // 单条回复上限 64MB，防异常长度
            failed_ = true;
            return -1;
        }
        const std::size_t need = static_cast<std::size_t>(len) + 2;  // 数据 + CRLF
        if (buf_.size() - pos_ < need) {
            pos_ = start;  // 数据不足：整体回退
            return 0;
        }
        out.type = RespValue::Type::Bulk;
        out.str.assign(buf_, pos_, static_cast<std::size_t>(len));
        out.integer = 0;
        out.array.clear();
        pos_ += need;
        return 1;
    }
    case '*': {
        std::string line;
        if (!read_line(line)) {
            pos_ = start;
            return 0;
        }
        const std::int64_t len = std::strtoll(line.c_str(), nullptr, 10);
        if (len < 0) {  // *-1 空数组
            out.type = RespValue::Type::Null;
            out.str.clear();
            out.integer = 0;
            out.array.clear();
            return 1;
        }
        out.type = RespValue::Type::Array;
        out.str.clear();
        out.integer = 0;
        out.array.resize(static_cast<std::size_t>(len));
        for (auto& e : out.array) {
            const int r = try_parse(e);
            if (r != 1) {
                if (r == 0) pos_ = start;  // 子元素不完整：整体回退（不能停在子元素中间）
                return r;
            }
        }
        return 1;
    }
    }
    return -1;  // 不可达
}

void RespParser::feed(const std::uint8_t* data, std::size_t n, std::vector<RespValue>& out) {
    if (failed_) return;
    buf_.append(reinterpret_cast<const char*>(data), n);
    for (;;) {
        RespValue v;
        const int r = try_parse(v);
        if (r == 1) {
            out.push_back(std::move(v));
            continue;
        }
        if (r == -1) return;  // failed_
        break;                // 数据不足
    }
    if (pos_ > 0) {
        buf_.erase(0, pos_);
        pos_ = 0;
    }
}

// ---- 阻塞客户端 ----

bool RespClient::connect(const std::string& host, std::uint16_t port) {
    close();
    const int fd = net::tcp_connect(host, port);
    if (fd < 0) return false;
    fd_.reset(fd);
    net::set_recv_timeout_ms(fd_.get(), 5000);  // 半死连接不挂死调用方（超时按连接失败处理）
    return true;
}

bool RespClient::write_all(const std::string& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        const ssize_t n = ::send(fd_.get(), data.data() + sent, data.size() - sent, 0);
        if (n <= 0) {
            if (n < 0 && (errno == EINTR)) continue;
            return false;
        }
        sent += static_cast<std::size_t>(n);
    }
    return true;
}

bool RespClient::exec(const std::vector<std::vector<std::string>>& commands,
                      std::vector<RespValue>& replies) {
    if (!connected() || commands.empty()) return false;
    replies.clear();
    replies.reserve(commands.size());

    std::string wire;
    for (const auto& cmd : commands) encode_command(cmd, wire);
    if (!write_all(wire)) return false;

    // 临时喂给一个带缓冲收件的解析循环：read_some 推进 parser_，
    // parser_ 里解出的完整回复由 out 承接。
    while (replies.size() < commands.size()) {
        std::uint8_t tmp[16384];
        const ssize_t n = ::recv(fd_.get(), tmp, sizeof(tmp), 0);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            return false;  // 连接失败/超时
        }
        parser_.feed(tmp, static_cast<std::size_t>(n), replies);
        if (parser_.failed()) return false;
    }
    return true;
}

}  // namespace tickshelf
