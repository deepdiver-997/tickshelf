#include "resp_client.hpp"

#include <poll.h>
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
    drain(out);
}

void RespParser::drain(std::vector<RespValue>& out) {
    if (failed_) return;
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

// ---- 就绪驱动的客户端 ----

void RespClient::interrupt() {
    interrupted_.store(true, std::memory_order_release);
    if (fd_.is_open()) net::shutdown_fd(fd_.get());  // 阻塞中的 poll/recv 立即返回
}

bool RespClient::connect(const std::string& host, std::uint16_t port) {
    close();
    const int fd = net::tcp_connect(host, port);
    if (fd < 0) return false;
    fd_.reset(fd);
    net::set_nonblock(fd_.get(), true);  // 等待一律 poll 就绪驱动；recv 只在就绪后调用
    return true;
}

bool RespClient::wait_ready(short events, RespDeadline deadline) {
    for (;;) {
        if (interrupted()) return false;
        if (deadline.expired()) return false;
        pollfd p{};
        p.fd = fd_.get();
        p.events = events;
        const int rc = ::poll(&p, 1, static_cast<int>(deadline.remaining_ms()));
        if (rc < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (rc == 0) return false;  // 截止到期
        if (p.revents & (POLLERR | POLLNVAL)) return false;
        // POLLHUP 也返回 true：交给 recv 用 EOF/连接重置定论，不在这里猜。
        if (p.revents & (events | POLLHUP)) return true;
    }
}

bool RespClient::send_all(const char* data, std::size_t size, RespDeadline deadline) {
    std::size_t sent = 0;
    while (sent < size) {
        if (interrupted() || deadline.expired()) return false;
        const ssize_t n = ::send(fd_.get(), data + sent, size - sent, 0);
        if (n > 0) {
            sent += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (!wait_ready(POLLOUT, deadline)) return false;
            continue;
        }
        return false;  // 连接错误 / 对端关闭
    }
    return true;
}

bool RespClient::send_pipeline(const std::vector<std::vector<std::string>>& commands,
                               RespDeadline deadline) {
    if (!connected() || commands.empty()) return false;
    std::string wire;
    for (const auto& cmd : commands) encode_command(cmd, wire);
    return send_all(wire.data(), wire.size(), deadline);
}

bool RespClient::collect_replies(std::size_t expected, RespDeadline deadline,
                                 std::vector<RespValue>& out) {
    if (!connected()) return false;
    if (expected == 0) return true;
    parser_.drain(out);  // 先收割上次调用残留在缓冲里的完整回复，不等网络
    while (out.size() < expected) {
        if (parser_.failed()) return false;
        if (interrupted() || deadline.expired()) return false;
        if (!wait_ready(POLLIN, deadline)) return false;
        std::uint8_t tmp[16384];
        const ssize_t n = ::recv(fd_.get(), tmp, sizeof(tmp), 0);
        if (n > 0) {
            parser_.feed(tmp, static_cast<std::size_t>(n), out);
            continue;
        }
        if (n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) continue;
        return false;  // EOF / 连接重置（含被 interrupt 的 shutdown）
    }
    return true;
}

bool RespClient::exec(const std::vector<std::vector<std::string>>& commands,
                      std::vector<RespValue>& replies) {
    if (!connected() || commands.empty()) return false;
    replies.clear();
    replies.reserve(commands.size());
    if (!send_pipeline(commands, default_deadline())) return false;
    return collect_replies(commands.size(), default_deadline(), replies);
}

}  // namespace tickshelf
