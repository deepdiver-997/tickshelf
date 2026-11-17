#pragma once
// RESP2 客户端，手写不用 hiredis（README：协议层手写是学习目标）。
//
// 两层结构：
//   RespParser  纯增量回复解析器——与网络无关，可直连单测（喂任意切分字节）。
//   RespClient  非阻塞 socket + poll 就绪驱动。send_pipeline 只把字节交给内核
//               即返回；collect_replies 在 POLLIN 就绪（回复字节真正到达）时被
//               内核唤醒——不是轮询，也不占用超时等待。两者显式带截止时间，
//               且可从其他线程 interrupt() 立即唤醒（Engine 超时放弃时用）。
//               exec() = send + collect 的兼容封装（旧行为：按序收齐全部回复）。
//
// 请求是 bulk string 数组（binary-safe：64B Tick 字节直接做 member/参数）。
#include <atomic>
#include <chrono>
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
    // 不收新字节，只解析缓冲里已有的完整回复（send/collect 解耦后跨调用收割残留）。
    void drain(std::vector<RespValue>& out);
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

// I/O 截止时间（steady clock）。remaining_ms() 直接喂 poll：-1 = 无限等待。
class RespDeadline {
public:
    static RespDeadline after_ms(std::int64_t ms) {
        return RespDeadline(std::chrono::steady_clock::now() + std::chrono::milliseconds(ms));
    }
    static RespDeadline never() { return RespDeadline(Clock::time_point::max()); }
    bool expired() const {
        return at_ != Clock::time_point::max() && Clock::now() >= at_;
    }
    std::int64_t remaining_ms() const {
        if (at_ == Clock::time_point::max()) return -1;
        const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
            at_ - Clock::now()).count();
        return left < 0 ? 0 : left;
    }

private:
    using Clock = std::chrono::steady_clock;
    explicit RespDeadline(Clock::time_point at) : at_(at) {}
    Clock::time_point at_;
};

class RespClient {
public:
    // default_timeout_ms：未显式给截止时间的调用（exec）用它。连接后 fd 非阻塞，
    // 一切等待由 poll 就绪驱动，不再依赖 SO_RCVTIMEO。
    explicit RespClient(std::int64_t default_timeout_ms = 5000)
        : default_timeout_ms_(default_timeout_ms) {}

    // 阻塞 connect；false = 连不上（errno 保留在日志侧）。
    bool connect(const std::string& host, std::uint16_t port);
    void close() {
        fd_.reset();
        parser_ = RespParser{};
        interrupted_.store(false, std::memory_order_release);
    }
    bool connected() const { return fd_.is_open(); }
    bool interrupted() const { return interrupted_.load(std::memory_order_acquire); }

    // 立即唤醒任何阻塞中的 send/collect；被中断的连接不可信，调用方应 close 后
    // 重连。幂等、线程安全（Engine 超时放弃时从其他线程调用）。
    void interrupt();

    // 发送：编码并把全部字节交给内核缓冲即返回，不等任何回复。
    // false = 连接失败 / 超时 / 被打断；连接不可信，调用方应 close 后重连。
    bool send_pipeline(const std::vector<std::vector<std::string>>& commands,
                       RespDeadline deadline);
    // 收回复：out 追加（不清空）。上次调用已在缓冲的回复先收割、不等网络；
    // out.size() 可能超过 expected（流水线多发的部分留给下次收）。
    bool collect_replies(std::size_t expected, RespDeadline deadline,
                         std::vector<RespValue>& out);

    // 兼容旧路径：发送 + 按序收齐全部回复（默认截止时间）。
    // false = 连接层失败（写/读/超时），此时连接不可信，调用方应 close 后重连。
    // 注意：回复里的 Error 类型（如 -ERR）不是连接失败，由调用方按需检查。
    bool exec(const std::vector<std::vector<std::string>>& commands,
              std::vector<RespValue>& replies);

    RespDeadline default_deadline() const { return RespDeadline::after_ms(default_timeout_ms_); }

private:
    bool send_all(const char* data, std::size_t size, RespDeadline deadline);
    bool wait_ready(short events, RespDeadline deadline);

    net::UniqueFd fd_;
    RespParser parser_;
    std::int64_t default_timeout_ms_;
    std::atomic<bool> interrupted_{false};
};

}  // namespace tickshelf
