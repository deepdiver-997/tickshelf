#pragma once
// 阻塞 TCP socket 小工具（ADR-008：阻塞读写 + pipeline；本仓库只面向 POSIX）。
// RAII UniqueFd 是 move-only 的——ADR-011：零 shared_ptr。
#include <cstdint>
#include <string>
#include <unistd.h>

namespace tickshelf::net {

class UniqueFd {
public:
    UniqueFd() = default;
    explicit UniqueFd(int fd) : fd_(fd) {}
    ~UniqueFd() { reset(); }
    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;
    UniqueFd(UniqueFd&& o) noexcept : fd_(o.fd_) { o.fd_ = -1; }
    UniqueFd& operator=(UniqueFd&& o) noexcept {
        if (this != &o) {
            reset();
            fd_ = o.fd_;
            o.fd_ = -1;
        }
        return *this;
    }
    bool is_open() const { return fd_ >= 0; }
    int get() const { return fd_; }
    int release() {
        const int f = fd_;
        fd_ = -1;
        return f;
    }
    void reset(int fd = -1) {
        if (fd_ >= 0) ::close(fd_);
        fd_ = fd;
    }

private:
    int fd_ = -1;
};

// 以下失败一律返回 -1（errno 保留）。
int tcp_connect(const std::string& host, std::uint16_t port);              // 阻塞 connect
int tcp_listen(const std::string& bind_host, std::uint16_t port, int backlog = 64);
int tcp_accept(int listen_fd);                                             // 阻塞 accept
void set_recv_timeout_ms(int fd, int ms);                                  // SO_RCVTIMEO
void set_nonblock(int fd, bool on);

}  // namespace tickshelf::net
