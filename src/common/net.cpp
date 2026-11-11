#include "net.hpp"

#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>

namespace tickshelf::net {

int tcp_connect(const std::string& host, std::uint16_t port) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* list = nullptr;
    char service[16];
    std::snprintf(service, sizeof(service), "%u", static_cast<unsigned>(port));
    if (getaddrinfo(host.c_str(), service, &hints, &list) != 0) return -1;

    int fd = -1;
    for (const addrinfo* ai = list; ai != nullptr; ai = ai->ai_next) {
        const int s = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (s < 0) continue;
        if (::connect(s, ai->ai_addr, ai->ai_addrlen) == 0) {
            fd = s;
            break;
        }
        ::close(s);
    }
    freeaddrinfo(list);
    if (fd >= 0) {
        int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));  // 帧/命令都别攒
    }
    return fd;
}

int tcp_listen(const std::string& bind_host, std::uint16_t port, int backlog) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE | AI_NUMERICHOST;  // 绑定地址总是字面量
    addrinfo* list = nullptr;
    char service[16];
    std::snprintf(service, sizeof(service), "%u", static_cast<unsigned>(port));
    if (getaddrinfo(bind_host.empty() ? "0.0.0.0" : bind_host.c_str(), service, &hints, &list) != 0)
        return -1;

    int fd = -1;
    for (const addrinfo* ai = list; ai != nullptr; ai = ai->ai_next) {
        const int s = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (s < 0) continue;
        int one = 1;
        ::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        if (::bind(s, ai->ai_addr, ai->ai_addrlen) == 0 && ::listen(s, backlog) == 0) {
            fd = s;
            break;
        }
        ::close(s);
    }
    freeaddrinfo(list);
    return fd;
}

int tcp_accept(int listen_fd) {
    const int fd = ::accept(listen_fd, nullptr, nullptr);
    if (fd >= 0) {
        int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    }
    return fd;
}

void set_recv_timeout_ms(int fd, int ms) {
    timeval tv{};
    tv.tv_sec = ms / 1000;
    tv.tv_usec = (ms % 1000) * 1000;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}

void set_nonblock(int fd, bool on) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) return;
    ::fcntl(fd, F_SETFL, on ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK));
}

}  // namespace tickshelf::net
