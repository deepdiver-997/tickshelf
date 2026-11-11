// queryd —— TTAP 双向查询进程（ADR-015）：libtickshelf 查询语义的协议薄壳。
//
// 查询请求（client→daemon，type=2，payload 24B）：sym[8] + from_ms u64LE + to_ms u64LE
// 查询响应（daemon→client，type=2，flags bit1=带缺失区间）：
//   payload = N×Tick64B + u32LE missing_count + missing_count × (u64 first + u64 last)
// 查询命中的缺口由 RangeReader 懒触发回补（backfill 键），feedgen 执行——本进程只读。
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "common/endian.hpp"
#include "common/frame_writer.hpp"
#include "common/net.hpp"
#include "libtickshelf/frame_parser.hpp"
#include "libtickshelf/reader.hpp"
#include "libtickshelf/sharded_store.hpp"

namespace {

using namespace tickshelf;

volatile sig_atomic_t g_stop = 0;
void on_signal(int) { g_stop = 1; }

std::uint64_t steady_ms() {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                          std::chrono::steady_clock::now().time_since_epoch())
                                          .count());
}

constexpr std::uint64_t kIdleTimeoutMs = 30000;  // 查询客户端可长连，空闲阈值放宽

struct QueryOptions {
    std::string bind_host = "127.0.0.1";
    std::uint16_t port = 9910;
    std::string redis_host = "127.0.0.1";
    std::uint16_t redis_port = 6379;
    std::string db = "ticks.db";
    int shards = 1;  // 落库分片数（ADR-020）——必须与写入侧一致（纯函数路由）
};

struct QuerySession {
    net::UniqueFd fd;
    FrameParser parser;
    std::string out_buf;       // 待写响应（非阻塞 + POLLOUT 排空）
    std::size_t out_pos = 0;
    std::uint64_t last_rx_ms = 0;
    bool dead = false;
};

class QueryDaemon {
public:
    QueryDaemon(const QueryOptions& opt, TickStore& store) : opt_(opt), store_(store) {}

    bool start() {
        listen_fd_.reset(net::tcp_listen(opt_.bind_host, opt_.port));
        if (!listen_fd_.is_open()) {
            std::fprintf(stderr, "queryd: listen %s:%u failed\n", opt_.bind_host.c_str(),
                         opt_.port);
            return false;
        }
        net::set_nonblock(listen_fd_.get(), true);
        if (!rc_.connect(opt_.redis_host, opt_.redis_port))
            std::fprintf(stderr, "queryd: redis unreachable at startup, will retry\n");
        return true;
    }

    int run() {
        std::vector<pollfd> pfds;
        while (!g_stop) {
            pfds.clear();
            pfds.push_back({listen_fd_.get(), POLLIN, 0});
            for (const QuerySession& s : sessions_) {
                const short ev = POLLIN | (s.out_pos < s.out_buf.size() ? POLLOUT : 0);
                pfds.push_back({s.fd.get(), ev, 0});
            }
            const int rc = ::poll(pfds.data(), static_cast<nfds_t>(pfds.size()), 500);
            if (rc < 0 && errno != EINTR) break;
            const std::uint64_t now2 = steady_ms();

            if (pfds[0].revents & POLLIN) accept_conns(now2);
            for (std::size_t i = 0; i < sessions_.size(); ++i) {
                QuerySession& s = sessions_[i];
                const short rev = pfds[1 + i].revents;
                if (rev & POLLOUT) flush_out(s);
                if (rev & (POLLIN | POLLHUP | POLLERR)) read_session(s, now2);
                if (!s.dead && now2 - s.last_rx_ms > kIdleTimeoutMs) s.dead = true;
            }
            compact();
        }
        return 0;
    }

private:
    void accept_conns(std::uint64_t now) {
        for (;;) {
            const int fd = net::tcp_accept(listen_fd_.get());
            if (fd < 0) return;
            net::set_nonblock(fd, true);
            QuerySession s;
            s.fd.reset(fd);
            s.last_rx_ms = now;
            sessions_.push_back(std::move(s));
        }
    }

    void read_session(QuerySession& s, std::uint64_t now) {
        if (s.dead) return;
        std::uint8_t buf[65536];
        std::vector<Tick> ticks;             // tick 型帧出口（查询客户端不发）
        std::vector<ControlFrame> controls;  // 查询请求出口
        for (;;) {
            const ssize_t n = ::recv(s.fd.get(), buf, sizeof(buf), 0);
            if (n > 0) {
                ticks.clear();
                controls.clear();
                if (!s.parser.on_bytes(buf, static_cast<std::size_t>(n), ticks, &controls)) {
                    s.dead = true;  // version ≠ 1：无法降级
                    return;
                }
                for (const ControlFrame& c : controls) {
                    if (c.type == kFrameTypeQuery && !handle_query(c, s)) s.dead = true;
                    if (c.type == kFrameTypeLatest && !handle_latest(c, s)) s.dead = true;
                }
                s.last_rx_ms = now;
                if (n < static_cast<ssize_t>(sizeof(buf))) return;
                continue;
            }
            if (n == 0) {
                s.dead = true;
                return;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            if (errno == EINTR) continue;
            s.dead = true;
            return;
        }
    }

    // 查询请求 → RangeReader → 响应帧入 out_buf
    bool handle_query(const ControlFrame& c, QuerySession& s) {
        if (c.payload.size() < 24) return true;  // 坏请求忽略（不致命）
        char sym[9] = {};
        std::memcpy(sym, c.payload.data(), 8);
        const std::string symbol(sym, strnlen(sym, 8));
        const std::uint64_t from = get_u64(c.payload.data() + 8);
        const std::uint64_t to = get_u64(c.payload.data() + 16);

        QueryResult res;
        if (!reader_.read(symbol, from, to, res)) return true;  // Redis 故障：静默丢查询

        // 段式 v2：热侧载荷已 verbatim，直接拼响应（无逐 tick 编解码）
        std::string payload = std::move(res.payload);
        payload.reserve(payload.size() + 4 + res.missing.size() * 16);
        std::uint8_t mc[4];
        put_u32(mc, static_cast<std::uint32_t>(res.missing.size()));
        payload.append(reinterpret_cast<const char*>(mc), 4);
        for (const auto& [lo, hi] : res.missing) {
            std::uint8_t pair[16];
            put_u64(pair, lo);
            put_u64(pair + 8, hi);
            payload.append(reinterpret_cast<const char*>(pair), 16);
        }
        const std::uint16_t flags = res.missing.empty() ? 0x0000 : 0x0002;  // bit1=带缺失区间
        s.out_buf += build_raw_frame(kFrameTypeQuery, flags, payload,
                                     static_cast<std::uint16_t>(res.ts.size()));
        return true;
    }

    // 最新价请求（payload=sym[8]）→ HGET latest → 响应帧（payload=64B tick 或空）
    bool handle_latest(const ControlFrame& c, QuerySession& s) {
        if (c.payload.size() < 8) return true;
        char sym[9] = {};
        std::memcpy(sym, c.payload.data(), 8);
        const std::string symbol(sym, strnlen(sym, 8));

        bool found = false;
        Tick t{};
        if (!read_latest(rc_, symbol, found, t)) return true;  // 后端故障：静默丢查询
        std::string payload;
        if (found) {
            payload.resize(kTickBytes);
            encode_tick(t, reinterpret_cast<std::uint8_t*>(payload.data()));
        }
        s.out_buf += build_raw_frame(kFrameTypeLatest, 0x0000, payload,
                                     found ? 1 : 0);  // count=1 有投影 / 0 无
        return true;
    }

    void flush_out(QuerySession& s) {
        while (s.out_pos < s.out_buf.size()) {
            const ssize_t n = ::send(s.fd.get(), s.out_buf.data() + s.out_pos,
                                     s.out_buf.size() - s.out_pos, MSG_NOSIGNAL);
            if (n > 0) {
                s.out_pos += static_cast<std::size_t>(n);
                continue;
            }
            if (n < 0 && errno == EINTR) continue;
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
            s.dead = true;
            return;
        }
        s.out_buf.clear();
        s.out_pos = 0;
    }

    void compact() {
        sessions_.erase(std::remove_if(sessions_.begin(), sessions_.end(),
                                       [](const QuerySession& s) { return s.dead; }),
                        sessions_.end());
    }

    QueryOptions opt_;
    net::UniqueFd listen_fd_;
    std::vector<QuerySession> sessions_;
    RespClient rc_;
    TickStore& store_;  // 外部持有（main 按 --shards 装配，存活期覆盖本进程）
    RangeReader reader_{rc_, store_};
};

}  // namespace

int main(int argc, char** argv) {
    QueryOptions opt;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--bind") opt.bind_host = argv[++i];
        else if (a == "--port") opt.port = static_cast<std::uint16_t>(std::atoi(argv[++i]));
        else if (a == "--redis-host") opt.redis_host = argv[++i];
        else if (a == "--redis-port") opt.redis_port = static_cast<std::uint16_t>(std::atoi(argv[++i]));
        else if (a == "--db") opt.db = argv[++i];
        else if (a == "--shards") opt.shards = std::atoi(argv[++i]);
        else {
            std::fprintf(stderr,
                         "usage: queryd [--bind H] [--port P] [--redis-host H] [--redis-port P] "
                         "[--db PATH] [--shards N]\n");
            return 2;
        }
    }

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    signal(SIGPIPE, SIG_IGN);

    auto store = make_tick_store(opt.shards);
    if (!store->open(opt.db)) {
        std::fprintf(stderr, "queryd: cannot open db %s (%d shards)\n", opt.db.c_str(),
                     opt.shards);
        return 1;
    }
    QueryDaemon d(opt, *store);
    if (!d.start()) return 1;
    std::fprintf(stderr, "queryd: listening %s:%u, redis %s:%u, db %s\n", opt.bind_host.c_str(),
                 opt.port, opt.redis_host.c_str(), opt.redis_port, opt.db.c_str());
    return d.run();
}
