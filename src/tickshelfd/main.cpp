// tickshelfd —— TTAP 接入守护进程（ADR-009 第二层）。
//
// 单线程 poll 时间片循环（ADR-011）：poll(连接 fds, timeout=距下次 flush)
//   → 可读连接逐个 on_bytes（非阻塞读）→ FrameParser → 进程内 ingest
//   → 到点 flush（阻塞 RESP pipeline 写 Redis）。
// 背压交给 TCP 窗口：Redis 慢 → flush 慢 → 接收放缓 → 内核反压 driver。
// 无状态：崩溃即重启，丢的只是在途帧（driver 重连续发，M1 口径内）。
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>

#include <algorithm>
#include <memory>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "common/net.hpp"
#include "common/startup_config.hpp"
#include "libtickshelf/frame_parser.hpp"
#include "libtickshelf/ingestor.hpp"
#include "libtickshelf/resp_client.hpp"
#include "libtickshelf/sharded_store.hpp"

namespace {

volatile sig_atomic_t g_stop = 0;

void on_signal(int) { g_stop = 1; }

std::uint64_t steady_ms() {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                          std::chrono::steady_clock::now().time_since_epoch())
                                          .count());
}

constexpr std::uint64_t kHeartbeatTimeoutMs = 5000;  // protocol.md：5s 无任何帧判死

// 每会话每轮读配额（ADR-011 注记，灌流基准抓到的饿死缺陷）：时间片循环的 flush/stats
// 定时器只在 read_session 返回后才被检查；持续洪泛下内核缓冲永远有数据（每次 recv 满
// 64KB → 循环继续），无配额 = 单个高速客户端可无限推迟 flush，pending 在内存里无限膨胀。
// 配额用尽即让出（数据留内核，下轮 poll 立刻可读），总吞吐不变，flush 节奏有界。
constexpr std::uint64_t kSessionReadBudgetBytes = 1 << 20;  // 1MB，64KB 的整数倍

// ConnSession（ADR-011 传输层）：每 TCP 连接一个——字节缓冲在内核，
// 帧状态机随连接生死；与 Ingestor（数据层，随 symbol/交易日生死）分离。
struct ConnSession {
    tickshelf::net::UniqueFd fd;
    tickshelf::FrameParser parser;
    tickshelf::FrameParser::Stats reported;  // 已计入守护进程聚合的部分（增量累加用）
    std::uint64_t last_rx_ms = 0;
    bool dead = false;
};

using Options = tickshelf::startup::Options;

struct DaemonStats {
    std::uint64_t conns_total = 0;
    std::uint64_t ticks_ingested = 0;
    std::uint64_t flushes_ok = 0;
    std::uint64_t flush_errors = 0;
    std::uint64_t frames_ok = 0;
    std::uint64_t crc_errors = 0;
    std::uint64_t resync_events = 0;
    std::uint64_t protocol_errors = 0;
    // 写穿模式：内联落库统计（同步阻塞主循环的时长——ADR-012 线程化触发条件的观测来源）
    std::uint64_t rows_upserted = 0;
    std::uint64_t stall_sum_ms = 0;
    std::uint64_t stall_max_ms = 0;
    std::uint64_t stall_count = 0;
};

class Daemon {
public:
    explicit Daemon(const Options& opt) : opt_(opt) {}

    bool start() {
        listen_fd_.reset(tickshelf::net::tcp_listen(opt_.bind_host, opt_.port));
        if (!listen_fd_.is_open()) {
            std::fprintf(stderr, "tickshelfd: listen %s:%u failed\n", opt_.bind_host.c_str(),
                         opt_.port);
            return false;
        }
        tickshelf::net::set_nonblock(listen_fd_.get(), true);
        if (opt_.persist == "sqlite") {
            store_ = tickshelf::make_tick_store(opt_.shards);
            if (!store_->open(opt_.db)) {
                std::fprintf(stderr, "tickshelfd: cannot open db %s (%d shards)\n",
                             opt_.db.c_str(), opt_.shards);
                return false;
            }
        }
        return true;
    }

    int run() {
        std::uint64_t next_flush = steady_ms() + opt_.flush_interval_ms;
        std::uint64_t next_stats = steady_ms() + 10000;
        std::uint64_t next_publish = steady_ms() + 2000;
        std::vector<pollfd> pfds;
        std::vector<tickshelf::Tick> ticks;  // 复用的解出缓冲

        while (!g_stop) {
            const std::uint64_t now = steady_ms();
            pfds.clear();
            pfds.push_back({listen_fd_.get(), POLLIN, 0});
            for (const ConnSession& s : sessions_) pfds.push_back({s.fd.get(), POLLIN, 0});

            std::uint64_t timeout = next_flush > now ? next_flush - now : 0;
            timeout = std::min<std::uint64_t>(timeout, 500);  // 心跳巡检上限
            const int rc = ::poll(pfds.data(), static_cast<nfds_t>(pfds.size()),
                                  static_cast<int>(timeout));
            if (rc < 0 && errno != EINTR) break;

            const std::uint64_t now2 = steady_ms();
            if (pfds[0].revents & POLLIN) accept_conns(now2);

            for (std::size_t i = 0; i < sessions_.size(); ++i) {
                const short rev = pfds[1 + i].revents;
                if (rev & (POLLIN | POLLHUP | POLLERR)) read_session(sessions_[i], now2, ticks);
                if (!sessions_[i].dead && now2 - sessions_[i].last_rx_ms > kHeartbeatTimeoutMs) {
                    std::fprintf(stderr, "tickshelfd: heartbeat timeout, drop fd=%d\n",
                                 sessions_[i].fd.get());
                    sessions_[i].dead = true;
                }
            }
            compact();

            if (now2 >= next_flush) {
                flush(now2);
                next_flush = now2 + opt_.flush_interval_ms;
            }
            if (now2 >= next_stats) {
                print_stats();
                next_stats = now2 + 10000;
            }
            // 负载暴露（ADR-013）：观测用，发布失败静默
            if (now2 >= next_publish) {
                char body[256];
                const std::uint64_t stall_avg =
                    stats_.stall_count ? stats_.stall_sum_ms / stats_.stall_count : 0;
                std::snprintf(body, sizeof(body),
                              "persist=%s;ingested=%llu;pending=%llu;flushes=%llu;"
                              "flush_err=%llu;rows=%llu;stall_avg=%llu;stall_max=%llu",
                              opt_.persist.c_str(),
                              static_cast<unsigned long long>(stats_.ticks_ingested),
                              static_cast<unsigned long long>(ingestor_.pending_ticks()),
                              static_cast<unsigned long long>(stats_.flushes_ok),
                              static_cast<unsigned long long>(stats_.flush_errors),
                              static_cast<unsigned long long>(stats_.rows_upserted),
                              static_cast<unsigned long long>(stall_avg),
                              static_cast<unsigned long long>(stats_.stall_max_ms));
                tickshelf::publish_stats(rc_, "tickshelfd", body);
                next_publish = now2 + 2000;
            }
        }

        std::fprintf(stderr, "tickshelfd: stopping, final flush\n");
        flush(steady_ms());
        print_stats();
        return 0;
    }

private:
    void accept_conns(std::uint64_t now) {
        for (;;) {
            const int fd = tickshelf::net::tcp_accept(listen_fd_.get());
            if (fd < 0) return;  // EAGAIN / 错误：本轮结束
            tickshelf::net::set_nonblock(fd, true);
            ConnSession s;
            s.fd.reset(fd);
            s.last_rx_ms = now;
            sessions_.push_back(std::move(s));
            ++stats_.conns_total;
        }
    }

    void read_session(ConnSession& s, std::uint64_t now, std::vector<tickshelf::Tick>& ticks) {
        if (s.dead) return;
        std::uint8_t buf[65536];
        std::uint64_t budget = kSessionReadBudgetBytes;
        for (;;) {
            const ssize_t n = ::recv(s.fd.get(), buf, sizeof(buf), 0);
            if (n > 0) {
                ticks.clear();
                if (!s.parser.on_bytes(buf, static_cast<std::size_t>(n), ticks)) {
                    std::fprintf(stderr, "tickshelfd: protocol error (version), close fd=%d\n",
                                 s.fd.get());
                    ++stats_.protocol_errors;
                    s.dead = true;
                    return;
                }
                for (const tickshelf::Tick& t : ticks) {
                    ingestor_.ingest(t);
                    ++stats_.ticks_ingested;
                }
                const tickshelf::FrameParser::Stats& st = s.parser.stats();
                stats_.frames_ok += st.frames_ok - s.reported.frames_ok;
                stats_.crc_errors += st.crc_errors - s.reported.crc_errors;
                stats_.resync_events += st.resync_events - s.reported.resync_events;
                s.reported = st;
                s.last_rx_ms = now;
                budget -= static_cast<std::uint64_t>(n);
                if (budget == 0) return;  // 配额用尽让出：flush/stats 定时器不被洪泛饿死
                if (n < static_cast<ssize_t>(sizeof(buf))) return;  // 本轮内核缓冲已读干
                continue;
            }
            if (n == 0) {  // 对端关闭
                s.dead = true;
                return;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            if (errno == EINTR) continue;
            s.dead = true;  // 真错误
            return;
        }
    }

    void compact() {
        sessions_.erase(std::remove_if(sessions_.begin(), sessions_.end(),
                                       [](const ConnSession& s) { return s.dead; }),
                        sessions_.end());
    }

    // 固定间隔冲刷（ADR-003）。连接失败/写失败不 drain——数据留在 Ingestor，
    // TCP 反压 driver；写失败已 drain 的批重投回 Ingestor（at-least-once，ZSET 按
    // member 幂等）。
    //
    // 写穿模式（ADR-012 修订）：flush pipeline 即认领租约（lease→state→ZADD 顺序，
    // persister 抢占被仲裁挡下）→ 内联 SQLite upsert（同步阻塞主循环，时长计入
    // stall 统计）→ 批量标记 done。SQLite 故障 → release_claims 降级回读回路径。
    void flush(std::uint64_t now) {
        (void)now;
        if (!rc_.connected() && !rc_.connect(opt_.redis_host, opt_.redis_port)) return;
        auto batches = ingestor_.drain();
        if (batches.empty()) return;

        auto reingest = [&] {
            for (const tickshelf::Batch& b : batches)
                for (const tickshelf::Tick& t : b.ticks) ingestor_.ingest(t);
            ++stats_.flush_errors;
            std::fprintf(stderr, "tickshelfd: flush failed, %zu batches re-ingested\n",
                         batches.size());
        };

        if (opt_.persist != "sqlite") {  // classic：数据进 Redis，落库交给 persister
            if (!tickshelf::flush_batches(rc_, batches)) {
                rc_.close();
                reingest();
                return;
            }
            ++stats_.flushes_ok;
            return;
        }

        // ---- 写穿 ----
        std::vector<char> claimed;
        tickshelf::FlushOptions fo;
        fo.claim_lease = true;
        fo.worker_id = "daemon";
        if (!tickshelf::flush_batches(rc_, batches, fo, &claimed)) {
            rc_.close();
            reingest();
            return;
        }

        // flush 级合并事务（ADR-012 修订）：全部已认领批一次 BEGIN…COMMIT，fsync
        // 从每批一次降到每 flush 一次——实测 stall 地板 64ms（256 符号 × ~0.25ms
        // fsync，仅地板就越过 ADR-013 的 10% 预算触发线）→ 亚毫秒。故障回滚半径
        // 从单批扩大到整个 flush，恢复语义不变（release_claims → persister 读回）。
        const std::uint64_t t0 = steady_ms();
        std::vector<const std::vector<tickshelf::Tick>*> mine;
        mine.reserve(batches.size());
        for (std::size_t i = 0; i < batches.size(); ++i)
            if (claimed[i]) mine.push_back(&batches[i].ticks);  // 租约被他占：留给 persister
        const std::int64_t ins = mine.empty() ? 0 : store_->upsert_flush(mine);
        const std::uint64_t stall = steady_ms() - t0;
        stats_.stall_sum_ms += stall;
        stats_.stall_max_ms = std::max(stats_.stall_max_ms, stall);
        ++stats_.stall_count;

        if (ins < 0) {  // SQLite 故障：整 flush 放弃认领，退回读回路径
            tickshelf::release_claims(rc_, batches);  // 批回无主 pending，persister 接手
            ++stats_.flush_errors;
            std::fprintf(stderr, "tickshelfd: sqlite write failed, %zu batches released\n",
                         batches.size());
            return;
        }
        stats_.rows_upserted += static_cast<std::uint64_t>(ins);
        if (!tickshelf::finish_written_batches(rc_, batches)) {
            rc_.close();  // 数据已落库；state/lease 由 TTL+persister 最终收尾
            ++stats_.flush_errors;
            return;
        }
        ++stats_.flushes_ok;
    }

    void print_stats() {
        const char* stall = "";
        char stall_buf[64] = {};
        if (stats_.stall_count > 0) {
            std::snprintf(stall_buf, sizeof(stall_buf), " stall=%llu/%llums",
                          static_cast<unsigned long long>(stats_.stall_sum_ms /
                                                          stats_.stall_count),
                          static_cast<unsigned long long>(stats_.stall_max_ms));
            stall = stall_buf;
        }
        std::fprintf(stderr,
                     "tickshelfd: ingested=%llu pending=%llu flushes=%llu flush_err=%llu "
                     "frames=%llu crc_err=%llu resync=%llu conns=%zu rows=%llu%s\n",
                     static_cast<unsigned long long>(stats_.ticks_ingested),
                     static_cast<unsigned long long>(ingestor_.pending_ticks()),
                     static_cast<unsigned long long>(stats_.flushes_ok),
                     static_cast<unsigned long long>(stats_.flush_errors),
                     static_cast<unsigned long long>(stats_.frames_ok),
                     static_cast<unsigned long long>(stats_.crc_errors),
                     static_cast<unsigned long long>(stats_.resync_events), sessions_.size(),
                     static_cast<unsigned long long>(stats_.rows_upserted), stall);
    }

    Options opt_;
    tickshelf::net::UniqueFd listen_fd_;
    std::vector<ConnSession> sessions_;
    tickshelf::Ingestor ingestor_;
    tickshelf::RespClient rc_;
    std::unique_ptr<tickshelf::TickStore> store_;  // N=1 单文件 / N>1 分片（ADR-020）
    DaemonStats stats_;
};

}  // namespace

int main(int argc, char** argv) {
    Options opt;
    const auto status = tickshelf::startup::configure(
        tickshelf::startup::Component::Tickshelfd, argc, argv, opt);
    if (status != tickshelf::startup::Status::Run)
        return status == tickshelf::startup::Status::ExitSuccess ? 0 : 2;

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    signal(SIGPIPE, SIG_IGN);  // 写断连 socket 走 send 返回值，不靠信号

    Daemon d(opt);
    if (!d.start()) return 1;
    std::fprintf(stderr, "tickshelfd: listening %s:%u, redis %s:%u, flush %llums, persist=%s%s\n",
                 opt.bind_host.c_str(), opt.port, opt.redis_host.c_str(), opt.redis_port,
                 static_cast<unsigned long long>(opt.flush_interval_ms), opt.persist.c_str(),
                 opt.persist == "sqlite" ? opt.db.c_str() : "");
    return d.run();
}
