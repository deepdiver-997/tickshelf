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
#include <unistd.h>

#include <algorithm>
#include <memory>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <string>
#include <vector>

#include "common/net.hpp"
#include "common/startup_config.hpp"
#include "components/engine/engine.hpp"
#include "components/engine/write_policy.hpp"
#include "components/ingest/frame_parser.hpp"
#include "components/ingest/ingestor.hpp"
#include "components/redis/redis_shard_pool.hpp"
#include "components/redis/resp_client.hpp"
#include "components/storage/raw_journal.hpp"
#include "components/storage/sharded_store.hpp"
#include "components/storage/storage_shard_pool.hpp"

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

// Engine 背压：submit 拒绝的尾部留在 retry buffer；超过上限就不再 poll 会话
// （数据留内核，TCP 窗口反压 driver），等 worker 排空后恢复读取。
constexpr std::size_t kMaxEngineRetryTicks = 65536;
constexpr std::size_t kRecvBytes = 65536;
// A recv can finish the parser's buffered maximum frame and emit more frames.
// Reserve the entire conservative upper bound BEFORE reading any socket.
constexpr std::size_t kMaxTicksPerRecv = tickshelf::kMaxFrameTicks +
                                        kRecvBytes / tickshelf::kTickBytes + 1;
static_assert(kMaxEngineRetryTicks >= kMaxTicksPerRecv);

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
        if (opt_.persist == "storage" && !opt_.engine) {
            std::fprintf(stderr,
                         "tickshelfd: --persist storage requires --engine on "
                         "(classic path has no direct-to-storage mode)\n");
            return false;
        }
        if (opt_.raw_journal && !(opt_.engine && opt_.persist == "storage")) {
            std::fprintf(stderr,
                         "tickshelfd: --raw-journal requires --engine on --persist storage "
                         "(write-through uses the Redis lease as its recovery authority)\n");
            return false;
        }
        if (opt_.engine_pipeline && !(opt_.engine && opt_.persist == "sqlite")) {
            std::fprintf(stderr,
                         "tickshelfd: --engine-pipeline requires --engine on --persist sqlite "
                         "(pipeline is a write-through topology)\n");
            return false;
        }
        if (opt_.engine) {
            if (!start_engine()) return false;
        } else if (opt_.persist == "sqlite") {
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
        std::uint64_t next_engine = steady_ms() + engine_tick_interval();
        std::uint64_t next_stats = steady_ms() + 10000;
        std::uint64_t next_publish = steady_ms() + 2000;
        std::vector<pollfd> pfds;
        std::vector<tickshelf::Tick> ticks;  // 复用的解出缓冲

        while (!g_stop) {
            const std::uint64_t now = steady_ms();
            // Retry the oldest decoded tail before admitting any new socket input.
            if (engine_) engine_pump(now);
            // Do not spin polling readable sockets whose older input is blocked.
            const bool engine_backpressure = engine_ && !engine_can_read();
            pfds.clear();
            pfds.push_back({listen_fd_.get(), POLLIN, 0});
            if (!engine_backpressure)
                for (const ConnSession& s : sessions_) pfds.push_back({s.fd.get(), POLLIN, 0});

            const std::uint64_t next_deadline = engine_ ? next_engine : next_flush;
            std::uint64_t timeout = next_deadline > now ? next_deadline - now : 0;
            timeout = std::min<std::uint64_t>(timeout, 500);  // 心跳巡检上限
            const int rc = ::poll(pfds.data(), static_cast<nfds_t>(pfds.size()),
                                  static_cast<int>(timeout));
            if (rc < 0 && errno != EINTR) break;

            const std::uint64_t now2 = steady_ms();
            // accept 前快照：pfds 只覆盖快照时的会话。accept_conns 会向 sessions_
            // 追加新连接，旧代码按新 size 索引旧 pfds → 越界读（UB，ASAN 必炸）。
            // 新连接下轮 poll 再服务（timeout ≤500ms 本就是巡检节奏）。
            const std::size_t ready = engine_backpressure ? 0 : sessions_.size();
            if (pfds[0].revents & POLLIN) accept_conns(now2);

            for (std::size_t i = 0; i < ready; ++i) {
                const short rev = pfds[1 + i].revents;
                if (rev & (POLLIN | POLLHUP | POLLERR)) read_session(sessions_[i], now2, ticks);
                if (!sessions_[i].dead && now2 - sessions_[i].last_rx_ms > kHeartbeatTimeoutMs) {
                    std::fprintf(stderr, "tickshelfd: heartbeat timeout, drop fd=%d\n",
                                 sessions_[i].fd.get());
                    sessions_[i].dead = true;
                }
            }
            compact();

            if (engine_) {
                if (now2 >= next_engine) {
                    engine_pump(now2);
                    next_engine = now2 + engine_tick_interval();
                }
            } else if (now2 >= next_flush) {
                flush(now2);
                next_flush = now2 + opt_.flush_interval_ms;
            }
            if (now2 >= next_stats) {
                print_stats();
                next_stats = now2 + 10000;
            }
            // 负载暴露（ADR-013）：观测用，发布失败静默。StorageOnly 没有 Redis
            // 端口，负载键发布整体跳过（也不做无意义的重连尝试）。
            if (now2 >= next_publish && !engine_storage_only_) {
                char body[2048];
                const std::uint64_t stall_avg =
                    stats_.stall_count ? stats_.stall_sum_ms / stats_.stall_count : 0;
                const tickshelf::EngineStats engine_stats =
                    engine_ ? engine_->stats() : tickshelf::EngineStats{};
                const std::uint64_t pending =
                    engine_ ? engine_stats.pending_records : ingestor_.pending_ticks();
                const std::uint64_t rows =
                    engine_ ? engine_stats.inserted_unique_records : stats_.rows_upserted;
                std::snprintf(body, sizeof(body),
                              "persist=%s;engine=%s;ingested=%llu;pending=%llu;flushes=%llu;"
                              "flush_err=%llu;rows=%llu;stall_avg=%llu;stall_max=%llu",
                              opt_.persist.c_str(), engine_ ? "on" : "off",
                              static_cast<unsigned long long>(stats_.ticks_ingested),
                              static_cast<unsigned long long>(pending),
                              static_cast<unsigned long long>(stats_.flushes_ok),
                              static_cast<unsigned long long>(stats_.flush_errors),
                              static_cast<unsigned long long>(rows),
                              static_cast<unsigned long long>(stall_avg),
                              static_cast<unsigned long long>(stats_.stall_max_ms));
                if (engine_) {
                    char extra[1536];
                    std::snprintf(extra, sizeof(extra),
                                  ";e_submitted=%llu;e_accepted=%llu;e_rejected=%llu;"
                                  "e_dedup=%llu;e_durable=%llu;e_publish=%llu;e_degraded=%llu;"
                                  "e_retries=%llu;e_finish_pending=%llu;e_pending=%llu;"
                                  "e_queue=%llu;retry_buf=%zu;e_published=%llu;"
                                  "e_committed_input=%llu;e_inserted_unique=%llu;e_sink_consumed=%llu",
                                  static_cast<unsigned long long>(engine_stats.submitted_records),
                                  static_cast<unsigned long long>(engine_stats.accepted_records),
                                  static_cast<unsigned long long>(engine_stats.rejected_records),
                                  static_cast<unsigned long long>(engine_stats.dedup_dropped),
                                  static_cast<unsigned long long>(engine_stats.durable_records),
                                  static_cast<unsigned long long>(
                                      engine_stats.publish_only_records),
                                  static_cast<unsigned long long>(engine_stats.degraded_records),
                                  static_cast<unsigned long long>(engine_stats.retries),
                                  static_cast<unsigned long long>(
                                      engine_stats.finish_pending_records),
                                  static_cast<unsigned long long>(engine_stats.pending_records),
                                  static_cast<unsigned long long>(engine_stats.queue_depth),
                                  retry_ticks_.size(),
                                  static_cast<unsigned long long>(engine_stats.published_records),
                                  static_cast<unsigned long long>(engine_stats.committed_input_records),
                                  static_cast<unsigned long long>(engine_stats.inserted_unique_records),
                                  static_cast<unsigned long long>(engine_stats.sink_consumed_records));
                    std::strncat(body, extra, sizeof(body) - std::strlen(body) - 1);
                }
                if (!rc_.connected()) rc_.connect(opt_.redis_host, opt_.redis_port);
                tickshelf::publish_stats(rc_, "tickshelfd", body);
                next_publish = now2 + 2000;
            }
        }

        if (engine_) {
            const auto deadline = std::chrono::steady_clock::now() +
                                  std::chrono::milliseconds(opt_.engine_shutdown_ms);
            std::fprintf(stderr, "tickshelfd: stopping, engine drain (retry_buf=%zu)\n",
                         retry_ticks_.size());
            while (!retry_ticks_.empty() && std::chrono::steady_clock::now() < deadline) {
                engine_pump(steady_ms());
                if (!retry_ticks_.empty())
                    engine_->wait_idle(std::chrono::milliseconds(20));
            }
            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - std::chrono::steady_clock::now());
            const auto report = engine_->drain(
                remaining.count() > 0 ? remaining : std::chrono::milliseconds(0));
            if (!report.queue_complete || !retry_ticks_.empty() ||
                (engine_durable_mode_ && !report.storage_complete))
                std::fprintf(stderr,
                             "tickshelfd: WARNING engine drain incomplete pending=%llu committed_input=%llu "
                             "degraded=%llu retry_buf=%zu\n",
                             static_cast<unsigned long long>(report.pending_records),
                             static_cast<unsigned long long>(report.durable_records),
                             static_cast<unsigned long long>(report.degraded_records),
                             retry_ticks_.size());
        } else {
            std::fprintf(stderr, "tickshelfd: stopping, final flush\n");
            flush(steady_ms());
        }
        print_stats();
        return 0;
    }

private:
    // Engine 装配：N 条 Redis 连接 + 写穿时 N 个 SQLite 分片文件。Redis 连接
    // 由分片 worker 懒连接/重连；存储分片在这里打开失败即启动失败。
    bool start_engine() {
        tickshelf::EngineConfig cfg;
        cfg.shards = static_cast<std::size_t>(std::max(1, opt_.shards));
        cfg.write_mode = tickshelf::write_mode_from_persist(opt_.persist);
        cfg.pipeline = opt_.engine_pipeline;
        cfg.batch_records = opt_.engine_batch_records;
        cfg.batch_bytes = opt_.engine_batch_bytes;
        cfg.batch_ms = opt_.engine_batch_ms;
        cfg.inflight = static_cast<std::size_t>(opt_.engine_inflight);
        cfg.max_pending_records = opt_.engine_max_pending_records;
        cfg.dedup_bytes = opt_.engine_dedup ? opt_.engine_dedup_bytes : 0;
        cfg.lease_ttl_s = opt_.engine_lease_ttl_s;
        cfg.worker_id = "daemon-" + std::to_string(::getpid());
        cfg.retry_ms = opt_.engine_retry_ms;
        cfg.shutdown_ms = opt_.engine_shutdown_ms;

        auto redis_shards = cfg.write_mode == tickshelf::WriteMode::StorageOnly
                                ? std::vector<std::unique_ptr<tickshelf::RedisShard>>{}
                                : tickshelf::make_redis_shards(
                                      opt_.redis_host, opt_.redis_port,
                                      cfg.shards * (cfg.pipeline ? 2 : 1));
        std::vector<std::unique_ptr<tickshelf::StorageShard>> storage_shards;
        if (cfg.write_mode != tickshelf::WriteMode::PublishOnly) {
            storage_shards = tickshelf::make_sqlite_storage_shards(opt_.db, cfg.shards);
            if (storage_shards.size() != cfg.shards) {
                std::fprintf(stderr, "tickshelfd: cannot open engine db %s (%zu shards)\n",
                             opt_.db.c_str(), cfg.shards);
                return false;
            }
            // 非拥有指针：identity 计数聚合用（RawJournalShard 装饰后基类指针取不到）。
            for (const auto& s : storage_shards)
                if (auto* p = dynamic_cast<tickshelf::SqliteStorageShard*>(s.get()))
                    sqlite_shards_.push_back(p);
            if (opt_.raw_journal) {
                // ADR-027：本体先于索引；open 内完成水位之后帧的重放（幂等 upsert）。
                std::vector<tickshelf::RawJournalShard::RecoveryStats> replay;
                auto journaled = tickshelf::make_raw_journal_shards(
                    opt_.db, cfg.shards, /*wm_interval=*/32, std::move(storage_shards), &replay);
                if (journaled.size() != cfg.shards) {
                    std::fprintf(stderr, "tickshelfd: raw journal open/replay failed for %s\n",
                                 opt_.db.c_str());
                    return false;
                }
                for (std::size_t i = 0; i < replay.size(); ++i)
                    std::fprintf(stderr,
                                 "tickshelfd: raw journal shard %zu replayed=%llu inserted=%llu "
                                 "torn_tail=%llu\n",
                                 i,
                                 static_cast<unsigned long long>(replay[i].replayed_records),
                                 static_cast<unsigned long long>(replay[i].replayed_inserted),
                                 static_cast<unsigned long long>(replay[i].torn_tail_bytes));
                storage_shards = std::move(journaled);
            }
        }
        try {
            engine_ = std::make_unique<tickshelf::Engine>(
                std::move(cfg), std::move(redis_shards), std::move(storage_shards));
        } catch (const std::exception& e) {
            std::fprintf(stderr, "tickshelfd: engine start failed: %s\n", e.what());
            return false;
        }
        engine_storage_only_ = cfg.write_mode == tickshelf::WriteMode::StorageOnly;
        engine_durable_mode_ = tickshelf::persists_final_records(cfg.write_mode);
        return true;
    }

    std::uint64_t engine_tick_interval() const {
        if (!engine_) return 0;
        const std::uint64_t batch = opt_.engine_batch_ms == 0 ? 100 : opt_.engine_batch_ms;
        return std::min<std::uint64_t>(batch, 100);
    }

    // 源线程的周期泵：年龄封口 + 尝试把背压留下的 retry buffer 重新交给 Engine。
    void engine_pump(std::uint64_t now) {
        engine_->tick(now);
        if (retry_ticks_.empty()) return;
        const std::size_t accepted = engine_->submit(retry_ticks_, now);
        if (accepted == 0) return;
        stats_.ticks_ingested += accepted;
        retry_ticks_.erase(retry_ticks_.begin(),
                           retry_ticks_.begin() + static_cast<std::ptrdiff_t>(accepted));
    }

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

    bool engine_can_read() const {
        return retry_ticks_.empty() &&
               kMaxEngineRetryTicks - retry_ticks_.size() >= kMaxTicksPerRecv;
    }

    void read_session(ConnSession& s, std::uint64_t now, std::vector<tickshelf::Tick>& ticks) {
        if (s.dead) return;
        std::uint8_t buf[kRecvBytes];
        std::uint64_t budget = kSessionReadBudgetBytes;
        for (;;) {
            if (engine_) {
                engine_pump(now);
                if (!engine_can_read()) return; // old tail first, reserve one recv's maximum output
            }
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
                if (engine_) {
                    // Parser's length bound plus the pre-recv reservation makes this strict.
                    if (ticks.size() > kMaxTicksPerRecv) {
                        std::fprintf(stderr, "tickshelfd: parser output exceeded recv reservation\n");
                        s.dead = true;
                        g_stop = 1;
                        return;
                    }
                    const std::size_t accepted = engine_->submit(ticks, now);
                    stats_.ticks_ingested += accepted;
                    for (std::size_t i = accepted; i < ticks.size(); ++i)
                        retry_ticks_.push_back(ticks[i]);
                } else {
                    for (const tickshelf::Tick& t : ticks) {
                        ingestor_.ingest(t);
                        ++stats_.ticks_ingested;
                    }
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
    // stall 统计）→ 仅认领成功的批标记 done/删租约（按 mine_ids 精确集合）。
    // SQLite 故障 → release_claim_ids 只放自己的认领，降级回读回路径。
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

        // flush 级合并事务（ADR-012 修订）：全部已认领批一次 BEGIN…COMMIT，减少
        // 事务边界，FULL 连接下也摊薄同步次数。历史 64ms→亚毫秒是总 stall 观察，
        // 不是单独 fsync 计时；此路径未设置 synchronous，实际值需回读，不能假定 FULL。
        // 故障回滚半径扩大到整个 flush，恢复语义不变（release_claim_ids → persister 读回）。
        const std::uint64_t t0 = steady_ms();
        std::vector<const std::vector<tickshelf::Tick>*> mine;
        std::vector<std::string> mine_ids;  // 精确集合：只有这份批允许被收尾/放弃认领
        mine.reserve(batches.size());
        for (std::size_t i = 0; i < batches.size(); ++i)
            if (claimed[i]) {
                mine.push_back(&batches[i].ticks);  // 租约被他占：留给 persister
                mine_ids.push_back(tickshelf::batch_id_of(batches[i]));
            }
        const std::int64_t ins = mine.empty() ? 0 : store_->upsert_flush(mine);
        const std::uint64_t stall = steady_ms() - t0;
        stats_.stall_sum_ms += stall;
        stats_.stall_max_ms = std::max(stats_.stall_max_ms, stall);
        ++stats_.stall_count;

        if (ins < 0) {  // SQLite 故障：只放弃自己的认领，退回读回路径
            tickshelf::release_claim_ids(rc_, mine_ids);  // 他占的租约不动——那是别人的在途批
            ++stats_.flush_errors;
            std::fprintf(stderr, "tickshelfd: sqlite write failed, %zu batches released\n",
                         mine_ids.size());
            return;
        }
        stats_.rows_upserted += static_cast<std::uint64_t>(ins);
        // 收尾只标自己写过的批：全量标 done 会把"租约被他占"的在途批也标完成——
        // persister 从此跳过、tier1 淘汰按 done=已落库删段（ADR-017）→ 静默丢数据
        if (!tickshelf::finish_written_batch_ids(rc_, mine_ids)) {
            rc_.close();  // 数据已落库；state/lease 由 TTL+persister 最终收尾
            ++stats_.flush_errors;
            return;
        }
        ++stats_.flushes_ok;
    }

    void print_stats() {
        if (engine_) {
            const auto es = engine_->stats();
            tickshelf::SqliteIdentityStats identity;  // 行身份计数（contract §4.2）
            for (const auto* p : sqlite_shards_) {
                const auto st = p->identity_stats();
                identity.exact_duplicates += st.exact_duplicates;
                identity.content_conflicts += st.content_conflicts;
            }
            std::fprintf(stderr,
                         "tickshelfd: engine ingested=%llu submitted=%llu accepted=%llu "
                         "rejected=%llu dedup=%llu durable=%llu publish=%llu skipped=%llu "
                         "degraded=%llu pending=%llu retries=%llu finish_pending=%llu "
                         "queue=%llu/%llu retry_buf=%zu frames=%llu crc_err=%llu resync=%llu "
                         "conns=%zu published=%llu committed_input=%llu inserted_unique=%llu "
                         "sink_consumed=%llu identity_dup=%llu identity_conflict=%llu\n",
                         static_cast<unsigned long long>(stats_.ticks_ingested),
                         static_cast<unsigned long long>(es.submitted_records),
                         static_cast<unsigned long long>(es.accepted_records),
                         static_cast<unsigned long long>(es.rejected_records),
                         static_cast<unsigned long long>(es.dedup_dropped),
                         static_cast<unsigned long long>(es.durable_records),
                         static_cast<unsigned long long>(es.publish_only_records),
                         static_cast<unsigned long long>(es.skipped_records),
                         static_cast<unsigned long long>(es.degraded_records),
                         static_cast<unsigned long long>(es.pending_records),
                         static_cast<unsigned long long>(es.retries),
                         static_cast<unsigned long long>(es.finish_pending_records),
                         static_cast<unsigned long long>(es.queue_depth),
                         static_cast<unsigned long long>(es.queue_peak), retry_ticks_.size(),
                         static_cast<unsigned long long>(stats_.frames_ok),
                         static_cast<unsigned long long>(stats_.crc_errors),
                         static_cast<unsigned long long>(stats_.resync_events), sessions_.size(),
                         static_cast<unsigned long long>(es.published_records),
                         static_cast<unsigned long long>(es.committed_input_records),
                         static_cast<unsigned long long>(es.inserted_unique_records),
                         static_cast<unsigned long long>(es.sink_consumed_records),
                         static_cast<unsigned long long>(identity.exact_duplicates),
                         static_cast<unsigned long long>(identity.content_conflicts));
            return;
        }
        const char* stall = "";
        char stall_buf[64] = {};
        if (stats_.stall_count > 0) {
            std::snprintf(stall_buf, sizeof(stall_buf), " stall=%llu/%llums",
                          static_cast<unsigned long long>(stats_.stall_sum_ms /
                                                          stats_.stall_count),
                          static_cast<unsigned long long>(stats_.stall_max_ms));
            stall = stall_buf;
        }
        tickshelf::SqliteIdentityStats identity;
        if (auto* sharded = dynamic_cast<tickshelf::ShardedTickStore*>(store_.get())) {
            identity = sharded->identity_stats();
        } else if (auto* single = dynamic_cast<tickshelf::SqliteTickStore*>(store_.get())) {
            identity = single->identity_stats();
        }
        std::fprintf(stderr,
                     "tickshelfd: ingested=%llu pending=%llu flushes=%llu flush_err=%llu "
                     "frames=%llu crc_err=%llu resync=%llu conns=%zu rows=%llu "
                     "identity_dup=%llu identity_conflict=%llu%s\n",
                     static_cast<unsigned long long>(stats_.ticks_ingested),
                     static_cast<unsigned long long>(ingestor_.pending_ticks()),
                     static_cast<unsigned long long>(stats_.flushes_ok),
                     static_cast<unsigned long long>(stats_.flush_errors),
                     static_cast<unsigned long long>(stats_.frames_ok),
                     static_cast<unsigned long long>(stats_.crc_errors),
                     static_cast<unsigned long long>(stats_.resync_events), sessions_.size(),
                     static_cast<unsigned long long>(stats_.rows_upserted),
                     static_cast<unsigned long long>(identity.exact_duplicates),
                     static_cast<unsigned long long>(identity.content_conflicts), stall);
    }

    Options opt_;
    tickshelf::net::UniqueFd listen_fd_;
    std::vector<ConnSession> sessions_;
    tickshelf::Ingestor ingestor_;
    tickshelf::RespClient rc_;
    std::unique_ptr<tickshelf::TickStore> store_;  // N=1 单文件 / N>1 分片（ADR-020）
    std::unique_ptr<tickshelf::Engine> engine_;    // --engine on 时替代旧 flush 路径
    std::vector<tickshelf::SqliteStorageShard*> sqlite_shards_;  // 非拥有：引擎路径身份计数聚合
    std::vector<tickshelf::Tick> retry_ticks_;     // Engine 背压留下的未接收尾部
    bool engine_storage_only_ = false;  // Engine StorageOnly：无 Redis，跳过负载发布
    bool engine_durable_mode_ = false;  // WriteThrough/StorageOnly：退出时检查 storage_complete
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
    std::fprintf(stderr,
                 "tickshelfd: listening %s:%u, redis %s:%u, engine=%s, flush %llums, "
                 "persist=%s%s, shards=%d\n",
                 opt.bind_host.c_str(), opt.port, opt.redis_host.c_str(), opt.redis_port,
                 opt.engine ? "on" : "off",
                 static_cast<unsigned long long>(opt.flush_interval_ms), opt.persist.c_str(),
                 opt.persist == "sqlite" ? opt.db.c_str() : "", opt.shards);
    return d.run();
}
