// feedgen —— 参考行情驱动（ADR-009 第三层：任意语言按 TTAP 推数据，这里用 C++ 证明）。
//
// 随机游走 + 开盘/收盘式突发（ADR-006，只用模拟数据）。速率口径：
// 每轮给每个 symbol 各发 1 个 tick（交易所时间戳 +1ms/轮，严格递增 →
// 唯一键 (symbol, ts) 天然唯一，单 symbol 速率上限 1000/s），轮频 = 每 symbol 速率。
// 对账口径：stdout 最后一行 "SENT <ticks> <frames>" 是全量发送上界（不含 chaos 丢弃）。
//
// 混沌模式（ADR-006/M3）：所有生成 tick 先入 ring（回补源），再按混沌规则决定发送：
//   --chaos-gap-pct   按概率丢弃发送（ring 仍在）→ 制造真缺口，等缺口回补
//   --chaos-ooo-pct   按概率延迟发送（side 队列，乱序+批区间交错）
//   --chaos-dup-pct   帧级重发（幂等测试：ZSET member 去重 + SQLite IGNORE）
//   --chaos-stall-s   每 stall-every 秒断流一次（side 积压，>5s 触发 daemon 心跳判死
//                     → 断连 → 恢复后重连突发；生成不停，只有发送停）
// 回补执行器（ADR-014）：--backfill-db 启用。轮询 backfill:{sym}:{from}-{to} 键，
//   值 requested → 从 ring 取区间 → 直接 SQLite 幂等 upsert（不进热缓冲）→ 键值
//   done / unrepairable（ring 头已越过区间起点 = 窗外，显式标记）。
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "common/frame_writer.hpp"
#include "common/net.hpp"
#include "libtickshelf/ingestor.hpp"  // publish_stats / backfill 键助手
#include "libtickshelf/sqlite_store.hpp"

namespace {

using namespace tickshelf;

volatile sig_atomic_t g_stop = 0;

void on_signal(int) { g_stop = 1; }

std::uint64_t steady_ms() {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                          std::chrono::steady_clock::now().time_since_epoch())
                                          .count());
}

std::uint64_t system_ms() {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                          std::chrono::system_clock::now().time_since_epoch())
                                          .count());
}

struct SymState {
    char symbol[8];
    price_t mid;              // 随机游走中枢（定点 1e-4）
    std::uint64_t base_ts_ms = 0;  // 该 symbol 第 i 个 tick 的交易所 ts = base + i
    std::uint64_t generated = 0;   // 生成总数（含被 chaos 丢弃的）
    std::deque<Tick> ring;         // 回补源：全部生成 tick，按 ts 有序（生成序即 ts 序）
};

// ring 容量：head 原子提交的单线程简化——deque 头即保留窗起点，超限从头弹出
void ring_push(SymState& s, const Tick& t, std::size_t cap) {
    s.ring.push_back(t);
    while (s.ring.size() > cap) s.ring.pop_front();
}

// ring 区间提取 [from, to]：生成序 = ts 序，二分定位。空/窗外返回 false。
bool ring_extract(const SymState& s, std::uint64_t from, std::uint64_t to,
                  std::vector<Tick>& out, std::size_t cap_per_pass) {
    if (s.ring.empty()) return false;
    const auto begin = std::lower_bound(
        s.ring.begin(), s.ring.end(), from,
        [](const Tick& t, std::uint64_t v) { return t.ts_exchange_ms < v; });
    if (begin == s.ring.end() || begin->ts_exchange_ms > to) return false;
    for (auto it = begin; it != s.ring.end() && it->ts_exchange_ms <= to; ++it) {
        out.push_back(*it);
        if (out.size() >= cap_per_pass) break;
    }
    return true;
}

struct Options {
    std::string host = "127.0.0.1";
    std::uint16_t port = 9900;
    int symbols = 4;
    double rate = 200.0;         // 每 symbol 每秒 tick 数（≤ 1000）
    double burst_mult = 10.0;    // 突发倍率（≤1 关闭；有效速率封顶 1000）
    double burst_every_s = 10.0;
    double burst_len_s = 2.0;
    double duration_s = 30.0;    // 0 = 永续
    std::uint32_t seed = 42;
    // 混沌（M3/ADR-006）
    double chaos_gap_pct = 0;   // 丢弃发送的概率 %
    double chaos_ooo_pct = 0;   // 延迟发送的概率 %
    double chaos_dup_pct = 0;   // 帧重发概率 %
    double chaos_stall_every_s = 0;  // 断流周期（0=关）
    double chaos_stall_s = 0;        // 断流时长
    std::size_t ring_cap = 1000000;  // 每 symbol ring 容量
    // 回补执行器（ADR-014）
    std::string backfill_db;             // 空 = 关
    std::string redis_host = "127.0.0.1";
    std::uint16_t redis_port = 6379;
    std::uint64_t backfill_every_ms = 1000;
};

class Driver {
public:
    explicit Driver(const Options& opt) : opt_(opt), rng_(opt.seed) {
        syms_.resize(static_cast<std::size_t>(opt.symbols));
        for (std::size_t i = 0; i < syms_.size(); ++i) {
            std::snprintf(syms_[i].symbol, sizeof(syms_[i].symbol), "600%03d",
                          static_cast<int>(i));
            syms_[i].mid = 10000000 + static_cast<price_t>(rng_() % 4000000);
            syms_[i].base_ts_ms = ts_base_;
        }
        start_ = steady_ms();
        if (!opt_.backfill_db.empty()) {
            if (!store_.open(opt_.backfill_db)) {
                std::fprintf(stderr, "feedgen: cannot open backfill db %s\n",
                             opt_.backfill_db.c_str());
                std::exit(1);
            }
        }
    }

    int run() {
        std::uint64_t next_round = start_;
        std::uint64_t round = 0;
        std::uint64_t frames = 0, sent = 0, heartbeats = 0, reconnects = 0;
        std::uint64_t dropped = 0, delayed = 0, dups = 0, stalls = 0;
        std::uint64_t backfills_done = 0, backfills_unrepairable = 0;
        std::uint64_t last_frame_ms = steady_ms();
        std::uint64_t next_stats = start_ + 5000;
        std::deque<Tick> side_;  // 乱序/断流的延迟发送队列

        while (!g_stop) {
            if (opt_.duration_s > 0 && steady_ms() - start_ >= ms_of(opt_.duration_s)) break;

            const double phase =
                std::fmod(static_cast<double>(steady_ms() - start_) / 1000.0, opt_.burst_every_s);
            const bool in_burst = opt_.burst_mult > 1.0 && phase < opt_.burst_len_s;
            const bool in_stall = opt_.chaos_stall_s > 0 && opt_.chaos_stall_every_s > 0 &&
                                  std::fmod(static_cast<double>(steady_ms() - start_) / 1000.0,
                                            opt_.chaos_stall_every_s) < opt_.chaos_stall_s;
            const double rate_capped = std::min(
                in_burst ? opt_.rate * opt_.burst_mult : opt_.rate, 1000.0);

            if (!connect_if_needed(reconnects)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                continue;
            }
            if (steady_ms() < next_round) {
                std::this_thread::sleep_for(std::chrono::milliseconds(next_round - steady_ms()));
            }
            next_round = steady_ms() + std::max<std::uint64_t>(
                                              1, static_cast<std::uint64_t>(1000.0 / rate_capped));

            // 生成 + ring + 混沌分流
            std::vector<Tick> to_send;
            to_send.reserve(syms_.size());
            const double gap_p = opt_.chaos_gap_pct / 100.0;
            const double ooo_p = opt_.chaos_ooo_pct / 100.0;
            std::uint64_t delayed_total_round = 0;
            for (SymState& s : syms_) {
                Tick t = make_tick(s, round);
                ring_push(s, t, opt_.ring_cap);
                const double roll = std::uniform_real_distribution<>(0.0, 1.0)(rng_);
                if (roll < gap_p) {
                    ++dropped;  // 真缺口：不发送，ring 保留等回补
                    continue;
                }
                if (!in_stall && roll < gap_p + ooo_p) {
                    side_.push_back(t);  // 延迟发送
                    ++delayed;
                    ++delayed_total_round;
                    continue;
                }
                to_send.push_back(t);
            }
            if (in_stall && !to_send.empty()) {
                side_.insert(side_.end(), to_send.begin(), to_send.end());
                to_send.clear();
            }
            // side 队列限速排空：每轮最多补 4×symbols 个，避免恢复后一帧超大
            const std::size_t side_budget = syms_.size() * 4;
            while (!side_.empty() && to_send.size() < side_budget) {
                to_send.push_back(side_.front());
                side_.pop_front();
            }

            // 断流期跳过发送（连接会被 daemon 心跳判死，恢复后重连突发）
            if (!to_send.empty()) {
                const bool chaos_marked = in_stall || delayed_total_round > 0;
                const std::string frame = build_frame_bytes(
                    kFrameTypeTickBatch, to_send, chaos_marked ? 0x0001 : 0x0000);
                if (!send_all(frame)) {
                    reconnect();
                } else {
                    ++frames;
                    sent += to_send.size();
                    last_frame_ms = steady_ms();
                    if (opt_.chaos_dup_pct > 0) {
                        const double roll = std::uniform_real_distribution<>(0.0, 1.0)(rng_);
                        if (roll < opt_.chaos_dup_pct / 100.0) {
                            if (send_all(frame)) ++dups;  // 幂等测试：原帧重发
                        }
                    }
                }
            } else if (steady_ms() - last_frame_ms > 2000 && !in_stall) {
                if (send_all(build_frame_bytes(kFrameTypeHeartbeat, {}))) ++heartbeats;
                last_frame_ms = steady_ms();
            }

            // 回补执行器（ADR-014）：轮询 backfill 键，ring → 直接 SQLite upsert
            if (!opt_.backfill_db.empty() && steady_ms() >= next_backfill_) {
                run_backfill(backfills_done, backfills_unrepairable);
                next_backfill_ = steady_ms() + opt_.backfill_every_ms;
            }

            if (steady_ms() >= next_stats) {
                std::fprintf(stderr,
                             "feedgen: sent=%llu frames=%llu hb=%llu round=%llu burst=%d "
                             "reconnects=%llu gap=%llu ooo=%llu dup=%llu stall=%llu "
                             "bf_done=%llu bf_unrep=%llu side=%zu "
                             "ts_first=%llu ts_last=%llu\n",
                             static_cast<unsigned long long>(sent),
                             static_cast<unsigned long long>(frames),
                             static_cast<unsigned long long>(heartbeats),
                             static_cast<unsigned long long>(round), int(in_burst),
                             static_cast<unsigned long long>(reconnects),
                             static_cast<unsigned long long>(dropped),
                             static_cast<unsigned long long>(delayed),
                             static_cast<unsigned long long>(dups),
                             static_cast<unsigned long long>(stalls),
                             static_cast<unsigned long long>(backfills_done),
                             static_cast<unsigned long long>(backfills_unrepairable),
                             side_.size(),
                             static_cast<unsigned long long>(ts_base_),
                             static_cast<unsigned long long>(ts_base_ + round));
                next_stats = steady_ms() + 5000;
            }
            if (in_stall && !was_in_stall_) ++stalls;  // 断流窗口进入计数
            was_in_stall_ = in_stall;
            ++round;
        }

        std::printf("SENT %llu %llu\n", static_cast<unsigned long long>(sent),
                    static_cast<unsigned long long>(frames));
        return 0;
    }

private:
    static std::uint64_t ms_of(double s) { return static_cast<std::uint64_t>(s * 1000.0); }

    tickshelf::Tick make_tick(SymState& s, std::uint64_t round) {
        Tick t{};
        std::memcpy(t.symbol, s.symbol, 8);
        t.ts_exchange_ms = s.base_ts_ms + round;
        t.ts_receive_ms = system_ms();
        const price_t step = static_cast<price_t>(rng_() % 201) - 100;
        s.mid += step + (10000000 - s.mid) / 10000;
        const price_t spread = 100 + static_cast<price_t>(rng_() % 300);
        t.bid_price = s.mid - spread / 2;
        t.ask_price = s.mid + spread / 2;
        if (rng_() % 5 != 0) {
            t.last_price = t.bid_price + static_cast<price_t>(rng_() % (spread + 1));
            t.last_size = 1 + rng_() % 500;
        } else {
            t.last_price = 0;
            t.flags = kTickFlagNoLast;
        }
        t.bid_size = 10 + rng_() % 1000;
        t.ask_size = 10 + rng_() % 1000;
        ++s.generated;
        return t;
    }

    // 缺口回补（ADR-014）：backfill:{sym}:{from}-{to} requested → ring 提取 → 直接 SQLite
    void run_backfill(std::uint64_t& done_n, std::uint64_t& unrep_n) {
        if (!brc_.connected() && !brc_.connect(opt_.redis_host, opt_.redis_port)) return;
        std::vector<RespValue> r;
        std::vector<std::string> ids;
        std::string cursor = "0";
        do {
            std::vector<RespValue> page;
            if (!brc_.exec({{"SCAN", cursor, "MATCH", "backfill:*", "COUNT", "100"}}, page) ||
                page.size() != 1 || page[0].type != RespValue::Type::Array ||
                page[0].array.size() != 2)
                return;
            cursor = page[0].array[0].str;
            for (const RespValue& k : page[0].array[1].array) ids.push_back(k.str);
        } while (cursor != "0");
        if (ids.empty()) return;

        for (const std::string& full_key : ids) {
            constexpr const char* kPrefix = "backfill:";
            if (full_key.rfind(kPrefix, 0) != 0) continue;
            const std::string spec = full_key.substr(9);  // {sym}:{from}-{to}
            const std::size_t c1 = spec.find(':');
            const std::size_t dash = spec.find('-', c1 == std::string::npos ? 0 : c1);
            if (c1 == std::string::npos || dash == std::string::npos) continue;
            const std::string sym = spec.substr(0, c1);
            const std::uint64_t from = std::strtoull(spec.substr(c1 + 1, dash - c1 - 1).c_str(),
                                                     nullptr, 10);
            const std::uint64_t to = std::strtoull(spec.substr(dash + 1).c_str(), nullptr, 10);

            SymState* s = nullptr;
            for (SymState& cand : syms_)
                if (sym == cand.symbol) s = &cand;
            const bool in_window = s != nullptr && !s->ring.empty() &&
                                   s->ring.front().ts_exchange_ms <= from;

            std::vector<Tick> ticks;
            if (in_window) ring_extract(*s, from, to, ticks, 50000);

            // GET+DEL 认领（竞态双做被幂等 upsert 兜底，ADR-013 的取舍哲学）
            std::vector<RespValue> r2;
            if (!brc_.exec({{"GET", full_key}, {"DEL", full_key}}, r2) || r2.size() != 2) continue;
            if (r2[0].type != RespValue::Type::Bulk || r2[0].str != "requested") continue;

            if (!in_window) {
                brc_.exec({{"SET", full_key, "unrepairable", "EX", "3600"}}, r2);
                ++unrep_n;
                std::fprintf(stderr, "feedgen: backfill %s unrepairable（ring 窗外）\n",
                             spec.c_str());
                continue;
            }
            store_.upsert_batch(ticks);  // 直接 SQLite：不进热缓冲（ADR-014）
            brc_.exec({{"SET", full_key, "done", "EX", "3600"}}, r2);
            ++done_n;
            std::fprintf(stderr, "feedgen: backfill %s done（%zu ticks）\n", spec.c_str(),
                         ticks.size());
        }
    }

    bool connect_if_needed(std::uint64_t& reconnects) {
        if (fd_.is_open()) return true;
        fd_.reset(tickshelf::net::tcp_connect(opt_.host, opt_.port));
        if (!fd_.is_open()) {
            std::fprintf(stderr, "feedgen: connect %s:%u failed, retry in 1s\n", opt_.host.c_str(),
                         opt_.port);
            ++reconnects;
            return false;
        }
        tickshelf::net::set_nonblock(fd_.get(), true);
        return true;
    }

    void reconnect() {
        fd_.reset();
        std::fprintf(stderr, "feedgen: connection lost, will reconnect\n");
    }

    bool send_all(const std::string& data) {
        if (!fd_.is_open()) return false;
        std::size_t sent = 0;
        while (sent < data.size()) {
            const ssize_t n = ::send(fd_.get(), data.data() + sent, data.size() - sent,
                                     MSG_NOSIGNAL);
            if (n > 0) {
                sent += static_cast<std::size_t>(n);
                continue;
            }
            if (n < 0 && errno == EINTR) continue;
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                pollfd p{fd_.get(), POLLOUT, 0};
                if (::poll(&p, 1, 1000) <= 0 || !(p.revents & POLLOUT)) return false;
                continue;
            }
            return false;
        }
        return true;
    }

    Options opt_;
    std::mt19937 rng_;
    std::vector<SymState> syms_;
    std::uint64_t ts_base_ = system_ms();
    std::uint64_t start_;
    std::uint64_t next_backfill_ = 0;
    bool was_in_stall_ = false;
    tickshelf::net::UniqueFd fd_;
    SqliteTickStore store_;  // 回补直写库（--backfill-db 启用）
    RespClient brc_;         // backfill 键轮询专用连接
};

const char* arg_str(char** argv, int& i) { return argv[++i]; }
double arg_d(char** argv, int& i) { return std::atof(argv[++i]); }

}  // namespace

int main(int argc, char** argv) {
    Options opt;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--host") opt.host = arg_str(argv, i);
        else if (a == "--port") opt.port = static_cast<std::uint16_t>(std::atoi(argv[++i]));
        else if (a == "--symbols") opt.symbols = std::atoi(argv[++i]);
        else if (a == "--rate") opt.rate = arg_d(argv, i);
        else if (a == "--burst-mult") opt.burst_mult = arg_d(argv, i);
        else if (a == "--burst-every-s") opt.burst_every_s = arg_d(argv, i);
        else if (a == "--burst-len-s") opt.burst_len_s = arg_d(argv, i);
        else if (a == "--duration-s") opt.duration_s = arg_d(argv, i);
        else if (a == "--seed") opt.seed = static_cast<std::uint32_t>(std::atoi(argv[++i]));
        else if (a == "--chaos-gap-pct") opt.chaos_gap_pct = arg_d(argv, i);
        else if (a == "--chaos-ooo-pct") opt.chaos_ooo_pct = arg_d(argv, i);
        else if (a == "--chaos-dup-pct") opt.chaos_dup_pct = arg_d(argv, i);
        else if (a == "--chaos-stall-every-s") opt.chaos_stall_every_s = arg_d(argv, i);
        else if (a == "--chaos-stall-s") opt.chaos_stall_s = arg_d(argv, i);
        else if (a == "--ring-cap") opt.ring_cap = std::strtoull(argv[++i], nullptr, 10);
        else if (a == "--backfill-db") opt.backfill_db = arg_str(argv, i);
        else if (a == "--redis-host") opt.redis_host = arg_str(argv, i);
        else if (a == "--redis-port") opt.redis_port = static_cast<std::uint16_t>(std::atoi(argv[++i]));
        else if (a == "--backfill-every-ms") opt.backfill_every_ms = std::strtoull(argv[++i], nullptr, 10);
        else {
            std::fprintf(stderr,
                         "usage: feedgen [--host H] [--port P] [--symbols N] [--rate R] "
                         "[--burst-mult M] [--burst-every-s S] [--burst-len-s S] "
                         "[--duration-s S(0=forever)] [--seed N] "
                         "[--chaos-gap-pct P] [--chaos-ooo-pct P] [--chaos-dup-pct P] "
                         "[--chaos-stall-every-s S] [--chaos-stall-s S] [--ring-cap N] "
                         "[--backfill-db PATH] [--backfill-every-ms MS]\n");
            return 2;
        }
    }
    if (opt.symbols < 1 || opt.symbols > 1000 || opt.rate <= 0 || opt.rate > 1000) {
        std::fprintf(stderr, "feedgen: need 1<=symbols<=1000, 0<rate<=1000 (ts 步长 1ms)\n");
        return 2;
    }

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    signal(SIGPIPE, SIG_IGN);

    Driver d(opt);
    return d.run();
}
