// floodgen —— 灌流驱动（benchmarks.md 未测清单 #1 的工具）：不受 1ms ts 网格限制的
// 极限摄入基准源，与 feedgen 只差速率口径：
//   ts 每 tick +1（摄入不要求唯一，基准只看吞吐）；每帧打满 kMaxFramePayloadBytes
//   （4096 tick = 256KB）；聚合速率 --rate R（0 = 不限速，帧粒度突发、均值贴近目标）。
// 单进程发端有 CRC/生成的单核上限（表驱动 CRC ~几百 MB/s）——测 daemon 天花板时
// 用多进程并行发端（scripts/ingest_bench.sh 用 4 进程拆 symbol）。
// 非阻塞 send + poll(POLLOUT)（M1 教训：EAGAIN 不是断连）。stdout 统计：
//   RATE sent=<累计ticks> inst=<近1s> frames=<累计帧>  每秒一行
//   SENT <ticks> <frames>                              末行总账
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "common/frame_writer.hpp"
#include "common/net.hpp"
#include "common/tick.hpp"

namespace {

using namespace tickshelf;

volatile sig_atomic_t g_stop = 0;

void on_signal(int) { g_stop = 1; }

double steady_s() {
    return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

}  // namespace

int main(int argc, char** argv) {
    std::string host = "127.0.0.1";
    std::uint16_t port = 9000;
    int symbols = 256;
    int sym_base = 0;  // 本进程负责的 symbol 起始编号（多进程拆分不重叠）
    std::uint64_t ts_base = 1000;  // ts 起点（分步基准各步错开，防 (sym,ts) 跨步碰撞）
    int ticks_per_frame = static_cast<int>(kMaxFramePayloadBytes / kTickBytes);
    double rate = 0;         // 聚合 ticks/s，0 = 不限速
    double duration_s = 10;  // 0 = 一直跑到 SIGINT

    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--host") host = argv[++i];
        else if (a == "--port") port = static_cast<std::uint16_t>(std::atoi(argv[++i]));
        else if (a == "--symbols") symbols = std::atoi(argv[++i]);
        else if (a == "--sym-base") sym_base = std::atoi(argv[++i]);
        else if (a == "--ts-base") ts_base = std::strtoull(argv[++i], nullptr, 10);
        else if (a == "--ticks-per-frame") ticks_per_frame = std::atoi(argv[++i]);
        else if (a == "--rate") rate = std::atof(argv[++i]);
        else if (a == "--duration") duration_s = std::atof(argv[++i]);
        else {
            std::fprintf(stderr,
                         "usage: floodgen [--host H] [--port P] [--symbols N] [--sym-base N] "
                         "[--ts-base N] [--ticks-per-frame N] [--rate R] [--duration S]\n");
            return 2;
        }
    }
    if (symbols <= 0 || ticks_per_frame <= 0) return 2;
    if (ticks_per_frame > static_cast<int>(kMaxFramePayloadBytes / kTickBytes))
        ticks_per_frame = static_cast<int>(kMaxFramePayloadBytes / kTickBytes);

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);

    const int fd = net::tcp_connect(host, port);
    if (fd < 0) {
        std::fprintf(stderr, "floodgen: connect %s:%u failed (errno=%d)\n", host.c_str(), port,
                     errno);
        return 1;
    }
    net::UniqueFd fd_guard(fd);
    net::set_nonblock(fd, true);

    // symbol 表（"F" + 7 位十进制 = 8B 无 NUL，批 id 切分安全）+ 每 symbol ts 计数器
    std::vector<std::array<char, 8>> syms(static_cast<std::size_t>(symbols));
    std::vector<std::uint64_t> ts(static_cast<std::size_t>(symbols), ts_base);
    for (int i = 0; i < symbols; ++i)
        std::snprintf(syms[static_cast<std::size_t>(i)].data(), 9, "F%07d", sym_base + i);

    std::vector<Tick> buf(static_cast<std::size_t>(ticks_per_frame));
    std::uint64_t sent_ticks = 0, sent_frames = 0, prev_report = 0;
    std::uint64_t gen_idx = 0;
    const double t0 = steady_s();
    double next_report = t0 + 1.0;

    while (!g_stop && (duration_s <= 0 || steady_s() - t0 < duration_s)) {
        if (rate > 0) {  // 限速：均值贴近目标，突发粒度 = 一帧
            if (static_cast<double>(sent_ticks) >= rate * (steady_s() - t0)) {
                std::this_thread::sleep_for(std::chrono::microseconds(200));
                continue;
            }
        }
        for (std::size_t j = 0; j < buf.size(); ++j) {
            Tick& t = buf[j];
            const std::size_t s = gen_idx++ % syms.size();
            std::memcpy(t.symbol, syms[s].data(), 8);
            t.ts_exchange_ms = ++ts[static_cast<std::size_t>(s)];
            t.ts_receive_ms = t.ts_exchange_ms + 1;
            t.bid_price = 10000000 + static_cast<std::int64_t>(ts[static_cast<std::size_t>(s)] % 1000);
            t.ask_price = t.bid_price + 100;
            t.last_price = t.bid_price + 50;
            t.bid_size = 10;
            t.ask_size = 10;
            t.last_size = 1;
            t.flags = 0;
        }
        const std::string frame = build_frame_bytes(kFrameTypeTickBatch, buf, 0);
        std::size_t off = 0;
        while (off < frame.size()) {
            const ssize_t k = ::send(fd, frame.data() + off, frame.size() - off, 0);
            if (k > 0) {
                off += static_cast<std::size_t>(k);
                continue;
            }
            if (k < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                pollfd p{};
                p.fd = fd;
                p.events = POLLOUT;
                ::poll(&p, 1, 1000);
                continue;
            }
            if (k < 0 && errno == EINTR) continue;
            std::fprintf(stderr, "floodgen: send failed (errno=%d)，停止\n", errno);
            return 1;
        }
        sent_ticks += buf.size();
        ++sent_frames;

        if (steady_s() >= next_report) {
            std::printf("RATE sent=%llu inst=%llu frames=%llu\n",
                        static_cast<unsigned long long>(sent_ticks),
                        static_cast<unsigned long long>(sent_ticks - prev_report),
                        static_cast<unsigned long long>(sent_frames));
            std::fflush(stdout);
            prev_report = sent_ticks;
            next_report += 1.0;
        }
    }
    std::printf("SENT %llu %llu\n", static_cast<unsigned long long>(sent_ticks),
                static_cast<unsigned long long>(sent_frames));
    return 0;
}
