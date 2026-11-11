// persister —— 租约落盘进程（薄入口，逻辑全在 libtickshelf::LeasePersister）。
// 多实例安全：SETNX 租约仲裁 + SQLite 唯一键幂等（ADR-002/005）。
#include <signal.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>

#include "libtickshelf/ingestor.hpp"  // publish_stats（ADR-013）
#include "libtickshelf/lease_persister.hpp"
#include "libtickshelf/resp_client.hpp"
#include "libtickshelf/sqlite_store.hpp"

namespace {

volatile sig_atomic_t g_stop = 0;
void on_signal(int) { g_stop = 1; }

}  // namespace

int main(int argc, char** argv) {
    std::string redis_host = "127.0.0.1";
    std::uint16_t redis_port = 6379;
    std::string db_path = "ticks.db";
    std::string worker = "persister";
    std::uint64_t poll_ms = 1000;
    bool once = false;

    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--redis-host") redis_host = argv[++i];
        else if (a == "--redis-port") redis_port = static_cast<std::uint16_t>(std::atoi(argv[++i]));
        else if (a == "--db") db_path = argv[++i];
        else if (a == "--worker") worker = argv[++i];
        else if (a == "--poll-ms") poll_ms = std::strtoull(argv[++i], nullptr, 10);
        else if (a == "--once") once = true;
        else {
            std::fprintf(stderr,
                         "usage: persister [--redis-host H] [--redis-port P] [--db PATH] "
                         "[--worker ID] [--poll-ms MS] [--once]\n");
            return 2;
        }
    }

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);

    tickshelf::SqliteTickStore store;
    if (!store.open(db_path)) {
        std::fprintf(stderr, "persister: cannot open db %s\n", db_path.c_str());
        return 1;
    }

    tickshelf::RespClient rc;
    tickshelf::LeasePersister::Options opt;
    opt.worker_id = worker + "-" + std::to_string(getpid());
    tickshelf::LeasePersister lp(rc, store, opt);

    std::uint64_t next_stats = 0;
    std::uint64_t next_publish = 0;
    while (!g_stop) {
        if (!rc.connected() && !rc.connect(redis_host, redis_port)) {
            std::fprintf(stderr, "persister: redis %s:%u unreachable, retry 1s\n",
                         redis_host.c_str(), redis_port);
            if (once) return 1;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        const int done = lp.poll_once();
        if (done < 0) {
            rc.close();  // 连接层错误：重连下轮再试（已处理批不受影响，幂等兜底）
            if (once) return 1;
            std::this_thread::sleep_for(std::chrono::milliseconds(poll_ms));
            continue;
        }

        // 负载暴露（ADR-013）：backlog 是未来模式切换控制器的核心信号，先积累观测
        const std::uint64_t now_ms = static_cast<std::uint64_t>(std::chrono::duration_cast<
            std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
        if (now_ms >= next_publish) {
            const auto& st = lp.stats();
            char body[256];
            std::snprintf(body, sizeof(body),
                          "worker=%s;backlog=%llu;claimed=%llu;skipped=%llu;empty=%llu;"
                          "rows=%llu;total=%llu",
                          opt.worker_id.c_str(),
                          static_cast<unsigned long long>(st.last_backlog),
                          static_cast<unsigned long long>(st.claimed),
                          static_cast<unsigned long long>(st.skipped),
                          static_cast<unsigned long long>(st.empty_batches),
                          static_cast<unsigned long long>(st.ticks_upserted),
                          static_cast<unsigned long long>(store.count()));
            tickshelf::publish_stats(rc, "persister", body);
            next_publish = now_ms + 2000;
        }

        const std::uint64_t now = static_cast<std::uint64_t>(std::chrono::duration_cast<
            std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
        if (next_stats == 0) next_stats = now + 10000;
        if (now >= next_stats) {
            const auto& st = lp.stats();
            std::fprintf(stderr,
                         "persister: polls=%llu claimed=%llu skipped=%llu empty=%llu rows=%llu total=%llu\n",
                         static_cast<unsigned long long>(st.polls),
                         static_cast<unsigned long long>(st.claimed),
                         static_cast<unsigned long long>(st.skipped),
                         static_cast<unsigned long long>(st.empty_batches),
                         static_cast<unsigned long long>(st.ticks_upserted),
                         static_cast<unsigned long long>(store.count()));
            next_stats = now + 10000;
        }

        if (once) {
            std::printf("DONE claimed=%llu skipped=%llu rows=%llu total=%llu\n",
                        static_cast<unsigned long long>(lp.stats().claimed),
                        static_cast<unsigned long long>(lp.stats().skipped),
                        static_cast<unsigned long long>(lp.stats().ticks_upserted),
                        static_cast<unsigned long long>(store.count()));
            return 0;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(poll_ms));
    }

    std::fprintf(stderr, "persister: stopping, total rows=%llu\n",
                 static_cast<unsigned long long>(store.count()));
    return 0;
}
