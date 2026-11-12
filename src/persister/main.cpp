// persister —— 租约落盘进程（薄入口，逻辑全在 libtickshelf::LeasePersister）。
// 多实例安全：SETNX 租约仲裁 + SQLite 唯一键幂等（ADR-002/005）。
#include <signal.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>

#include "common/startup_config.hpp"
#include "libtickshelf/ingestor.hpp"  // publish_stats（ADR-013）
#include "libtickshelf/lease_persister.hpp"
#include "libtickshelf/resp_client.hpp"
#include "libtickshelf/sharded_store.hpp"

namespace {

volatile sig_atomic_t g_stop = 0;
void on_signal(int) { g_stop = 1; }

}  // namespace

int main(int argc, char** argv) {
    tickshelf::startup::Options cfg;
    const auto status = tickshelf::startup::configure(
        tickshelf::startup::Component::Persister, argc, argv, cfg);
    if (status != tickshelf::startup::Status::Run)
        return status == tickshelf::startup::Status::ExitSuccess ? 0 : 2;

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);

    auto store = tickshelf::make_tick_store(cfg.shards);
    if (!store->open(cfg.db)) {
        std::fprintf(stderr, "persister: cannot open db %s (%d shards)\n", cfg.db.c_str(),
                     cfg.shards);
        return 1;
    }

    tickshelf::RespClient rc;
    tickshelf::LeasePersister::Options opt;
    opt.worker_id = cfg.worker + "-" + std::to_string(getpid());
    tickshelf::LeasePersister lp(rc, *store, opt);

    std::uint64_t next_stats = 0;
    std::uint64_t next_publish = 0;
    while (!g_stop) {
        if (!rc.connected() && !rc.connect(cfg.redis_host, cfg.redis_port)) {
            std::fprintf(stderr, "persister: redis %s:%u unreachable, retry 1s\n",
                         cfg.redis_host.c_str(), cfg.redis_port);
            if (cfg.once) return 1;
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        const int done = lp.poll_once();
        if (done < 0) {
            rc.close();  // 连接层错误：重连下轮再试（已处理批不受影响，幂等兜底）
            if (cfg.once) return 1;
            std::this_thread::sleep_for(std::chrono::milliseconds(cfg.poll_ms));
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
                          static_cast<unsigned long long>(store->count()));
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
                         static_cast<unsigned long long>(store->count()));
            next_stats = now + 10000;
        }

        if (cfg.once) {
            std::printf("DONE claimed=%llu skipped=%llu rows=%llu total=%llu\n",
                        static_cast<unsigned long long>(lp.stats().claimed),
                        static_cast<unsigned long long>(lp.stats().skipped),
                        static_cast<unsigned long long>(lp.stats().ticks_upserted),
                        static_cast<unsigned long long>(store->count()));
            return 0;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(cfg.poll_ms));
    }

    std::fprintf(stderr, "persister: stopping, total rows=%llu\n",
                 static_cast<unsigned long long>(store->count()));
    return 0;
}
