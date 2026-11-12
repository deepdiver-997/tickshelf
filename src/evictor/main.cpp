// evictor —— 水位淘汰进程（薄入口，逻辑在 libtickshelf::Evictor）。
//
// 循环：采样 Redis 内存（INFO used_memory）→ 超软水位才跑淘汰（平时零打扰）；
// tier3 仅在紧急水位触发（默认关——真丢，ADR-004 最底层）。
// 附带 ADR-007 的预测/实测对账：账面增速（stats:tickshelfd 的 ingested 增量 ×
// 每 tick 占用）vs 实测内存增速，偏差超容限告警——抓配置漂移也抓编码 bug。
#include <signal.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

#include "common/net.hpp"
#include "common/startup_config.hpp"
#include "libtickshelf/evictor.hpp"
#include "libtickshelf/ingestor.hpp"  // publish_stats

namespace {

using namespace tickshelf;

volatile sig_atomic_t g_stop = 0;
void on_signal(int) { g_stop = 1; }

bool info_used_memory(RespClient& rc, std::uint64_t& used) {
    std::vector<RespValue> r;
    if (!rc.exec({{"INFO", "memory"}}, r) || r.size() != 1 || r[0].type != RespValue::Type::Bulk)
        return false;
    const std::size_t p = r[0].str.find("used_memory:");
    if (p == std::string::npos) return false;
    used = std::strtoull(r[0].str.c_str() + p + 12, nullptr, 10);
    return true;
}

// stats:tickshelfd 的 ingested=N（ADR-013 负载键的跨组件消费）
bool daemon_ingested(RespClient& rc, std::uint64_t& ingested) {
    std::vector<RespValue> r;
    if (!rc.exec({{"GET", "stats:tickshelfd"}}, r) || r.size() != 1 ||
        r[0].type != RespValue::Type::Bulk)
        return false;
    const std::size_t p = r[0].str.find("ingested=");
    if (p == std::string::npos) return false;
    ingested = std::strtoull(r[0].str.c_str() + p + 9, nullptr, 10);
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    startup::Options cfg;
    const auto status = startup::configure(startup::Component::Evictor, argc, argv, cfg);
    if (status != startup::Status::Run)
        return status == startup::Status::ExitSuccess ? 0 : 2;

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);

    RespClient rc;
    if (!rc.connect(cfg.redis_host, cfg.redis_port)) {
        std::fprintf(stderr, "evictor: redis %s:%u unreachable\n", cfg.redis_host.c_str(),
                     cfg.redis_port);
        return 1;
    }

    Evictor::Options opt;
    opt.allow_inflight_evict = cfg.inflight;
    Evictor ev(rc, opt);

    // 对账状态
    bool recon_has = false;
    std::uint64_t recon_ingested = 0, recon_used = 0, recon_freed = 0;

    std::fprintf(stderr, "evictor: soft=%llu emergency=%llu inflight=%d sample=%llums\n",
                 static_cast<unsigned long long>(cfg.soft_watermark_bytes),
                 static_cast<unsigned long long>(cfg.emergency_watermark_bytes), cfg.inflight,
                 static_cast<unsigned long long>(cfg.sample_ms));

    while (!g_stop) {
        std::this_thread::sleep_for(std::chrono::milliseconds(cfg.sample_ms));
        if (!rc.connected() && !rc.connect(cfg.redis_host, cfg.redis_port)) continue;

        std::uint64_t used = 0;
        if (!info_used_memory(rc, used)) {
            rc.close();
            continue;
        }

        const auto& st = ev.stats();
        char body[256];
        std::snprintf(body, sizeof(body),
                      "used=%llu;soft=%llu;runs=%llu;segs=%llu;states=%llu;"
                      "emergency=%llu;batches=%llu",
                      static_cast<unsigned long long>(used),
                      static_cast<unsigned long long>(cfg.soft_watermark_bytes),
                      static_cast<unsigned long long>(st.runs),
                      static_cast<unsigned long long>(st.segs_deleted),
                      static_cast<unsigned long long>(st.states_deleted),
                      static_cast<unsigned long long>(st.emergency_segs),
                      static_cast<unsigned long long>(st.batches_total));
        tickshelf::publish_stats(rc, "evictor", body);

        // 预测/实测对账（ADR-007）：累计口径——起点锚定后比较 (Δingested × bytes/tick)
        // 与 (Δused_memory)。短窗口增量会被分配器页粒度平滑，必须累计足够 ticks 再裁决；
        // 只在无淘汰的窗口比较（淘汰本身会压内存，口径不成立）。
        if (cfg.bytes_per_tick > 0) {
            std::uint64_t ingested = 0;
            const bool has_ing = daemon_ingested(rc, ingested);
            if (!recon_has && has_ing) {  // 锚定起点
                recon_has = true;
                recon_ingested = ingested;
                recon_used = used;
                recon_freed = st.segs_deleted;
            } else if (recon_has && has_ing) {
                const std::uint64_t d_ticks = ingested >= recon_ingested ? ingested - recon_ingested : 0;
                const std::int64_t d_used = static_cast<std::int64_t>(used) -
                                            static_cast<std::int64_t>(recon_used);
                const bool no_eviction = recon_freed == st.segs_deleted;
                if (no_eviction && d_ticks >= 5000 && d_used > 0) {
                    const double predicted = static_cast<double>(d_ticks) * cfg.bytes_per_tick;
                    const double dev =
                        (static_cast<double>(d_used) - predicted) / predicted * 100.0;
                    if (dev > cfg.growth_tolerance_pct || dev < -cfg.growth_tolerance_pct) {
                        std::fprintf(stderr,
                                     "evictor: 增速偏差 %.0f%%（实测 %lldB vs 预测 %.0fB，"
                                     "%llu ticks × %lluB/tick）——查配置漂移或内存账错误\n",
                                     dev, static_cast<long long>(d_used), predicted,
                                     static_cast<unsigned long long>(d_ticks),
                                     static_cast<unsigned long long>(cfg.bytes_per_tick));
                        // 重锚：告警一次后从当前点重新累计，避免每采样重复刷屏
                        recon_ingested = ingested;
                        recon_used = used;
                    }
                }
            }
            recon_freed = st.segs_deleted;
        }

        if (used < cfg.soft_watermark_bytes) continue;  // 水下：零打扰

        const bool emergency = cfg.emergency_watermark_bytes != 0 &&
                               used >= cfg.emergency_watermark_bytes;
        if (emergency)
            std::fprintf(stderr, "evictor: 紧急水位 %llu ≥ %llu，tier3 启用（真丢）\n",
                         static_cast<unsigned long long>(used),
                         static_cast<unsigned long long>(cfg.emergency_watermark_bytes));
        if (!ev.run_once(emergency)) {
            rc.close();
            continue;
        }
        if (emergency && ev.stats().emergency_segs > 0)
            std::fprintf(stderr, "evictor: tier3 删除 %llu 个 pending 段（数据真丢，已告警）\n",
                         static_cast<unsigned long long>(ev.stats().emergency_segs));
    }

    const auto& st = ev.stats();
    std::fprintf(stderr,
                 "evictor: stopping, runs=%llu segs=%llu states=%llu idx=%llu "
                 "emergency=%llu batches=%llu\n",
                 static_cast<unsigned long long>(st.runs),
                 static_cast<unsigned long long>(st.segs_deleted),
                 static_cast<unsigned long long>(st.states_deleted),
                 static_cast<unsigned long long>(st.idx_removed),
                 static_cast<unsigned long long>(st.emergency_segs),
                 static_cast<unsigned long long>(st.batches_total));
    return 0;
}
