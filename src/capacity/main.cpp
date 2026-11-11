// capacity —— 容量校验子命令（ADR-007）：容量是一笔静态可验证的账，启动/配置变更时跑。
//
// 五条不等式（ADR-007 ②原文即"净增速"，⑤把它具体化）：
//   ① 稳定性:      persist_rate ≥ peak_ingest × safety
//                  （最坏落盘吞吐必须压住突发峰值摄入，安全系数默认 2）
//   ② 突发积压:    peak × (burst_len + flush) × member_bytes < watermark_bytes
//                  （一次突发的全部积压要装进淘汰水位以下的余量；排水按零记，保守）
//   ③ 出口带宽:    peak × member_bytes × mode_factor < nic_bytes × margin
//                  （classic 模式读回让 Redis 字节翻倍 factor=2；写穿=1）
//   ④ 单 symbol 日占用: rate × 86400 × member_bytes < daily_cap
//   ⑤ 持续净增长:  max(0, peak − persist_rate) × session_s × member_bytes < reserve_bytes
//                  （持久化最慢速率与最大接收速率之差 = 内存净增速；整个时段的净增要装进
//                   预留内存，默认 = 软水位线以下全部余量。②管单次突发，⑤管持续工况；
//                   超账 ≠ 必溢出 = 保障失效，靠 evictor 增速对账看实测增速兜底）
//
// 账面常数不写死：--calibrate 用探针实测 Redis 每 member 占用（MEMORY USAGE 差分）；
// --persist-rate 0 时用 SQLite 微基准实测最坏落盘速率；--read-bench 另测读吞吐
// （queries/s 与 rows/s，库大小可调——查询是随库容增长的第一个瓶颈候选）。
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "libtickshelf/resp_client.hpp"
#include "libtickshelf/sqlite_store.hpp"

namespace {

using namespace tickshelf;

std::uint64_t now_ns() {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                          std::chrono::steady_clock::now().time_since_epoch())
                                          .count());
}

// SQLite 微基准：临时文件库插 N 行测最坏落盘速率（rows/s）
double measure_persist_rate() {
    const std::string path = "/tmp/tickshelf_capacity_bench.db";
    std::remove(path.c_str());
    SqliteTickStore store;
    if (!store.open(path)) return -1;
    Tick t{};
    std::memcpy(t.symbol, "bench", 6);
    constexpr int kN = 20000;
    std::vector<Tick> ticks(kN);
    for (int i = 0; i < kN; ++i) {
        t.ts_exchange_ms = static_cast<std::uint64_t>(i);
        ticks[static_cast<std::size_t>(i)] = t;
    }
    const std::uint64_t t0 = now_ns();
    const std::int64_t ins = store.upsert_batch(ticks);
    const double sec = static_cast<double>(now_ns() - t0) / 1e9;
    std::remove(path.c_str());
    if (ins <= 0 || sec <= 0) return -1;
    return static_cast<double>(ins) / sec;
}

// SQLite 读微基准：临时库插 rows 行（单 symbol，ts 每 ms 一行），跑 queries 次
// 均分窗口范围查，报 queries/s 与 rows/s（库大小影响页缓存命中率——数字随 --read-bench-rows 变）
bool measure_read_rate(int rows, int queries, double& qps, double& rps) {
    const std::string path = "/tmp/tickshelf_capacity_read.db";
    std::remove(path.c_str());
    const std::string wal = path + "-wal";
    const std::string shm = path + "-shm";
    SqliteTickStore store;
    if (!store.open(path)) return false;
    constexpr int kBatch = 20000;
    Tick t{};
    std::memcpy(t.symbol, "bench", 6);
    std::vector<Tick> ticks(kBatch);
    for (int i = 0; i < rows; i += kBatch) {
        for (int j = 0; j < kBatch; ++j) {
            t.ts_exchange_ms = static_cast<std::uint64_t>(i + j);
            ticks[static_cast<std::size_t>(j)] = t;
        }
        if (store.upsert_batch(ticks) < 0) {
            std::remove(path.c_str());
            return false;
        }
    }
    const double window_ms = static_cast<double>(rows) / static_cast<double>(queries);
    const std::uint64_t t0 = now_ns();
    std::uint64_t total = 0;
    for (int q = 0; q < queries; ++q) {
        const std::uint64_t from = static_cast<std::uint64_t>(q * window_ms);
        const std::uint64_t to = from + static_cast<std::uint64_t>(window_ms);
        total += store.query_range("bench", from, to).size();
        if (store.query_failed()) {
            std::remove(path.c_str());
            std::remove(wal.c_str());
            std::remove(shm.c_str());
            return false;
        }
    }
    const double sec = static_cast<double>(now_ns() - t0) / 1e9;
    std::remove(path.c_str());
    std::remove(wal.c_str());
    std::remove(shm.c_str());
    if (sec <= 0 || total == 0) return false;
    qps = static_cast<double>(queries) / sec;
    rps = static_cast<double>(total) / sec;
    return true;
}

// Redis 探针：临时 ZSET 装 N 个 64B member，MEMORY USAGE 差分算每 member 实际占用
double calibrate_member_overhead(RespClient& rc, int n_members) {
    std::vector<RespValue> r;
    rc.exec({{"SELECT", "15"}}, r);
    rc.exec({{"DEL", "capacity:probe"}}, r);
    std::vector<std::vector<std::string>> cmds;
    cmds.reserve(static_cast<std::size_t>(n_members));
    std::string member(64, '\0');
    for (int i = 0; i < n_members; ++i) {
        // member 必须全局唯一：低位嵌 i 的 LE 编码，其余铺周期填充——
        // 教训：纯 (i+j)&0xFF 是周期 256 的循环模式，i 与 i+256 的 member 相同，
        // ZSET 按 member 去重后 2000 发只落 256 个，MEMORY USAGE 直接腰斩
        member[0] = static_cast<char>(i & 0xFF);
        member[1] = static_cast<char>((i >> 8) & 0xFF);
        member[2] = static_cast<char>((i >> 16) & 0xFF);
        member[3] = static_cast<char>((i >> 24) & 0xFF);
        for (int j = 4; j < 64; ++j)
            member[static_cast<std::size_t>(j)] = static_cast<char>((i + j) & 0xFF);
        cmds.push_back({"ZADD", "capacity:probe", std::to_string(i), member});
    }
    if (!rc.exec(cmds, r)) return -1;
    if (!rc.exec({{"MEMORY", "USAGE", "capacity:probe", "SAMPLES", "0"}}, r) ||
        r.size() != 1 || r[0].type != RespValue::Type::Integer) {
        return -1;
    }
    const std::int64_t usage = r[0].integer;
    rc.exec({{"DEL", "capacity:probe"}}, r);
    rc.exec({{"SELECT", "0"}}, r);
    if (usage <= 0) return -1;
    return (static_cast<double>(usage) - static_cast<double>(n_members) * 64.0) /
           static_cast<double>(n_members);
}

struct Check {
    const char* name;
    bool pass;
    std::string detail;
};

}  // namespace

int main(int argc, char** argv) {
    double rate = 500;               // 每 symbol 每秒 tick 数（常态）
    int symbols = 8;
    double burst_mult = 5;
    double burst_len_s = 2;
    double flush_s = 0.5;
    double safety = 2.0;
    double nic_mbps = 1000;
    double margin = 0.5;
    std::uint64_t bytes_per_tick = 64;
    double member_overhead = -1;     // <0 = 先跑校准探针
    std::uint64_t watermark_bytes = 268435456;  // 256MB
    std::uint64_t daily_cap_bytes = 1073741824; // 1GB
    std::uint64_t reserve_bytes = 0;  // 0 = 用 watermark（预留 = 软水位线以下全部余量）
    double session_s = 7200;          // 持续工况窗口（默认 2h 开盘）
    bool read_bench = false;
    int read_rows = 1000000;
    int read_queries = 50;
    std::string persist_mode = "classic";
    double persist_rate = 0;         // 0 = 微基准实测
    bool calibrate_only = false;
    std::string redis_host = "127.0.0.1";
    std::uint16_t redis_port = 6379;

    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--rate") rate = std::atof(argv[++i]);
        else if (a == "--symbols") symbols = std::atoi(argv[++i]);
        else if (a == "--burst-mult") burst_mult = std::atof(argv[++i]);
        else if (a == "--burst-len-s") burst_len_s = std::atof(argv[++i]);
        else if (a == "--flush-s") flush_s = std::atof(argv[++i]);
        else if (a == "--safety") safety = std::atof(argv[++i]);
        else if (a == "--nic-mbps") nic_mbps = std::atof(argv[++i]);
        else if (a == "--margin") margin = std::atof(argv[++i]);
        else if (a == "--bytes-per-tick") bytes_per_tick = std::strtoull(argv[++i], nullptr, 10);
        else if (a == "--member-overhead") member_overhead = std::atof(argv[++i]);
        else if (a == "--watermark-bytes") watermark_bytes = std::strtoull(argv[++i], nullptr, 10);
        else if (a == "--daily-cap-bytes") daily_cap_bytes = std::strtoull(argv[++i], nullptr, 10);
        else if (a == "--reserve-bytes") reserve_bytes = std::strtoull(argv[++i], nullptr, 10);
        else if (a == "--session-s") session_s = std::atof(argv[++i]);
        else if (a == "--read-bench") read_bench = true;
        else if (a == "--read-bench-rows") read_rows = std::atoi(argv[++i]);
        else if (a == "--read-bench-queries") read_queries = std::atoi(argv[++i]);
        else if (a == "--persist-mode") persist_mode = argv[++i];
        else if (a == "--persist-rate") persist_rate = std::atof(argv[++i]);
        else if (a == "--calibrate") calibrate_only = true;
        else if (a == "--redis-host") redis_host = argv[++i];
        else if (a == "--redis-port") redis_port = static_cast<std::uint16_t>(std::atoi(argv[++i]));
        else {
            std::fprintf(stderr, "usage: capacity [参数见源码头部注释] [--calibrate]\n");
            return 2;
        }
    }

    RespClient rc;
    const bool redis_ok = rc.connect(redis_host, redis_port);
    if (member_overhead < 0) {
        if (!redis_ok) {
            std::fprintf(stderr, "capacity: member-overhead 未给且 Redis 不可达，无法校准\n");
            return 1;
        }
        member_overhead = calibrate_member_overhead(rc, 2000);
        if (member_overhead < 0) {
            std::fprintf(stderr, "capacity: 校准探针失败，用默认 156B\n");
            member_overhead = 156;
        }
    }
    if (calibrate_only) {
        std::printf("calibrated member_overhead=%.1f B/tick（含 ZSET 条目与键摊销）\n",
                    member_overhead);
        std::printf("用法建议: --member-overhead %.0f\n", member_overhead);
        return 0;
    }

    const double peak = static_cast<double>(symbols) * rate * burst_mult;
    const double member_bytes = static_cast<double>(bytes_per_tick) + member_overhead;
    const double nic_bytes = nic_mbps * 1e6 / 8.0;
    const double mode_factor = persist_mode == "sqlite" ? 1.0 : 2.0;

    if (persist_rate <= 0) {
        persist_rate = measure_persist_rate();
        if (persist_rate <= 0) {
            std::fprintf(stderr, "capacity: SQLite 微基准失败，请显式给 --persist-rate\n");
            return 1;
        }
        std::printf("微基准 persist_rate = %.0f rows/s\n", persist_rate);
    }

    std::vector<Check> checks;

    const double need1 = peak * safety;
    checks.push_back({"①稳定性 persist_rate ≥ peak×safety", persist_rate >= need1,
                      "persist=" + std::to_string(static_cast<long long>(persist_rate)) +
                          " 需≥ " + std::to_string(static_cast<long long>(need1)) +
                          "（peak=" + std::to_string(static_cast<long long>(peak)) + "×" +
                          std::to_string(safety) + "）"});

    const double burst_bytes = peak * (burst_len_s + flush_s) * member_bytes;
    checks.push_back({"②突发积压 < watermark", burst_bytes < static_cast<double>(watermark_bytes),
                      "积压=" + std::to_string(static_cast<long long>(burst_bytes)) + "B < " +
                          std::to_string(static_cast<long long>(watermark_bytes)) + "B（peak×(" +
                          std::to_string(burst_len_s) + "+" + std::to_string(flush_s) + "s)×" +
                          std::to_string(static_cast<int>(member_bytes)) + "B）"});

    const double redis_bps = peak * member_bytes * mode_factor;
    checks.push_back({"③出口带宽 < NIC×margin", redis_bps < nic_bytes * margin,
                      "redis=" + std::to_string(static_cast<long long>(redis_bps)) + "B/s < " +
                          std::to_string(static_cast<long long>(nic_bytes * margin)) +
                          "B/s（NIC " + std::to_string(nic_mbps) + "Mbps×" +
                          std::to_string(margin) + "，factor " + std::to_string(mode_factor) +
                          "）"});

    const double daily_bytes = rate * 86400.0 * member_bytes;
    checks.push_back({"④单symbol日占用 < cap", daily_bytes < static_cast<double>(daily_cap_bytes),
                      "日占=" + std::to_string(static_cast<long long>(daily_bytes)) + "B < " +
                          std::to_string(static_cast<long long>(daily_cap_bytes)) + "B"});

    if (reserve_bytes == 0) reserve_bytes = watermark_bytes;
    const double net_rate = peak - persist_rate;
    if (net_rate <= 0) {
        checks.push_back({"⑤持续净增长 < 预留", true,
                          "persist_rate ≥ peak（" +
                              std::to_string(static_cast<long long>(persist_rate)) + " ≥ " +
                              std::to_string(static_cast<long long>(peak)) + "），无净增长工况"});
    } else {
        const double net_bytes = net_rate * session_s * member_bytes;
        checks.push_back(
            {"⑤持续净增长 < 预留", net_bytes < static_cast<double>(reserve_bytes),
             "净增=" + std::to_string(static_cast<long long>(net_bytes)) + "B < " +
                 std::to_string(static_cast<long long>(reserve_bytes)) + "B（净速 " +
                 std::to_string(static_cast<long long>(net_rate)) + " ticks/s × " +
                 std::to_string(session_s) + "s × " + std::to_string(static_cast<int>(member_bytes)) +
                 "B；超账≠必溢出，是保障失效，靠 evictor 增速对账兜底）"});
    }

    std::printf("\n容量账（peak=%lld ticks/s, member=%.0fB, mode=%s）\n",
                static_cast<long long>(peak), member_bytes, persist_mode.c_str());
    int fails = 0;
    for (const Check& c : checks) {
        std::printf("  [%s] %s: %s\n", c.pass ? "PASS" : "FAIL", c.name, c.detail.c_str());
        if (!c.pass) ++fails;
    }
    std::printf("结论: %s（%zu/%zu 通过）\n", fails == 0 ? "容量充足 ✓" : "容量不足 ✗",
                checks.size() - static_cast<std::size_t>(fails), checks.size());

    if (read_bench) {
        double qps = 0, rps = 0;
        if (measure_read_rate(read_rows, read_queries, qps, rps))
            std::printf("SQLite 读微基准（%d 行库，%d 次均分窗口）: %.0f queries/s, %.0f rows/s\n",
                        read_rows, read_queries, qps, rps);
        else
            std::printf("SQLite 读微基准失败\n");
    }
    return fails == 0 ? 0 : 1;
}
