#pragma once
// 进程内 ingest API + 攒批（ADR-009 第一层入口 / ADR-003 固定冲刷间隔）。
//
// Ingestor 是 ADR-011 的"数据层"：symbol → 累积缓冲表（随 symbol/交易日生死），
// 与网络层 ConnSession 的生命周期解耦。累积按 (symbol, yyyymmdd) 分桶——
// 批次单位与 Redis 键/批 id 的粒度一致；drain() 取走全部桶，由 flush_batches
// 经 RESP pipeline 写出（SET seg 段 + ZADD idx 索引 + SET state 批标记，ADR-017）。
//
// 键空间（design.md 数据模型节，v1 行情键 ts:{sym}:{date} 已随 ADR-017 退役）：
//   段   seg:{批id}                批内 tick 64B 打包（段键 = "seg:" + 批id，1:1）
//   索引 idx:{symbol}              ZSET score=批首ts，member={date}:{首}-{尾}
//   状态 state（HASH，field=批id） 值 pending/done（ADR-019：S 批一条 HSET，
//                                  命令数与符号数解耦；遍历走 HSCAN 不走 SCAN）
#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/tick.hpp"
#include "resp_client.hpp"

namespace tickshelf {

struct Batch {
    std::string symbol;
    std::uint32_t yyyymmdd = 0;
    std::uint64_t first_ts = 0;  // 批内最小 ts_exchange_ms（含）
    std::uint64_t last_ts = 0;   // 批内最大 ts_exchange_ms（含；与邻批边界可重叠，幂等兜底）
    std::vector<Tick> ticks;
};

inline std::string batch_id_of(const Batch& b) {
    return b.symbol + ":" + std::to_string(b.yyyymmdd) + ":" + std::to_string(b.first_ts) + "-" +
           std::to_string(b.last_ts);
}

// 状态 HASH（ADR-019）：键固定 "state"，field = 裸批id——注意没有 state_key_of(id)：
// field 不是键，SET state_key_of(id) 会制造游离键。操作一律显式 HSET/HGET/HDEL state ...
inline std::string state_key() { return "state"; }
inline std::string lease_key_of(const std::string& id) { return "lease:" + id; }

class Ingestor {
public:
    // 进程内 ingest API：嵌入者/守护进程都从这里进数据。
    void ingest(const Tick& t);

    // 取走全部累积（move 语义清空桶）。返回顺序：symbol 乱序、桶内按日期升序。
    std::vector<Batch> drain();

    std::size_t pending_ticks() const;
    std::size_t pending_symbols() const { return buckets_.size(); }

private:
    std::unordered_map<std::uint64_t, std::map<std::uint32_t, std::vector<Tick>>> buckets_;
};

struct FlushOptions {
    // 写穿模式（ADR-012 修订）：flush 即 SETNX 认领租约——pipeline 内 lease 先于
    // state 落地，persister 看到 pending 时租约必已存在，抢占被仲裁挡下，不会重复劳动。
    // classic 模式（默认）不认领，落库交给 persister 读回路径。
    bool claim_lease = false;
    std::string worker_id = "daemon";
    std::uint64_t lease_ttl_s = 60;
};

// 最新行情投影（ADR-016）：全局 HASH latest，field=symbol，value=64B 最新 tick 字节。
// 每次 flush 一条 HSET 更新全部 symbol（进程内先聚合取最新，再一次性写——实测教训：
// 逐 tick Lua 更新投影，N tick/M symbol = N 次调用而非 M 次）。
// 只由 flush 路径维护；回补路径不改投影（历史数据不该覆盖最新）。
inline std::string latest_key() { return "latest"; }

// ---- 段式存储（v2，ADR-017）：每批一个原始字节段 + 每符号索引 ----
// 段键 seg:{sym}:{date}:{首ts}-{尾ts}（STRING，批内 tick 64B 打包，= "seg:" + 批id）
// 索引 idx:{sym}（ZSET，score=批首 ts，member="{date}:{首ts}-{尾ts}"）
// 生命周期原子化：段的 tick 全体同生共死——done 即整段删（数据必然已落库），
// 不再需要水位机（ADR-004 的水位是逐 tick ZSET 下"批区间混住 done/pending 成员"的产物）。
inline std::string seg_key_of(const std::string& batch_id) { return "seg:" + batch_id; }
inline std::string seg_key(const std::string& symbol, std::uint32_t yyyymmdd,
                           std::uint64_t first_ts, std::uint64_t last_ts) {
    return "seg:" + symbol + ":" + std::to_string(yyyymmdd) + ":" + std::to_string(first_ts) +
           "-" + std::to_string(last_ts);
}
inline std::string idx_key(const std::string& symbol) { return "idx:" + symbol; }
inline std::string idx_member(std::uint32_t yyyymmdd, std::uint64_t first_ts,
                              std::uint64_t last_ts) {
    return std::to_string(yyyymmdd) + ":" + std::to_string(first_ts) + "-" +
           std::to_string(last_ts);
}

// 段打包：批内 tick 逐个 64B 编码拼接
std::string pack_batch(const std::vector<Tick>& ticks);

// 纯函数（可直连单测）：批次 → RESP 命令序列（段式 v2）。
//   classic:    每批 [SET seg <打包字节>] + [ZADD idx first member] + [SET state pending]，
//               末尾 [HSET latest 全 symbol 最新]。
//   写穿:       每批 [SET lease NX EX, SET state pending, SET seg, ZADD idx] + 末尾 HSET。
// claimed_idx：写穿时回填各批 SETNX 回复在 commands 里的下标（classic 为空）。
void build_flush_commands(const std::vector<Batch>& batches, const FlushOptions& opt,
                          std::vector<std::vector<std::string>>& commands,
                          std::vector<std::size_t>& claimed_idx);

// 批次 → RESP pipeline（一条 exec，一次往返）。任一回复是 Error 类型即整体失败并返回
// false（pipeline 可能已部分生效——下游幂等兜底，不重试不回滚）。
// claimed 非空时回填各批 SETNX 是否抢到。
bool flush_batches(RespClient& rc, const std::vector<Batch>& batches,
                   const FlushOptions& opt = {}, std::vector<char>* claimed = nullptr);

// 写穿收尾：[SET state done, DEL lease] ×N 合并一条 pipeline。崩在中途：
// state 已 done 则不重做；仅 lease 残留靠 TTL 兜底。
bool finish_written_batches(RespClient& rc, const std::vector<Batch>& batches);
// 按 id 的精确版本（daemon 写穿路径用）：只对「本进程认领并落库」的批收尾。
// 全量收尾会把认领失败的批（租约被他占 = 别人的在途批）也标 done——persister
// 从此跳过它、tier1 淘汰按"done=必然已落库"删段（ADR-017）→ 静默丢数据。
// 空集合 = 无操作成功。
bool finish_written_batch_ids(RespClient& rc, const std::vector<std::string>& batch_ids);

// 放弃认领（SQLite 故障降级）：[DEL lease] ×N——批回到无主 pending，
// persister 走读回路径接手，数据已在 Redis，不重发不重灌。
bool release_claims(RespClient& rc, const std::vector<Batch>& batches);
// 按 id 的精确版本：只删自己的租约。删他占的租约破坏仲裁（在途 worker 被抢跑
// 重复劳动，tier2 stale-lease 丢数据窗口被放大）。空集合 = 无操作成功。
bool release_claim_ids(RespClient& rc, const std::vector<std::string>& batch_ids);

// 负载暴露（ADR-013）：组件把 stats:{component} 写成 k=v;... 自过期键（EX ttl）。
// 只做观测不做控制——在线模式切换的取舍见 ADR-013。发布失败静默（观测不反噬主流程）。
bool publish_stats(RespClient& rc, const std::string& component, const std::string& body,
                   std::uint64_t ttl_s = 10);

}  // namespace tickshelf
