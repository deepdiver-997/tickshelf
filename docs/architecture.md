# 架构与数据流

> 本文是 M1 实现后的**实然架构**描述（as-built），与设计取利的差异看 design.md 的 ADR。
> 线格式与键空间见 protocol.md；里程碑划分见 roadmap.md。

## 组件与进程边界

```
feedgen / driver.py ──TCP·TTAP帧──▶ tickshelfd ──RESP pipeline──▶ Redis
 （任意语言，接入方）        │            （阻塞socket）        （热缓冲）
                            │进程边界1：driver 崩溃被隔离        │
                     tickshelf_lib（静态库，进程内链接）           │进程边界2：persister 崩溃被隔离
                            │                                   ▼
                     ┌──────┴─────────────┐               persister ──SQL──▶ SQLite
                     │ FrameParser（帧→Tick）│              （租约+幂等upsert）  （耐久源）
                     │ Ingestor（分桶攒批）  │
                     │ RespClient（RESP2）  │
                     │ LeasePersister      │
                     └────────────────────┘
```

- **tickshelfd**：单线程守护进程，自己管理网络 poll（ADR-011 时间片循环）。无状态：
  所有真状态要么在 Redis（攒批产物），要么随连接死亡（半包缓冲）。崩溃即重启。
- **persister**：单线程落盘进程。只通过 Redis 键空间与 tickshelfd 的产物交互，
  两个进程互相不知道对方存在——Redis 是它们之间唯一的契约。
- **libtickshelf**：核心库，tickshelfd 与 persister 都只是薄入口（`main.cpp` 各 ~200 行），
  可测试性靠直连核心库（测试不开网络）。

## 数据流转（三条路径）

### ① 摄入路径（tickshelfd 主循环内，微秒级）

```
字节流 ──recv──▶ ConnSession(fd+收缓冲) ──▶ FrameParser.on_bytes()
   │                                          │ 状态机 Scan/Header/Payload
   │                                          │ 防线1 长度互证 → 防线2 CRC → 防线3 重同步
   │                                          ▼
   │                                    std::vector<Tick>& out
   │                                          ▼
   └──────────────────────────────▶ Ingestor.ingest(Tick)
                                                │ pack_symbol → u64 key
                                                │ buckets_[sym][yyyymmdd].push_back
                                                ▼
                                    内存分桶（每 symbol×交易日 一个 vector<Tick>）
```

- 生命周期：`ConnSession`/`FrameParser` 随 TCP 连接生死；`Ingestor` 分桶随
  symbol×交易日生死、随 daemon 进程生死（ADR-011 两层生命周期分离的原因）。
- 任意字节切分（半包/粘包）都能喂：状态机逐字节推进，攒满 20B 帧头先互证再移动。
- 每会话每轮 1MB 读配额（ADR-011 修订）：洪泛下 recv 循环让出，flush/stats 定时器不被单个高速连接饿死。
- version ≠ 1 → 解析器报致命 → daemon 断连（协议不可降级）。

### ② 攒批冲刷路径（tickshelfd，每 flush_interval=500ms 一次）

两种持久化模式（`--persist`，ADR-012），**同一租约协议仲裁，可 A/B 对比**：

```
Ingestor.drain() ──▶ vector<Batch>{symbol, yyyymmdd, first_ts, last_ts, ticks}
                          │ 批id = {symbol}:{yyyymmdd}:{首ts}-{尾ts}（首尾=min/max，邻批可重叠）
        ┌─────────────────┴──────────────────┐
        ▼ classic（默认，落库交给 persister）  ▼ 写穿 --persist sqlite（daemon 自己落库）
  一条 RESP pipeline（2S+2 条）：          一条 RESP pipeline（3S+2 条，认领块前置防抢占）：
    SET seg:{批id} 段字节  ×每批一条         SET lease:{批id} daemon NX EX 60 ×批（连续块）
    ZADD idx:{sym} 批首ts member ×每批      SET seg:{批id} ×批 + ZADD idx:{sym} ×批
    HSET state 批id=pending… ×flush 一条    HSET state 批id=pending… ×flush 一条
    HSET latest <sym> <最新64B>… ×flush 一条（最新价投影，两模式同款，ADR-016）
        │                                        │
        ▼                                        ▼ 内联 SQLite upsert（flush 级单事务 ADR-012 修订，
  persister 走路径③                           stall ≈ 0.79µs/tick + checkpoint 尖峰）
                                            [HSET state 批id=done…, DEL lease:1…N] 两条命令
                                            （SQLite 故障 → 多键 DEL lease 降级回路径③）
```

- 顺序即正确性：Redis 逐命令执行、其他客户端可穿插——classic 的 state HSET 必须在全部
  ZADD **之后**落地（否则 persister 会读到半批）；写穿的认领块必须在 state **之前**
  （否则 persister 在 daemon 落库期间抢走批，重复劳动）。
- **flush 失败（Redis 写不进/回复 Error）→ 批重投回 Ingestor**，下轮重试。
  这是 at-least-once：重复段 SET/idx ZADD 无害——段与批 1:1 原地覆盖，索引重复添加幂等。
- 不 drain 就不发：Redis 不可达时数据留在内存桶里，内核 TCP 窗口反压 driver（ADR-011 背压）。
- 两种模式性能对比见 `scripts/bench_compare.sh` 与 ADR-012 实测段。

### ③ 租约落盘路径（persister，每 poll_ms=300ms 一轮）

```
poll_once():
  HSCAN state (游标循环) ──▶ 过滤 ==pending 的 field（field = 裸批id，ADR-019）
    └─ for 每个批id:
       pipeline① [ SET lease:{id} w NX EX 60   ← 唯一仲裁点（ADR-002）
                 , GET seg:{id} ]              （段式读回：批id 直接推导段键，RTT 合并）
       │ 段 = 本批 tick 全体（1:1），无邻批重叠脏读；幂等兜底保留
       ▼
       SQLite INSERT OR IGNORE ... WITHOUT ROWID PK(symbol, ts_exchange_ms)
       │ 重复 upsert 不产生第二行 —— at-least-once + 唯一键 = 效果上的 exactly-once
       ▼
       pipeline② [ HSET state 批id done    ← 无条件 HSET，与 M2 淘汰无竞态
                 , DEL lease:{id} ]       ← 崩在中间：lease 靠 TTL 兜底，state 已 done 不重做
```

- **正确性不依赖租约**：租约只减少重复劳动。租约过期双活、worker 崩溃后重做，
  代价都只是多写一次相同的行（被唯一键吃掉）。
- M1 简化：批级处理（500ms 窗口的批 <1s 完成），不做分块续期（ADR-002 的批量续期留给大批场景）。

## 崩溃窗口与恢复

| 崩溃点 | 丢什么 | 谁恢复 | 语义 |
|---|---|---|---|
| feedgen → tickshelfd 在途 | TCP 断连丢在途帧 | feedgen 重连，新帧继续（不重发旧帧） | M1 口径：计为丢失上界（crash_drill MISSING 的来源） |
| tickshelfd 进程 | 已 ingest 未 flush 的内存桶 | **丢**（这就是 500ms×速率 的窗口） | 同上计入 MISSING；重启后从新连接继续 |
| classic flush pipeline 中途 | 部分命令已执行 | daemon 重投整批 | 段原地覆盖幂等 + state 重复 HSET 无害 |
| 写穿 pipeline 中途 | lease/state 已落地、ZADD 残缺 | 租约 TTL 过期 → persister 接手读回 | 崩溃恢复走路径③，语义不变 |
| 写穿 SQLite upsert 中途 | 无（事务回滚） | daemon release_claims 或租约 TTL → persister | 行要么全在要么不在 |
| Redis 进程 | 缓冲内未落库数据 | M1 不演练（AOF 收窄窗口，M4 对账兜底） | 排除在 drill 口径外 |
| persister 进程 | 什么都不丢 | lease TTL 过期 → 重认领；或另一实例接手 | 重做被唯一键吃掉 |
| upsert 后、标记 done 前 | 无 | state 仍 pending → 重做一遍 | 幂等，行数不变 |

对账口径：`SENT(发送) ≥ ingested(已解析入桶) ≥ SQLite rows`，差值全部可归因到上表。
crash_drill.sh 的三层断言就是这个不等式 + `rows ≤ SENT` + `重复=0`。

## 淘汰路径（M2 evictor，独立进程）

```
evictor 循环：采样 used_memory → 超软水位才动 → HSCAN state 枚举全部批（field=批id，ADR-019）
   → done 批整删（DEL seg + ZREM idx + HDEL state）——"已持久化先淘汰"由段/批 1:1 构造成立，
     水位机退役（ADR-017）；pending 全在飞（--inflight 时）同理，state/lease 留给 worker 收尾
   → 紧急水位才允许 tier3（按批粒度删最老 pending 未认领批，真丢，默认关）
```

- **为什么 done 段整删是安全的**：段与批 1:1，done 标记 = 数据必然已落库，整段删不触碰
  任何未落库数据；v1 逐 tick ZSET 时代"批区间混住 done/pending 成员"的正确性问题在段式下
  构造性消失（ADR-017）。
- **tier2 默认关**：stale lease（worker 死后 TTL 未过）× tier2 = 真丢失窗口（演练实测
  丢 1,428 ticks，ADR-004 注记）。evictor 对 persister/落库故障的响应 = 停住：done 段
  不再产生 → 保护集非空 → 不删 → 内存上涨，恢复后排干继续——故障演练验证的就是这个行为。

## 存储布局 v2：段式 + 索引（ADR-017）

读取侧与写入侧（路径②③）的数据面已从"逐 tick ZSET"切到"段式"：

```
写入：每批 → SET seg:{sym}:{date}:{首}-{尾}（批内 tick 64B 打包）
            + ZADD idx:{sym} <批首ts> "{date}:{首}-{尾}"（索引）
查询：ZRANGEBYSCORE idx:{sym} -inf to → 候选段（member 携带 last 过滤）
      → GET 命中段 → 段字节 verbatim 进响应（服务端零解码零重编码）
淘汰：done 段整删（seg + idx + state），不再需要水位机——段/批 1:1 使
      "已持久化先淘汰"由构造成立（ADR-004 水位是逐 tick ZSET 的产物，已退役）
```

实测（read_bench.sh）：10s 窗口查询 53→13.5ms（−75%）、30s 窗口 119→23ms（−81%）、
内存 198→113B/tick（−43%）。窗口越大收益越大：查询成本从 O(ticks) 变为 O(命中段数)。

## 观测面（ADR-013）

各组件每 2s 发布自过期负载键（`stats:{组件}`，EX 10，观测失败静默）：

```
stats:tickshelfd  persist=sqlite;ingested=...;pending=...;stall_avg=...;stall_max=...
stats:persister   worker=...;backlog=...;claimed=...;skipped=...;rows=...;total=...
```

`redis-cli GET stats:persister` 随时可看。只做观测不做控制：写穿/经典的在线切换
有备案的策略草图（写穿常态、降级信号 = daemon flush 死线连续失约），重访触发条件
见 ADR-013——当前 stall 对预算余量 30~160×，控制器暂无工况。

## 状态与生命周期小结



| 对象 | 生命周期 | 所在 |
|---|---|---|
| ConnSession / FrameParser | 随 TCP 连接 | tickshelfd 内存 |
| Ingestor 分桶 | 随 symbol×交易日，进程死即清 | tickshelfd 内存 |
| Batch（批id） | flush 时产生，标记 done 后终结 | Redis state 键 |
| 行情 ZSET member | 写入起，直到 M2 淘汰 | Redis |
| lease 键 | 认领起，DEL 或 TTL 60s | Redis |
| stats 负载键 | 每 2s 重写，EX 10 自过期 | Redis |
| SQLite 行 | 永久（唯一键去重） | SQLite |

## M1 有意不做（后续里程碑领走）

- 淘汰/水位（M2 evictor：只删 state=done 的 member，层间按丢失代价 ADR-004）
- 容量不等式校验（M2，ADR-007）
- 租约分块续期（批大到跨 60s TTL 时）
- 混沌源与对账补拉（M3，ADR-006）
- 异步持久化接口与 daemon 写穿——评估结论见 ADR-012
