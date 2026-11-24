# 架构与数据流

> TTAP 线格式和键空间见 [TTAP 协议](protocol.md)。iQuant 可经 IQTS 独立运行，见 [采集手册](iquant-capture.md)。
> TTAP 交付和耐久边界以 [`tickshelf-delivery-v1-20261117`](delivery-contract.md) 为准。
> 最终 Storage 提交前允许永久 loss，没有固定 RPO；Redis（可无 AOF/RDB）不是恢复权威。
> `accepted` ≠ `published` ≠ `storage_committed`；TTAP 无摄入 ACK，不承诺上游 replay。
> 服务 SQLite 保持现有设置，以实际连接 PRAGMA 回读解释，不断言默认 FULL；显式 FULL 部署不得降级。
> Benchmark 的 FULL/WAL sync 基线不降低；健康 benchmark 仍不得意外丢数据。

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

- **tickshelfd**：默认单线程守护进程，自己管理网络 poll（ADR-011 时间片循环）。
  半包、内存桶及可选 Engine staging/队列都是易失状态；Redis 攒批产物也是缓存。
  进程可重启，但不能由此推出未提交数据可恢复。
- **persister**：单线程落盘进程。只通过 Redis 键空间与 tickshelfd 的产物交互，
  两个进程互相不知道对方存在——Redis 是它们之间唯一的契约。
- **libtickshelf**：核心库，tickshelfd 与 persister 都只是薄入口（`main.cpp` 各 ~200 行），
  可测试性靠直连核心库（测试不开网络）。

## iQuant 数据流与恢复

```text
iQuant SDK → Python 策略 → IQTS (TCP 回环) → 接收器 → raw 文件同步
                                 ▲                │
                                 └────── ACK ─────┘
                                                    │
                                 内存中的最新批 + 从 raw 顺序补扫
                                                    ↓
                          解析变长快照 → iquant_engine → SQLite 历史库
                                                             │
                                最近已提交批 → 有界队列 → Redis Stream（可选）
```

接收器持有实例目录的锁，C++ worker 另持有一把写入锁。Python 调度先尝试一组近期
数据，再尝试一组历史帧。实时通知覆盖时，待处理数据留在 raw 中。worker 为 SQLite
的单写者，按 `(stream, batch_seq, ordinal)` 标识快照，提交记录、帧状态和可推进的
连续游标。新批可以先入库，游标等补扫经过完整前缀后才推进。回放时核对已提交帧的
元数据，避免再次插入记录。历史补扫直接写历史库。

| 事件 | 恢复结果 |
|---|---|
| raw 已同步，ACK 未到或通知丢失 | 重启扫描 raw 校验帧并恢复接收进度，历史 worker 从 SQLite 游标补扫 |
| 历史提交失败或进程中断 | 编码和事务可从 raw 重做，来源身份保持稳定 |
| 源端窗口溢出或收到不可解析快照 | 分别登记为 `gap` 和 `quarantine`，缺失数据保持可见 |
| Redis 不可用或发布队列满 | 热层可能缺少该批，历史事务与接收继续独立推进 |

历史查询连接 `history.sqlite3`，热消费者读取 Redis Stream。目录布局、查询示例和
备份恢复步骤见 [iQuant 操作手册](iquant-capture.md)。

## TTAP 数据流转（三条路径）

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
  这是存活进程/批副本上的 at-least-once 尝试，不是端到端交付保证：重复段 SET/idx ZADD
  无害——段与批 1:1 原地覆盖，索引重复添加幂等；副本全丢时无法重试。
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
       │ 存活批的重复 upsert 不产生第二行 —— 仅 Storage 幂等效果，不是端到端 exactly-once
       ▼
       pipeline② [ HSET state 批id done    ← 无条件 HSET，与 M2 淘汰无竞态
                 , DEL lease:{id} ]       ← 崩在中间：lease 靠 TTL 兜底，state 已 done 不重做
```

- **幂等正确性不依赖租约**：租约只减少重复劳动。在完整段与可发现 pending 状态仍存活时，
  租约过期双活、worker 崩溃后可重做，相同行被唯一键吃掉；租约不能恢复丢失的 Redis 副本。
- M1 简化：批级处理（500ms 窗口的批 <1s 完成），不做分块续期（ADR-002 的批量续期留给大批场景）。

## 崩溃窗口与恢复

| 崩溃点 | 丢什么 | 谁恢复 | 语义 |
|---|---|---|---|
| feedgen → tickshelfd 在途 | TCP 断连可能丢在途帧 | feedgen 重连，新帧继续（不保证重发旧帧） | TTAP 无摄入 ACK，无上游 replay 保证 |
| tickshelfd / Engine 进程 | 内存桶、accepted/staging、队列/retry/in-flight 中未提交数据 | 副本全丢则永久丢失 | 不是固定 500ms×速率 窗口，没有固定 RPO |
| classic flush pipeline 中途 | 部分命令已执行，整批未必可发现/可读 | daemon 及其完整批仍活着才可重投；否则可能丢 | 重复 SET/ZADD/HSET 幂等不等于 pipeline 原子 |
| 写穿 pipeline 中途 | lease、段、索引、state 可能只发布了一部分 | 仅完整、可发现缓存批可在 TTL 后被 persister 接手 | 缺段/缺状态不承诺可恢复 |
| 写穿 SQLite upsert 中途 | 未提交事务可回滚 | 完整缓存副本仍在才可 release/TTL 后读回 | 回滚本身不重建输入；可永久丢失 |
| Redis 进程/缓存丢失 | 未提交的 Redis pending 与发现元数据 | 无持久恢复权威；其他完整副本若存在才有机会重试 | 允许 no-AOF/no-RDB，无固定 RPO |
| persister 进程 | 其本地未提交副本 | 完整段与 pending 状态仍在才可 TTL 后重认领 | 条件式 at-least-once 尝试，唯一键收敛 |
| Storage 成功后、标记 done 前 | 已提交数据强度依 backend/sync | 缓存 pending 若还在可重做；否则查 Storage | Storage 幂等效果，不是端到端 exactly-once |

旧 drill 的 `SENT ≥ ingested ≥ SQLite rows`、`rows ≤ SENT` 与 `重复=0` 是计数诊断，
不是健康运行允许缺数据的 PASS 条件，也不能证明内容完整。健康 benchmark 仍须按独立输入
oracle 对最终 Storage 检查零意外 missing、重复及内容错误；故障 drill 则明确报告实际
loss 与条件恢复范围，不能用本合同把健康丢失豁免为 PASS。

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

## 可选：分片异步持久化引擎（ADR-023）

默认关闭；`--engine on` 时 `tickshelfd` 不再在 poll 线程里同步 flush：

```
read_session → Engine::submit
  → 按 symbol 路由到每分片 Ingestor staging（仅源线程）
  → 条数/字节/年龄封口成不可变 Slice（内部仍是 vector<Batch>，批 id 协议不变）
  → 每分片有界队列 + 单 worker
      publish-only: flush_batches → 完成
      write-through: flush_batches(claim) → 过滤 claimed
                     → SqliteTickStore::upsert_flush → finish 精确 id 集合
```

- 每分片一条 Redis 连接 + 一个 SQLite 文件；文件布局/路由与 `ShardedTickStore`
  完全一致，persister/queryd 继续读写同一批分片。
- 失败：发布失败在进程/完整 Slice 仍存活时退避重试；存储失败释放自己的租约，
  仅缓存完整且可发现时可由 persister 补偿；finish 失败计 `finish_pending`（Storage 已成功，
  强度依实际 backend/sync）；drain 超时如实报告未完成，不承诺这些条目可恢复。
- 背压：每分片 pending 上限；`submit` 只接前缀，尾部进有界 retry buffer；
  buffer 满则停止 poll 会话（TCP 反压）。每 ≤100ms 的 `engine_pump` 做年龄封口
  与重投。
- 实测边界：合成 TTAP flood 下旧同步 flush 与 engine 都能完整落库且稳态吞吐
  接近（见 ADR-023）；本引擎是解耦/可配置性，不是自动的性能倍数。

## 观测面（ADR-013）

各组件每 2s 发布自过期负载键（`stats:{组件}`，EX 10，观测失败静默）：

```
stats:tickshelfd  persist=sqlite;engine=on;ingested=...;pending=...;...
                  e_submitted=...;e_accepted=...;e_rejected=...;e_dedup=...
                  e_durable=...;e_publish=...;e_degraded=...;e_retries=...
                  e_finish_pending=...;e_pending=...;e_queue=...;retry_buf=...
stats:persister   worker=...;backlog=...;claimed=...;skipped=...;rows=...;total=...
```

`e_accepted` 是进程内接收，`e_publish` 是 Redis 发布；包括 `e_durable` 在内的旧计数
必须结合 publish-only/write-through 模式解释，不能因名字含 durable 就视为
`storage_committed` 或 TTAP ACK。运行态配置另由 `scripts/stack_runtime.py` 的只读
`redis_runtime_snapshot` 记录逐项 CONFIG/INFO、时间、版本/PID、ownership 与
requested/observed；ACL/未知/错误显式保留，`--print-config` 仍不连网络。

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
- 异步持久化接口与 daemon 写穿——已由 ADR-023 的 opt-in 分片引擎实现；默认关闭，旧同步路径保留
