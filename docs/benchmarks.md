# 性能基准（实测账本）

> 本页是**当下配置的实测快照**，不是跨机绝对值承诺。环境：Apple Silicon Mac（arm64）、
> loopback、homebrew Redis、单机单进程。改动负载/代码/环境后重跑脚本更新数字。
> 所有数字可复现（每行标了命令）；没有对账链的吞吐数字不进本页（讲故事不算数）。

## 方法论（口径纪律）

1. **同负载 A/B**：只改一个变量（存储布局/持久化模式），其余钉死——`read_bench.sh`、`bench_compare.sh` 是这个纪律的产物。
2. **对账式集成基准**：端到端吞吐必须带 `SENT ≥ ingested ≥ ROWS` 对账链，差值可归因——否则是"看起来很快"。
3. **三层归因**：组件极限 → 计数器链 → profiler（见 §归因方法），先便宜后贵，不要一上来 perf。
4. **口径即代码**：窗口/速率全部用交易所 ts（feedgen ts 时钟 ≠ 墙钟的教训，M3 教训 2）；基准里的速率条件写进脚本，不靠口头约定。
5. **库容要生产级**：SQLite 小库基准数字虚高（页缓存全热）；`--read-bench-rows` 调到目标库容再报数。
6. **必须 Release 构建**（`build-release`，-O2）：Debug 的解析/落库数字显著偏慢。2026-11-11 前的历史数字为 Debug 口径，已全部重测替换。

## 组件极限矩阵（`scripts/component_bench.sh`）

实测 2026-11-11：临时 Redis 实例（maxmemory=512MB，noeviction，无持久化）、redis-benchmark
`--threads 3` 单连接、loopback。tick 档 = 64B（1 tick/命令），seg 档 = 16KB（≈256 tick/段，
v2 常态 500ms×500 tick/s/symbol 的段大小）。参考负载：常态 4k ticks/s（8×500），突发峰值 20k。

| 组件 | 指标 | 实测 | 折算 ticks/s | vs 常态余量 |
|---|---|---|---|---|
| Redis SET | 64B，pipeline=1 | 46,860 req/s | 46.9k | 11.7× |
| Redis SET | 64B，pipeline=16 | 397,614 req/s | 397.6k | 99× |
| Redis SET | **16KB 段**，pipeline=1 | 26,525 req/s | **6.79M** | **~1700×** |
| Redis SET | 16KB 段，pipeline=16 | 79,365 req/s | 20.3M | ~5000× |
| Redis GET | 16KB 段，pipeline=1 | 26,525 req/s | 6.79M | ~1700× |
| Redis GET | 16KB 段，pipeline=16 | 79,365 req/s | 20.3M | ~5000× |
| SQLite 写 | 单事务批量 upsert | 1.20M rows/s（1.2~1.43M 波动带，Release） | 1.20M | 60× |
| SQLite 读 | 1M 行库，均分窗口范围查 | 336 queries/s，**6.71M rows/s** | — | — |

两个组件层的结论：

- **段式摊薄 ~145×**（6.79M / 46.9k，同为 pipeline=1 的每 tick 成本比）：ADR-017 的
  "命令数与 tick 数解耦"在纯组件基准上就成立，不依赖集成条件。
- **最窄的 Redis 余量也有 11.7×**（逐 tick、零流水线的最坏姿势对常态），段式姿势下余量
  是千倍级——组件账说明 Redis 不是第一瓶颈。

复现：`./scripts/component_bench.sh`（自起临时实例 :6399，结束自动关停，不碰主实例）；
SQLite 读单独复现：`./build-release/capacity --read-bench [--read-bench-rows N]`。

## Daemon 摄入天花板（scripts/ingest_bench.sh，2026-11-11，Release）

**阶段A：纯摄入**（4×floodgen 并行不限速灌 15s，classic `--persist none`，256 符号，evictor tier3 清场）：

- **稳态 ≈ 2.2M ticks/s**（连续 6 个 2s 窗口 2.16~2.23M/s；4 发端 SENT 合计 33.1M/15s 与摄入完全咬合——TCP 反压握手成立，瓶颈在 daemon 不在 Redis 的 6.8M）
- pending 有界 ≤ 1M，evictor 跟得上（used_mem 1GB↔80MB 振荡 = 清场循环健康）
- **热路径定位（栈抓样）**：100% CPU 在 `FrameParser::feed` 逐字节状态机 + 表驱动 CRC32——下一个明确优化目标（slice-by-8 或 ARM 硬件 CRC32，预期 ×4~8）

**阶段B：写穿分步速率**（`--persist sqlite`，256 符号；前后对照 = 每批一事务 → **flush 级单事务**，ADR-012 修订）：

| 目标速率 | avg_ing 前→后 | stall_max 前→后 | 状态 |
|---|---|---|---|
| 50k | ~50k / ~50k | 64 → **41ms** | 追平；地板已消（fsync 256→1） |
| 100k | 93k / 93k | 96 → 69ms | 追平 |
| 200k | 186k / 186k | 189 → 238ms | 追平；尖峰换源 = WAL checkpoint |
| 400k | 370k / 367k | 340 → 238ms | 追平（预算内线性区） |
| 800k | 627k / **693k** | 1357 → 1088ms | 越过 500ms 预算 → 自节流 + 积压 |
| 2M | 754k / **887k** | 平台 | 钳位 |
| 3M | 680k / **784k** | 平台 | 钳位 |

三条结论：

1. **stall 模型修订**：合并前 = 地板（每批一事务 × 256 符号 × fsync）+ 线性斜率
   （0.79µs/tick ≈ 1/1.2M，与微基准吻合）；合并后地板消失，残量尖峰换源 =
   **WAL checkpoint**（autocheckpoint 1000 页 ≈ 4MB，持续写下几秒一次，单次
   200~300ms）。ADR-013 触发线在 256 符号下不再被地板越线（50k 步 stall_avg
   33ms < 10% 预算），恢复与 8 符号同式：≈ 13万 ticks/s。
2. **钳位 700k → ~850k**，未到天真的 1.3M：500ms 预算被三个串行分项共享
   （upsert 斜率 + Redis 管道收发 + 解析，全在同一根线程）。下一个杠杆候选：
   ①`PRAGMA synchronous=NORMAL`（WAL 下进程崩溃安全，仅断电丢最近提交——需独立
   ADR 改永久层耐久契约）；②ADR-012 本就 deferred 的工作线程持久化（upsert 挪出
   主循环，分项与解析重叠）。
3. **pure-ingest 2.2M vs 写穿 0.85M ≈ 2.6 倍差** = inline upsert 的代价量化，
   ADR-012 "写穿 vs classic" 的取舍从此有数：低速率常态写穿省读回，高速率场景
   classic 的 daemon 天花板高 2.6 倍（落库交隔离进程）。

复现：`./scripts/ingest_bench.sh`（~2.5 分钟，含阶段A/B；全程临时 Redis :6399）；生产基数对照加 `SYMS=5400`。

**生产基数对照（`SYMS=5400`，A股全市场口径，2026-11-11）**：符号基数本身是一笔要交的账——

- classic 摄入 2.2M → **1.5M**（−30%：每 flush 16.2k 命令的打包与管道成本随符号数线性涨）
- 写穿预算内安全线 400~500k → **~250~300k**（50k 步 stall_max 41→70ms：每 flush 固定成本
  × 5,400 批放大）；钳位 ~850k → **~700k**
- 启示：**汇报基数必须写明符号数**——256 符号的数字好看 30%，但真实部署是 5,400 只。

**ADR-019 状态 HASH 化后重测（同基数）**：classic 1.5M → **1.9M**（基数税 −30% → **−14%**，
收复过半）；写穿固定成本地板 70 → 51ms（50k 步 stall_max）。剩余的 −14% 在每符号一对
seg/ZADD 命令 + 回复解析，进一步解耦需聚合键设计（备案观察）。

## 分片规模曲线（scripts/shard_scale_bench.py，2026-11-11）

问 题：SQLite 按 symbol 哈希分片到 N 个文件 + N 个独立写者，聚合写吞吐在哪个 N 饱和？
方法：N 个文件 × N 个进程（`--mode proc`，生产同构），每写者独占文件（WAL、
`BEGIN IMMEDIATE` + 500 行 `INSERT OR IGNORE` 一事务），每档测 4s 聚合 rows/s：

| N | 1 | 2 | 4 | 6 | 8 | 12 | 16 | 24 |
|---|---|---|---|---|---|---|---|---|
| 聚合 rows/s | 454k | 865k | 1.44M | 1.71M | **2.06M** | 1.93M | 1.92M | 1.95M |
| 加速比 | 1.0× | 1.9× | 3.2× | 3.8× | **4.5×** | 4.2× | 4.2× | 4.3× |

- **拐点 N=8，饱和 ~2.06M rows/s（4.5×）**；N≥12 聚合不再增长（≤5%），N=24 出现超订阅
  抖动（写者间离散度 16%）。
- **归因修正（2026-11-12）**：服务器 32 核实测同样 N=8 最大——32 核远未吃满，服务器瓶颈
  是**磁盘**（SSD 写放大/fsync 设备级串行化），本机瓶颈是**核数**（8 性能核，N=8 时写者
  合计 ~7 核）。两台机器拐点重合是**不同瓶颈的巧合收敛**，不是 N=8 的普适常数——
  "递减饱和曲线"普适，"拐点位置"是机器最弱资源（核/盘）的函数。教训：跨机器对照先问
  瓶颈是不是同一种，再谈"互相印证"。
- **方法学警告（实测抓到）**：同模型用 **Python 线程**跑会得到单调塌缩的假曲线（N=1 即
  峰值，N≥8 地板 4 万 rows/s）——CPython GIL 的 convoy 效应，executemany 逐行 GIL 交接，
  那条曲线度量的是解释器锁与生产无关。判据：`synchronous=OFF` 剥离磁盘后照样塌缩 +
  进程 CPU 时间 ≫ 墙钟 = 线程在空转争抢。**测存储扩展性必须用进程同构口径。**
- 对本项目的含义：SQLite 分片的聚合上限 ~2M rows/s，与 classic 摄入天花板同量级（同堵
  CPU 墙）——分片只在持续 >1.2M 的 L2 级数据源下才有意义（ADR-003 死区），复现
  `python3 -I scripts/shard_scale_bench.py --mode proc`（~40s）。

## 容量账（`capacity` 五条不等式，ADR-007）

```
$ ./build-release/capacity --member-overhead 49 --persist-mode sqlite
微基准 persist_rate = 1319385 rows/s
容量账（peak=20000 ticks/s, member=113B, mode=sqlite）
  [PASS] ①稳定性 persist_rate ≥ peak×safety: persist=1319385 需≥ 40000
  [PASS] ②突发积压 < watermark: 积压=5650000B < 268435456B
  [PASS] ③出口带宽 < NIC×margin: redis=2260000B/s < 62500000B/s
  [FAIL] ④单symbol日占用 < cap: 日占=4881600000B < 1073741824B   ← 如实报警（M2 教训）
  [PASS] ⑤持续净增长 < 预留: persist_rate ≥ peak（1319385 ≥ 20000），无净增长工况
```

- **⑤ 持续净增长**（2026-11-11 加）：`max(0, peak − persist_rate) × 时段 ×
  bytes/tick < 预留`。②管单次突发（排水记零，保守）；⑤管持续工况（按最慢排水抵扣）。
  **降级演示**：persist 掉到 15k ticks/s → 净速 5000 ticks/s → 2h 净增 4.07GB vs 预留
  256MB FAIL——换算即 **~8 分钟撞软水位**（256MB ÷ 565KB/s）。这个指标回答"持久化降级
  后还能撑多久"，不是"会不会溢出"（那是 evictor 增速对账的运行时职责）。
- **超账 ≠ 必溢出**：不等式是保障条件，超出 = 保障失效。真实行情不是恒速（"固定频率 vs
  变化才推送"），实际净增速常低于上限——所以账面用上限保底，运行面用 evictor 的
  预测/实测增速对账看真值，两者互补不互替。
- **写穿内联 upsert 的 stall 随速率线性涨**（组件账推导，连接 ADR-013 控制器触发线）：
  `stall ≈ R × flush_s ÷ persist_rate`。代入 R=1.09M×10%=54.5k ticks/flush →
  **持续 R ≈ 11万 ticks/s 时 stall p99 首次越过 flush 预算的 10%**——ADR-013 备案的
  "在线切换控制器"从死代码变活代码的工况线。当前演练负载（4~8k ticks/s）离它一个
  数量级；到那天的对冲手段优先考虑工作线程持久化（ADR-012），而非运行时换拓扑。
- **v2 口径注意**：校准探针仍是 v1 ZSET 口径（78.1B/tick）；v2 段式实测总占用 113B/tick
  （ADR-017），用 `--member-overhead 49`（=113−64）对齐。

## 集成端到端（演练即基准）

| 演练 | 负载 | 对账结果 | 复现 |
|---|---|---|---|
| crash_drill（10min，kill 注入） | ≈3.6k ticks/s 持续 | SENT 2,171,600，MISSING 9,016（0.4%，kill 在途窗口，可归因） | `scripts/crash_drill.sh` |
| crash_drill 写穿 1min | ≈4.5k ticks/s | 269,520 = 269,520，stall_avg/max 3/16ms | 同上（`TICKSHELF_PERSIST=sqlite`） |
| evict_drill（淘汰+kill persister） | 8×500/s 突发5x | v2: 152,192 = 152,192，MISSING 0 | `scripts/evict_drill.sh` |
| chaos_drill（缺口→回补→收敛） | gap/ooo/dup/stall | 缺口 64→0（5 轮渐进收敛），SQLite 100,876 ≥ SENT 94,314 | `scripts/chaos_drill.sh` |
| read_bench（存储布局 A/B） | 45s 灌入后查询 | 10s 窗 53→13.5ms（−75%）、30s 窗 119→23ms（−81%）、内存 198→113B/tick | `scripts/read_bench.sh` |
| bench_compare（持久化模式 A/B） | 同负载 classic vs 写穿 | ADR-012 实测段（stall 归属对比） | `scripts/bench_compare.sh` |

**已知负载上限**：演练端到端速率被 feedgen 封顶在 8k ticks/s（ts 1ms 网格 → 1000/s/symbol
生成上限，M1 教训）。因此上表验证的是正确性口径下的吞吐，**不是** tickshelfd 的摄入天花板
（见未测清单）。

## 归因方法：计数器链优先，profiler 兜底

"哪个环节拖累"按成本递增三层排查，多数情况停在第一层：

| 层 | 工具 | 回答的问题 | 观测者效应 | 成本 |
|---|---|---|---|---|
| ① 计数器链 | 对账链 + `stats:{组件}`（ADR-013） | **哪个环节**在拖 | 零 | 免费常驻 |
| ② 组件极限 | `component_bench.sh` | 这环节**天花板多高、余量几倍** | 零（无人观测） | 分钟级，离线 |
| ③ profiler | perf / sample / Instruments | 这环节慢在**哪个函数** | 采样型 ~1–5%（非零但小） | 需要压载复现 |

**战例（2026-11-11，③仲裁①的活例）**：灌流阶段A最初的现象是"ingested 高涨、Redis 为空、
flush_err 增长"——日志把嫌疑指向 flush 层；栈抓样（macOS `sample <pid>` 30 秒）却显示
100% CPU 在解析循环。真凶是读循环无配额饿死定时器（ADR-011 修订的由来），flush 错误只是
并发症。教训：**计数器故事与栈不一致时，信栈**——profiler 层不是最后手段，是矛盾仲裁者。

①的直接读法（全部已有）：

- `SENT − ingested`：断连/解析丢失窗口（crash_drill 的 MISSING 来源）；
- `ingested − ROWS`：落库积压，`stats:persister` 的 `backlog` 直接给出数量；
- `stall_avg / stall_max`：写穿 flush 超预算（ADR-013 控制器重访条件用 p99）；
- `used_memory` 增速 vs 预测：evictor 内置对账告警（⑤的运行时对应物）；
- `pending` 段数：flush 管道健康度。

与"直接 perf"的关键区别，修正一个直觉：**区别不在观测损失**（采样型 profiler 开销只有
1–5%，"组件基准零损失、perf 损失大"这个对比不成立），而在**回答的问题不同**——组件基准
是静态余量账（改一行代码后跑一次，数字稳定可比），profiler 是动态归因（需要先有负载、
结果随工况波动），计数器链是免费的连续运行时视图。三层互不替代：①能定位环节但不给余量，
②给余量但测不到干扰效应，③给函数但最贵。

**组件和 ≠ 系统和**：缓存污染、锁竞争、队列化只在集成里出现——组件全 PASS 而集成 FAIL
是可能且值得抓的情形，这正是三层都要跑的理由（也是"集成测试总系统性能"那半问的答案：
先看计数器链，哪层计数异常再对哪层跑②，最后才③）。

## 未测清单（诚实账）

- ~~tickshelfd 纯摄入天花板~~ ✅ 已测：classic 稳态 2.2M ticks/s（见 Daemon 摄入天花板节）。
- **跨物理网卡 / 跨机**：本页全部 loopback 口径；容器 veth 与物理 NIC 要重测（基准方法讨论：
  同代码同负载两台机器，差异只剩硬件+虚拟化，才是决定性对比）。
- **SQLite 读吞吐随库容曲线**：1M 行测得 6.71M rows/s；10M/100M 行（页缓存失效点）未测。
- **WAL checkpoint 停顿**：已观测到尖峰（200k+ 持续写下 200~300ms/次，几秒一次）；
  分布特征与 `wal_autocheckpoint` 调参未做。
- **解析热路径细分与硬件 CRC**：2.2M ticks/s 的系统口径已测，但逐字节 feed 与表驱动 CRC
  各占多少未拆；slice-by-8 / ARM `__crc32*` 硬件指令是下一个优化候选。
- **`synchronous=NORMAL`**：WAL 下进程崩溃安全的耐久降档，可再削 checkpoint/fsync 成本——
  改永久层耐久契约，需独立 ADR。
- **工作线程持久化**：ADR-012 deferred 项，写穿钳位 ~850k 的下一个实质杠杆（现三串行分项
  共享单线程）。
- **capacity 校准探针**：仍为 v1 ZSET 口径，未随段式 v2 更新（暂用 `--member-overhead 49` 对齐）。
