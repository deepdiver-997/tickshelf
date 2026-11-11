# tickshelf 交接评审简报（2026-11-08，2026-11-11 更新）

> 给外部审计者（人或 agent）的自包含简报：项目现状、验收标准 vs 实测、踩坑清单、已知限制。
> 评审目标：查缺补漏——重点看"已知限制"与"重点质疑清单"两节，欢迎推翻任何设计决定。
> 依据：repo 的 docs/（roadmap/design.md 15 个 ADR/protocol v1.1/architecture.md）+ 三套演练脚本实测。

## 0. 现状与边界

- 仓库已公开（GitHub），审计者直接 clone 即可；本简报随仓库发布。
- 里程碑全部 ✅：M1 垂直切片、M2 水位淘汰+容量账、M3 混沌源+缺口回补，以及 M3.5（聚合写+最新价投影）、存储布局 v2（段式+索引）、格式版本戳+迁移/旁路策略、洪泛实测（稳态 2.2M ticks/s）；存储后端扩展（PG/ClickHouse/Parquet/共享内存环）未做。
- 形态：8 个可执行（tickshelfd/persister/evictor/capacity/queryd/feedgen/floodgen/tickshelf_tests）+ 2 个 Python 客户端；单元/集成测试套件 + 7 个演练与基准脚本。
- 一句话架构：模拟源 → TTAP 二进制协议（TCP push）→ tickshelfd（单线程 poll 收帧/攒批）→ Redis ZSET 热缓冲 → persister 租约认领读回落盘 SQLite（或 daemon 写穿模式直落）→ evictor 水位淘汰已持久化数据 → queryd 查询面（Redis+SQLite 合并读 + 缺口回补）。
- 核心契约：唯一键 (symbol, ts_exchange_ms)；at-least-once + 幂等 = 效果上 exactly-once；租约只是效率优化不是正确性保障；缺失永远显式。

## 1. 验收标准 vs 实测

### M1 垂直切片（5 条）
| # | 标准 | 实测 | 证据 |
|---|---|---|---|
| 1 | feedgen 可配置速率+突发 | ✓ 速率/突发倍率/断流/重连全部参数化 | feedgen --help |
| 2 | tickshelfd 解析帧（CRC 错帧丢弃计数）+ 500ms 攒批 ZADD | ✓ 联调 SENT=ZCARD 精确一致；错帧三道防线（长度互证/丢帧继续/重同步） | scripts/chaos_drill.sh、单测 |
| 3 | persister 租约→SQLite 幂等→标记 | ✓ 双实例/抢占/幂等重做/空批用例 | tests/test_lease_persister.cpp |
| 4 | 崩溃注入 10min：SQLite 零重复、无丢失（Redis 窗口除外） | **PASS：SENT 2,171,600 / ROWS 2,162,584 / 重复 0 / MISSING 9,016（0.4%）/ 残留 pending 0**；MISSING=14 次 kill tickshelfd 的 TCP 在途窗口 | scripts/crash_drill.sh |
| 5 | 解析器 fuzz：随机流 CRC 拒绝率 100% | ✓ fuzz 300 轮 + count/payload_bytes/crc32 单字段翻转全拦截 + 载荷假锚点重同步恢复 | tests/test_frame_parser.cpp |

### M2 水位淘汰 + 容量账
| 标准 | 实测 |
|---|---|
| evictor 只删已持久化（安全水位 = min(pending first_ts)−1，纯 state 键名推导） | ✓ 端点精确性单测（水位上成员不动） |
| state 键 GC | ✓ 水位覆盖整批即 GC；全 done 旧日键整键删 |
| capacity 五条不等式 + 常数采样校准 | ✓ `--calibrate` 实测 78.1B/tick；SQLite 微基准 124 万 rows/s；④如实 FAIL（500/s 连续流一天 9.5GB/symbol 超 1GB 默认预算——不等式在正确报警） |
| 预测/实测增速对账 | ✓ evictor 内置累计口径对账；错值（1600B/tick）被 -98% 偏差逮住 |
| 故障演练：淘汰中杀 persister 验证不变式 | **PASS：177,504=177,504 MISSING 0**；杀窗口内存 2.48→8.64MB（水位停住只涨不跌），恢复后追平 |

### M3 混沌源 + 缺口回补
| 标准 | 实测 |
|---|---|
| feedgen 混沌：乱序/重复/断流/重连突发 | ✓ gap（真缺口）/ooo（延迟乱序）/dup（帧重发）/stall（断流→心跳判死→重连突发），全部生成先入 ring |
| 缺口回补懒触发（backfill 键 SETNX 去重，ring 直写 SQLite 不进热缓冲，窗外显式 unrepairable） | ✓ 端到端闭环 |
| 查询组件（Redis+SQLite 合并去重 + 缺口检测 + 回补触发） | ✓ 单测（合并去重/头尾半开缺口/跨日界/触发去重） |
| queryd：TTAP v1.1 双向 | ✓ protocol.md 升版；老解析器向前兼容（未知 type 放行） |
| SDK 跨语言证明 | ✓ examples/query_client.py 纯 struct 手打包 |

## 2. 踩坑清单（已解决，按层）

### 协议/解析
1. **双向帧破坏 count 语义**（M3 最大坑）：查询请求 payload=24B、响应含缺失块，`count×64==payload_bytes` 互证只对 tick 批成立 → 查询帧永不过防线 1 → 永不解析，30s 空闲超时才暴露。修：互证按 type 分型（tick 批/心跳保持互证；控制型只查 256KB 上限防炸弹，结构由消费方校验）。**副作用有数**：控制型互证变弱后，随机流可拼出"看似合法但 version≠1"的控制帧候选 → 按协议致命断连（fuzz 预期同步更新：fatal=换连接重试）。
2. **防线顺序**：version 检查必须在长度互证之后——否则载荷里的假 magic + 随机 version 字节会误判致命断连（正确顺序：互证 → version → CRC → 重同步）。
3. RESP 解析器增量回退：数据不完整必须整体回退到 tag 字节前；部分回退会让重入把数据字节当类型字节解析。

### 键空间/数据结构
4. **键名 vs 批id 混淆踩了两次**（persister、evictor 各一次）：SCAN 返回带 `state:` 前缀的全键名，直接当裸批 id 解析 → 把 "state" 当 symbol。教训：键名↔id 转换应收口到一个函数，边界处显式剥前缀。
5. **ZSET member 去重被误当故障**（capacity 探针）：合成 member 用周期模式 `(i+j)&0xFF` → i 与 i+256 的 member 相同 → 2000 发只落 256 个 → MEMORY USAGE 腰斩、算出负开销。教训：探针数据的唯一性要显式构造（嵌序号）。
6. Tick 64B 布局：手工逐字段 LE 编解码 + offsetof 静态断言钉住，不依赖宿主端序/结构体 padding。

### 网络/节流/时钟
7. **双时钟口径**（⚠️ 审计重点）：feedgen 交易所 ts 每轮 +1ms 但轮速=速率——rate<1000 时 **ts 时钟比墙钟慢**（400/s 时 2.5 倍）。按墙钟定查询窗口会问进 ts 空间的"未来"→ 整窗缺失且 ring 窗外不可回补（语义全对、口径全错）。修：feedgen stats 暴露 ts_first/ts_last，查询用交易所 ts 显式定窗。**任何消费 ts 的组件必须声明用哪个时钟。**
8. 节流双 bug 叠加：单位换算错（吃秒喂毫秒）→ 修后 interval≤1ms 时 `now+1<next` 睡眠条件永不触发 → 32 万轮/s 自旋灌满内核缓冲 → 非阻塞 send 的 EAGAIN 被误判断连。修：条件改 `now<next`；**有效速率封顶 1000/s/symbol（ts 1ms 步长是唯一键硬约束，突发倍率不得突破）**；EAGAIN 走 poll(POLLOUT)。
9. TCP 反压是正确行为不是故障：Redis 慢 → flush 慢 → 接收放缓 → 内核反压 driver（ADR-011）。EAGAIN ≠ 断连。

### 持久化/故障语义
10. **tier2 stale lease 真丢失窗口**（M2 演练抓到，丢 1,428 ticks）：worker 被杀后租约 TTL（60s）未过，tier2 视为"在飞、worker 内存有数据"而删其 Redis 数据——worker 已死即真丢。tier2 默认关，`--inflight` 显式开（水位告急时的设计内取舍）。ADR-004 有状态注记。
11. **pipeline 顺序即正确性**：Redis pipeline 非事务、其他客户端可穿插——classic 模式 state 必须在全部 ZADD 之后（否则 persister 读到半批）；写穿模式 lease 必须在 state 之前（否则 persister 在 daemon 落库期间抢占重复劳动）。
12. SQLite"单写者"= 同时单写者：WAL + busy_timeout 下多进程排队写可行（写穿 daemon + persister 共存实测无争用）；但 journal_mode 切换的初始化锁**不吃** busy_timeout → 多进程同瞬间首开同一新库要退避重试。

### 观测/口径/脚本
13. 增速对账口径：短窗口内存增量被 jemalloc 页粒度平滑（500ms 窗口实测 ~30B/tick vs 逻辑 ~220B）→ 必须累计口径 + ≥5000 ticks 再裁决，容限 50%（rehash 相位实测 33% 波动）。
14. pipefail 陷阱：`VAR=$(grep … | tail -1)` 无匹配 × pipefail × set -e 无声杀脚本且无输出；**外层管道（`script | tail`）的 exit 0 是 tail 的，不是脚本的**——M2 首轮演练假 PASS 就是它。
15. 观测与控制分离（ADR-013）：stats 自过期键只做观测；写穿/经典在线切换的控制器备案不建（stall 对预算余量 30~160×，触发条件留档：stall p99 > flush 间隔 10%）。

### 设计决定（有 ADR 编号，可挑战）
16. 回补数据不进热缓冲（M2 oldest-first 会立即再淘汰）→ 直接 SQLite 直写；ring 窗外缺口显式 `unrepairable`（ADR-014）。
17. 懒触发回补：缺口检测免费发生在查询点（SDK 契约=显式缺失），同点触发回补；缺口数超单轮上限（64）时**渐进收敛**——客户端循环重查，每轮触发下一批，是流控不是缺陷。
18. 查询面独立进程 + TTAP 双向，不搭 tickshelfd（防查询 burst 顶住摄入）；HTTP 不进契约（ADR-015）；语义进核心库，进程只是薄壳。
19. 与 quantify（同目录项目）不复用代码只引用模式（ADR-010）：ring 参考 journal head 原子提交；崩溃一致性对比叙事（commit-point 恢复 vs 幂等重放）。

## 3. 已知限制 / 未做（查缺的抓手）

1. **代码未推远端**——审计者需先解决代码传输。
2. M4 全部未做：PG/ClickHouse 对比、Parquet 冷层、共享内存环接入。
3. 租约分块续期未做：批 > 60s TTL 的场景没有保护（当前批 ≤ 500ms 窗口，真实压力下未必）。
4. backfill 执行器认领是 GET+DEL（竞态双做窗口，靠幂等兜底），未做成 lease 式。
5. 缺口检测假设 1ms ts 网格（生成器契约）——真实源没有这个保证，step 可配但语义依赖源方承诺。
6. queryd 单线程：查询在 Redis/SQLite 读上串行阻塞，无并发查询能力；响应无分页/大小上限（大范围查询一次 build 全量 payload，内存无界）。
7. 查询响应的 missing 上限 64 区间：缺口更多时渐进收敛（客户端循环），无"一次要全"模式。
8. evictor tier3（紧急真丢层）只有单测覆盖，从未在演练参数下触发过。
9. 混沌的 ooo/dup 无独立端到端断言（正确性靠 ZSET member 去重 + SQLite IGNORE 间接保证；drill 只断言 gap 驱动的回补闭环）。
10. capacity 默认参数组合自相矛盾：默认 rate/symbols 下 ④ 永远 FAIL（9.5GB/day > 默认 1GB cap）——要么改默认 cap 要么文档标注。
11. 全部进程无认证/无 TLS——localhost 可信网络假设，契约里没写。
12. evictor 不跑时 state 键无限堆积（GC 只在淘汰轮里做）。
13. Python 两个客户端无重试/无超时处理。
14. tickshelfd 崩溃重启后内存桶全丢（500ms×速率的窗口），靠"无状态重启"语义兜着——没有 WAL/恢复机制，这是设计取舍但值得被质疑。
15. 写穿模式下 daemon 持有 SQLite 句柄——"无状态"破了一条口子（崩溃窗口靠租约 TTL + persister 补偿，语义等价但拓扑变了）。

## 4. 重点质疑清单（给审计者的靶子）

1. 双时钟问题（§2.7）是否还有未对齐的消费方？evictor 的 yyyymmdd 分桶、回补键的 range、容量账的"日占用"各自用哪个时钟，有没有隐性墙钟假设？
2. tier2 的丢失窗口之外，写穿模式还有没有类似"文档说免费、故障下收费"的层？
3. 控制型帧互证弱化后，恶意/损坏流对 queryd 的最坏影响是什么（大量 24B 假查询 = CPU/内存放大）？
4. backfill 键状态机（requested→done/unrepairable）有没有卡死态：requested 后 feedgen 死了谁来收尾？（当前答案：键 EX 600 过期，缺口在下次查询时重新触发——确认这闭环成立。）
5. 崩溃注入只打了 tickshelfd/persister；queryd/evictor/feedgen 的 kill 演练缺失。
6. 三层对账（SENT ≥ ingested ≥ rows）的"ingested"目前只在 daemon 内存里，没有持久化快照——重启后中间层证据丢失，审计链断一环。
7. 互证分型后 tick 批防线未变，但**控制型帧的 CRC 仍校验**（防线 2）——确认这条在协议文档与实现一致。
8. SQLite 单文件增长无上限（没有 compaction/分区）——长期运行的容量账没算它。
