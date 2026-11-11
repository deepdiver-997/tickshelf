# 里程碑

> 原则：每个 M 结束都要能 demo；**M1 完成就足以作为简历项目讲故事**，M2 起按兴趣推进。每个 M 收尾写一段"实测/教训"进本文件对应节。
> 2026-11-09 结构修订：接入契约改为"协议 + 守护进程"（ADR-009），原 M3 事件循环删除，后续里程碑重编号。

## M0 骨架 ✅（2026-11-08）

- CMake + doctest（vendored 单头文件）+ 空跑通过
- 文档四件套：README / design(ADR) / roadmap / protocol

## M1 垂直切片 ✅（2026-11-08）

**验收标准（全部达成才算完）**：

1. `feedgen` 以可配置速率发帧，支持突发模式
2. `tickshelfd`（核心库 + TCP 前端）解析帧（CRC 错帧丢弃并计数）、按 500ms 间隔攒批 ZADD 进 Redis
3. `persister` 租约认领 → SQLite 幂等落库 → 状态标记
4. **崩溃注入**：运行中随机 `kill -9` tickshelfd/persister 后重启，跑 10 分钟后断言：SQLite 无重复记录；对照 feedgen 发送计数无丢失（Redis 自身崩溃窗口除外，单独标注口径）
5. 解析器单测：随机字节流喂入，CRC 拒绝率 100%

组件 checklist（先核心库直连测试，再包 TCP 前端，见 ADR-009）：

- [x] `common/`：Tick 结构（static_assert(sizeof==64)）、定点价类型（i64, 1e-4）、CRC32
- [x] `libtickshelf/`：帧解析状态机（半包/粘包）、错帧三道防线（长度互证 / CRC 丢帧继续 / 失步重同步，见 protocol.md）、进程内 ingest API、RESP 客户端（连接 + pipeline ZADD）、攒批定时器（时间片循环）
- [x] `tickshelfd/`：TCP server 前端（TTAP 契约）→ 核心库 ingest；无状态，崩溃即重启
- [x] `feedgen/`：TCP **client**、速率与突发配置、周期性心跳帧（参考驱动，验证接入契约）
- [x] `persister/`：SCAN 候选批 → SETNX 租约 → 读批 → upsert → SET 标记
- [x] `tests/`：核心库直连测试（不走网络）、解析器 fuzz 测试、单字段翻转拦截（count/payload_bytes/crc32）、失步重同步（假 magic 跳过、真头恢复）、幂等注入（同 key 重复 upsert 不产生第二行）、租约抢占测试
- [x] （可选，证明跨语言）`examples/driver.py`：~100 行 Python 按 TTAP 协议喂帧

实测/教训（2026-11-08）：

- **基线零丢失**：5s×4sym×200/s 联调，feedgen SENT 与 Redis ZCARD 精确一致；端到端（含 persister）4s SENT 6396 = SQLite 6396，45 批全 done。
- **崩溃演练（10min，8×500/s 突发 5x）**：SENT 2,171,600 / SQLite 2,162,584（唯一键数）/ **重复 0** / MISSING 9,016（≈0.4%，kill tickshelfd 14 次的在途窗口：内核 socket 缓冲 + 未 flush 攒批，预算内）；kill persister 3 次只延迟不丢账（数据留在 Redis 等重认领），收尾残留 pending=0。
- 解析器验收全过：fuzz 300 轮垃圾流后干净帧完整恢复；count/payload_bytes/crc32 单字段翻转全部被对应防线拦截；载荷埋假 magic 被互证挡下并在真头恢复；CRC 拒绝率 100%。
- **教训 1（feedgen 节流）**：`ms_of()` 吃秒却喂了毫秒值 → 轮间隔 5s；改对后 interval≤1ms 时 `now+1 < next` 睡眠条件永不触发 → 32 万轮/s 自旋灌满内核缓冲 → 非阻塞 send EAGAIN 被误判连接死亡。修法：睡眠条件改 `now < next`、**有效速率封顶 1000/s/symbol**（ts 1ms 步长是唯一键硬约束，突发倍率不得突破）、EAGAIN 走 poll(POLLOUT) 等可写（TCP 反压是正常行为）。
- **教训 2（RESP 解析器）**：增量解析数据不完整时必须整体回退到 tag 字节之前——部分回退会让重入把数据字节当类型字节解析。
- **教训 3（键名 vs 批id）**：SCAN 返回带 `state:` 前缀的全键名，直接当批 id 传给处理函数 → 解析把 "state" 当 symbol。键名与 id 是两个概念，边界处剥前缀。
- 批id/键空间约定（symbol 不含 `:`/`-`、日期段 UTC、批id=min-max 可重叠）已落 protocol.md v0.1。

## M2 水位淘汰 + 容量账 ✅（2026-11-08）

- [x] `evictor/`：内存水位阈值 → 只删"已标记持久化"条目；层间按丢失代价、层内按行情时间（ADR-004）
- [x] **安全水位语义**：watermark(sym,date) = **min(所有 pending 批的 first_ts) − 1**，删 [0, watermark]；纯从 state:* 键名（含首尾ts）推导，零新结构
- [x] done state 键 GC：水位推进覆盖整批范围后删其 state 键
- [x] 容量校验子命令 `capacity`：五条不等式（ADR-007，⑤持续净增长 2026-11-11 补充）+ SQLite 微基准测 persist_rate + `--read-bench` 读吞吐 + `--calibrate` 探针实测每 member 占用
- [x] 预测/实测对账：evictor 内置（stats:tickshelfd 增量 × 校准值 vs MEMORY USAGE 增速，累计口径，偏差告警）
- [x] 故障演练 `evict_drill.sh`：淘汰进行中 kill persister，不变式保持（见实测）

实测/教训（2026-11-08）：

- **演练 PASS**：45s 8×500/s（突发5x）+ 软水位 4MB 强制全程淘汰（删 165,216 members）+ 中途 kill -9 persister 12s → kill 窗口内存 2.48MB→8.64MB（水位停住、只涨不跌）→ 恢复追平 **SENT 177,504 = ROWS 177,504，MISSING 0**，tier3 零触发。
- **教训 1（tier2 的 stale lease 边缘，演练抓到）**：worker 被杀后租约 TTL（60s）未过期，tier2 视为"在飞、worker 内存有数据"而删其数据——worker 已死即真丢（首轮演练丢 1,428 ticks）。tier2 默认改为关（ADR-004 状态注记），`--inflight` 显式开。**"几乎免费"的层在故障注入下不免费**。
- **教训 2（增速对账口径）**：短窗口内存增量被 jemalloc 页粒度平滑（500ms 窗口实测只有 ~30B/tick），必须累计口径 + ≥5000 ticks 再裁决；且校准探针的 member 生成用了周期模式 `(i+j)&0xFF`——ZSET 按 member 去重后 2000 发只落 256 个，MEMORY USAGE 腰斩（-45.5B 的负开销就是症状）。修：member 嵌 i 唯一化，实测开销 ≈78B/tick（64B member + ZSET/哈希表摊销）。容限默认 50%（rehash 相位波动 33%）。
- **教训 3（pipefail 陷阱）**：drill 脚本 `VAR=$(grep ... | tail -1)` 在无匹配时 grep 退出码 1 × pipefail × set -e 直接杀脚本且无输出——第一轮"PASS exit 0"是外层管道 tail 的退出码，全是假象。断言用的值改从日志参数展开提取。
- capacity 默认参数下 ④（单 symbol 日占用）如实 FAIL：500/s 连续流一天 9.5GB/symbol 超 1GB 默认预算——不等式在正确地报警，不是工具错。

## M3 混沌源与缺口回补 ✅（2026-11-08）

- [x] feedgen 混沌模式：gap（真缺口）/ ooo（延迟乱序）/ dup（帧重发幂等）/ stall（断流→心跳判死→重连突发），全部生成 tick 先入 ring（ADR-006）
- [x] 缺口回补（**懒触发**，ADR-014）：查询命中缺口 → `backfill:{sym}:{from}-{to}` SETNX 去重（租约仲裁第三次复用）→ feedgen 回补执行器 ring 提取 → **直接 SQLite 幂等 upsert，不进热缓冲** → done/unrepairable（ring 窗外显式标记）
- [x] libtickshelf 查询组件 `RangeReader`：Redis（热）+ SQLite（耐久，WAL 并发读）合并去重 + 缺口检测（期望 ts 网格）+ 回补触发语义（ADR-015）
- [x] `queryd` 薄进程：TTAP v1.1 双向查询（type=2，flags bit1=缺失区间），包核心库
- [x] SDK 证明：`examples/query_client.py`（纯 struct 手打包的查询客户端，含收敛重查循环）

实测/教训（2026-11-08）：

- **演练 PASS**（scripts/chaos_drill.sh）：混沌源 40s（gap 2%/ooo 2%/dup 5%/stall 6s 每 10s）→ 首查缺口 64 区间（上限截断）显式上报 → 5 轮收敛重查（每轮懒触发 64 区间回补，feedgen @1s 从 ring 直写 SQLite）→ 终态 missing=0、ticks 15529→15815、SQLite 100,876 ≥ 发送 94,314（回补把被丢的生成补回）。
- **教训 1（双向帧破坏 count 语义）**：查询请求 payload=24B、响应含缺失块——`count×64==payload_bytes` 互证只对 tick 批成立，查询帧永远过不了防线 1 → 永不解析 → 30s 空闲超时才暴露。修：互证按 type 分型（控制型只查 256KB 上限防炸弹，结构由消费方校验）。副作用要有数：控制型互证变弱后，随机流偶尔拼出"看似合法但 version≠1"的控制帧候选 → 按协议致命断连（fuzz 测试预期同步更新，fatal=换连接）。
- **教训 2（双时钟口径）**：feedgen 的交易所 ts 每轮 +1ms，轮速=速率——rate<1000 时 **ts 时钟比墙钟慢**（400/s 时 2.5 倍）。查询窗口按墙钟回溯会问进 ts 空间的"未来"→ 整窗缺失且 ring 窗外不可回补（语义全对，口径全错）。修：feedgen stats 暴露 ts_first/ts_last，查询用交易所 ts 显式定窗。
- 缺口数超单轮上限（64）时回补是**渐进收敛**的——客户端循环重查直至闭合，每轮触发下一批；这不是缺陷是流控。

## M3.5 聚合写 + 最新价投影 ✅（2026-11-10）

- [x] 按 key 聚合 ZADD：同批 tick 一条多对 ZADD，命令数与 tick 数解耦（N tick/M 股 = M 条）
- [x] 全局最新价投影 `latest`：flush 进程内聚合每 symbol 最新 → 单条 HSET；回补不改投影
- [x] 查询面 type=3 最新价帧 + queryd HGET 快速路径 + `query_client.py --latest`
- [x] flush 命令构建抽纯函数 `build_flush_commands`（无 Redis 单测命令结构）

实测/教训（2026-11-10）：同类反模式（逐 tick Lua 更新投影，N tick/M 股 = N 次调用）沉淀为 ADR-016；tickshelf 原实现 pipeline 批量已对但命令粒度未聚合；投影语义（最终一致/chaos 短暂回退/可重建）见 ADR-016。刻意不用 Lua——聚合之前不需要 Lua。

## M4-α 存储布局 v2：段式 + 索引 ✅（2026-11-11）

- [x] 段式布局：每批一个原始字节段（seg 键）+ 每符号索引（idx ZSET），段与批 1:1
- [x] 读路径 verbatim：索引查段 → 整段原始字节进响应（服务端零解码零重编码）
- [x] evictor 简化：水位机退役（段/批 1:1 使 done 段整删构造性安全），tier2/3 段粒度保留
- [x] 基准对比（read_bench.sh 同负载 A/B）：10s 窗口查询 53→13.5ms（−75%）、30s 窗口 119→23ms（−81%）、内存 198→113B/tick（−43%）

实测/教训：读取侧教训（索引记位置、只回命中段原始数据）与写入侧教训同源——逐 tick 结构在 Redis 里既有每成员开销又把查询成本绑在 tick 数上。三套演练（崩溃/淘汰/缺口回补）在 v2 布局下全部重跑 PASS。

## 第二批：格式版本戳 + 迁移/旁路策略 ✅（2026-11-11，ADR-018）

- [x] SQLite `PRAGMA user_version` 格式戳：新库盖 1、不匹配拒绝打开（永久层格式可检测，旧二进制响亮失败而非静默误解码）
- [x] v1 残留退役：`quote_key` 辅助、ingestor 头注释、design/protocol 两处文档的 ts: 行情键条目清理
- 备案不实现（真源/新需求出现时再领走）：Redis 段格式 v3 双读配方、TTAP 双版本退役窗口、回补任务预算/优先级/保底、派生数据旁路（K 线/指标消费者）

实测/教训：生产期三个通用课题（资源配额分级+保底、处理旁路隔离、迁移双读）对照盘点。判据——哪些立即做：SQLite 版本戳是现存真缺口（永久层格式不可检测）；哪些只备案：回补预算在自家 ring 源下无预算可耗（投机实现），旁路规则在组件模型里已由构造成立（写下来是为了不让下一个派生需求溜进 flush 时间片）。v1→v2 迁移是 dev 期 flag-day 侥幸，教训恰好补上"生产期怎么迁"。

## 摄入天花板实测 + 时间片循环饿死修复 ✅（2026-11-11）

- [x] `floodgen` 灌流驱动：不受 1ms ts 网格限制（ts 每 tick +1），帧粒度限速/不限速，`--sym-base`/`--ts-base` 支持多进程拆分
- [x] `ingest_bench.sh`：阶段A 纯摄入天花板（4 发端不限速 + evictor tier3 清场）+ 阶段B 写穿分步 stall 线性验证
- [x] daemon 修饿死缺陷（ADR-011 修订）：read_session 每会话每轮 1MB 读配额——洪泛下 recv 循环永不返回，flush/stats 定时器被单个高速客户端无限推迟（实测 42.9M ticks 只进内存不落 Redis、pending 2.7GB）
- [x] 实测（Release，benchmarks.md 有全表）：classic 稳态 **2.2M ticks/s**；写穿 stall = 地板（64ms = 256 符号 × fsync）+ 斜率（0.79µs/tick ≈ 1/1.2M），400k 预算内、800k 越线自节流、钳位 ~700k

实测/教训：①"时间片循环"没有读配额就不是时间片——计数器故事（flush failed）与栈抓样不一致时信栈，profiler 是矛盾仲裁者不是最后手段；②基准必须 Release（Debug 历史数字已全部重测替换）；③基准脚本要预检僵尸进程——上轮 PIDS[0] 误标漏杀的 daemon 占着 stats 键和监听端口，样本混入冻结计数器、新 daemon bind 失败后整个阶段跑在僵尸上。

## 写穿 flush 级单事务（ADR-012 修订）✅（2026-11-11）

- [x] `SqliteTickStore::upsert_flush`：多批指针一次 BEGIN…COMMIT，`upsert_batch` 变单批包装；daemon 写穿段接入
- [x] 语义：回滚半径批原子 → flush 原子（恢复路径不变，release_claims → persister 读回）；测试补跨批去重/幂等重放/空批
- [x] 实测（256 符号分步，benchmarks.md 有前后对照）：stall 地板 64ms → 消失（fsync 256→1，残量尖峰换源 WAL checkpoint）；ADR-013 触发线不再被 256 符号地板越线；钳位 700k → ~850k（未到 1.3M：预算被 upsert+管道+解析三串行分项共享同一根线程）
- 端到端冒烟：SENT 76,584 = ROWS 76,584 零重复零故障行

下一个杠杆（备案）：`synchronous=NORMAL`（需耐久契约 ADR）、工作线程持久化（ADR-012 deferred 项，钳位实质杠杆）。

## M4（可选）存储扩展

- PG 后端（`ON CONFLICT` / COPY）与 SQLite 行为对比记录
- ClickHouse POC：列存压缩比、范围扫描、ReplacingMergeTree 去重、异步 insert，拿实测数字
- Parquet 冷层 + DuckDB 查询验证
- （更远）共享内存环接入路径，替换 localhost TCP（ADR-009 代价项）
