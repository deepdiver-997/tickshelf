# tickshelf

TickShelf 有两条行情管线。TTAP 走 Redis 热缓冲和租约式落盘，适用于固定 64B Tick。iQuant 走 IQTS，先保存行情快照原文并确认接收，再由 C++ worker 处理变长历史记录，可选发布到 Redis Stream。Redis 客户端使用手写 RESP。

## 为什么做这个

行情数据基础设施是量化系统里最经典的"热缓冲 → 耐久存储"形态。本项目用 C++ 把整条管线从零打一遍，学习目标：

- **RESP 协议**：手写 Redis 客户端，理解协议层而不是只会调库
- **存活缓存上的重试 + 幂等落库**：租约协议、崩溃窗口、幂等 upsert，用 `kill -9` 验证限定恢复域；不承诺端到端 at-least-once/exactly-once
- **分层存储与水位淘汰**：层间按丢失代价、层内按时间价值
- **容量模型即代码**：启动时用不等式验证配置，预测/实测对账
- **接入契约设计**：协议即跨语言接口，进程边界做故障隔离——不做 C ABI 绑定（ADR-009）
- **崩溃一致性方案对比**：commit-point 恢复 vs 幂等重放，选型依据是恢复成本与重放成本之比（ADR-010）

## 架构与数据流

两条管线的进程边界、数据流与崩溃恢复见 [架构说明](docs/architecture.md)。已交付和待验证的范围见 [里程碑](docs/roadmap.md)。

**TTAP 交付/耐久合同 [`tickshelf-delivery-v1-20261117`](docs/delivery-contract.md)**：最终 Storage
提交前允许永久丢失，在途、accepted、staging、Redis pending 均不耐久，**没有固定 RPO**。
`accepted` ≠ `published` ≠ `storage_committed`，后者强度取决于实际 backend/sync；TTAP
当前无摄入 ACK，不承诺上游重放。Redis 可无 AOF/RDB且不是恢复权威；服务 SQLite 保持
现有设置，以实际连接 PRAGMA 回读解释，不断言默认 FULL；显式 FULL 部署不得降级。
Benchmark 的 FULL/WAL sync 基线不降低。故障允许 loss 不意味着健康 benchmark 丢数据也能 PASS。
2026-11-14/15 历史机器证据保持原样，不据此合同回填未观测的配置或宣称重跑。

```
driver(任意语言) ──TCP·TTAP──▶ tickshelfd ──RESP──▶ Redis (ZSET, score=行情时间)
                                     │                    │
                              核心库 libtickshelf    persister（租约 + 幂等 upsert）
                                     ▼                    ▼
                               进程内 ingest API        SQLite
```

- **TTAP 接入**：任意支持 TCP 的语言都可以按二进制协议发送行情，仓库包含 Python 参考驱动。见 [TTAP 协议](docs/protocol.md)。
- **feedgen**：参考驱动（C++），模拟行情源：随机游走 + 开盘/收盘突发；M3 加乱序/重复/断流混沌模式。只用模拟数据——repo 自包含，也回避真实行情数据的授权问题。
- **tickshelfd**：守护进程。收帧（CRC 校验）→ 核心库 ingest → 按固定时间间隔攒批写 Redis。可重启，但未提交数据不是耐久状态，崩溃可能永久丢失。
- **libtickshelf**：核心组件库（攒批、RESP 客户端、租约落盘），暴露进程内 ingest API，测试直连不走网络。
- **persister**：独立进程。租约认领 → 幂等落库 → 改标记。多实例安全。
- **evictor / queryd**：分别负责热缓存淘汰和 TTAP 热冷合并查询。

**iQuant 接入**以 raw 同步作为 ACK 边界。SQLite 事务提交历史记录与处理进度，后台从 raw 补扫，Redis Stream 只承载可选热发布。历史查询使用实例目录的 `history.sqlite3`。见 [iQuant 交付边界](docs/delivery-contract.md#9-iquant-iqts-delivery-contract)。

## 设计决定

全部取舍记录在 [docs/design.md](docs/design.md)（ADR 格式），包括：为什么热缓冲用 Redis 不用消息队列、为什么 v1 落库用 SQLite 不用 PG、为什么按时间间隔攒批不按条数、租约为什么不是正确性保障而只是效率优化。

## 快速开始

### iQuant 快照采集与历史处理

安装 SQLite 开发库并构建 worker。启动接收器前，按操作手册配置 iQuant 策略、回环端口和两端共用的环境变量 `TS_IQUANT_TOKEN`。

```sh
cmake -S . -B build-iquant -DTICKSHELF_IQUANT_ENGINE_ONLY=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build-iquant --config Release
python3 -B examples/iquant/receiver.py --work-dir ./run/iquant \
  --engine-bin ./build-iquant/iquant_engine
```

实例独占工作目录，raw、进度和 SQLite 均保存在目录内。可加 `--redis-port 6379` 开启热发布。证券订阅、Windows 启动、恢复与备份见 [iQuant 操作手册](docs/iquant-capture.md)，SQLite、Parquet 和 ClickHouse 的导入、查询及空间占用见 [本机存储对照](docs/iquant-storage-benchmark.md)。

### TTAP 服务

前置：CMake ≥ 3.24、C++20 编译器、本地 Redis（`brew install redis && brew services start redis`）。

```bash
cmake -B build
cmake --build build
./build/tickshelf_tests --test-case-exclude='*redis-server*'   # 不连接共享 Redis 的单元测试
```

部分既有 Redis 集成用例固定连接 `127.0.0.1:6379` 并清空 DB 15；完整 CTest 只能在专用测试环境运行，不能把“Redis 不可达会跳过”当成隔离保障。

**同一份配置启动四个服务**（Redis 须已就绪）：

```bash
python3 scripts/run_stack.py --build build --config examples/tickshelf.ini
# 前台运行；Ctrl-C 停止并等待自身子进程，不停止外部 Redis。
# 单服务仍可独立运行，显式 CLI 覆盖文件：
./build/tickshelfd --config examples/tickshelf.ini --port 9920
./build/persister --config examples/tickshelf.ini --print-config  # 只校验/打印，不建库
```

配置语法、路径与覆盖规则见 [docs/configuration.md](docs/configuration.md)。其中只读
`redis_runtime_snapshot` helper 为显式声明的 Redis host/port 记录 requested/observed、
ACL/缺失/错误及版本/PID；不修改外部 Redis，且不接入无网络的 `--print-config`。
可选外部 JSON mock 的私有 Redis 全链演练/压测见 [docs/real-source-sim.md](docs/real-source-sim.md)。

**分片异步持久化引擎（可选，默认关闭；ADR-023）**：

```bash
./build/tickshelfd --config examples/tickshelf-engine.ini   # engine=true, persist=sqlite, 4 shards
# CLI 覆盖示例：--engine --persist sqlite --shards 4 --engine-batch-records 8192
```

`--engine off`（默认）保持原同步 flush。写穿/仅发布/直落存储由 `--persist sqlite|none|storage` 决定（`storage` 为历史回填直落库，不开 Redis，ADR-026；可叠加 `--raw-journal` 让本体先于索引落盘、重启幂等重放，ADR-027）；写穿可再叠加 `--engine-pipeline`，每分片发布线程与存储线程经有界队列流水线重叠 RTT 与 SQLite 提交（ADR-028，默认关）；引擎为每分片开一条 Redis 连接和一个 SQLite 分片文件，文件布局/路由与 `ShardedTickStore` 一致，persister/queryd 继续兼容。私有端到端：`python3 scripts/test_engine_runtime.py --build build`；旧/新网络对比：`python3 scripts/engine_bench.py --suite network --build build-release --duration 2 --rate 250000 --senders 4 --output /tmp/my-new-engine-network`（新目录）。原生十六格用 `--suite native`，见 [消融实验说明](docs/ablation-benchmark.md)。实测边界见 [ADR-023](docs/design.md) 与 [历史引擎实测](docs/engine-refactor-20261115.md)。

**五档 JSON / 变长 Record 性能实验台（可选，非生产 TTAP 接口）**：

**默认输入全部是合成行情，不是真实生产捕获。** `backend=rocksdb` 已将完整记录接入 JSON/TCP/hash/真实 Redis 链路；原文捕获回放、独立对账、来源分级及数据库/syscall 口径见 [全记录基准说明](docs/full-record-benchmark.md)。正确性或合成容量 PASS 不代表生产基准合格。

2026-11-17 的 [原生消融与批量复核报告](docs/ablation-results-20261117.md) 提供 compact SQLite / RocksDB 各32行真值表、真实 Engine 八格、查重诊断及30万条三轮批量对照；开关与复跑命令见 [消融实验说明](docs/ablation-benchmark.md)。短路格不授容量认证，DCRB未适配。

全记录 SQLite/RocksDB 的百万条三轮、SQLite 判重 SQL 优化及 60 秒容量复核见 [全记录实测报告](docs/full-record-results-20261114.md)（[机器证据](docs/full-record-results-20261114.json)）。Mac 全记录 100k 目标仍未达到；下文旧 104,904 条/s 成绩属于“文件＋SQLite 稀疏索引”，不是全记录数据库成绩。

```bash
# 额外依赖：simdjson >= 5、OpenSSL Crypto；普通服务构建不需要。
cmake -S . -B build-bench-release -DCMAKE_BUILD_TYPE=Release -DTICKSHELF_BUILD_BENCHMARKS=ON
cmake --build build-bench-release
./build-bench-release/tickshelf_bench_tests
python3 scripts/pipeline_bench.py --build build-bench-release --suite smoke --output /tmp/tickshelf-pipeline-smoke  # 结果目录须不存在
```

原始 JSON → TCP → simdjson → 完整五档 Record → hash/查重 → 私有 Redis → SQLite FULL 或追加文件＋稀疏索引，支持 `--matrix` JSON 轴组合、配置化消融、文件直读对照、独立 oracle 与同轮异常触发采样。官方字段来源、实验包装边界、服务器矩阵和耐久说明见 [docs/pipeline-benchmark.md](docs/pipeline-benchmark.md)，第一轮诊断与未达标记录见 [docs/pipeline-bench-results.md](docs/pipeline-bench-results.md)，紧凑 SQLite、同步 RocksDB 对照及 5,400 证券/363 万条/实际 104,904 条每秒的有限验收见 [2026-11-14 优化报告](docs/pipeline-optimization-20261114.md)。新 `--min-durable-rps` 对完整 JSON/五档/SHA/Redis/FULL 执行绝对完成率验收；不把阶段消费完成算作持久化，不把本机结果等同于目标部署环境容量。

原有 CLI 用法不变（M1 垂直切片）：

```bash
./build/tickshelfd --flush-interval-ms 300        # 接入守护进程（收 TTAP 帧 → 攒批 → Redis）
./build/persister --db ticks.db --poll-ms 300     # 租约落盘（SCAN → SETNX 租约 → SQLite 幂等 upsert）
./build/feedgen --duration-s 30 --symbols 4 --rate 200   # C++ 参考驱动（随机游走 + 突发）
python3 examples/driver.py --symbols 2 --rate 100 --duration-s 10   # 跨语言证明（纯 struct 手打包）
```

持久化有两种模式共存（ADR-012，同一租约协议仲裁）：

```bash
./build/tickshelfd --persist none    # 经典：数据只进 Redis，persister 认领后读回落库（默认）
./build/tickshelfd --persist sqlite --db ticks.db   # 写穿：flush 即认领，daemon 内联落库
scripts/bench_compare.sh 15          # A/B 对比：Redis I/O 量 + 落库追平时延 + stall
```

运行中的组件每 2s 发布自过期负载键（ADR-013）：`redis-cli GET stats:tickshelfd` / `GET stats:persister` / `GET stats:evictor`。

水位淘汰与容量账（M2）：

```bash
./build/evictor --soft-watermark-bytes 268435456   # 超水位才淘汰，只删已持久化（安全水位语义）
./build/capacity --calibrate                        # 实测 Redis 每 tick 占用（账面常数不写死）
./build/capacity --rate 500 --symbols 8             # 四条不等式：启动/配置变更时的静态容量账
scripts/evict_drill.sh 45 15 12                     # 淘汰进行中杀 persister，验证不变式
```

混沌源 + 缺口回补 + 查询（M3/M3.5）：

```bash
./build/queryd --port 9910 --db ticks.db            # TTAP 双向查询进程（Redis+SQLite 合并读）
python3 examples/query_client.py --port 9910 --symbol 600000 --from-ms F --to-ms T
python3 examples/query_client.py --port 9910 --symbol 600000 --latest   # 最新价（HGET 快速路径）
scripts/chaos_drill.sh 40 5                          # 混沌源造缺口 → 查询 → 懒触发回补 → 缺口闭合
```

对账：feedgen/driver.py 最后一行 `SENT <ticks> <frames>` vs `sqlite3 ticks.db 'SELECT COUNT(*) FROM ticks'`。
崩溃注入验收（kill -9 循环 + 三层对账）：

```bash
scripts/crash_drill.sh 10    # 时长可调，默认 10 分钟
```

## 里程碑

见 [docs/roadmap.md](docs/roadmap.md)。**M1 = 可 demo 的垂直切片**：模拟源 → 采集 → Redis → 租约落盘 → 崩溃注入测试。M1 完成即可讲故事，后续按兴趣推进。

## License

[MIT](LICENSE)。
