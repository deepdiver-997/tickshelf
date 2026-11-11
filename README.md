# tickshelf

把行情流"摘"下来、分层上架的 C++20 管线：模拟行情源 → 采集 → Redis 热缓冲 → 租约式幂等落盘 → 水位淘汰。**全组件从零实现**——不用 hiredis、不用 asio，Redis 协议（RESP）手写。

## 为什么做这个

行情数据基础设施是量化系统里最经典的"热缓冲 → 耐久存储"形态。本项目用 C++ 把整条管线从零打一遍，学习目标：

- **RESP 协议**：手写 Redis 客户端，理解协议层而不是只会调库
- **at-least-once + 幂等 = 效果上的 exactly-once**：租约协议、崩溃窗口、幂等 upsert，用 `kill -9` 注入故障来证明
- **分层存储与水位淘汰**：层间按丢失代价、层内按时间价值
- **容量模型即代码**：启动时用不等式验证配置，预测/实测对账
- **接入契约设计**：协议即跨语言接口，进程边界做故障隔离——不做 C ABI 绑定（ADR-009）
- **崩溃一致性方案对比**：commit-point 恢复 vs 幂等重放，选型依据是恢复成本与重放成本之比（ADR-010）

## 架构与数据流

实然架构、三条数据路径（摄入/攒批冲刷/租约落盘）、崩溃窗口与恢复语义，见 [docs/architecture.md](docs/architecture.md)；v1 范围与里程碑见 [docs/roadmap.md](docs/roadmap.md)。

```
driver(任意语言) ──TCP·TTAP──▶ tickshelfd ──RESP──▶ Redis (ZSET, score=行情时间)
                                     │                    │
                              核心库 libtickshelf    persister（租约 + 幂等 upsert）
                                     ▼                    ▼
                               进程内 ingest API        SQLite
```

- **接入契约**：[docs/protocol.md](docs/protocol.md) 的 TTAP 二进制协议是**官方接入接口**——任何能开 TCP 的语言都能喂行情（`examples/driver.py`：~100 行纯 struct 手打包的 Python 驱动即证明）。数据源驱动生产上由接入方自研（协议/账号/限流都是源特定的），本项目只规范"数据进来之后"。
- **feedgen**：参考驱动（C++），模拟行情源：随机游走 + 开盘/收盘突发；M3 加乱序/重复/断流混沌模式。只用模拟数据——repo 自包含，也回避真实行情数据的授权问题。
- **tickshelfd**：守护进程。收帧（CRC 校验）→ 核心库 ingest → 按固定时间间隔攒批写 Redis。无状态，崩溃即重启。
- **libtickshelf**：核心组件库（攒批、RESP 客户端、租约落盘），暴露进程内 ingest API，测试直连不走网络。
- **persister**：独立进程。租约认领 → 幂等落库 → 改标记。多实例安全。
- **evictor / SDK / 多源切换**：后续里程碑。

## 设计决定

全部取舍记录在 [docs/design.md](docs/design.md)（ADR 格式），包括：为什么热缓冲用 Redis 不用消息队列、为什么 v1 落库用 SQLite 不用 PG、为什么按时间间隔攒批不按条数、租约为什么不是正确性保障而只是效率优化。

## 快速开始

前置：CMake ≥ 3.24、C++20 编译器、本地 Redis（`brew install redis && brew services start redis`）。

```bash
cmake -B build
cmake --build build
ctest --test-dir build --output-on-failure   # 测试（核心库直连单测；Redis 集成用例不可达时自动跳过）
```

四进程管线（M1 垂直切片）：

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
