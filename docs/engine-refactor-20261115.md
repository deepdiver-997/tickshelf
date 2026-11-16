# 分片异步持久化引擎重构与实测（2026-11-15）

## 结论边界

- 引擎已实现并默认关闭；`--engine on` 才替代旧同步 flush，`--engine off` 的旧路径逐字节不变。persister/queryd/evictor 不需要改。
- 正确性、跑通性已验收；**性能结论是“没有可归因的稳态吞吐提升”**，不是“重构后更快”。
- 实测全部是合成 TTAP flood、私有 Redis、Mac 本机；不是公司数据、不是目标服务器、不是生产验收。

## 1. 架构

```
read_session → Engine::submit
  → 按 symbol 路由到每分片 Ingestor staging（仅源线程）
  → 条数/字节/年龄封口成不可变 Slice（内部仍是 vector<Batch>，批 id 协议不变）
  → 每分片有界队列 + 单 worker
      publish-only: flush_batches → 完成
      write-through: flush_batches(claim) → 过滤 claimed
                     → SqliteTickStore::upsert_flush → finish 精确 id 集合
```

- 每分片一条 Redis 连接 + 一个 SQLite 文件；文件布局/路由与 `ShardedTickStore` 完全一致（共享 `shard_route.*`，含历史非标准 FNV 常量），persister/queryd 继续兼容。
- 写穿静态：`--persist sqlite` = 写穿，`none` = 仅发布；v1 不做负载控制器（ADR-013 仍成立）。
- 去重可选、默认关闭：每分片 FNV-1a(64B) + 完整 64B 比较，FIFO 预算可配；Tick 定长，不需要 SHA/OpenSSL。
- 批 ID 在重试间稳定：Slice 封口后不重新分组，段/索引/state/lease 语义与旧 flush 相同。

## 2. 失败与背压

- 发布失败：同一 Slice 退避重试；pipeline 部分生效后 `SETNX` 对自己租约返回 Null 时，补 `GET lease:{id}` 与 worker_id 比对，避免误判他人持有而漏写存储。
- 存储失败：只 `release` 自己的精确 id 集合，Slice 标 degraded，交 persister 补偿；不无限阻塞分片。
- finish 失败：数据已耐久；重试后计 `finish_pending`，由 TTL + persister 收敛，不谎报丢失。
- drain：整个 deadline 内允许重试；超时停止并报告未完成条数。
- 背压：每分片 pending 上限；`submit` 只接前缀，尾部进 daemon 的 65536 tick retry buffer；buffer 满则不再 poll 会话（数据留内核，TCP 反压），避免“可读但故意不读”的忙循环。

## 3. 验证

| 项 | 结果 |
|---|---|
| Debug 离线 | 98 cases / 34,964 assertions 通过（8 个共享 Redis 用例按既有规矩排除） |
| Release 离线 | 98 / 34,964 通过 |
| ASan+UBSan 离线 | 98 / 34,964 通过 |
| TSan engine 单测 | 11 / 94 通过 |
| TSan engine 私有 Redis 端到端 | 2 / 2 通过 |
| ASan engine 私有 Redis 端到端 | 2 / 2 通过 |
| 私有四服务 runtime | 10 / 10 通过 |
| 既有 Python 链路/源/资格/capture/syscall/进程/容量/异常 | 39+19+16+24+26+5+8+8 全过 |
| engine 私有 Redis 端到端 | 2 / 2：写穿行数精确等于 SENT、无重复、pending=0；kill -9 后 1s 租约 TTL，persister 接管排空、无重复 |

ASan 额外抓到两处**既有测试自身**的越界并已修：`test_frame_parser.cpp` 在 20B 头后写 4B 假锚点；`test_resp.cpp` 15B 二进制字面量按 17B 构造。不是引擎缺陷，但此前“ASan 全过”的说法在这两个文件上不成立，已在此更正。

## 4. 旧同步 flush vs Engine（写穿，4 分片）

方法：`floodgen` 4 进程、私有 Redis、同 `db`/分片布局；**按实际 SQLite 行数判定完成**（`rows >= floodgen SENT` 且 `distinct == rows`），不用每 2s 发布的 stats 判完成——早期用 stats 曾把 2s 的跑成 4s，已修正。全部 12 个 case 都满足 `rows == distinct == SENT`。

| offered 条/s | 旧 flush 中位墙钟 | Engine 中位墙钟 | 旧 flush 行/s | Engine 行/s | 差异 |
|---:|---:|---:|---:|---:|---:|
| 80,000 | 5.236s | 5.034s | 78,237 | 81,362 | Engine +4.0% |
| 400,000 | 5.330s | 5.342s | 378,057 | 377,253 | Engine −0.2% |
| 1,000,000 | 2.317s | 2.324s | 869,984 | 867,113 | Engine −0.3% |

- 每组两轮；机器 macOS 15.8.1 / arm64 / Apple M2 Pro / 12 核 / 16GiB；Release 二进制 `build-engine-release/tickshelfd` SHA-256 `60c81b56…a842c4`，源码 HEAD `7d690a2`。
- 80k 的 +4.0% 来自旧路径 100ms flush 边界的尾部；400k/1M 的 −0.2%/−0.3% 是队列/引用计数开销，量级属于噪声。
- 1M offered 的 2s 突发：两条路径都完整落库 2,015,232 行；旧路径没有出现“追不上”的尾部。**旧同步 flush 在这条合成 TTAP 负载上已经远超 30k。**

## 5. 如何解读

这次交付的是**解耦、分片并行、有界背压和可观测性**，不是免费的吞吐倍数：

- 旧路径的同步 flush 在这条 64B 固定 Tick、WAL+NORMAL、批事务的负载上不是瓶颈；Engine 把等待移到 worker 后，稳态仍由 SQLite/CPU/Redis 决定。
- 如果目标服务器仍只有 3–5k/s，应优先核对生产 schema、`synchronous`/FULL、事务粒度、单条 Record 的哈希/规范化/序列化成本；**不能把“flush 串行”当作已证实的根因，也不能指望本引擎自动消除它**。
- Engine 的真实收益在别处：收包线程不被 Redis RTT/SQLite 阻塞；每分片独立 Redis 连接与存储；队列满时是显式背压；失败/降级/未完成有明确计数；写穿策略可静态切换。

## 6. 复跑

```sh
cmake -S . -B build-engine-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-engine-release -j
./build-engine-release/tickshelf_tests --test-case-exclude='*redis-server*'
python3 scripts/test_engine_runtime.py --build build-engine-release

python3 scripts/engine_bench.py --build build-engine-release \
  --duration 5 --rate 20000 --senders 4 --shards 4 --repeat 2
python3 scripts/engine_bench.py --build build-engine-release \
  --duration 5 --rate 100000 --senders 4 --shards 4 --repeat 2
python3 scripts/engine_bench.py --build build-engine-release \
  --duration 2 --rate 250000 --senders 4 --shards 4 --repeat 2
```

机器证据：[engine-bench-results-20261115.json](engine-bench-results-20261115.json)。

## 7. 未做/边界

- 负载自适应写穿控制器（ADR-013 仍 deferred；v1 静态）。
- ClickHouse/Parquet 适配器（接口 `StorageShard` 已留，v1 只有 SQLite）。
- 目标服务器/A400/生产 schema/真实到达节奏、长期 soak、恢复耗时/P99、Windows 移植。
- 没有提交/推送之外的发布动作；旧路径默认不变。
