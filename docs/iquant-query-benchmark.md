# iQuant 边写边查压测

压测启动真实 IQTS 发送端、接收器、C++ 历史 worker 和临时私有 Redis。
输入是定速的合成五档快照，查询线程在持续接收、raw ACK、SQLite 提交和 Redis
发布期间运行。每种模式独立建库并启动 Redis，Redis 发布始终开启，便于比较查询负载。
冷层查询使用现有 SQLite 历史库，范围取自同一近期数据窗口。测试保留操作系统缓存，
因此冷层指存储层，查询延迟不能外推到冷缓存或很久以前的 Parquet 分区。

## 运行

```sh
cmake -S . -B build-iquant -DTICKSHELF_IQUANT_ENGINE_ONLY=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build-iquant --config Release
python3 -B scripts/iquant_query_bench.py \
  --engine-bin build-iquant/iquant_engine --output /tmp/iqts-query-256 \
  --rate 10000 --query-qps 80 --query-clients 4 --duration 15
```

加上 `--modes hot --redis-page-quotes 64` 可单独重跑纯 Redis 的小页对照。
默认预热 3 秒，然后在 256 个轮换证券中查询，单证券窗口为最近 48 个源批次，
每批 32 条。满窗口内每次查询期望命中 6 条。默认三种模式使用同一逻辑窗口和
请求速率。

| 模式 | 查询方式 |
|---|---|
| `hot` | Redis Stream 倒序分页，按源批次和证券过滤 |
| `cold` | 只查询 SQLite，绕过 Redis |
| `hybrid` | 窗口较新的一半尝试 Redis，SQLite 补旧区间和 Redis 缺失的批次 |

每次请求固定发起时已产生的最大源批号。Redis 第一页从当时的 Stream 尾部开始，
后续页使用上页最旧 ID 的排他边界继续向旧处查。新发布的更大 ID 不会混入同次分页，
被淘汰的旧 entry 仍可能缺失。测试根据源批序号和已知的完整帧大小找缺口。
SQLite 补查在一个只读事务内完成。两个存储之间的可见进度可以不同。

## Redis 单页限制与指标

`XREVRANGE COUNT` 限制的是 Stream entry 数。一条 iQuant entry 包含一整个源帧，
所以测试用 `floor(单页行情上限 / 每帧行情条数)` 换算。默认每帧 32 条，单页取
8 个 entry，最多返回 256 条原始行情，过滤掉其他证券也计入本页工作量。
生产想给可变批量的 Stream 实施同等限制，需要先保证单 entry 条数上限，或者把
超大 entry 拆分。单条特别大的快照还可能需要独立的字节上限。

- `ingest_rps` 分别记录源产出、raw ACK、SQLite 连续游标的实际速率。
- `latency_ms` 从请求开始到后端完成分页、读取与合并，给出 P50/P95/P99。
- `newest_returned_lag_ms` 是最新返回行相对请求开始时该证券已产生的最新源行的时间差。空结果单独统计。
- `missing_before_newest` 统计最新返回行之前的缺口。其余缺失落在尾部，或整个查询为空。
- `redis_max_page_quotes` 检查单页大小。`redis_commandstats` 记录服务端命令调用数与累计执行时间。

停止发送后核对 raw ACK 和 SQLite 游标与发送批数一致，再核对历史库总行数。
结果目录包含各模式查询日志、写入水位采样、进程日志和汇总 `results.json`。

## 2026-11-26 本机结果

Apple Silicon Mac，Redis 8.10.3。每种模式预热 3 秒，测量 15 秒，4 个客户端
实际完成 80 查询/s。目标摄入为 10,000 条/s。每格单轮，结束后历史库均核对
到 180,000 条。历史游标在测量期约为 10,000 条/s，采样到的峰值积压各为 192 条。

| 模式 | 单页行情上限 | 查询 P50 / P95 / P99 | Redis 页数 P50 | 最新返回行落后 P95 | 查询期缺失行占比 |
|---|---:|---:|---:|---:|---:|
| 纯 Redis | 256 | 4.26 / 4.61 / 4.81 ms | 6 | 26 ms | 8.19% |
| 纯 SQLite | — | 0.30 / 0.47 / 0.59 ms | — | 26 ms | 7.07% |
| 冷热合并 | 256 | 2.57 / 2.84 / 3.06 ms | 3 | 26 ms | 5.49% |
| 纯 Redis | 64 | 4.74 / 5.29 / 6.12 ms | 22 | 26 ms | 8.28% |

以请求开始时源端已产生的记录为对照，四组缺失都位于查询尾部，最新返回行之前
的缺口计数均为零。停止发送后 raw ACK 和历史游标全部追平。纯 SQLite 的查询在
本负载下按来源流和批号走主键范围，Redis 则需遍历多个证券混装的 Stream entry。
这个查询结构会影响延迟，不宜把名次外推到按证券建键的 Redis 或需要实际读盘的
历史范围。

纯 Redis 的单页上限从 256 降到 64 时，`XREVRANGE` 服务端平均执行时间从
51.0 µs 降到 12.8 µs。调用数同时从 7,148 增至 26,694，整次查询 P99 上升。
这种设置适合按 Redis 单命令服务时间和聚合请求延迟分别验收。
