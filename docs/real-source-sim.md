# 合成数据源全链演练与压测

实测结果：[三轮 21 组负载、13,608,000 条记录](ifind-sim-results.md)；
可复核的逐轮计数与时间：[ifind-sim-results.json](ifind-sim-results.json)。

## 范围与边界

链路：外部 `quant-data-feeds` 的**合成 JSON mock** → simdjson → TTAP v1 →
`tickshelfd` → 私有 Redis → `persister` / daemon 写穿 → SQLite。
同时启动 `queryd` 和安全模式 `evictor`，用同一份 tickshelf 配置文件。

这是本地合成负载，不是 iFinD 官方服务的压测，也没有真实 SDK、账号或行情。
mock 只覆盖约定的七列快照，不能证明真实厂商 ABI、JSON 格式、权限、限流、
推送、断线恢复或多种行情的语义兼容。mock 编译进桥，不是运行时替换的官方动态库。

桥留在同级外部工程；本仓库只存编排、验证和测量方法。该演练依赖外部桥，
因此**不是只 clone tickshelf 就能执行的自包含回归**。缺少桥或 Redis 时非零退出，
不能把未执行算作 PASS。协议/存储单元测试与该可选集成入口分开。

## 运行

前置：Python 3、redis-server、已构建的 tickshelf 四个服务，以及外部 Release mock 桥。

```bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release

# 在同级工程构建，无厂商 SDK；不得同时设置 IFIND_SDK_DIR
cmake -S ../quant-data-feeds -B ../quant-data-feeds/build-mock-release \
  -DCMAKE_BUILD_TYPE=Release -DIFIND_MOCK=ON -DIFIND_SDK_DIR=
cmake --build ../quant-data-feeds/build-mock-release

# 小样本冒烟：54 证券 × 10 批
scripts/ifind_sim_drill.sh

# 有界分级负载：54/540/5400 证券，经典/写穿，加一轮四分片
scripts/ifind_sim_drill.sh --suite stress

# 自定义固定量，参数也可传给 scripts/ifind_sim_bench.py
scripts/ifind_sim_drill.sh --symbols 5400 --batches 200 --persist sqlite --shards 4
```

`BUILD`、`BRIDGE_BIN` 可指向其他本地构建。`--output` 必须为尚不存在的新目录；
默认用唯一临时目录。结果、配置、SQLite、所有进程日志和采样明细均保留，方便复核。
旧脚本的位置参数秒数现在只作为源端超时上限，不再作为吞吐分母。

每轮单独启动 Redis（仅 loopback，无 Redis 落盘，512 MiB `noeviction` 上限）。
启动时核对 Redis 的 `process_id`，其他服务同时核对自身进程存活及自身日志。
端口候选被竞争时失败，不复用已有服务。退出仅向自己创建的子进程发信号并 wait；
没有共享实例的 `FLUSHALL`，没有端口式 `SHUTDOWN` 或广泛 `pkill`。

## 负载定义与判据

- `--backend mock` 明确选择 JSON 路径，不能回退到绕过解析的 stub。
- 固定种子 1；源代码 `600000.SH` 起，线上 symbol 是对应六位字符串。
- 每批每证券一条；交易时间为逻辑时钟 `1700000000000 + batch_index`。
  此设计保证同证券唯一键递增；**逻辑交易时间不能拿来计算墙钟行情延迟**。
- 接收时间在桥转换 tick 时取实际系统时钟。
- 加速模式无厂商 QPS 节流。QPS 是请求数，不是记录数；一个响应可以含很多记录，
  不能用“QPS≤10”推断管线永远不会成为瓶颈。

PASS 要求：

1. `expected = generated = parsed = SENT = daemon ingested = 所有 SQLite 分片行数`。
2. 去重丢弃、CRC、重同步、flush 错误、最终未刷出条数均为零。
3. 每证券记录数及交易时间范围精确匹配，证券集合和分片路由正确。
4. 首尾证券 × 首尾批次字段与独立 Python 固定种子公式相符。
5. `queryd` 的最新价响应 CRC/字段与对应 SQLite 样本一致；查询成功本身不替代落库验证。
6. 所有子进程正常退出；超时强杀、缺日志、缺计数、缺依赖不记成功。

此处合成时钟恰好是 1ms 网格，不代表真实 L1 总是固定网格，更不证明现有回补
适合真实不规则快照或逐笔流。真实源的 symbol、唯一键、时间、量纲仍须单独定义。

## 测量口径

| 输出 | 精确定义 |
|---|---|
| `send_elapsed_s` / `send_records_per_s` | 桥内 steady clock 测得的合成、解析、编码、发送用时及记录数/实际用时；不是请求的 duration |
| `source.source_ms / parse_ms / encode_ms / send_ms` | 桥各阶段累计墙钟耗时，包含本地 JSON 合成；不是纯解析器微基准 |
| `drain_observed_ms` | 观测到源进程退出后，至首次查询到 SQLite 总数完整的时间；受查询/调度分辨率影响，不是逐条落库延迟 |
| `end_to_end_elapsed_s` / `durable_completion_records_per_s` | 源进程启动至总行数完整可见的实际耗时与平均记录数/秒，包含源初始化和排空 |
| `visibility.*` | 固定采样证券 `600000` 的接收时间→第一次被 SQL 查询观察到的延迟分位数；不是全证券逐条 P99 |
| `peak_observed_redis_bytes` | 定期 `INFO memory` 观察到的 Redis 分配量峰值，不是进程 RSS，也可能漏掉采样间峰值 |

可见延迟默认每 20ms 轮询。明细保存每条样本的接收时间、首次观测墙钟及差值，
同时报告实际最大轮询间隔与查询耗时。它是包含轮询、查询和调度开销的**观测值**；
观测器也会消耗 CPU/I/O。系统时钟回拨导致负值时拒绝报告该轮成功。
SQLite 查询可见不等于断电后 fsync 耐久保证。

**撤回旧指标**：先前的 `fresh_lag_ms≈36ms` 实际为“桥退出时间 - 库中最大接收时间”，
没有观测提交可见时刻，不能作为端到端延迟。旧的 `SENT/请求秒数` 也不是可靠吞吐。

这些是有界合成工作负载的实测，不自动等于系统饱和上限。源端 JSON 生成本身可能
成为瓶颈；判断引擎上限需另用 `floodgen` 等不含 JSON 的对照，并记录实际观察结果。
历史引擎基准见 [benchmarks.md](benchmarks.md)，不同负载数字不能直接混比。

## 启动配置与公私边界

四个服务已经接入共享 INI 与 CLI 覆盖；具体语法、默认值和一键启动见
[configuration.md](configuration.md)。不再沿用旧文档中未实现的 TOML 草案。

保持 `quant-data-feeds` 独立便于个人数据源迭代。可公开的是自写通用代码、合成样本
及测试方法；账号、真实行情、个人股票池、付费 SDK 是否可以上传，必须分别审查
敏感性、许可证和数据商合同。**“自用”不自动免除存储、设备、显示/非显示或再分发条款。**

不合并整个适配器是项目边界选择，不是“tickshelf 零第三方库”——本项目本来就依赖
SQLite。干净的合成测试代码也并非天然不能进入公共仓库；本轮先保持外部桥独立。
