# 原生消融组合表与性能归因

本实验把多个阶段的开关组合投影为一张表，回答“哪些工作实际执行、在这些条件下完成多少”。**它不是硬件或组件的理论极限表，也不把短路吞吐作为完整链路验收。**

已执行结果见 [2026-11-17报告](ablation-results-20261117.md)：[compact真值表](ablation-truth-compact-20261117.md)、[RocksDB真值表](ablation-truth-rocks-20261117.md)及独立Engine八格。

交付语义见 [delivery-contract.md](delivery-contract.md)：最终 Storage 提交前允许故障丢失，Redis 不承担耐久队列保证；无故障性能实验仍要求精确对账，不能丢数据换吞吐。最终提交的强度由实际后端配置决定，不因 Redis 允许丢失而自动降低。

## 两个实验剖面不能混称

| 剖面 | 输入与执行范围 | 不包含的工作 |
|---|---|---|
| JSON/IQB1/RCD1 | 原始 JSON → TCP/IQB1 → simdjson → RCD1 → SHA/FIFO → Redis → 实验 Store → finish/ACK | 生产 TTAP 的租约补偿；DCRB/父批原帧/E/Q 索引合同 |
| Tick64/Engine | 固定完整 Tick → 真实 Engine 的路由、切片、队列 → 注入的 RedisShard/StorageShard | 原生进程内测试不含 TCP/源JSON解析；不是 RCD1 成绩 |

当前 RCD1 是按公开字段构造的实验格式，默认标量使用 i64/1e-4；不是 16B decimal 的 DCRB。尚无获授权的精确 DCRB 编码、事件身份及 E/Q 索引规范，故结果固定标记 `dcrb_compatibility=not-adapted`。不能用自制近似格式改称兼容。

## RCD1：五位真值表

| 位 | 1 | 0 | 必须披露的剩余工作 |
|---|---|---|---|
| P | 原始 JSON 实时解析 | 同一原文的预解析回放 | 原文等值检查、Record 复制、canonical 编码仍执行，准备成本另报 |
| H | 入口 SHA-256 | 不算入口摘要 | compact SQLite/RocksDB 可能在后端重算 SHA |
| D | 入口 FIFO 精确判重 | 不查入口 FIFO | 后端仍可能查重；关闭 FIFO 不等于已证明数据唯一 |
| R | Redis 真实 SET/索引/GET/清理 | 不调用 Redis 网络 | 阶段队列、批量与转交仍保留 |
| S | 选定真实后端提交 | NullStore 消费 | Null 仍验证、校验和并消费字节；没有持久化结果 |

已有原生 `hash=off|only|dedup` 分别表示 H/D=00、10、11。H/D=01 没有实现，8 个组合在表中保留 `invalid`，不启动进程。其他24格全部复用同一个原生 Pipeline，不另写一条“方便跑分快路径”。

只读查看组合，不需要编译、不创建结果目录或启动 Redis：

```sh
python3 scripts/pipeline_bench.py --describe-truth-table \
  --config examples/bench/ablation-full-record.json
```

运行一张 compact SQLite 表（先小样本）：

```sh
python3 scripts/pipeline_bench.py --build build-record-release \
  --suite truth-table --config examples/bench/ablation-full-record.json \
  --records 5400 --repeat 1 --profile off --timeout 120 \
  --max-disk-mib 2048 --reserve-free-mib 10240 \
  --output /tmp/my-new-compact-truth-table
```

每个选定后端各出一张表。RocksDB 示例（需要构建时显式开启已固定依赖）：

```sh
python3 scripts/pipeline_bench.py --build build-record-release \
  --suite truth-table \
  --config '{"backend":"rocksdb","symbols":5400,"batch_records":1024,"hash_batch_records":1024,"redis_batch_records":8192,"transaction_rows":8192,"transaction_bytes":4194304,"sqlite_fullfsync":"on"}' \
  --records 5400 --repeat 1 --profile off --timeout 120 \
  --max-disk-mib 2048 --reserve-free-mib 10240 \
  --output /tmp/my-new-rocks-truth-table
```

`--suite truth-table` 固定 JSON/SHA/FULL/exact 后端查重剖面，不与 capture、自定义 matrix 或追加 NORMAL 格混用。自定义诊断继续用已有 `--suite diagnostic --matrix ...`。

## 表怎么读

新目录内的 `truth-table.md/.csv/.json` 来自同一份32行描述：

- 0/1、非法原因和 `not-run` 都保留；失败、预算停止或未测格不能填0。
- 每格存配置、输入 SHA、二进制指纹、独立 oracle、每轮现场路径。
- 区分输入完成条/s、唯一提交条/s、raw/canonical/wire 字节/s、ACK/finish尾部、实际事务大小和积压。
- `effective_work` 把配置期望与原生计数对账：Record编码、入口哈希/FIFO、Redis构帧/网络/读回、后端重算摘要/候选查找/插入/忽略。缺失证据为 unknown，不当作“没做”。
- SQLite subquery 内部候选数没有从 C++ 枚举时，`backend_candidates=null`，不是0；API计数不是B-tree操作数或内核syscall次数。
- Null 格核对实际消费行数和独立预期字节流校验和，不仅看上游 ACK；仍不能证明耐久。
- 全1格只可能具备当前完整路径资格；短时不限速表本身不提供容量认证。源限速会遮住下游余量，短跑不证明饱和。

各格速度差不是可相加的“组件占比”。例如关闭入口SHA后，后端可能重算同一摘要；关闭FIFO会让重复数据继续消耗Redis和Storage。因此，比较差值必须同时读实际工作和输入/forwarded/唯一条数，不能机械相加。

## 数据库候选查重的独立诊断

`store_lookup=exact|assume_unique` 仅供 opt-in 实验后端，默认 exact。`assume_unique` 只支持 compact SQLite 和完整记录 RocksDB：跳过应用候选查找及批内判重，保留完整数据、摘要、索引、状态和相同同步。

它不是“没有哈希碰撞”的假设，而是更强的**本诊断输入没有重复事件内容**假设。仅接受独立验证为零重复的合成固定输入；拒绝 capture 和重复负载，诊断库与默认精确格式隔离。它永远不具备完整精确查重链路资格。

```sh
python3 scripts/pipeline_bench.py --build build-record-release \
  --suite diagnostic --config examples/bench/ablation-full-record.json \
  --matrix examples/bench/ablation-lookup-matrix.json \
  --records 54000 --repeat 3 --profile off --timeout 180 \
  --max-disk-mib 4096 --reserve-free-mib 10240 \
  --output /tmp/my-new-lookup-diagnostic
```

这只能测“去掉这项义务后少做多少工作”，不能把它当成可上线的等价提速。真正的候选优化必须回到 exact、完整路径、同输入和同同步强度后再验证。

## Tick64 / Engine：真实组件十六格

原生诊断目标仅在 `TICKSHELF_BUILD_BENCHMARKS=ON` 时构建，不向生产守护进程暴露 Null/造数开关：

```sh
python3 scripts/engine_bench.py --suite native --build build-bench-release \
  --output /tmp/my-new-engine-truth-table \
  --records 10000 --symbols 64 --duplicate-every 5 --shards 2 \
  --warmup-records 1000 --repeat 1 \
  --max-disk-mib 2048 --reserve-free-mib 10240
```

十六格为 Redis real/null × Storage sqlite/null × FIFO off/on × pipeline off/on（ADR-028）。输入是独立 Python 生成的完整 64B LE Tick 文件，所有格共享前后 SHA；计时覆盖 submit、timed-CV 背压、finish、drain 和 worker join，不含输入解码、打开数据库和计时外 SQLite oracle。SQLite 逐字段、逐键集合核验；Null 仅消费并核对完整字节 checksum。每格有独立 warmup、日志、实际参数、计数和 SQLite 连接 PRAGMA。

- pipeline=on 是写穿两级流水线：每分片发布线程与存储线程经有界交接队列重叠发布 RTT 与 SQLite 提交；存储线程用第二条 Redis 连接做 finish/release。它不减少任何工作，只重叠等待；上界是两段中较慢的一段。
- NullRedis 模拟每批认领成功，不提供真实租约仲裁验收；`published_records` 在此只表示适配器成功调用。
- NullStorage 不执行真实 Redis 的 done 标记，也不增加 committed/inserted；`queue_complete=true` 不等于 `storage_complete=true`。
- committed input 与 inserted unique 分开；有重复时二者不能混作同一分子。背压 rejected 是重试提交次数，不是输入永久缺失；源必须成功交完全部输入。
- `storage_complete` 只表示本次非去重输入全部交给非 Null 存储提交，不承诺物理断电安全；finish_pending 单独要求为0。
- 本机服务连接实际回读为 WAL / synchronous=1（NORMAL），不能混入 RCD1 的 FULL 成绩。`durable_records` 是保留的旧计数别名，不是掉电保证。
- 这是进程内真实 Engine，而不是 TTAP/TCP/JSON 入口测试；短时小样本只能验证组件路径与本规模完成率，不能当作可持续上限。

同条件事务批量对照使用 `examples/bench/ablation-batch-matrix.json`，命令与 lookup 诊断相同，仅替换 matrix；推荐至少三轮，并在更大输入量复核。调大行数上限仍可能被字节/年龄上限提前切批，必须读取实际事务计数。

## 存储布局对照：行式索引 vs 批段稀疏索引 vs 按天 run

`examples/bench/ablation-storage-matrix.json` 把四种最终存储放到同一输入、同一 FULL 同步、同一 oracle 下对照：

| 后端 | 数据布局 | 索引写入（每批） | 查询路径 |
|---|---|---|---|
| `sqlite_compact` | 无数据文件，canonical 入库 | 每记录 1 行候选查找 | SQL 索引 |
| `rocksdb` | canonical 作 value | 每记录 1 次 Seek | Seek 迭代器 |
| `segment` | 到达顺序批帧（TSG1） | 1 行段 + 每 symbol 1 行范围 | 候选段整读＋过滤 |
| `segment_day` | 按 (UTC 日, symbol) 连续 run（TSD1，chunk 化保留区） | 每 run/chunk 1 行，按刷盘批次更新 | 直接定位该 symbol 的 run 顺序读＋过滤 |

`segment_day` 是"本体先行＋稀疏元数据"的参考实现（按用户设计定稿）：进程内持有全部 symbol 的当前可写块状态；`write` 只做内存分流——能装进当前 chunk 剩余空间的帧进**增量缓冲**（攒批），装不下的进溢出缓冲；write 末尾有溢出或缓冲达 8MiB 上限时刷盘一次：按帧边界贪心申请 N 个新 chunk（**一个 SQLite 事务先登记分配**），溢出数据按分配序合并为顺序写（新 chunk 在文件尾连续），增量每 symbol 一次 pwrite，fsync 后元数据提交事务才是提交点。崩溃丢失窗口 = 内存缓冲 + 未 flush 批；恢复逐帧校验（len/CRC/归属 symbol/chunk 边界）**采纳**已 fsync 的有效帧，撕裂帧停止、残余由下次写覆盖。它对单 symbol 范围查询只读该 symbol 自己的字节（无跨 symbol 过滤浪费）；写侧固有代价是增量散布 pwrite（POSIX 无带偏移的 scatter-gather，能顺序化的只有溢出部分）——缓冲摊销其频率，但摊销不掉其存在。同口径对照命令：

```sh
python3 scripts/pipeline_bench.py --build build-record-release \
  --suite diagnostic --config examples/bench/ablation-full-record.json \
  --matrix examples/bench/ablation-storage-matrix.json \
  --records 300000 --repeat 3 --profile off --timeout 240 \
  --max-disk-mib 8192 --reserve-free-mib 10240 \
  --output /tmp/my-new-storage-comparison
```

### 布局取舍：文件为谁服务（2026-11-20 实测）

300k 条、5400 证券、每 case 三轮、同输入/同 FULL 同步/同一集合 oracle（synthetic，`production_qualified=false`；Mac NVMe、单轮小库、不外推长期容量；现场 `/tmp/tickshelf-ablation-20261120.dayrun/backends-300k-v3`）：

| 指标 | segment 批帧 | day-run v2（无缓冲） | day-run v3（缓冲+批量分配） |
|---|---:|---:|---:|
| 完成率中位数（条/s） | **92,032** | 39,242 | 59,144 |
| pwrite 次数 | 37 | 199,488 | 53,688 |
| fsync 次数 | 37 | 37 | 11 |
| SQLite 提交 | 37 | 37 | 20 |
| 索引行 | 199,525 | 32,400 | 107,376 |
| 查询路径 | 候选段整读＋过滤 | 该 symbol 自己的字节 | 该 symbol 自己的字节 |

三个结论，按证据强度排列：

1. **元数据可以按需稀疏**（用户设计假设成立）：day-run 的索引行比批帧少 84%（32,400 vs 199,525），SQLite 提交也更少，且不损失任何正确性——范围元数据本来就冗余于"帧自校验 + 恢复扫描"。
2. **写吞吐的差距是布局的物理代价，不是实现缺陷**：5400 个 symbol 摊薄后每批每 symbol 只有 ~1.5 条（约 450B），查询友好的连续 run 把"每批 1 次顺序大写"变成 5 万多次散布 pwrite。v3 的内存缓冲 + 批量分配 + 顺序溢出写把 v2 的 199k 次 pwrite 砍到 54k（吞吐 +51%），但**缓冲只能摊销散布写的频率，不能消除散布本身**。symbol 越少、每 symbol 每批数据越大，差距越小——把这套结论套到别的负载前必须先看这两个分布。
3. **取舍的本质是文件布局为谁服务**：到达顺序对写最优（顺序大写天然摊薄 fsync），查询顺序对读最优（单 symbol 连续、无跨 symbol 过滤浪费）。工业界的通用解是写路径保持顺序追加、后台 compaction 再把数据重排成查询友好的布局——LSM 的 compaction 正是这个角色的通用化。day-run 直接在写路径产出查询布局，等于跳过了 compaction、由写路径预付其代价。

另注意两处语义差异（都是自洽的合同，不是优劣）：segment 的提交点是每批的索引事务、崩溃尾部**截断丢弃**靠上游重试；day-run 的提交点是惰性元数据 flush、崩溃尾部**逐帧采纳**（重试重写产生物理重复帧，由导出端精确去重并报告）。两个后端都只在 bench 存在，不进入生产 CLI。

## 隔离、资源与复跑

- 结果目录必须新建；原始结果、失败现场不覆盖。只启动 owned 子进程、私有 Redis；不使用清共享 DB15/FLUSHALL 的旧入口。
- 私有 Redis 无 AOF/RDB，仍为 noeviction；保存实际只读 CONFIG/INFO 快照，ACL拒绝明确为 unknown/denied。不修改外部实例。
- 重复、warmup、随机运行顺序、fixture准备与计时边界明确保存。内存预解析有上限，不允许单独缩小某个慢格后继续称同输入对照。
- 启动前估算产物，运行中检查预算和free space；超限停止并保留未测行。旧证据不重标、不重生成。
- FULL/WAL sync 不是物理断电或硬件 PLP 测试；`production_qualified=false` 不因内容对账或某项容量通过而改变。

纯逻辑测试：

```sh
python3 scripts/test_pipeline_bench_truth.py
python3 scripts/test_redis_snapshot.py --private-redis
```
