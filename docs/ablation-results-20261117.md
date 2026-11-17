# 2026-11-17：原生消融、交付边界与批量复核

## 结论先行

1. 已把“最终 Storage 提交前允许故障永久丢失”写入版本化[交付合同](delivery-contract.md)。Redis 是缓存，不是耐久队列；这不允许健康测试缺失输入，也不降低本实验最终 Storage 的同步条件。
2. 已实现并执行两张 **P/H/D/R/S 共32行**真值表：每个后端24个合法组合通过实际工作计数与独立内容核验，8个无实现的组合明确保留为 invalid。另有真实 Engine 的8格组件表。
3. 当前最明确的受控收益来自**已有事务行数参数**：在30万条、三轮完整路径中，1000→8192让 SQLite 完成率中位数从29,396升至57,350条/s，RocksDB从64,383升至86,133条/s。**不是相对之前已经使用8192的实现又获得同样倍数提升，也不是新 Engine 算法提速证明。**
4. 跳过数据库查重的诊断在54k三轮中只提高约9–10%；不能据此删除精确查重义务。
5. RocksDB完整强同步路径的一次15秒供给复核完成540,000条，端到端34,511条/s，通过30k floor；这不是长期饱和证明，更不是目标硬件或生产认证。Mac完整记录100k目标仍未获得证明。

所有输入均为 **synthetic**，`production_qualified=false`；**DCRB `not-adapted`**。RCD1内容去重、Tick自然键去重、事件身份去重不能混为同一合同；本轮不接入尚在独立重构的网络组件。

## 证据入口

| 内容 | 可读表 | 机器证据 |
|---|---|---|
| RCD1 / compact SQLite | [32行真值表](ablation-truth-compact-20261117.md) | [JSON](ablation-truth-compact-20261117.json) · [CSV](ablation-truth-compact-20261117.csv) |
| RCD1 / full-record RocksDB | [32行真值表](ablation-truth-rocks-20261117.md) | [JSON](ablation-truth-rocks-20261117.json) · [CSV](ablation-truth-rocks-20261117.csv) |
| 查重、批量三轮对照及 Engine 八格 | 本报告下表 | [对照机器摘要](ablation-comparisons-20261117.json) |
| 同步、输入、源码/二进制指纹、有限供给与验证 | 本报告 | [最终机器证据](ablation-results-20261117.json) |
| 开关语义、剩余工作和命令 | [实验使用说明](ablation-benchmark.md) | 每格原始现场保存在本机新目录 |

本轮现场根：`/tmp/tickshelf-ablation-20261117.UYJcYD`。发布的表是最终通过现场的逐字节副本，SHA-256列于机器证据；没有重写旧结果。测量时工作树未提交，HEAD为`b023b83`，应使用二进制及逐文件源码指纹，而不能只靠这个HEAD声称复现了未提交代码。

## 1. 合同与测量条件

- 合同ID：`tickshelf-delivery-v1-20261117`。在途、accepted、staging、queued/in-flight Slice及Redis pending在最终提交前均允许故障永久丢失，**没有固定RPO**。
- accepted、published、committed input、inserted unique、Null consumed分别计数；TTAP当前没有摄入耐久ACK，不能把TCP发送成功当成Storage完成。
- 私有Redis实际回读：`appendonly=no`、`save=""`、`maxmemory-policy=noeviction`。配置中的`appendfsync=everysec`处于 **inactive**，不表示正在每秒刷盘。不修改外部Redis。
- RCD1 SQLite实际回读：SQLite **3.43.2**，WAL、`synchronous=2/FULL`、`fullfsync=1`、`checkpoint_fullfsync=1`。不能拿Python或SDK头文件的SQLite版本替代进程运行态。
- RocksDB **11.13.2**，源码commit `17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa`；完整payload、摘要索引及状态在同一个WriteBatch中提交，`disableWAL=false`、`sync=true`、`use_fsync=true`、无压缩。finish包含Flush、等待后台整理、SyncWAL及关闭/只读重开，均纳入完成边界。
- 同步API完成不是物理断电或硬件PLP证明。本轮没有实际断电测试。
- 本机：Mac14,9、12物理核、16GiB、Darwin arm64，Release构建。两张最终RCD1表及批量对照使用同一Pipeline二进制SHA：`cfc8786570a9a2d844c7ec7fda595120abe94adb8e1f100b5efb200ce5e3a432`。

## 2. RCD1真值表怎样读

P=实时JSON解析，H=入口SHA，D=入口FIFO，R=真实Redis往返，S=真实Storage。0表示使用原生短路，并不表示该阶段及其邻接工作完全免费。H/D=01没有合法实现，32行中8行不运行。

每个后端固定54,000条、5,400证券、五档、无optional、零重复、291B canonical/条；源/hash批1024，Redis批8192，事务上限8192行/4MiB/100ms。每格独立实例及不计分warmup，合法格随机顺序、**各1轮**。全部测量格使用同一IQB1输入SHA：

`7ced3afc37962660491a5e1aaadf62da837de11362260f069c0ad9c86000bb21`

以下是最终表的关键格，单位为**同一完成边界的输入条/s**；零重复真实存储格的唯一提交率相同，Null格没有唯一耐久率：

| P H D R S | 变化 | compact SQLite | RocksDB | 结果性质 |
|---|---|---:|---:|---|
| 1 1 1 1 1 | 完整路径 | 82,343 | 92,992 | 本规模完整路径观测 |
| 1 1 0 1 1 | 不查入口FIFO | 85,584 | 96,870 | 消融，不替代完整合同 |
| 1 0 0 1 1 | 不算入口SHA | 81,102 | 94,006 | 后端仍重算54,000次SHA |
| 1 1 1 0 1 | Redis off | 98,944 | 123,280 | 仍有队列/转交，非完整路径 |
| 1 1 1 1 0 | Null Storage | 133,961 | 142,906 | 消费率，不是落库率 |
| 0 1 1 1 1 | 同原文预解析回放 | 86,692 | 105,828 | 仍验证原文、复制及编码 |
| 0 0 0 0 0 | 全部可合法短路 | 341,295 | 343,386 | 消费诊断，不是系统容量 |

两张完整表均保留全部32行、各轮工作计数、min/median/max、输入/唯一提交/字节率和现场路径。单轮的min=median=max不代表无抖动；每次新库、短时规模也不能说明数据库增长后的长期表现。

**不能把差值相加为组件耗时。** 关闭入口SHA会把计算转移到后端；关闭FIFO在有重复时会增加下游工作。stage timer是wall time且部分嵌套，queue age/lag不是CPU热点；API计数不是内核syscall数。本轮没有在Mac上伪造Linux真实syscall采样。

## 3. 查重义务成本：独立诊断，不是可上线删功能

54,000条、每case三轮，其他输入、批量、索引、同步不变，表中为端到端唯一完成条/s中位数：

| 后端 | exact | assume_unique | 诊断变化 |
|---|---:|---:|---:|
| compact SQLite | 78,669 | 85,729 | +8.97% |
| RocksDB | 85,875 | 94,511 | +10.06% |

原生计数确认exact每轮执行54,000次候选查找，assume_unique为0；两者仍保留完整payload、摘要、索引、状态和7次事务提交。后者只接受独立验证的零重复合成输入，并使用隔离目录/格式戳，不能充当默认exact库恢复。它假设的是**输入内容唯一**，不是“不同内容绝无哈希碰撞”。

## 4. 可保留语义的参数优化：事务1000→8192

源/hash/Redis批、4MiB字节上限、100ms年龄上限及其余条件固定，只改事务行数上限。下面为**300,000条、每case三轮**完整JSON/TCP/SHA/FIFO/Redis/exact/FULL路径，单位条/s：

| 后端 / 事务行数上限 | 最小 | 中位数 | 最大 |
|---|---:|---:|---:|
| compact / 1000 | 27,706 | 29,396 | 29,683 |
| compact / 8192 | 57,302 | 57,350 | 59,271 |
| RocksDB / 1000 | 62,265 | 64,383 | 65,783 |
| RocksDB / 8192 | 85,554 | 86,133 | 94,480 |

SQLite约**1.95×**，RocksDB约**1.34×**。实际事务从每轮329–330次降到37次，而非简单的300次对37次；已有分批边界/年龄等会让事务提前结束。四case三轮合计独立物理核对 **3,600,000条**，并非跨实验去重后的总事件数。

54k初筛的同条件三轮也保留在机器摘要：SQLite约60,359→78,637，RocksDB约62,650→88,186条/s。更大规模变化说明不能只选小库最高数宣传。

**适用建议：** 在提交前允许丢失且能接受更大批量等待/在途内存时，先实测调大批量；仍同时限制行数、字节、年龄和队列。吞吐、延迟、内存和故障丢失窗口是取舍，不是“确定性与性能绝不可能兼得”的一般定理。本轮没有改变生产默认批量或同步等级，也没有证明某个8192参数适合所有负载。

## 5. 有限供给复核：不是不限速上限

完整RocksDB路径，8192行上限，输入36,000条/s、15秒、一次测量，要求端到端至少30,000条/s：

| 项目 | 实际结果 |
|---|---:|
| 输入/独立唯一物理记录 | 540,000 / 540,000 |
| 实际发送时间 | 14.994659375s |
| 至finish/ACK完成时间 | 15.647325083s |
| 端到端唯一完成率 | 34,510.69条/s |
| 发送结束至ACK尾部 | 652.67ms |
| 最大send-complete迟滞 | 6.67ms |
| 实际事务 | 99 |
| 独立内容对账与30k floor | PASS |

较低供给下会由年龄先切批，不能预期540k/8192次事务。最终后台状态没有待处理compaction/flush、后台错误为0；原始阶段状态和finish合同在机器证据中保留。源限速只能验证本次供给承接，不说明36k是系统上限，也不能把本机RCD1结论外推为DCRB部署达标。

## 6. Tick64 / 真实Engine八格

这是独立剖面：进程内真实Engine，不含TTAP/TCP/JSON入口。每格10,000条、64证券、每5条一次重复，共8,000唯一Tick；2分片、256条切片、4 inflight，FIFO开时每分片1MiB。全部格使用同一输入SHA，独立warmup；**每格仅1轮**。

SQLite实际为WAL / synchronous=1（NORMAL）/ fullfsync=0，故不能与前述RCD1 FULL混排。表中输入完成率=全部accepted输入/submit至finish和join时间；唯一插入率单列：

| Redis | Storage | FIFO | 完成耗时ms | 输入完成条/s | 唯一插入条/s |
|---|---|---|---:|---:|---:|
| real | sqlite | off | 19.704 | 507,522 | 406,018 |
| real | sqlite | on | 19.034 | 525,389 | 420,312 |
| real | null | off | 14.387 | 695,094 | — |
| real | null | on | 12.330 | 811,035 | — |
| null | sqlite | off | 10.442 | 957,644 | 766,115 |
| null | sqlite | on | 7.840 | 1,275,585 | 1,020,468 |
| null | null | off | 1.238 | 8,079,724 | — |
| null | null | on | 1.509 | 6,629,102 | — |

全部八格PASS：真实SQLite进行十字段/精确自然键集合核验；Null核对消费数与完整字节多重集checksum，committed/inserted保持0。NullRedis只是模拟每批claim成功，不能称真实租约仲裁通过。FIFO开时实际过滤2,000条；off时提交10,000输入、只新增8,000唯一行，两种分子分开。

毫秒级小样本尤其受调度和固定成本影响，只作组件路径及本规模完成率证据，**不把数百万Null吞吐称为理论极限**。这里也没有做旧Engine与新Engine的等条件提速归因。

## 7. 实现与安全回归

- 原生Pipeline加实际编码、入口哈希/FIFO、Redis构帧/网络/读回、后端摘要及查找计数；报告重核原始计数、输入SHA、有效配置和repeat编号，不能仅信已保存的verified标签。
- 缺失、非整数、负数核心计数不能通过；Null消费数/checksum独立核验，Null不再报告正的durable_unique_count。
- Engine区分queue_complete、storage_complete与finish_pending，Null不标done；committed input和new unique分开。重试退避改为可唤醒timed-CV，daemon优先旧retry尾部；drain预算不能硬中断阻塞I/O。
- 修复floodgen八字节symbol缓冲的终止符越界，并验证`F9999999`边界及非法参数。

本轮实际结果：

| 验证 | 结果 |
|---|---|
| Release / Debug生产离线 | 各102 cases / 35,005 assertions通过；8个共享Redis用例明确排除 |
| Release / Debug bench | 各95 cases / 106,283 assertions通过 |
| ASan+UBSan生产离线 / bench | 同上均通过 |
| ASan+UBSan私有Engine运行 / runner及原生smoke | 6 / 11项通过 |
| TSan Engine/queue/dedupe | 23 cases / 1,345 assertions通过，其余按scope过滤 |
| TSan私有Engine运行 | 6项通过 |
| Python真值表 / Pipeline（实际构建） | 22 / 39项全部通过 |
| Engine runner / 私有运行 / Redis只读快照 | 11 / 6 / 11项全部通过 |

ASan未启用leak检测；系统依赖和预构建RocksDB未加sanitizer，不能据此声称整个外部库无缺陷。私有故障测试验证已提交字段保留、未提交允许缺失但不算健康完成；不是物理断电证明。初次Python Pipeline未传build时有29项skip，随后已用实际构建重跑39项全部通过，不能把skip计入通过。

## 8. 修正、未测项与复跑

早期shakedown/54k现场有Null `durable_unique_count`误标（rate和资格当时已不具耐久性），保留原件；本报告使用修后新目录重跑表。Rocks最终重跑曾误带SQLite专用`separate`配置，被启动预检拒绝、未生成负载，失败现场保留；修正后在`truth-final-rocks-54k-v2`运行。未伪装第一次就通过。

未测/未完成：精确DCRB编码、八位小数/16B decimal、事件身份、父批原帧/E/Q索引、实际目标硬件、真实生产捕获、长期soak、物理断电、Linux真实syscall。新的真值表不替代这些验收。

使用[实验说明](ablation-benchmark.md)的命令复跑；核心批量复核命令：

```sh
python3 scripts/pipeline_bench.py --build build-bench-release \
  --suite diagnostic --config examples/bench/ablation-full-record.json \
  --matrix examples/bench/ablation-batch-matrix.json \
  --records 300000 --repeat 3 --profile off --timeout 180 \
  --max-disk-mib 4096 --reserve-free-mib 10240 \
  --output /tmp/my-new-batch-confirmation
```

Rocks目标复核配置在机器证据中完整保留：在同一full-record配置上设`backend=rocksdb, sqlite_compact_lookup=subquery`，使用`--suite target --rate 36000 --min-durable-rps 30000 --duration 15 --repeat 1`。输出目录必须不存在，不覆盖旧结果。

本轮新现场/构建约9.12GiB，低于12GiB预算，且可用空间远高于10GiB保留线；未删历史现场。所有服务进程/Redis均为自有实例，不使用共享DB15、FLUSHALL或广泛pkill。没有读取私有业务资料，没有提交或推送；2026-11-14/15历史证据未改写。
