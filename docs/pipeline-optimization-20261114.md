# 五档 JSON 全链路优化与容量复核（2026-11-14）

## 结论先行

**Mac 的 100k 条/s 工程目标已在一次 5,400 证券、363 万条、超过 30 秒的完整链路有限观察中通过。** 实际端到端完成吞吐 **104,904 条/s**，包括排空、必要存储收尾和最终 ACK；不是源发送速度、空存储消费速度，也不是只测二进制文件。

达标的是 **完整数据追加文件＋SQLite 稀疏查询索引**，不是 SQLite 全记录表。紧凑 SQLite 确有改善，但百万条三轮中位数仍只有 **35,876 条/s**，没有达到 100k。数据完整与容量达标分别判定，失败或未达标布局没有从报告中删除。

**不能据此保证目标部署 ≥30k/s。** 本机是 macOS 15.8.1 / arm64 / Mac14,9、12 核、16GiB，未在目标硬件或目标服务器操作系统上跑本轮代码。100k 结果只有约 5% 的完成率余量，且依赖更大的批量；它不是长期生产认证。

未 commit、未 push；生产 TTAP 64B Tick、四服务租约/补偿和生产同步默认值未被此实验替换。2026-11-13 的旧结果及原始现场保留，见 [第一轮诊断](pipeline-bench-results.md)。

## 1. 工作量与测量边界

主路径始终是：

> 原始五档 JSON → TCP/IQB1 解帧与 CRC → simdjson 真实解析 → 完整 RCD1 Record → 每记录 SHA-256 与精确查重 → 私有 Redis SET/索引、真实 GET、校验及清理 → 耐久后端 → finish → ACK。

- 12 个归档手册标量＋四个五档数组；canonical 为 291B/条。零重复、optional_fields=false，证券基数明确列出。
- 原始 JSON 按公开 iQuant 手册字段构造；JSON 包装、IQB1/RCD1 是实验格式，不是声称已取得真实生产导出或当前券商 wire。
- 所有耐久结果均用独立 Python 公式重建 **完整 canonical 字节**逐条比对导出，核验无缺失、无多余、symbol/time 一致。不是只看 COUNT 或复用 C++ 生成器自证。
- 文件重放仍通过 TCP 和真实解析；在线 C++ 构造 JSON 是另外一项对照。输入生成、导出与 oracle 在完成吞吐计时外，必要后端收尾在计时内。
- 完成吞吐采用 `unique_count / max(source_elapsed_through_ACK, receiver_elapsed_through_finish)`；源实际供给、积压与尾部另行检查。
- 性能测量串行进行，正式基线无 profiler；每格新目录、新库、私有 loopback Redis。编译与 sanitizer 不与正式跑分同时进行。

## 2. 相同版本、相同输入的布局与合批筛选

54 证券，每格 108,000 条，单轮筛选；仅用于选参数，**不能单独证明持续容量**。所有格全内容 oracle 通过。

| 后端 | 默认批量完成条/s | 合批后完成条/s |
|---|---:|---:|
| 原宽主键 SQLite FULL | 24,894 | 58,359 |
| 紧凑 SQLite FULL | 43,217 | 64,218 |
| 数据文件＋SQLite FULL 索引 | 129,835 | 155,257 |
| 相同数据文件＋RocksDB 同步索引 | 85,764 | 145,678 |

默认源/hash/Redis 批为 256，store 上限 1,000 条。合批改为源/hash/Redis 各 1,024，store 上限 8,192 条、4MiB，年龄上限仍 100ms。这是一组参数组合对照，不把组合改善全部归因于某一个单点。

RocksDB 与 SQLite 的底层同步强度必须区别：本机 RocksDB 固定构建的 `HAVE_FULLFSYNC=1` 走 `F_FULLFSYNC`；原 SQLite 对照为 FULL 但 fullfsync=0。额外单轮 54 证券、合批后 SQLite 索引 fullfsync=1 得到 158,955 条/s；单次结果的波动不能解释为“开启更强同步会更快”。

矩阵：[layout-screen-matrix.json](../examples/bench/layout-screen-matrix.json)。

## 3. 百万条三轮：不能拿小库短跑代替

每格 **1,000,000 条 × 3 轮**，54 证券，共 **12,000,000 条**全部通过独立完整内容核对。参数为上一节合批配置，使用同一个原始输入文件。数值包含最终 ACK。

| 后端 | 最小条/s | 中位条/s | 最大条/s | 100k 结果解释 |
|---|---:|---:|---:|---|
| 原宽主键 SQLite FULL | 22,925 | **23,003** | 23,532 | 未达 |
| 紧凑 SQLite FULL | 35,812 | **35,876** | 36,395 | 未达，约为宽表 1.56× |
| 数据文件＋SQLite FULL 索引 | 153,382 | **154,951** | 160,082 | 候选；本格只持续约6.3–6.5秒，不授予持续容量标签 |
| 数据文件＋RocksDB 同步索引 | 144,080 | **144,575** | 145,029 | 候选；不是全记录 SQL 表成绩 |

每格实际发生 **384,323 次 FIFO 驱逐**，不是仅测未装满查重缓存的阶段。驱逐不导致本负载丢数据：生成器的重复模型是相邻重放，零重复基线中旧 ID 不会再次出现；不能外推为全生命周期/任意延迟重复都由该缓存去重。

### 数据库究竟慢在哪里

代表性宽表第1轮：

| 阶段 | 摊销 μs/输入条 | 占 receiver wall time |
|---|---:|---:|
| SQLite step | 11.959 | 28.15% |
| SQLite COMMIT（含其内部工作） | 24.868 | 58.52% |
| store_total（包含上两项） | 37.016 | 87.11% |
| JSON 解析/规范化 | 1.172 | 2.76% |
| SHA-256 | 0.221 | 0.52% |
| dedup lookup | 0.786 | 1.85% |
| Redis 真实读回解码 | 0.080 | 0.19% |

123 次事务，单次 COMMIT 最大约337ms。源 `send_wait` 占其 wall time 约94%，主要在承受下游背压，不能把低实际供给误报成系统已经接住 120k。

这是 **wall-clock 阶段计时，不是 CPU 百分比**；store_total 与其子阶段不可相加，COMMIT 也不等于纯 fsync。队列年龄直方图是年龄观察，不能作为工作耗时或 CPU 占比。源码中 `stage_summary` 已明确区分两者，机器报告也按此语义重算旧阶段摘要。

所以用户“主要在数据库操作”的判断在此工作量下基本成立，更准确是 **全记录 B-tree/索引写放大、step、WAL/自动 checkpoint 和同步提交路径**，不是“SQL 算术必然慢”或“裸磁盘顺序带宽不足”。既有生产 SQL/schema/hash 实现未知，不能把本机比例直接当成其根因的精确百分比。

## 4. 证券基数与批量/延迟代价

108,000 条单轮探索，源/hash 批1,024；不能当长期容量证明。

| 证券数/后端 | Redis批上限 | store条数/年龄上限 | 完成条/s |
|---|---:|---|---:|
| 5,400 / 文件＋SQLite | 1,024 | 8,192 / 100ms | 95,828 |
| 5,400 / 文件＋SQLite | 8,192 | 8,192 / 100ms | 105,947 |
| 5,400 / 文件＋RocksDB | 1,024 | 8,192 / 100ms | 100,774 |
| 5,400 / 文件＋RocksDB | 8,192 | 8,192 / 100ms | 109,422 |
| 5,400 / 文件＋SQLite | 8,192 | 32,768 / 250ms | 119,561 |
| 5,400 / 文件＋RocksDB | 8,192 | 32,768 / 250ms | 119,229 |

证券基数增加会增加 Redis 每批证券索引命令与存储稀疏范围数。相同“每秒条数”并不代表相同工作量。较大 store 批减少每条记录摊到的 symbol/time 索引行，但增加等待时间、候选段读取及查询过滤成本。

矩阵：[cardinality-screen-matrix.json](../examples/bench/cardinality-screen-matrix.json)。

## 5. 5,400 证券、超过30秒的硬容量验收

配置：[capacity-throughput.json](../examples/bench/capacity-throughput.json)。源/hash批1,024，Redis批8,192，store上限32,768条/16MiB/250ms；SQLite索引显式 fullfsync=on。

| 项目 | 实测 |
|---|---:|
| 记录数（独立 oracle 全字节通过） | **3,630,000** |
| 实际证券数 | **5,400** |
| 配置 offer | 110,000 条/s |
| 实际源发送 | 105,576 条/s，配置 offer 的约95.98% |
| 实际发送区间 | 34.3828 秒 |
| 最终 ACK 完成时间 | **34.6030 秒** |
| 端到端完成唯一吞吐 | **104,904 条/s** |
| 停发后 ACK 尾部 | 0.2202 秒 |
| 实际事务数 | 116 |
| receiver queue-gap slope | −66.15 条/s |
| 对齐源→提交 gap slope（插值估计） | +170.63 条/s |
| 内部稳态样本 | receiver / source-aligned 各33 |
| 实际 PRAGMA | WAL，synchronous=2，fullfsync=1，checkpoint_fullfsync=1 |
| 判定 | **pass-full-chain-capacity** |

绝对 floor 为 **100,000**，不是95,000。供给、积压、尾部与完整合同都必须通过；本次实际供给达到配置 offer 的≥95%是另外的条件，不能声称实际接住了110k或120k。

**限制不能藏在 PASS 后面：**

- 只有一次33秒计划窗口/34.6秒实际完成观察，未做10分钟/整交易日 soak，不证明无限持续容量；观察到的 gap slope 不是严格零。
- `store_complete_oldest_age` 最大约 **360ms**，超过250ms配置，因为同步I/O无法被定时器抢占。此年龄从解析后进入内部 pipeline 起算，**不是交易所→查询可见/网络源→落库的端到端P99**。
- 达标依赖吞吐优先的大批量，不是“100ms内每条都耐久”的低延迟合同。较小等待上限的负载应独立复跑。
- 稀疏索引支持真实范围读取并有正确性测试，但本轮未测生产范围/点查P99；读取候选段有读放大，不能用写入优势掩盖查询代价。

复跑（结果目录必须不存在）：

```sh
python3 scripts/pipeline_bench.py --build build-opt-release --suite target \
  --config examples/bench/capacity-throughput.json \
  --rate 110000 --min-durable-rps 100000 --duration 33 \
  --record-limit 4000000 --repeat 1 --profile off --timeout 180 \
  --max-disk-mib 16384 --reserve-free-mib 10240 \
  --output /tmp/my-new-capacity-run
```

## 6. 实现与耐久合同

1. **紧凑 SQLite**：完整 payload 只放主表，INTEGER PRIMARY KEY；固定32B非唯一摘要缩小候选，再比对完整内容。symbol/time二级索引携带rowid。没有 `UNIQUE(digest)` 丢碰撞，也没有把大payload重新放回去重索引。
2. **文件＋索引是真的写全数据**：TSG1含完整291B canonical Record，先同步数据文件，再提交SQLite FULL或RocksDB WAL+sync整批索引/高水位，最后ACK。不是null、只写offset或索引异步派生。
3. **恢复先验证再回收**：校验全部已索引历史、header/CRC/完整Record与symbol/time范围一致性，篡改/缺失/孤儿索引失败；只截去未索引尾。两种资源没有跨文件原子事务，顺序和恢复协议不可省。
4. **热路径**：hash缓存单份稳定所有权、预算FIFO、摘要碰撞全内容确认；Redis实际GET字节严格校验后直接下游，取消重建book再编码；可信SHA缓存不进入wire/hash，只有字节完整相等后才能转移。
5. **批处理可信度**：各阶段真实剩余队首决定年龄，最近deadline控制poll；记录rows/bytes/age/tail与实际批宽。没有把同步I/O宣称为可抢占的硬实时任务。
6. **暂不加后台checkpoint**：已用更小索引/合批得到完整链路达标证据，未为追数引入额外并发、异步ACK或降低FULL；全记录SQLite的进一步专项优化仍可独立研究。

RocksDB源码、固定commit、底层同步与必要收尾，以及ClickHouse MergeTree、InfluxDB TSM/WAL的固定源码设计借鉴见 [开源参考](rocksdb-reference-20261114.md)。后两者只审读关键源码，没有构建/运行服务，不引用它们的吞吐成绩。

### 6.1 真正拉取、构建并运行的开源同步参考

RocksDB **v11.13.2**，固定源码commit `17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa`。本机修复RTTI后静态库SHA-256为 `1508fc170461d924952d331c106bff0bb731af49b0eb46c1b691de6c7bfaf74b`，没有静默改用系统另一个版本。

| 原生库参考项目 | 实测 |
|---|---:|
| 记录数 / WriteBatch上限 | 500,000 / 8,192 |
| key / value | 16B有序ID / 完整291B canonical |
| WAL / 同步 / 压缩 | WAL开启，sync=true，use_fsync=true，无压缩 |
| 包括生成、写入及必要收尾的总时间 | 1.068962秒 |
| 原生库级完成吞吐 | **467,743 records/s** |
| 必要末尾Flush(wait) | 39.27ms |
| 末尾WaitForCompact / SyncWAL | 0.75ms / 6.04ms |
| 最后债务属性 | pending/running flush、compaction与estimated pending bytes均0，background_errors=0 |
| 独立全内容核验 | 500,000条通过，另有关闭后只读重开/无额外键检查 |

这是**短时原生库参考，不是JSON网络全链路成绩**：其有序ID键、无symbol/time二级索引和无业务查重，工作量与SQLite全记录表不同。不能把467k与全链路23k相除，宣称换引擎必快20倍；也不能用1秒参考证明长时compaction表现。

初始上游Release默认 `-fno-rtti` 在本机AppleClang17引发标准异常typeinfo身份冲突，已用最小helper对象和四种真实链接变体复现。固定源码另建 `USE_RTTI=ON` 库后全部异常负路径正常；项目新增链接异常身份回归测试。旧构建只留取证，**没有用它的性能当正式结果**。

### 6.2 低完成率同轮自动抓现场

另跑同二进制、相同百万条JSON输入的紧凑SQLite目标格：配置offer120k、低完成率trigger100k。连续两个窗口偏低后，在本轮 **3.621秒**自动启动macOS `sample`；真实tool退出0，保存该PID的现场，无须另一次复现。

- 完整内容核验通过，实际完成 **36,204条/s**；target verdict为 **`inconclusive-offer-not-achieved`**，整个该阶段退出1，未伪装为性能通过。
- 156个主线程wall-stack样本中，119个进入store写路径；其中COMMIT子树59、逐条step子树56，COMMIT下自动checkpoint子树48（嵌套）。可见checkpoint→unixSync/fcntl、pwrite，以及B-tree/分页缓存分配/释放。
- 这是**包含等待的wall-stack抽样，不是CPU占比**；嵌套子树不可求和。相对三轮未采样compact中位数35,876，此单轮36,204的差异不能证明采样“无开销”，只是同量级且受运行波动影响。
- checkpoint与step均有显著代价，后台checkpoint不是自动把35k提升到100k的充分条件。本轮保持前台合同，不引入新的并发恢复机制来掩盖全记录SQL未达目标。

### 6.3 在线构造JSON与回归

在线C++生成源另跑 **300,000条、5,400证券**，实际构造Record/JSON/帧与CRC，经同一TCP、真实simdjson、SHA查重、Redis和更强同步的文件＋SQLite索引；独立oracle全部通过。短跑完成 **125,894条/s**。源没有重放Python输入文件；保留Python参考fixture只供独立核对。它持续约2.4秒、库更小，**不是新的持续容量证明，也不能与34.6秒结果直接当成源生成的加速比**。

最终回归：

- Release、Debug、ASan+UBSan（含vptr）实验各 **72 cases / 102,967 assertions**全通过。
- 既有离线C++ Release/Debug各 **75 / 33,564**，8个共享Redis用例明确排除；私有四服务runtime **10/10**通过。
- Python真实网络/Redis与codec/store **37项**在Release、Debug、sanitizer均通过；capacity gates **8项**、anomaly/matrix **8项**通过。
- 默认bench OFF的生产构建通过；未把simdjson/OpenSSL/RocksDB变成生产强制依赖。`git diff --check`通过。
- 本机LeakSanitizer不支持；没有声称外部RocksDB/simdjson/OpenSSL已用sanitizer重建或跑过其官方全套测试。保留既有 `tests/test_write_through.cpp:40` unused helper warning，未为无关事项修改该测试。

## 7. 目标服务器的复跑建议

- 确认实际OS、磁盘容量SKU、文件系统、磁盘健康/剩余空间、实际SQLite运行库与schema、真实Record字节分布和hash算法。不要用头文件版本或另一个连接的PRAGMA替代被测连接。
- 先运行配置/单元/私有Redis正确性回归，再用同配置完整路径测30k唯一完成floor；例如 offer36k、floor30k、60秒，显式增加record/disk预算。
- 全记录SQLite与文件＋索引分开报告；记录库容增长、真实事务行数、WAL/checkpoint、CPU与发送背压。若只有前处理40k，另测JSON/规范化/拷贝/hash/查重的本地对照，不能先归罪磁盘。
- 标称顺序写MB/s不说明小随机写、同步延迟、持续写入/GC或PLP；291B×30k仅8.73MB/s的逻辑载荷不等于SQLite实际物理写量。
- 本轮没有物理断电、裸盘、Linux perf/fdatasync实机或目标磁盘测量；FULL/OS sync返回成功不是硬件断电证明。稀疏索引恢复、查询和文件回收的运维成本也应先在目标环境验收。

## 8. 原始证据

统一现场：`/tmp/tickshelf-opt-20261114/`，每个case保留resolved config、原始JSON帧文件及SHA、source/receiver JSON、进度、进程观测、导出和oracle、profiling状态、二进制前后SHA。旧现场没有删除。

- `layout-screen/summary.json`：八格布局/参数筛选。
- `million-three/summary.json`：四布局百万条三轮。
- `cardinality-screen/summary.json`：高基数与更强同步对照。
- `capacity-5400-33s/summary.json`：完整硬容量验收。
- `native-rocks-8192.reference.json` / `.oracle.json`：原生同步参考及独立核验。
- `compact-profiled/summary.json`：保留正确性通过、性能目标未达到的同轮采样现场。
- `generated-json-5400/summary.json`：在线C++原始JSON生成源对照。

累计独立核对 **19,050,000条物理记录**（不含warmup，包含重复实验及原生参考，不是跨实验的唯一记录数）。上游源码/两套构建/异常诊断、全部新测量及 `build-opt-*` 合计约 **22.21GiB**，低于24GiB预算；证据冻结时仍有约247GiB可用空间，未删旧现场。所有正式全链路格的二进制SHA-256一致：`e733d2311f110eba5fda29c1d35dbbcc152cd5fc0fd155d924ee8b27d43a2078`。

保存的机器结果见 [pipeline-optimization-20261114.json](pipeline-optimization-20261114.json)。运行方法及验收细则见 [实验台说明](pipeline-benchmark.md)。
