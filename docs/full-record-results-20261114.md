# 全记录 RocksDB 接入与基准整改实测（2026-11-14）

## 最重要的结论边界

**本轮仍使用合成行情，不是真实生产捕获。生产基准尚未完成验收。** 当前没有获授权的原始生产JSON、真实Record/schema或系统调用trace可供核验。真实解析、Redis读回、数据库写入和同步边界都实际执行，但不能据此把数据来源改称生产。

本轮真正接通了 **全记录RocksDB**，并加上原文回放、独立oracle、来源分级和供给迟滞检查。正确性、合成有限容量、生产代表性分开报告，所有性能阶段都明确 `input_source_kind=synthetic`、`production_qualified=false`。

**Mac全记录数据库的100k目标未达到。** 百万条三轮中，RocksDB完整链路完成吞吐中位数86,182条/s。不能用上轮文件＋SQLite稀疏索引的104,904条/s，或原生RocksDB的467k，替代这项结果。

## 1. 接入的是完整数据，不是只换索引

主路径：合成原始五档JSON → TCP/IQB1/CRC → simdjson → 完整RCD1 Record → 每记录SHA-256与精确FIFO查重 → 私有Redis真实SET/GET/索引/清理 → **RocksDB完整value＋symbol/time索引** → 必要finish → ACK。

- `backend=rocksdb`：D value保存完整canonical，R保存逐记录symbol/time索引，F/S保存版本和状态；heavy另有T逐记录副索引、M批元数据。同一同步WriteBatch提交，不另外写自制段payload。
- `segment_rocksdb`仍是“TSG1文件＋RocksDB索引”，两者明确分开；native reference不走该完整链路，也不进入本报告全链路排名。
- 固定RocksDB11.13.2，commit `17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa`，使用已修复RTTI的构建。WAL开启、sync=true、use_fsync=true，无压缩；本机构建HAVE_FULLFSYNC=1。
- 摘要碰撞必须比较完整字节，不能用digest或symbol/time覆盖不同内容。单writer读判重→ID分配→提交串行保护，不把WriteBatch称为多writer事务。
- finish计入Flush(wait)、等待compaction/purge、SyncWAL、Close和只读重开；严格全历史恢复在启动、完整读回在独立export/验证，不额外塞入finish来惩罚RocksDB，也不把未完成写入藏到export。

## 2. 相同同步强度的初筛

本机macOS15.8.1 / arm64 / Mac14,9、12核、16GiB；非目标部署硬件。5,400证券，12个归档标量＋四个五档数组，零重复，canonical291B/条。SQLite显式WAL＋FULL＋fullfsync=1，对齐该固定Rocks构建的底层同步原语；这不证明硬件PLP。

每格108,000条，单轮；源/hash各1,024，Redis上限8,192，store年龄上限100ms、字节上限4MiB。所有格完整内容oracle通过。

| 后端 | store条数上限1,000 | store条数上限8,192 |
|---|---:|---:|
| 宽主键SQLite | 39,143条/s | 49,565条/s |
| 紧凑SQLite，原subquery判重 | 42,934条/s | 58,254条/s |
| **全记录RocksDB** | 67,707条/s | **91,022条/s** |

这些是小库候选筛选，不是持续容量证明。最大批量不等于实际批量：例如1000行格实际事务117–119次，受年龄/尾批影响；8192行格14次。

## 3. 数据库层实证优化：避免每条临时B-tree

之前同轮采样在紧凑SQLite的逐条step下看到了Vdbe游标关闭、Btree/Pager关闭和分页缓存/VM释放。对本机**同运行库SQLite3.43.2**做等价紧凑schema最小EXPLAIN：

```sql
INSERT INTO records(payload,symbol,time,digest)
SELECT ?1,?2,?3,?4
WHERE NOT EXISTS(SELECT 1 FROM records INDEXED BY records_digest WHERE digest=?4 AND payload=?1);
```

该形式含 `OpenEphemeral`，并向临时表Insert后再写主表。单独prepared候选SELECT及 `INSERT VALUES`均不含该opcode。证据为现场 `sqlite_explain.cpp/.txt`；这是查询执行计划证据，不是既有生产SQL或系统调用耗时证明。

新增受控开关 `sqlite_compact_lookup=subquery|separate`，默认保留旧subquery。separate仍在同一BEGIN IMMEDIATE内全内容判重，再INSERT VALUES；不改schema、不丢碰撞、不逐条开事务，lookup stmt也按lifetime或每事务复用。

54,000条、相同完整JSON/Redis链路、相同新二进制、相同参数：

| 方案 | 完成条/s | 实际事务 | aggregate step API次数 |
|---|---:|---:|---:|
| compact subquery | 61,269 | 7 | 54,000 |
| compact separate | **79,784** | 7 | **108,000** |
| full RocksDB | 91,131 | 7 | 不适用；另报Seek/Put/Write |

短跑separate约改善30.2%，而step API次数反而增加。**不能把API、事务、内核调用或累计耗时混为一项。** lookup计数属于aggregate子集，不能再次相加。

另一个实际计数样本为初筛compact_8192：108,000条、14次BEGIN/COMMIT、1次prepare、108,000次step、432,000次bind。这只说明本实现已按批提交，不能推定其他环境十万次相关调用究竟是哪个API/syscall。

**大库收益不能由该短跑外推。** 本轮随后仅为两个候选跑百万条三轮；没有把本轮未测的同尺寸subquery三轮捏造成30%长期收益。全记录SQLite的库容/索引/cache/WAL成本仍明显。

## 4. 百万条三轮：候选真正走全链路

两个候选各1,000,000条×3轮，共6,000,000条，全部独立完整canonical对账通过。仍是5,400证券、同一输入文件及同批量/同步参数，基线无profiler/strace。

| 全记录方案 | 最小条/s | 中位条/s | 最大条/s |
|---|---:|---:|---:|
| compact separate | 29,021 | **30,634** | 30,834 |
| full RocksDB | 85,844 | **86,182** | 86,379 |

RocksDB中位数约为优化紧凑SQLite的 **2.81倍**，但没有达到100k。每格实际事务123次，均触发384,323次FIFO驱逐，不是只测未满查重缓存。

这是一组默认/明确配置的工程对照，不是引擎最优调参排名：SQLite页缓存2000×4096=8,192,000B；固定Rocks版本实际block cache32MiB、write buffer64MiB、最多2个write buffers，另有Bloom/index等开销。**未声称相同总内存**。Rocks摘要前缀33字节、Bloom10 bits/key，判重iterator每批复用，其余范围/恢复读取total-order。

库越大，SQL小库成绩越可能虚高。没有在本轮把旧54证券结果当作同负载5400证券的控制组；宽表本轮只有初筛，预算优先留给两个候选的三轮与持续负载。

## 5. 60秒、216万条的36k输入/30k完成floor

新 `full-record-rocks.json` 配置：源/hash1,024，Redis8,192，store上限8,192条/4MiB/100ms；完整RocksDB WAL同步，所有主链路阶段开启。

| 指标 | 实测 |
|---|---:|
| 输入记录、实际物理唯一记录 | **2,160,000**，完整oracle通过 |
| 实际证券数 | 5,400 |
| 配置offer | 36,000输入条/s |
| 实际源发送 | 36,003.75条/s |
| 实际发送时间 | 59.99375秒 |
| 到最终ACK总时间 | **60.14669秒** |
| 端到端唯一耐久完成 | **35,912.20条/s** |
| 停发后ACK尾部 | 152.94ms |
| send-complete相对计划最大迟滞 | **8.87ms**（预设上限1000ms） |
| 计划已到达但未发送峰值 | 319条 |
| 计划未发积压slope | −0.17条/s |
| source→commit gap slope（插值） | +9.07条/s |
| 实际WriteBatch / 内部pipeline事务 | 365 / 365 |
| FIFO驱逐 | 1,544,323 |
| 内部最老记录完成年龄最大值 | 221.08ms |
| 有限验收 | **pass-full-chain-capacity，floor=30,000** |
| 生产合格性 | **false：合成输入，非生产捕获/目标服务器** |

这不是100k达标、不是目标硬件达标，也不是100ms内每条都耐久。内部年龄从解析后进入pipeline起算，不是交易所→查询可见的端到端P99；同步I/O不能被批年龄定时器抢占。插值/周期观测也不是全量逐记录延迟或无限期积压证明。

实际完整数据工作：2,160,000次摘要前缀Seek，4,320,365次逻辑Put，365次同步Write；最终4,320,002个物理KV键，其中2,160,000个完整D value和等量symbol/time索引，另有F/S。`Get_calls=0`不代表没有查重读取，不能隐藏Seek。最终pending/running flush/compaction及estimated pending bytes均0、background_errors=0；这只描述当时必要债务状态，不证明以后无需compaction。

复跑（合成工程验收，不是实盘）：

```sh
python3 scripts/pipeline_bench.py --build build-record-release --suite target \
  --config examples/bench/full-record-rocks.json \
  --rate 36000 --min-durable-rps 30000 --duration 60 --record-limit 3000000 \
  --profile off --syscalls off --max-source-lag-ms 1000 --timeout 240 \
  --max-disk-mib 8192 --reserve-free-mib 10240 \
  --output /tmp/my-new-rocks-60s
```

## 6. 基准整改与还未具备的生产证据

已完成：

- 显式原文JSONL/单行JSON回放，保留原字节、CRLF、空回调与变长帧；实际count/SHA/END/EOF核对。
- Decimal独立canonical oracle，磁盘辅助的有序/unique/forwarded多重集；独立FIFO按编译策略建预期，不从实际dropped反推。
- 实际字段/深度、大小、symbol倾斜、自然重复、同symbol/time不同内容、乱序画像。配置不能替代输入画像。
- 来源、正确性、有限容量与生产合格分开；CLI也打印来源和`production_qualified=false`。新输出/输入/工作目录为private权限，源文件不改写。
- 计划源供给、未发积压和发送迟滞单列，背压拖慢source不能静默变成“按时接住”。
- 系统调用按名称、FD文件种类、线程、字节、错误与耗时解析；Linux owned launch-under-strace可选并与无采集格分开。本机Mac实际预检unsupported，**没有编造实际目标Linux环境的系统调用次数，也没有把SQLite API计数标成syscalls**。

尚缺：获授权的原始生产捕获、实际schema/Record/hash范围、真实到达节奏、目标OS与磁盘、生产trace中的具体名称/累计耗时。原文不符合当前声明schema就明确拒绝，不能先删字段/补零再称生产解析。来源声明/hash也不独立证明真实性。目标机如果是Windows，需要另做移植/同步与ETW等诊断适配，本轮不是Windows支持证明。

原始捕获数据可在其所属环境本地回放、不必外传，见 [本地回放命令与输入边界](full-record-benchmark.md)；实际trace复跑/分析见 [系统调用成本对账](pipeline-syscalls.md)。不能用公开日线、其他市场数据、重复循环样本或手册合成值代替生产行情认证。

## 7. 验证与证据

- Release、Debug、ASan+UBSan（含vptr）实验各 **88 cases / 105,528 assertions通过**；曾由ASan发现并修复新增测试自身的simdjson临时array引用寿命问题，未掩盖失败。
- 既有离线Release/Debug各75 cases / 33,564 assertions；8个共享Redis用例明确排除，私有四服务runtime10/10通过。
- Python链路39、源/manifest19、qualification16、capture24、syscall26、owned-process5、capacity8、anomaly/matrix8项通过；链路及qualification另有Debug/sanitizer实际进程回归。
- 收尾复跑时，capacity命令首次漏传`--build`，误用旧`build-opt-release`二进制，两个子场景因不认识新`input_manifest`配置失败；未将该次运行记为通过。显式指定`--build build-record-release`后8项全部通过，其余上述Python套件也完成最终复跑。复跑时必须指定对应构建，不能混用旧二进制。
- 当前没有物理断电、PLP、目标硬件、Linux syscall运行、Windows移植、真实SDK/生产捕获、交易日soak、生产查询P99的验收。
- 默认bench OFF的生产构建通过；不强制生产依赖simdjson/OpenSSL/RocksDB，不改生产TTAP/四服务默认值。保留既有unused helper warning，不为无关事项改测试。
- 没有commit/push/reset/clean；旧现场和旧结果均保留。

现场：`/tmp/tickshelf-full-record-20261114.HlMHrj/`：

- `full-record-screen/summary.json`：六格初筛，binary `7fcfa24b80756d863423a259c663bff6c6e0432663b29ed2de8d67e592492769`；另留`pre-lookup-pipeline_bench`。
- `sqlite_explain.cpp/.txt`：同运行库VM最小复现。
- `sqlite-lookup-screen/summary.json`：三格SQL对照。
- `million-three/summary.json`：两个候选百万条三轮。
- `rocks-60s-30k/summary.json`：216万条有限验收。
- 后三阶段binary一致：`cefe4aa26927cecdd535357cac6d617513f27f95f1b21190d857cab7358b2219`。

机器证据：[full-record-results-20261114.json](full-record-results-20261114.json)。本轮累计独立核对 **8,970,000条物理记录**（排除warmup，含重复实验，不是跨实验的唯一数）。产物总预算12GiB且预留10GiB；原始结果、输入SHA、配置、进度、内存/CPU观测、导出和oracle均保留。
