# 全记录数据库链路与生产数据回放

> **默认数据仍是合成行情，不是生产捕获。** 真实执行JSON解析、TCP、Redis、数据库同步，不等于用了真实生产数据。`production_qualified=false` 与正确性/有限容量 verdict 分开保存。没有获授权的原始生产输入，就不能宣布生产基准合格。

已完成的同条件筛选、SQLite 判重 SQL 对照、百万条三轮和 60 秒复核见 [2026-11-14 全记录实测报告](full-record-results-20261114.md)（[机器证据 JSON](full-record-results-20261114.json)）。其中 Mac 全记录数据库 100k 目标未达到，合成负载的有限 30k 完成率验收通过；两项结论不混用。

## 三种 RocksDB 路径不能混称

| 入口 | 完整payload在哪里 | 是否经过JSON/TCP/hash/Redis |
|---|---|---|
| `rocks_reference` | RocksDB value | 否；原生库短参考 |
| `backend=segment_rocksdb` | 自制TSG1段文件；RocksDB只存稀疏索引 | 是 |
| **`backend=rocksdb`** | **RocksDB value，另有真实symbol/time查询索引** | **是** |

新全记录后端使用独立 `full-record-rocksdb-v1` 目录。摘要＋稳定ID定位完整value，摘要相等后仍比较全部业务字节；同symbol/time不同内容必须保留。范围索引、完整数据、ID/批状态由同一个WAL开启、`sync=true` WriteBatch提交，不是数据耐久后异步派生索引。

- 批内pending精确判重；单实例串行保护判重至提交，独立writer由数据库LOCK拒绝。不将WriteBatch称为多writer事务。
- 每批复用判重iterator，capped33摘要前缀与10 bits/key Bloom；恢复/全量/范围读取明确total-order，避免前缀优化截断查询。
- 默认无压缩，披露block cache、memtable、Bloom额外内存及逻辑Get/Seek/Put；没有声称不同引擎相同名字的cache就是相同总内存。
- heavy模式增加真实逐记录time/symbol索引与批元数据，不拿段级CRC KV冒充SQL额外索引。
- 启动严格核验完整payload、双向索引及状态；写结果不确定后poison，物理计数标unknown/null，不伪报零行。
- finish计入Flush(wait)、等待compaction/purge、SyncWAL、Close和只读重开；不在finish额外全历史扫描来人为惩罚它，也不等到export才补写。

## 构建与隔离回归

沿用已固定的RocksDB 11.13.2（`USE_RTTI=ON`）；来源及Mac链接/同步注意见 [固定源码证据](rocksdb-reference-20261114.md)。不自动下载依赖。

```sh
cmake -S . -B build-record-release -DCMAKE_BUILD_TYPE=Release \
  -DTICKSHELF_BUILD_BENCHMARKS=ON -DTICKSHELF_BENCH_ROCKSDB=ON \
  -DRocksDB_DIR=/absolute/path/to/install-rtti/lib/cmake/rocksdb
cmake --build build-record-release
./build-record-release/tickshelf_bench_tests
python3 scripts/test_pipeline_bench.py --build build-record-release
python3 scripts/test_pipeline_bench_source.py --build build-record-release
python3 scripts/test_pipeline_bench_qualification.py --build build-record-release
python3 scripts/test_pipeline_bench_capture.py
python3 scripts/test_pipeline_bench_syscalls.py
python3 scripts/test_pipeline_bench_processes.py
python3 scripts/test_pipeline_bench_capacity.py --build build-record-release
python3 scripts/test_pipeline_bench_anomaly.py
```

生产TTAP/四服务默认值不变。实验依赖仍opt-in；共享Redis的旧集成测试不能不加过滤直接运行。

## 同原文、同同步强度的全链路比较

```sh
python3 scripts/pipeline_bench.py --build build-record-release --suite diagnostic \
  --matrix examples/bench/full-record-matrix.json \
  --config '{"symbols":5400,"batch_records":1024,"hash_batch_records":1024,"redis_batch_records":8192,"transaction_rows":8192,"transaction_bytes":4194304,"sqlite_fullfsync":"on"}' \
  --records 1000000 --repeat 3 --profile off --syscalls off \
  --timeout 180 --max-disk-mib 12288 --reserve-free-mib 10240 \
  --output /tmp/my-new-full-record-comparison
```

这是**合成工程对照命令，不是生产数据命令**。结果目录必须不存在；每格新库、私有Redis。先做小样本并检查预算，不保证所有机器能在该预算内跑完全部格子；超限明确失败，不自动删旧现场。

Mac上显式记录SQLite FULL的fullfsync与RocksDB固定构建的F_FULLFSYNC能力。逻辑查询功能可比，不声称两个引擎产生相同物理索引/写入量。SQLite全记录表、文件＋索引、原生库参考分开排名。

## 获授权生产原文的回放

### 支持的输入边界

当前入口接受**每行一个完整JSON payload的JSONL，或单行JSON**。每个payload符合已声明的实验schema：

```json
{"ticks":[{"symbol":"…","time":1700000000000,"quoter":{"lastPrice":10.12}}]}
```

省略示例不是完整五档。它不证明实际生产的iQuant导出就是此结构。未知字段、重复键、不支持精度/类型显式拒绝；若实际schema不匹配，应基于获授权样本增加版本化、计时内的接收端适配，不能先删字段、补零或规范化后冒充原始解析成本。

- 每行原始payload字节（包括CRLF）保留，仅加IQB1实验封装，不重序列化。
- 空ticks回调也保留；帧数与记录数分开。变长帧按实际count推进累计记录偏移；END/EOF严格验证。
- 每payload至多4MiB，每Record至多64KiB；sidecar至多16MiB/100,000帧，总记录受`--record-limit`限制。超限拒绝，不静默截断/循环/重分批。
- 输入必须是显式指定regular file，前后hash核验。不会搜索其他目录、读取`.local-docs`、登录真实行情账户或上传文件。

### 本地/目标服务器命令

原始数据不需要离开其所在环境。在获授权的服务器上指定本地路径：

```sh
python3 scripts/pipeline_bench.py --build build-record-release --suite diagnostic \
  --capture /internal/authorized-capture.jsonl \
  --capture-source-kind production_capture \
  --capture-provenance '实际源/交易日期/schema版本/脱敏或转换说明' --capture-authorized \
  --matrix examples/bench/full-record-matrix.json \
  --config '{"batch_records":1024,"hash_batch_records":1024,"redis_batch_records":8192,"transaction_rows":8192,"transaction_bytes":4194304,"sqlite_fullfsync":"on"}' \
  --repeat 3 --timeout 600 --max-disk-mib 12288 --reserve-free-mib 10240 \
  --output /internal/new-replay-results
```

`--capture-source-kind`必须明确为`synthetic|production_shape|production_capture`。后两者需要显式授权/来源声明。**声明和文件SHA不是历史真实性的独立证明**；生产捕获回放也不等于已接入、验证整个线上SDK/部署系统。

Capture默认不做warmup，明确记录为“不截取、不循环用户原文”。用户给的`--records`/matrix计数与实际不符即失败，不把它解释为偷偷取前N条。没有到达时间证据时只称内容/固定率回放，不按业务时间排序，也不称重现生产节奏。

### 独立验证不是合成公式套真实输入

Python独立从原JSON用Decimal生成完整canonical预期，保留missing/null/zero、精确整数/价格、Unicode/NUL、大symbol及乱序，拒绝未知schema；不调用被测C++ parser生成答案。

私有磁盘索引保存有序输入、全局unique与预期forwarded多重集。独立FIFO模型使用测试二进制无副作用输出的编译计费策略：

```sh
./build-record-release/pipeline_bench --dedup-policy
```

重复不刷新FIFO；远距离重复被淘汰后可以合法再次forward。不能用实际dropped倒推“预期”，也不能拿全局unique数代替forwarded顺序checksum。数据库导出逐完整字节核对unique物理一次；段后端另核对forwarded多重集。FNV/ACK只辅助检查顺序，不取代真实持久化内容核验。

## 基准合格性分四层

1. **来源**：合成、已确认结构、用户声明的授权捕获；hash仅证完整性。
2. **实际画像**：实际字段、盘口深度、symbol基数/倾斜、raw/canonical大小、自然重复与乱序。配置里的5档/5,400证券不能替代输入测量。
3. **正确性**：解析、FIFO、原文不变、完整落库/范围索引、失败/恢复、ACK对账。
4. **有限容量**：实际输入/forwarded/唯一新增耐久条数及bytes分别计量，包含必要finish和ACK；检查源供给、计划未发积压、发送迟滞、接收→提交积压及尾部，不把慢发送掩盖成按时承接。

新增`--max-source-lag-ms`默认1000，目标验收要求新source计划供给telemetry；不支持的旧二进制不能获得同名新验收。计划供给为独立于实际send进度的时钟计划，阻塞发送只在前后采样，有覆盖限制。`source_send_*_lag`是观察，不是工作时间/CPU占比。

自然重复下“30k输入/s”不等于“30k唯一新增/s”。`--min-durable-rps`是唯一完成floor，应结合实际画像声明目标；不能靠循环同样本、提高重复率或减少字段换成绩。单次有限PASS不证明长期持续、恢复耗时、查询P99、硬件PLP或目标磁盘容量。

## 数据库调用与系统调用

SQLite begin/commit/prepare/step/bind/reset等**library API**计数单列；RocksDB Get/Seek/Put/WriteBatch同样是逻辑API，不是内核syscall。实际calls成本需看名称、对象文件、次数、耗时、读写字节与错误，不能由“60次pwrite对十万次相关调用”直接推出时间比例。

Linux owned launch-under-strace及离线分析见 [系统调用成本对账](pipeline-syscalls.md)。有trace的格子不是无采集性能基线。Mac不能免提权采等价syscalls时明确unsupported，保留sample/引擎计数，不sudo、不改安全策略、不编造零值。

`sqlite_compact_lookup=subquery|separate`为紧凑表判重SQL的受控开关：前者保留原单语句；后者在相同BEGIN IMMEDIATE中复用候选SELECT及INSERT VALUES。完整内容碰撞确认与同批/并发语义不变。更多SELECT/step API调用不一定意味着更多内核调用或更慢；实际收益须同输入测量，不能仅靠EXPLAIN或调用数宣称。
