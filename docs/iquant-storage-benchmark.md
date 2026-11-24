# iQuant 变长快照的本机存储对照

2026-11-25，Apple Silicon Mac。输入为 300,000 条合成 iQuant 形状的快照，覆盖
1,000 个证券。每种布局三轮，复用同一份 61.21 MB CSV，每轮新建存储并轮换执行顺序。
输入包含五档数组、可空字段和可变长度快照。三种布局都保存完整快照字节。

| 存储 | 导入速率中位数（条/s） | 热范围查询 P50 的三轮中位数 | 逻辑 / 实占空间（MB） |
|---|---:|---:|---:|
| SQLite | 82,406 | 0.48 ms | 79.84 / 84.46 |
| Parquet + ZSTD | 2,168,052 | 2.48 ms | 5.19 / 5.19 |
| 嵌入式 ClickHouse MergeTree | 1,521,495 | 1.60 ms | 9.47 / 9.56 |

查询按同一证券和时间范围过滤并读取快照长度，每轮核对 300 条、42,443
字节，另核对总行数。Parquet 由 DuckDB 写出，使用 16,384 行的 row group
设置，并由 PyArrow 独立读取元数据检查行数。SQLite 建证券和时间索引，使用
8,192 行事务、WAL、FULL 和 macOS fullfsync，导入时间包括 WAL checkpoint。
Parquet 按证券和时间等字段排序，计入文件和目录 fsync。ClickHouse 用有序
MergeTree，开启插入和 part 目录同步，计时至该次插入返回。各引擎的原生同步
实现、批次粒度和索引成本均有差别。

这是一次批量导入和热读对照。输出版式、运行库和软硬件会影响结果。
持续行情写入、Redis、发布目录延迟、并发查询及长时间合并开销需要另测。
表中空间只统计各轮的存储输出，实例做整套备份还要加上 raw 与进度。
在这个样本上，Parquet 适合评估为归档或分析副本，SQLite 点范围查询较快。

用 Python 3.12 虚拟环境安装可选基准依赖。运行服务只需要 Python 标准库和
编译后链接的 SQLite。

```sh
python3.12 -m venv /tmp/iquant-bench-venv
/tmp/iquant-bench-venv/bin/pip install duckdb==2.0.1 chdb==4.5.1 pyarrow==26.0.1
/tmp/iquant-bench-venv/bin/python scripts/iquant_storage_bench.py \
  --output /tmp/iquant-backends-new --rows 300000 --repeat 3
```

每轮指标与实际版本写在输出目录的 `results.json`。
