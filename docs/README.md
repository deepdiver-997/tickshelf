# 文档索引

全部文档按用途分三类：**总纲**（随代码演进）、**方法**（怎么测/怎么跑）、**实测证据**（带日期，冻结不改写）。新报告只用新日期追加，不回填旧文件。

## 总纲（随代码演进）

| 文档 | 内容 |
|---|---|
| [architecture.md](architecture.md) | TTAP 与 iQuant 的进程边界、数据流和恢复 |
| [design.md](design.md) | 全部 ADR（含 ADR-023 分片引擎、ADR-024 交付边界） |
| [protocol.md](protocol.md) | TTAP 接入契约（官方跨语言接口） |
| [configuration.md](configuration.md) | 配置语法、路径、覆盖规则、Redis 运行态快照 helper |
| [delivery-contract.md](delivery-contract.md) | TTAP 交付合同 `tickshelf-delivery-v1-20261117` 与 iQuant 的 raw ACK、历史提交、热发布边界 |
| [roadmap.md](roadmap.md) | 里程碑与范围 |
| [benchmarks.md](benchmarks.md) | 未测清单总账 |
| [review-brief.md](review-brief.md) | 评审摘要 |

## 实验方法与操作手册

| 文档 | 内容 |
|---|---|
| [ablation-benchmark.md](ablation-benchmark.md) | 消融真值表使用说明：P/H/D/R/S 语义、Engine 八格、lookup/批量对照命令 |
| [pipeline-benchmark.md](pipeline-benchmark.md) | JSON/RCD1 实验台口径：字段来源、来源分级、耐久边界 |
| [full-record-benchmark.md](full-record-benchmark.md) | 全记录基准说明：RocksDB 接入、对账、捕获回放 |
| [pipeline-syscalls.md](pipeline-syscalls.md) | syscall 口径与采集方法 |
| [real-source-sim.md](real-source-sim.md) | 外部 JSON mock 源的私有 Redis 全链演练 |
| [iquant-capture.md](iquant-capture.md) | iQuant 实盘快照采集、raw ACK、历史重放与 Redis 热发布 |
| [iquant-archive.md](iquant-archive.md) | iQuant 增量归档、压缩 raw、Parquet 类型和恢复 |
| [iquant-storage-benchmark.md](iquant-storage-benchmark.md) | iQuant 变长快照的 SQLite、Parquet、ClickHouse 导入、查询与空间对照 |

## 实测证据（带日期，冻结：只追加新日期，不改写旧文件）

### 2026-11-17 · 消融真值表与批量复核

- [ablation-results-20261117.md](ablation-results-20261117.md) — 主报告（结论、合同、Engine 八格、验证）
- [ablation-results-20261117.json](ablation-results-20261117.json) — 机器证据（同步/指纹/有限供给/回归）
- [ablation-comparisons-20261117.json](ablation-comparisons-20261117.json) — lookup/批量三轮对照与 Engine 八格摘要
- 真值表：[compact](ablation-truth-compact-20261117.md) / [rocks](ablation-truth-rocks-20261117.md)（各配 `.csv/.json`）

### 2026-11-15 · 引擎重构

- [engine-refactor-20261115.md](engine-refactor-20261115.md) — 引擎实测边界与结论
- [engine-bench-results-20261115.json](engine-bench-results-20261115.json) — 机器证据

### 2026-11-14 · 全记录与紧凑 SQLite

- [pipeline-optimization-20261114.md](pipeline-optimization-20261114.md) — 紧凑 SQLite 优化与有限验收（配 `.json`）
- [full-record-results-20261114.md](full-record-results-20261114.md) — 全记录百万条三轮 + RocksDB 对照（配 `.json`）
- [rocksdb-reference-20261114.md](rocksdb-reference-20261114.md) — 原生 Rocks 短参考
- [pipeline-bench-results.md](pipeline-bench-results.md) — 第一轮诊断与未达标记录（配 `.json`）

### 更早

- [ifind-sim-results.md](ifind-sim-results.md) — 模拟源实测（配 `.json`）

## 命名与追加纪律

- 实测文件一律 `主题-YYYYMMDD.后缀`；同日多份共享日期段。
- 旧证据只读：发现口径错误时在**新日期报告**中修正并引用旧文件，不就地修改。
- 性能结论必须携带：输入 SHA、二进制/源码指纹、实际同步配置、`synthetic` 与 `production_qualified=false` 声明。
