# iQuant 增量归档与原文恢复

归档工具从运行中或已停止的 iQuant 工作目录读取已对账的连续前缀。每个归档分片
包含字节可还原的 raw 和可直接查询的 Parquet。工具读取 SQLite 游标与原文件，
采集进程不需要等待 Parquet 封口。归档进度随已发布的分片保存。

## 导出

在单独的 Python 环境安装 PyArrow。当前在 PyArrow 26.0.1 上验证。

```sh
python3.12 -m venv ./archive-venv
./archive-venv/bin/python -m pip install pyarrow==26.0.1
./archive-venv/bin/python scripts/iquant_archive.py export --work-dir ./run/instance-a
```

默认写到实例目录的 `archive/`。可用 `--archive-dir` 指定另一块盘上的目录，
其父目录应已存在。每个目标目录同一时间允许一个归档操作。单次运行取开始时
`replay_cursor` 所覆盖的前缀，处理该前缀尚未发布的分片后退出。
后续运行接续同一前缀。默认每片至多约 64 MiB raw 或 8,192 帧，也可通过
`--max-part-mib` 调整大小。立即导出当前尾批会封一个小文件，可用更长的导出周期
减少此类文件。

```text
archive/<stream>/<first_seq>-<last_seq>/
  raw.zst         原样压缩的完整 IQTS 帧
  quotes.parquet  结构化行情投影
  frames.parquet  帧状态、CRC、原文件偏移、GAP 和隔离原因
  manifest.json   序号范围、文件大小、行数和 SHA-256
```

工具逐帧核对 raw 头、序号和 CRC，再在临时目录写文件，封口并同步后把整个
分片目录发布。消费者可在已发布的序号目录中读取分片。每发布一片，命令在
标准输出打印一条 JSON，可汇总原文压缩率与归档总空间。进程异常退出留下的
`.pending-*` 目录不参与对账和恢复。确认目录没有其他归档进程在用后可清理。

原文与 Parquet 的共同边界是历史库的连续游标。实时提前入库的帧、已 ACK 但
仍未完成历史处理的 raw 都等下一轮导出。遇到历史积压时，应核对该范围并保留
当前工作区。导出和验证都不会清理源 raw。完整实例备份另见
[iQuant 运维手册](iquant-capture.md#备份与恢复)。

## Parquet 类型规则

- 批内序号和单帧行数由 IQTS 帧上限约束，使用 `INT32`。
- 成交量及各档数量使用 `INT64`，时间为可空的 `INT64` 毫秒值。
- 常用价格列使用 `DECIMAL(18,6)`，在 Parquet 中用 `INT64` 存缩放整数。
  成交额使用 `DECIMAL(28,8)`。按原始 JSON 数值字面量做精确转换。
- 任一字段超出范围、精度或类型约束时，该投影列为 NULL，错误字段名写入
  `projection_errors`。原值以及未投影的其他字段可按
  `(stream,batch_seq,ordinal)` 回到 raw。显式 null 和缺字段在投影中均为 NULL，
  原文保留原始区别。每片 manifest 记录包含投影问题的行情行数。
- 同一个 schema 的文件始终使用相同列类型。调整金额或价格精度应另行版本化，
  并在实际行情样本上审查报错率和 reader 兼容性。

## 校验与恢复

```sh
./archive-venv/bin/python scripts/iquant_archive.py verify \
  --archive-dir ./run/instance-a/archive

./archive-venv/bin/python scripts/iquant_archive.py restore-raw \
  --archive-dir ./run/instance-a/archive --work-dir ./run/restored
```

`verify` 校验分片连续性、全部归档文件的 SHA-256、解压后的原文 SHA-256 以及
Parquet schema 与行数。建议在把归档复制到备份介质后校验副本。恢复要求一个
尚不存在的工作目录，校验通过后才发布还原的 `raw/`。启动 iQuant worker 后可用
这些 raw 重建 SQLite，Redis 热层由后续实时数据建立。Windows 的目录同步能力
与采集端文档相同。

集成测试覆盖增量导出、超出 INT32 的金额与数量、超精度标记、CRC 拒绝、
归档损坏拒绝恢复和 raw 逐字节还原。

```sh
./archive-venv/bin/python -B scripts/test_iquant_archive.py \
  --engine-bin ./build-iquant/iquant_engine
```
