# iQuant 实盘行情本机采集（IQTS v1）

这条采集链路和 TickShelf 的 [TTAP 接入口](protocol.md) 各自接收数据。iQuant 策略把实时回调快照打成批次，接收器把收到的批次写入本机 `.raw` 文件，同步成功后逐批 ACK。这里的 raw 保存 SDK 回调字段的序列化字节，快照的事件粒度由来源决定。接收器可同时启动 iQuant 历史库及 Redis 热发布。

## 启动

1. 把整个 [`examples/iquant`](../examples/iquant/) 目录复制到运行 iQuant 的机器。策略和接收器只用 Python 标准库，代码兼容 Python 3.6 及以上。国信旧版 iQuant 的公开手册没有列出全推接口；运行端必须实际提供带回调的 [`ContextInfo.subscribe_whole_quote`](https://dict.thinktrader.net/innerApi/data_function.html)。不支持时脚本报错停止，不回退到最新价轮询。
2. 从仓库根目录打开 PowerShell，设置令牌和模块目录，从这个窗口启动 iQuant，让其继承环境变量。若客户端已经在运行，先完全退出再启动。不要把真实令牌放进策略源码或配置文件。

   ```powershell
   $secret = New-Object byte[] 32
   $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
   $rng.GetBytes($secret)
   $rng.Dispose()
   $env:TS_IQUANT_TOKEN = [Convert]::ToBase64String($secret)
   $env:TS_IQUANT_MODULE_DIR = (Resolve-Path .\examples).Path
   $env:TS_IQUANT_PORT = '19781'
   # Start-Process 'C:\实际安装路径\iQuant.exe'
   ```

3. 在 iQuant 新建 Python **实盘/实时**策略，导入或粘贴 [`strategy.py`](../examples/iquant/strategy.py) 并运行。`TS_IQUANT_MODULE_DIR` 指向含 `iquant` 子目录的目录，不是 `iquant` 目录本身。`init()` 打开监听并注册订阅；回测不监听。若客户端版本不会调用 `stop()`，退出策略运行进程可释放监听端口。
4. 用继承相同令牌的终端启动接收器。常规断线会自动重连；`Ctrl+C` 停止并保存进度。策略和接收器都默认使用 `127.0.0.1:19781`：只接受 IPv4 loopback，令牌在本机 TCP 明文传输，不可把它作为远程服务暴露。

   ```powershell
   py -3 .\examples\iquant\receiver.py --output .\market-raw
   ```

## 启动历史库和热发布

在装有 SQLite 开发库的环境构建 iQuant worker。在 Windows 可以使用 CMake 的
SQLite3 包配置，生成文件的位置由所选生成器决定。

```sh
cmake -S . -B build-iquant -DTICKSHELF_IQUANT_ENGINE_ONLY=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build-iquant --config Release
python3 -B examples/iquant/receiver.py --work-dir /path/to/instance-a \
  --engine-bin ./build-iquant/iquant_engine --redis-port 6379
```

如果用 Visual Studio 生成器，PowerShell 启动示例如下。路径按实际构建输出调整。

```powershell
py -3 -B .\examples\iquant\receiver.py --work-dir .\run\instance-a --engine-bin .\build-iquant\Release\iquant_engine.exe
```

`--work-dir` 内含 `raw/`、`history.sqlite3` 及 WAL、进度与锁文件。接收器和
C++ worker 各持有独占锁，强杀接收器后旧 worker 退出前仍会占用自己的锁。
多实例使用各自的目录与来源端口。配置独立的 Redis 端口和数据目录可以一并隔离
Redis 文件。省略 `--redis-port` 时只处理历史库。共享 Redis 时各目录默认使用
不同的 Stream 键，也可显式设置 `--redis-key`。

从已有采集目录迁移时，先停旧接收器，再把整份采集目录复制到新工作目录的
`raw/` 后启动。已有原文会从历史库的消费游标继续扫，同一来源批次的历史写入
可以重放。SQLite 使用 `FULL` 同步，在 macOS 上也开启 `fullfsync`。

- 接收端在 raw 同步后发 ACK，随后提交已同步范围的通知。最新的一组可在内存中直接交给解析器。忙时新的通知覆盖上一组，后台始终以已同步的文件范围为准补齐积压。
- 每轮先处理一组最近数据，再处理一组历史，历史流轮转，每组至多 32 帧和约 4 MiB。历史库故障时接收继续，worker 重试时从库中恢复游标。
- 变长记录 schema 为 `iqts-snapshot/v1`，SQLite 中有对应版本登记。每条保存完整证券代码、可空的 SDK `time` 毫秒值及原样的快照 JSON 字节。整数、小数、数组、空值、非有限数和扩展字段保留在字节中。用 `(stream, batch_seq, ordinal)` 识别一条交付记录，同毫秒的不同交付可以并存。批号以 20 位数字文本保存，覆盖 IQTS 的无符号 64 位范围。
- `raw_frames` 同时记录原文件偏移、CRC、GAP 和隔离状态。无法解析的整批保留在 raw 中，通过状态 `quarantine` 定位。源端缺失和本地待处理积压可分别对账。记录、帧状态和可推进的连续游标在同一事务提交。实时批可在较早批次追赶前按来源身份先落盘。连续游标等扫过缺口之前的积压才继续前进。
- 热发布在历史批提交后进入有界队列，积压、Redis 错误或重启可能跳过该次发布。后台补扫直接写历史库。Stream 包含来源流、批号、schema 以及按批内位置编号的证券和快照字段，使用近似 10,000 条 Stream 记录的上限。

## 对账和故障处理

在实例目录中打开只读会话 `sqlite3 -readonly history.sqlite3`。查询同一来源流
的处理位置和异常帧。

```sql
SELECT source_time_ms, CAST(snapshot AS TEXT)
FROM quotes
WHERE symbol='IO2712-C-6000.IF'
  AND source_time_ms BETWEEN 1700000000000 AND 1700001000000
ORDER BY source_time_ms;

SELECT stream, first_seq, last_seq, byte_start, byte_end, status, detail
FROM raw_frames WHERE status IN ('gap', 'quarantine');

SELECT stream, raw_end, last_seq FROM replay_cursor;
```

| 现象 | 排查位置 |
|---|---|
| 接收正常，历史进度落后 | 按 `stream` 对照 `raw/progress.json` 的 `resume_watermark` 和 `replay_cursor.last_seq`。后者是固定宽度的数字字符串，未建游标的流从零开始。差值表示待顺序核对的批号跨度，也可能包含已提前入库的实时批或 GAP。持续扩大时检查 worker 日志和磁盘剩余空间。 |
| 历史进度正常，但有缺失 | 查询 `raw_frames`。 `gap` 表示来源端当前无法重放，`quarantine` 表示原文已保存，需要检查解析或提供端数据。可按记录中的 `byte_start`、`byte_end` 定位原帧。 |
| 历史库有记录，Redis 查不到 | 检查端口、Stream 键、队列满和发布失败日志。后台补扫只补历史库。 |
| 路径锁报错 | 检查同目录的接收进程或强杀后仍在退出的 worker。应保留锁文件，由持有进程退出释放锁。 |

## 备份与恢复

1. 停止接收器，确认 worker 已退出。若来源策略继续推送，停机期间能否重放取决于策略的内存窗口。
2. 一起复制 `raw/`、`history.sqlite3` 和当时存在的 SQLite WAL。将备份恢复到空工作目录。
3. 启动实例后核对各流的接收进度、历史游标、GAP 与隔离记录，抽查证券范围查询。仅从 raw 恢复时，worker 会在空历史库重建记录和游标。

raw 目前需要人工规划保留空间。实例启动时核验历史库所引用的 raw 文件，归档和清理需同时考虑所有这些引用。Redis Stream 可以作为独立的短期热数据管理。

如需按批备份已对账数据，使用 [iQuant 增量归档](iquant-archive.md)。它可在实例运行时输出压缩 raw 和可查询的 Parquet。

运行包含连续强杀和 Redis 堵塞的测试。

```sh
python3 -B scripts/test_iquant_engine.py --engine-bin build-iquant/iquant_engine
```

## 订阅范围与实盘验收

| 覆盖组 | 默认请求市场 | 目标 |
|---|---|---|
| `SSE_EQUITIES` / `SZSE_EQUITIES` | `SH` / `SZ` | 上交所、深交所股票（A/B 股）。默认全市场推送可能还包含指数、基金、债券等。 |
| `SSE_OPTIONS` / `SZSE_OPTIONS` | `SHO` / `SZO` | 沪深交易所上市期权。 |
| `SHFE_DERIVATIVES` / `INE_DERIVATIVES` | `SF` / `INE` | 上期所、上期能源的期货及期货期权。 |
| `CFFEX_DERIVATIVES` | `IF` | 中金所的期货及期货期权。 |
| `DCE_DERIVATIVES` / `CZCE_DERIVATIVES` | `DF` / `ZF` | 大商所、郑商所的期货及期货期权。 |
| `GFEX_DERIVATIVES` | `GF` | 广期所的期货及期货期权。 |

这些是 [QMT 嵌入式文档](https://dict.thinktrader.net/innerApi/data_function.html)中的市场代码，不代表国信所有版本和账户都支持对应的全推。`REGISTERED` 只表示拿到了订阅号；`FIRST PUSH` 只表示有行情到达。**首次上线请在交易时段把落盘证券码与客户端当日合约目录对账**：分别核对沪、深的 A/B 股及期权，逐所核对期货和期货期权，不要只凭注册日志判断。非交易时段、停牌、无更新和无行情权限也可能没有推送。

如果券商市场代码不同、全市场推送不含期货期权，或想严格限制沪深股票范围，可设置 `TS_IQUANT_SUBSCRIPTIONS` 为一个 UTF-8 JSON 文件路径。仅覆盖指定组；每组填一个非空的 `markets`、`sectors` 或 `symbols` 列表。`sectors` 由客户端 `get_stock_list_in_sector` 解析，`symbols` 用完整 `code.market`；这两种都通过全推 API 按合约代码订阅。下面的单个合约只是格式示例，实际请用客户端导出的完整目录；换月或新合约上线后重新导出并重启策略。

```json
{
  "SSE_EQUITIES": {"sectors": ["用户自建沪市A股", "用户自建沪市B股"]},
  "SZSE_OPTIONS": {"symbols": ["90000001.SZO"]}
}
```

明确的订阅失败默认使策略启动失败。确实要先采集能收到的市场时，可设置 `TS_IQUANT_ALLOW_PARTIAL=1`，日志会标记 `PARTIAL CAPTURE`。遇到无法序列化或单条大于批次上限的 SDK 快照，会计入 `rejected`；这类尚未分配批次号的源端丢失不在 IQTS `GAP` 中体现。

## 水位、溢出和耐久

接入方可在 `Receiver(..., on_durable=callback)` 注入通知函数，签名为
`callback(stream_id_hex, resolved_sequence)`。每组 raw 同步且 ACK 已尝试发送后调用；
断线收尾的已同步数据也会通知。回调在接收线程执行，应只做快速唤醒，不能把它当作
可靠任务队列：异常只记日志，进程崩溃也可能丢失通知，消费者必须在启动及运行中扫描
raw 并持久化自己的处理进度。通知不代表 Redis 发布或内容持久化完成。
`Sender(..., on_ack=callback)` 可注入来源端确认回调，签名为
`callback(stream_id_bytes, ack_kind, first, last)`；只在当前连接验证过的新 ACK 上调用，
在收 ACK 线程且锁外执行，不保证跨断线或崩溃恰好一次。回调应快速返回。

- **单活连接。** 新接收器首次以正确令牌和合法的 `RESUME` 接入才接管；错误令牌、未完成握手和旧 socket 的迟到 ACK 不推进水位。被取代的接收器在自动重连时收到 `SUPERSEDED` 后退出，不会抢回去。换接收器进程但要续接同一份数据，先停止旧进程再用原 `--output`；一个目录只允许一个写者。
- **窗口和环形缓冲。** 从 1 开始分配批次号。发送方一次可有多个未确认批次；先收到 `2、4、3` 的 ACK，仍不能释放连续前缀，等 `1` 的 ACK 到了才一起推进到 `4`。缓冲超限淘汰最早批次；若被淘汰的批次已发出，仍可确认它。需要重发但已不可用的序号（也可能已被之前另一个目录的接收器确认）会发 `GAP`，而不是跳号或冒充已落盘。
- **两个水位。** `data_watermark` 是从批次 1 起全部 DATA 已落盘的最大连续序号，即第一段缺失前的最小待处理位置减一；以后有 DATA 落盘也不会越过第一个 `GAP`。`resume_watermark` 是已落盘的 DATA **或明确的缺失记录**覆盖的最大连续序号，可以跨过已持久化的 `GAP` 继续采集。不同 `stream_id` 的序号各自从 1 开始，不互相取最小值；`progress.json` 的 `current_stream` 指向当前流。
- **ACK 以 raw 为准。** 接收器按小组追加并 `fsync`，成功才发 ACK。`progress.json` 默认每 2 秒更新：同目录临时文件 → `fsync` → `os.replace`，POSIX 上再同步目录。Windows 标准库没有目录 `fsync`，因此 Windows 的目录项在异常断电时不作同等保证。磁盘写入或同步报错时接收器停止，而不是 ACK 未确认的数据。
- **重启。** 接收器扫描 `<stream_id>.raw` 校验和重建游标，不依赖可能滞后的进度文件。只截掉进度未覆盖的不完整尾帧；完整帧校验失败、进度超前或缺少已记录的 raw 文件会停止并报错。策略进程重启会分配新的 `stream_id`，旧进程内存中未发送的数据无法恢复。缓冲丢失、客户端权限不足、原始行情本身没有推送，都不在无损承诺内。raw 文件没有自动轮转或清理；请监控磁盘，不要在接收器写入时搬移或删除文件。

### 调整批次和窗口

| 设置 | 默认值 | 作用 |
|---|---:|---|
| `TS_IQUANT_HOST` / `TS_IQUANT_PORT` | `127.0.0.1` / `19781` | 两端共用的本机监听地址/端口。 |
| `TS_IQUANT_WINDOW` | `128` | 在途 DATA/GAP 帧上限；只有连续 ACK 才释放窗口。 |
| `TS_IQUANT_BUFFER_BATCHES` / `TS_IQUANT_BUFFER_BYTES` | `8192` / `67108864` | 保留的 DATA 批数和编码字节上限，任一超出即淘汰最早批。正在发的单帧及回调临时对象另计。 |
| `TS_IQUANT_BATCH_TICKS` / `TS_IQUANT_BATCH_BYTES` | `256` / `262144` | 单批最多的快照数及 DATA 载荷字节数，单次回调按这两个限制分批。 |
| `--sync-batches` / `--sync-bytes` / `--sync-ms` | `32` / `4194304` / `10` | 接收器在批数、字节数或等待时间任一达到时同步一组。 |
| `--checkpoint-seconds` / `--retry-seconds` | `2` / `1` | 进度刷新和普通断线重试间隔。 |

## 二进制格式

所有整数使用网络字节序（大端）。固定头 **32 字节**，按顺序为 `magic[4]="IQTS"`、`version:u8=1`、`type:u8`、`reserved:u16=0`、`first:u64`、`last:u64`、`payload_bytes:u32`、`crc32:u32`。CRC-32 覆盖头的前 28 字节及载荷；序号范围均为闭区间。进程启动时为整条流生成 16 字节 UUID；在同一 TCP 会话内不重复发送流 ID。

| type | 帧 | `first` / `last` | 载荷 |
|---:|---|---|---|
| 1 | `AUTH` | `0` / `0` | UTF-8 令牌，1–1024 字节。 |
| 2 | `WELCOME` | `0` / `0` | 16 字节流 UUID。 |
| 3 | `RESUME` | `data_watermark` / `resume_watermark` | 模式 `u8`（`0` 初次接管，`1` 自动续连）+ 16 字节接收进程 ID。 |
| 4 | `READY` | `0` / `0` | 空，已切换到该会话。 |
| 5 | `DATA` | 同一个正批号 | 1 字节至 4 MiB。本策略写 UTF-8 `[[证券代码, SDK快照], …]`；SDK 的非有限数保留为 `NaN`、`Infinity` 或 `-Infinity`，属于 Python JSON 的扩展。 |
| 6 | `GAP` | 起、止批号 | 空；表示该范围数据当前不可重放。 |
| 7 | `ACK_DATA` | 对应 DATA 的批号 | 空。 |
| 8 | `ACK_GAP` | 对应 GAP 的起、止批号 | 空。 |
| 9 | `SUPERSEDED` | `0` / `0` | 空；接收进程已被新的接管者替代，不应继续自动重连。 |

`.raw` 是 `DATA` / `GAP` 帧按序拼接，原样保存载荷，不含令牌或其他握手帧。独立解析可参考 [`protocol.py`](../examples/iquant/protocol.py) 的 `parse_header`、`parse_frame`。

不需要 iQuant 就能运行本机 TCP 和落盘故障测试：

```sh
python3 -m unittest discover -s scripts -p 'test_iquant_capture.py' -v
```
