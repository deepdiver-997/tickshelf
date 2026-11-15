# Owned pipeline 的 syscall / 数据库诊断

本工具回答“**实际进入内核的哪些 syscall，针对哪类文件，观察到多少次、多少字节、多少 elapsed 时间**”，不把 SQLite API 次数当成 syscall 次数，也不把调用次数当成耗时证据。实现为 Python 标准库模块 `scripts/pipeline_bench_syscalls.py`；默认关闭，不修改 C++ 计时或数据库行为。

## 安全边界与能力状态

- 只允许 Linux 上将**显式拥有的 benchmark executable**直接 launch-under-strace。不支持 `strace -p`、系统范围采样、搜索其他进程、共享 Redis 或现有业务进程。
- `prepare_launch` 要求 `enabled=True`、绝对 `argv[0]` 与 `owned_binary` 同一文件，并拒绝 shell/PID 替代这一所有权约定。此 API 不是恶意 executable 的沙箱；调用方不能把 `/bin/sh` 当作 benchmark。`-f` 跟随该 owned benchmark 创建的线程及后代，因此 benchmark **不得启动非 owned 的外部程序**。已有 Redis 不是 tracee，不会被 attach；Redis 客户端 socket 调用仍可出现在 receiver trace 中。
- 先检查平台与 `strace`，再用相同 flags 对本次新启动的 `python -I -c pass` 做很小的能力探测，结果丢弃到 `/dev/null`。不会运行正式性能实验、提权、使用 sudo，或修改 SIP、ptrace/Yama 策略。禁止为了“让诊断通过”绕过服务器策略。
- 非 Linux、缺少 strace、版本不认识必要 flags、权限拒绝、超时等均明确为 `unsupported`，CLI 退出码 **2**。不是“调用 0 次”，不是“性能测试通过”。预检成功也不保证之后不会有 runtime policy / 文件系统错误；runner 必须检查真实子进程返回码及 trace 的状态。
- macOS 只做这一预检及离线分析/合成单测。需要用户态调用栈时，可以检查**已经拥有、已经存在的 sample**；本功能不启动新的特权采样，不尝试 sudo / dtrace / SIP 变更。sample 不是 Linux syscall trace，不能用它编造 Linux syscall 数量。

直接预检（不需要输出目录，不启动 benchmark）：

```sh
python3 -B scripts/pipeline_bench_syscalls.py preflight
# runner 同样提供便捷入口
python3 -B scripts/pipeline_bench.py --check-syscalls
```

## 隐私 flags 与 trace 保存

wrapper 固定使用以下形式，实际 syscall allowlist 见模块 `TRACE_SYSCALLS`：

```text
strace -f -ttt -T -yy -s 0 \
  -e trace=<明确的I/O、同步、FD、内存映射、锁、线程生命周期allowlist> \
  -e raw=readv,writev,preadv,pwritev,preadv2,pwritev2,recvmsg,sendmsg \
  -o <owned-case-dir>/receiver.strace -- <absolute-owned-binary> <原参数>
```

- `-s 0` 不保存读写 buffer 的字符串内容；路径作为分类元数据仍会被 strace 完整显示。vector/message I/O 额外用 raw 指针参数，避免保存 iov / ancillary buffer 内容，代价是其中部分 requested bytes 不可知。
- 不使用 `-e read=...` / `-e write=...` 或 `--read` / `--write`：这些会独立于 string limit 产生完整 payload dump。也不增加 `-v`、十六进制内容 dump、网络抓包或环境变量 dump。不要移除 wrapper 的隐私 flags 后再称为相同实验。
- 这不是全面匿名化：trace 仍可能含文件名、IP/端口、PID、地址及锁范围，必须按敏感诊断文件管理。对外提供 trace 前先确认元数据授权。离线解析器不会复述不认识的原始行或 buffer 到 diagnostics，只给行号/错误码；它不会改写或脱敏用户提供的原文件。
- trace 仅在当前用户拥有、group/world 不可写的目录中以 `O_EXCL`、0600 新建；不覆盖旧文件或符号链接。不要把 trace 文件放到共享可写 `/tmp` 根目录；使用新建私有 case 目录。JSON CLI 输出同样为 exclusive-create 0600。
- 除 I/O/锁/同步外，allowlist 也包含 fstat/newfstatat/stat/statx/lseek、madvise/brk/mprotect、poll/ppoll/select/pselect6/epoll_wait/epoll_pwait、clock_gettime/gettimeofday，避免把元数据、内存或等待调用静默漏出所述范围。它仍然不是“所有内核活动”：比如 ioctl、未列出的新架构 syscall 不在采集范围。对任意目标环境 trace 可解析其他标准形态的 syscall 行，但不认识其 FD 语义时 filekind 是 `unknown`，不会虚构分类。trace 可相当大，应单独设置存储预算并检查磁盘，不能把 payload 计数预算误当 trace 大小预算。

## runner 集成 API 与进程生命周期

```python
from pathlib import Path
from pipeline_bench_syscalls import preflight, prepare_launch, analyze_trace

cap = preflight()  # 可在 trace 模式入口探测一次；不支持时明确拒绝该模式
plan = prepare_launch(
    original_argv,
    trace_path=Path(case_dir) / "receiver.strace",
    owned_binary=Path(absolute_benchmark_binary),
    enabled=True,
    capability=cap,
)
# 主 runner 负责原有 timeout、日志、owned session/process group、stop/wait。
receiver_handle = runner.start(plan.command, ...)
# 从已验证的 receiver BENCH_READY pid=... 获取 actual_receiver_pid。
# 等待 tracee/tracer 均结束并 flush 后，再解析完整 trace。
report = analyze_trace(
    plan.trace_path,
    db_paths=[exact_database_path],
    segment_paths=[exact_segment_path],
    root_pid=actual_receiver_pid,
)
```

`Capability` 含 `supported/status/reason/strace_path/probe_returncode`。不支持时 `prepare_launch` 抛 `UnsupportedError`，不产生伪零值报告。`enabled=False` 返回原 argv，不探测、不建 trace。

`LaunchPlan.command` 是 tuple argv；`trace_path` 为已预留文件路径；`wrapper_pid_is_target=False` 提醒：**Popen.pid 是 strace PID，而非 receiver PID**。实际目标来自已验证的 `BENCH_READY pid=...`，传给观察器/采样器和 `root_pid`。不要强制 marker PID 等于 tracer PID，也不要仅因某个日志给出 PID 就向该 PID 发信号。

trace 模式应要求 runner 有可靠的 owned process-group/session 清理及实际 receiver identity（Linux pidfd / start identity）的保护。结束/超时必须停止并回收 owned tracee 和 tracer，再分析已 flush 的文件；不能只 terminate tracer、遗留 benchmark，或向可能复用的裸 PID 发信号。身份/生命周期能力不足应拒绝模式而非放宽保护。模块不 attach、不自行启动/停止 benchmark；生命周期由主 runner 持有。

runner 入口为 `--syscalls off|trace`，默认 `off`。每个被跟踪的 case 保存 `receiver.strace` 和 `syscalls.json`，case/root 结果的 `syscalls` 项记录状态及文件引用，不将整个 raw trace 复制进 summary。`partial/invalid/empty` 不可作为完整的 syscall 归因成功。

## 目标服务器复跑

下面是**供以后在目标 Linux 服务器执行的命令，不是本次已经测得的结果**。先在本地批准的工作目录构建 benchmark，并检查链接库/编译配置；不要指向生产数据库或生产 Redis。runner 必须使用它创建的私有 Redis、owned receiver 和独立数据目录。

首先预检，再根据服务器预算运行小格验证；确认清理、磁盘预算及 `coverage` 后，才扩大 records / repeat。下面百万条配置只是已配置 full-record matrix 的复跑示例，**本次实现没有执行它**。已有目录不可复用；两个 output 是同一文件系统上、各自全新的独立目录。

```sh
python3 -B scripts/pipeline_bench.py --check-syscalls

# 配对的无 trace 格；backend 会由 matrix 各 case 覆盖。
python3 scripts/pipeline_bench.py \
  --build build-record-release --suite diagnostic \
  --config '{"backend":"rocksdb","sqlite_fullfsync":"on","batch_records":1024,"hash_batch_records":1024,"redis_batch_records":1024,"transaction_rows":8192,"transaction_bytes":4194304}' \
  --matrix examples/bench/full-record-matrix.json \
  --records 1000000 --repeat 1 --profile off --syscalls off \
  --timeout 180 --max-disk-mib 4096 --reserve-free-mib 10240 \
  --output /tmp/new-untraced

# 除 tracing 和输出目录外保持相同；不是同一时间并发跑。
python3 scripts/pipeline_bench.py \
  --build build-record-release --suite diagnostic \
  --config '{"backend":"rocksdb","sqlite_fullfsync":"on","batch_records":1024,"hash_batch_records":1024,"redis_batch_records":1024,"transaction_rows":8192,"transaction_bytes":4194304}' \
  --matrix examples/bench/full-record-matrix.json \
  --records 1000000 --repeat 1 --profile off --syscalls trace \
  --timeout 180 --max-disk-mib 4096 --reserve-free-mib 10240 \
  --output /tmp/new-traced
```

`--profile off` 避免同时叠加另一个 profiler。保留 timeout、磁盘上限及 reserve-free 保护；不要因为 strace 变慢就取消安全保护。`sqlite_fullfsync=on` 是配置意图，不证明 Linux 执行了 macOS F_FULLFSYNC；实际同步方式以平台行为、持久化设置和 trace 为准。

**观察器开销必须有配对无 trace 格。** strace 会暂停/恢复被跟踪线程，改变调度、锁竞争、短写分布、批次和端到端吞吐；trace 文件写入也争用 CPU/磁盘。trace 格的 syscall elapsed 不是无 trace 格的成本还原。无 trace 格用来报告原生吞吐/阶段 CPU/耗时；trace 格只提供受扰动的 syscall 分布和定位线索。比较后端时同机、同构建、同 records、同持久化语义、同配置分别采集，记录 cache 状态及运行顺序，必要时 AB/BA 多次配对，不从单次 trace 推总体优化百分比。

## 显式 rawtrace 离线分析

可以在 macOS 分析此后提供的 Linux trace。**只读取命令中这一条显式路径**，不会搜索仓库、`/proc`、其他用户目录或 `.local-docs`。通配符不是文件发现接口；请不要用 shell glob。也不自动读取相邻 trace 分片。当前采集为 `-f -o` 单个 multiplexed 文件；`-ff` 每 TID 分片需要用户先明确合并/保留 TID 和顺序，不能把无 TID 的分片当成全进程完整 trace。

```sh
python3 -B scripts/pipeline_bench_syscalls.py analyze \
  '/srv/explicit-case/receiver.strace' \
  --root-pid 12345 \
  --db '/srv/explicit-case/data/full-record.sqlite3' \
  --db '/srv/explicit-case/data/full-record.rocksdb' \
  --segment '/srv/explicit-case/data/records.segments' \
  --output '/srv/explicit-case/new-syscalls.json'
```

以上 PID/路径是**占位示例**，必须替换为真实 marker PID 与配置中的绝对文件名/DB 目录，而非照抄。`--db` / API `db_paths` 可重复，既可声明精确 SQLite 文件，也可声明 RocksDB 目录 root：

- 精确 SQLite 文件及其 `-wal/-shm` 的原有口径不变；例如 compact backend 应传 `compact-record.sqlite3`，segment SQLite index 可传 `segment-index.sqlite3`。
- 显式 DB root 下的子文件按规范化后的完整路径组件匹配（`root + '/'`），归 `db`；其中 basename 为纯 ASCII 数字加 `.log` 的文件（例如 `000123.log`、`archive/000123.log`）归 `wal`。
- `CURRENT`、`MANIFEST-*`、`OPTIONS-*`、`LOG`、`LOG.old.*` 及非数字 `info.log` 等统一归 `db`，本版不另设 `db_metadata` 桶。**不能把所有 `.log` 或 info `LOG` 当作 WAL**。
- 不因任意路径含 `rocks` 字符串就推断 DB；未声明 root 的 RocksDB 文件、相邻同名前缀目录（如 `index.rocksdb-backup/`）和 case 根下的 `progress.json` 都不会因此归为 DB。显式声明是一项范围约定，请只传实际 DB 文件/目录，**不要传整个 case_dir**。目录声明不依赖分析机器上该目录是否存在，便于在 Mac 离线分析目标服务器 trace；不会搜索目录或打开 DB 文件。

`--segment` 可重复，仍只识别显式的精确文件。无法确认 cwd 的相对 open 路径不从本机 cwd 猜测。

离线 CLI 正常完整解析返回 0；`partial`、`invalid`、`empty` 或 I/O/参数错误返回 2。已经提供 trace 并不自动证明采集范围、隐私 flags 和完整性正确，`status=ok` 只表示本文支持的格式成功解析；仍需检查来源、`coverage`、selected syscall mask 和运行退出状态。

### 支持格式与已知边界

- 标准 `name(args) = result <seconds>`，支持十进制/十六进制整数返回值、`-1 ERRNO`、普通/raw I/O 指针、`3</path with spaces, commas>` FD 注解。
- numeric TID 前缀或 `[pid N]`；可有 `-ttt` epoch 或 `-tt` 时分秒前缀。无 TID 的单进程 trace 需传 `--root-pid`，否则 TID/process 显式 unknown。不会把相邻 TID 或线程号大小当成父子证据。
- `<unfinished ...>` 与同 TID/name 的 `<... name resumed>` 组合为一个记录。FD 绑定在 syscall 进入时，open/close 等 FD 变化在返回时更新，以避免阻塞期间其他线程 close 后误归类。未完成记录独立计数，不伪造返回值/耗时。
- 解码的 clone/clone3 `CLONE_THREAD` / `CLONE_FILES`、fork/vfork 用于 thread/process 归属及共享/复制 FD 表；处理 child 先于 parent resumed 的记录、dup/fcntl dup、close-on-exec、unshare、close_range。raw/未知 clone flags 不猜测，诊断为 partial。close 返回错误也会保守清除路径状态，避免下一次复用 FD 被误认。
- 模拟普通的文件表生命周期；不承诺还原 Linux 所有竞态、TID 重用、复杂 exec TID remap、SCM_RIGHTS 传 FD、io_uring、未采集的 FD 操作等。缺失 ancestry/annotation 不从别的进程 FD 表猜测；竞态需要结合逐行 trace 审阅。超出支持格式的行必须看 diagnostics，不能悄悄当作“零成本”。
- 解析器在内存中保存记录/精确 latency 样本，空间随 trace 记录数增长；非常大的 trace 应先限制采集窗口与范围，不是一个无限流式分析器。

## 字段、对比与不可下的结论

`by_name_filekind` 是最小实用对比面：例如比较 `pwrite64 + wal` 的真实观察 counts、bytes、elapsed，再比较 `fdatasync + wal` 的 total/max/分位。另有 `by_tid`、`by_process`、`by_tid_fd_filekind` 和 totals。FD 可复用，因此同 TID/FD/filekind 项是桶，不声称始终指同一个物理文件。

filekind 值为 `db/wal/shm/segment/other/socket/unknown/not_fd`。`not_fd` 用于已知无直接 FD 的 futex、匿名 mmap、内存管理和 clock 等；未知 syscall 语义为 `unknown`，而不是 `not_fd`。`other` 表示已知非上述目标路径或 pipe/anon_inode，不等同于 DB，也不自动等同于磁盘 I/O。fstat/lseek 按首 FD 归类；stat/newfstatat/statx 按已知目标路径（或明确 `AT_EMPTY_PATH` 的 FD）归类，缺失 cwd 时不会猜相对路径。poll/select 多 FD 等待保留为 `unknown` 并计 `multi_fd_wait_calls`，不能把整个等待时间分摊给某个 DB；epoll_wait 首 FD 指 epoll 自身，不是事件中的目标文件。clock_gettime/gettimeofday 只统计真实 trace 条目；走 vDSO 的用户态 API 调用没有对应 kernel syscall，不会被补算。

- `counts`：成功解析、包含返回记录的 syscall 尝试次数；unfinished/resumed 成对只算一次。重新启动/中断的 kernel attempt 不等同于一个逻辑用户 API 调用。
- `failures` / `errno_counts`：`-1 ERRNO` 等明确错误；I/O 负返回且未解码 errno 时另记 `unknown_errno`。不能仅凭任意 syscall 的负号判断错误，例如 `fcntl(F_GETOWN)` 可成功返回负的进程组 ID。`unknown_results` 记录 `?`/restart 等非数值结果，不能算成功；错误次数不是性能耗时的替代。
- `bytes_requested/returned`：只针对支持的 read/write 类调用。请求和返回分别累计；失败返回是 **0 字节，不是 -1 字节**。短读/短写单独计 `short_io_calls`，不把请求字节当实际落盘字节，也不把短读自动解释成故障。mmap 的映射大小和 fdatasync 不计为已读写数据字节。
- `requested_samples/returned_samples` 与 `unknown_requested_byte_calls/unknown_returned_byte_calls` 必须一起读。raw vector/message 的请求长度经常不可得，sum 为 0 且 samples 为 0 是“未知”，不是“没有请求”。decoded iovec 仅在长度可完整读取时累计；未支持的消息/异步 I/O 不作字节推断。page cache、mmap 脏页、内核 writeback 和设备写放大不在这个字节指标里。
- `elapsed_total_s/max/quantiles_s` 来自真实 `-T` duration，即**被观察的 syscall elapsed/wall duration**，包含等待、阻塞、调度干扰及观察器影响，**不是 kernel CPU time**。`kernel_cpu_s=null` 是刻意保留的未知。
- 多线程的 syscall duration 可以重叠；signal/nested 调用也可能有重叠。总量可作为桶的观察值，**不能相加除以整机 wall time，声称是总体 CPU 百分比**。也不要将上面的不同汇总维度再次相加。
- 分位为精确 nearest rank；p50 至少 2 个、p95 至少 20 个、p99 至少 100 个 timed observations，不足时为 null。这只是最低显示门槛，边界样本的尾延迟仍很不稳定。`elapsed_samples/missing` 区分真正的零 duration 与缺测；无 duration 的 total/max 为 null。
- `coverage` 列出解析总数、unparsed、incomplete、missing duration、unknown FD/TID/process、unknown clone flags 等；diagnostics 最多 100 条（仅行号/代码），剩余计 `diagnostics_omitted`。未知分类即使保留 duration，也不能偷偷并入 DB 成本。

不要从“60 次 pwrite”推出“写入耗时很大”，应查看这些调用真实 duration；不要从 `sqlite3_step` / `sqlite3_exec` 次数推 kernel write 次数；不要在无 trace 的十万次/百万次工作负载上编造具体 syscall 归因。SQLite page cache、WAL 批写、checkpoint 与锁会改变两者映射。即使有 trace，它也不自动证明每次 fsync 的设备持久化延迟或所有 CPU 成本。

可以用显式的两个 JSON 路径对比同一个真实 bucket，而不是遍历文件找“看起来最慢”的结果：

```sh
python3 -I - '/srv/run-a/case/syscalls.json' '/srv/run-b/case/syscalls.json' <<'PY'
import json, sys
for filename in sys.argv[1:]:
    with open(filename) as source:
        report = json.load(source)
    print(filename, report['status'], report['coverage'])
    if report['status'] != 'ok':
        raise SystemExit('先审阅不完整/无法解析诊断，不生成完整归因排名')
    for row in report['by_name_filekind']:
        if row['name'] in ('pwrite64', 'fdatasync') and row['filekind'] in ('db', 'wal'):
            print(row['name'], row['filekind'], 'calls=', row['counts'],
                  'requested=', row['bytes_requested'], 'returned=', row['bytes_returned'],
                  'elapsed_s=', row['elapsed_total_s'], 'max_s=', row['elapsed_max_s'],
                  'quantiles_s=', row['elapsed_quantiles_s'])
PY
```

## 本次实现验证

```sh
python3 -B -m unittest discover -s scripts -p test_pipeline_bench_syscalls.py -v
```

测试 trace 全部为**人工合成**，不是 Linux 测量数据，覆盖 read/pwrite64/fdatasync/fcntl/futex/mmap/open/close、短读短写、errno、unfinished/resumed、带空白/逗号的 FD 路径、线程共享 FD / fork 复制 / process 汇总、raw I/O、分位门槛、unsupported 与 launch 保护。macOS 实测预检必须得到 `unsupported`。本次只运行小型本地测试与预检，未执行目标服务器命令、正式 perf、Redis 工作负载或新的特权采样。
