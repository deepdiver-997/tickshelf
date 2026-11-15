# RocksDB 固定版本原生参考与同步语义审读（2026-11-14）

## 范围与结论边界

原生参考入口为 `bench/rocks_reference.cpp`；本文记录固定来源、同步语义、构建和异常根因。RocksDB 源码、构建、安装及初始功能检查在独立 `/tmp/tickshelf-oss-rocksdb-20261114/`，正式应用测量在 `/tmp/tickshelf-opt-20261114/`。不修改全局Git身份，不运行sudo，不安装系统服务，不提交或推送。该原生参考与可选 `segment_rocksdb` 网络链路索引后端分开计时、分开解释。

这是调用官方 `rocksdb::DB` / `rocksdb::WriteBatch` 的**原生库级参考**，不是官方 `db_bench`，更不是包含网络、协议解析、Redis、SQLite 或整个 tickshelf 服务的全链基准。复用 tickshelf 的确定性 record 生成与编码，只表示业务 payload 相同，不表示两种测试的计时边界相同。

**本文保留最初构建和258行tiny功能检查的历史证据；随后已用修复RTTI的新库运行500,000条原生同步参考，以及完整网络链路的RocksDB稀疏索引对照。** 正式结果见 [2026-11-14优化报告](pipeline-optimization-20261114.md) 与其机器JSON：原生参考467,743 records/s仅为约1.07秒库级观察；完整网络链路另有百万条三轮。tiny JSON 自动计算的速率不能当性能结论。没有做物理掉电、长期持续写入或SSD硬件PLP验证；应用层索引故障/崩溃测试与上游库官方测试套件也不混为一谈。

**后续链接正确性修正：初始 Release 库的 `USE_RTTI=AUTO → -fno-rtti` 在本机造成标准异常 RTTI 身份冲突。正式链接必须改用 `/tmp/tickshelf-oss-rocksdb-20261114/install-rtti`，不要继续使用初始 `install`。独立 RTTI 重建、根因与小测见文末；旧目录原样保留取证。**

## 固定来源与初始构建产物（历史现场，已由 RTTI 构建替代）

2026-11-14 通过代理读取公开 GitHub API：`/repos/meta/rocksdb/releases/latest` 和 `/repos/meta/rocksdb/releases/tags/v11.13.2` 均返回正式版 `v11.13.2`，`prerelease=false`、`draft=false`，发布时间 `2026-11-04T00:35:04Z`。

- 上游：[meta/rocksdb](https://github.com/meta/rocksdb)，不是沿用旧 facebook 仓库地址。
- Release：[v11.13.2](https://github.com/meta/rocksdb/releases/tag/v11.13.2)。
- Annotated tag object：`e0bac924d7b08f81d6206ff01301fca3e4f5b0b8`。
- **实际 HEAD / peeled commit：`17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa`**。
- Commit 日期 `2026-10-28T15:47:22-07:00`、标题 `Update HISTORY.md for 11.13.2`；commit 日期不等于 release 发布时间。
- `clone --branch` 对 annotated tag 输出的 `not a commit` 提示针对 tag 对象，不意味着实际 checkout 失败；用 `rev-parse HEAD` 明确核对了上述 commit。
- `build/build_version.cc:11-17` 记录正确 SHA/tag，`HAS_GIT_CHANGES=0`。

| 项目 | 实际值 |
|---|---|
| 源码 | `/tmp/tickshelf-oss-rocksdb-20261114/src` |
| 构建 | `/tmp/tickshelf-oss-rocksdb-20261114/build` |
| 安装 prefix | `/tmp/tickshelf-oss-rocksdb-20261114/install` |
| `RocksDB_DIR` | `/tmp/tickshelf-oss-rocksdb-20261114/install/lib/cmake/rocksdb` |
| 导出 target | `RocksDB::rocksdb`，STATIC IMPORTED |
| 静态库 | `/tmp/tickshelf-oss-rocksdb-20261114/install/lib/librocksdb.a`，约 26 MiB |
| include | `/tmp/tickshelf-oss-rocksdb-20261114/install/include` |
| 静态库 SHA-256 | `96ec55d2e462fa525495c2562a10b52189ed3688998a4b7a0a168fb165a13212` |
| 平台 | macOS Darwin 24.6.0 arm64 |
| 编译器 | Apple clang 17.0.0 (`clang-1700.6.4.2`) |
| CMake | 4.4.4，Unix Makefiles |
| 编译模式 | Release，`-O3 -DNDEBUG -fno-rtti -std=gnu++20 -arch arm64`，armv8 CRC/crypto |
| 额外库依赖 | 本构建没有压缩/gflags/jemalloc/Folly 等第三方依赖；package config 仅需 Threads，macOS 由系统 pthread/libc++ 支持 |

安装后的 `RocksDBTargets.cmake:58-63` 声明该静态 target 与 include；`RocksDBTargets-release.cmake:8-13` 给出 archive 路径，没有额外导出的 link libraries。这些不是推测的 target 名。

### 构建前审读

先审读固定版本 CMake 和实际启用路径，再执行构建：

- [`CMakeLists.txt:96-205`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/CMakeLists.txt#L96-L205)：静态/共享、压缩、gflags、jemalloc 等依赖开关。
- [`CMakeLists.txt:614-759`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/CMakeLists.txt#L614-L759) 和 `1198-1242`：Folly/getdeps 可调用 Python；本构建明确关闭 Folly/Folly-lite/coroutines，因此不进入这些路径。
- [`CMakeLists.txt:1290-1317`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/CMakeLists.txt#L1290-L1317)：Git 元数据只读采集、version 文件生成、静态库 target。
- [`CMakeLists.txt:1350-1464`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/CMakeLists.txt#L1350-L1464) 和 `1816-1850`：关闭 JNI、测试、benchmark/tool/trace/example；不构建 gtest 或 `db_bench`。
- [`cmake/modules/ReadVersion.cmake:3-9`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/cmake/modules/ReadVersion.cmake#L3-L9) 只从头文件读版本；[`cmake/RocksDBConfig.cmake.in:9-49`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/cmake/RocksDBConfig.cmake.in#L9-L49) 按启用依赖生成 package config。
- 审查的启用配置路径没有 FetchContent/ExternalProject/自动下载；未执行 submodule/getdeps/依赖安装。构建和安装命令始终从可信项目工作目录运行，用 `-S`/`-B` 指定下载源与独立构建目录，不从下载目录运行 Python。

### 可复现的构建命令

下面是实际使用的选项；只适用于父目录尚不存在的初次构建，不删除或覆盖既有目录。源目录先独立创建为空。所有并行编译最多 4 个任务。CMake 内部只读 Git 命令通过该进程的环境获得相同代理/hooks 设置，不修改任何 Git 配置文件。

```sh
# cwd: /Users/zhuhongrui/code/c++/project/tickshelf
set -eu
ROOT=/tmp/tickshelf-oss-rocksdb-20261114
test ! -e "$ROOT" && mkdir "$ROOT" && \
  mkdir "$ROOT/src" "$ROOT/build" "$ROOT/install"
git -c http.proxy=http://127.0.0.1:12002 -c core.hooksPath=/dev/null \
  clone --depth 1 --branch v11.13.2 --single-branch \
  https://github.com/meta/rocksdb.git "$ROOT/src"
git -c http.proxy=http://127.0.0.1:12002 -c core.hooksPath=/dev/null \
  -C "$ROOT/src" rev-parse HEAD
# 在继续前，应等于 17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa。

GIT_CONFIG_COUNT=2 \
GIT_CONFIG_KEY_0=http.proxy GIT_CONFIG_VALUE_0=http://127.0.0.1:12002 \
GIT_CONFIG_KEY_1=core.hooksPath GIT_CONFIG_VALUE_1=/dev/null \
cmake -S "$ROOT/src" -B "$ROOT/build" \
  -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="$ROOT/install" \
  -DCMAKE_INSTALL_LIBDIR=lib -DCMAKE_CXX_STANDARD=20 \
  -DROCKSDB_BUILD_SHARED=OFF -DWITH_TESTS=OFF -DWITH_ALL_TESTS=OFF \
  -DWITH_BENCHMARK_TOOLS=OFF -DWITH_BENCHMARK=OFF \
  -DWITH_CORE_TOOLS=OFF -DWITH_TOOLS=OFF -DWITH_TRACE_TOOLS=OFF \
  -DWITH_EXAMPLES=OFF -DWITH_GFLAGS=OFF \
  -DWITH_SNAPPY=OFF -DWITH_LZ4=OFF -DWITH_ZLIB=OFF -DWITH_ZSTD=OFF -DWITH_BZ2=OFF \
  -DWITH_JEMALLOC=OFF -DWITH_LIBURING=OFF -DWITH_NUMA=OFF \
  -DWITH_JNI=OFF -DWITH_FALLOCATE=OFF \
  -DUSE_FOLLY=OFF -DUSE_FOLLY_LITE=OFF -DUSE_COROUTINES=OFF \
  -DROCKSDB_PLUGINS= -DFAIL_ON_WARNINGS=OFF
cmake --build "$ROOT/build" --target rocksdb --parallel 4
cmake --install "$ROOT/build"
```

## 同步、SST、compaction、close 语义：固定 SHA 的证据

以下行号全部对应上述固定 commit，而不是可能移动的 main/wiki。

| 问题 | 具体证据与本参考采用的解释 |
|---|---|
| 一批是什么 | [`include/rocksdb/write_batch.h:9-22`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/include/rocksdb/write_batch.h#L9-L22)：WriteBatch 是按顺序应用的一组原子更新。这里每批是一次 `DB::Write`；不是每行一次 fsync，也不声称多个独立批次构成一个事务。 |
| `sync=true` / WAL on | [`options.h:2687-2712`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/include/rocksdb/options.h#L2687-L2712) 描述同步写与 disableWAL；[`db_impl_write.cc:955-956`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/db/db_impl/db_impl_write.cc#L955-L956) 拒绝 sync+disableWAL。参考明确设置 `sync=true, disableWAL=false`，每次检查 Status。 |
| 同步写实际路径 | [`db_impl_write.cc:1377-1433`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/db/db_impl/db_impl_write.cc#L1377-L1433) 调用 WriteGroupToWAL 并记录同步结果；[`2365-2457`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/db/db_impl/db_impl_write.cc#L2365-L2457) 写 WAL 后按 use_fsync 调 Fsync/Sync，并在需要时同步 WAL 目录。 |
| `use_fsync` 的一般定义 | [`options.h:857-865`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/include/rocksdb/options.h#L857-L865)：通常在 fdatasync 与 fsync 之间选择；参考设置 true。但不能据此忽略平台实现。 |
| **此 Mac 构建实际是 F_FULLFSYNC** | [`env/io_posix.cc:1826-1851`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/env/io_posix.cc#L1826-L1851)：存在 HAVE_FULLFSYNC 时，PosixWritableFile 的 Sync 和 Fsync **都调用 `fcntl(fd, F_FULLFSYNC)`**；否则分别 fdatasync/fsync。[`CMakeLists.txt:606-609`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/CMakeLists.txt#L606-L609) 检测并定义宏。真实 `build/CMakeCache.txt:657` 为 `HAVE_FULLFSYNC:INTERNAL=1`，`build/CMakeFiles/rocksdb.dir/flags.make:5` 包含 `-DHAVE_FULLFSYNC`。这是被审读构建的源路径/编译证据，不是对任何其他 RocksDB 二进制的自动检测。 |
| writer 的同步和缓冲 | [`file/writable_file_writer.cc:554-650`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/file/writable_file_writer.cc#L554-L650)：Fsync/Sync 经 SyncWithFlush，先 Flush，再按 pending 状态调用底层同步。API 调用次数不应直接当实际硬件 flush 次数。 |
| `SyncWAL` 不等于生成 SST | [`db.h:2329-2350`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/include/rocksdb/db.h#L2329-L2350)：SyncWAL 只保证 WAL 同步且依赖 OS/硬件支持，不隐含 FlushWAL；manual_wal_flush=true 时推荐 FlushWAL(true)。参考保持 manual_wal_flush=false、allow_mmap_writes=false。实现 [`db_impl.cc:2326-2338,2436-2442`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/db/db_impl/db_impl.cc#L2326-L2442) 对当前 WAL 使用 SyncWithoutFlush。 |
| Flush 的等待范围 | [`db.h:2309-2327`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/include/rocksdb/db.h#L2309-L2327)、[`options.h:2778-2810`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/include/rocksdb/options.h#L2778-L2810)、[`db_impl_compaction_flush.cc:2951-2963`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/db/db_impl/db_impl_compaction_flush.cc#L2951-L2963)：Flush 处理 memtable，wait=true 等待对应 flush。它不表示全库已经压实到最底层。参考没有注册 listener。 |
| SST 文件与安装结果 | [`db/builder.cc:448-460`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/db/builder.cc#L448-L460) 同步并关闭非空 SST；[`db/flush_job.cc:1132-1135`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/db/flush_job.cc#L1132-L1135) 同步输出目录；[`flush_job.cc:297-339`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/db/flush_job.cc#L297-L339) 构建 SST 后安装 flush 结果。因此不能将批次 WAL 同步完成提前标为 SST 完成。 |
| compaction 不是 Flush 的同义词 | [`db.h:2260-2275`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/include/rocksdb/db.h#L2260-L2275)、[`options.h:3400-3426`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/include/rocksdb/options.h#L3400-L3426)：WaitForCompact 等待已调度和排队的 flush/compaction，需要先停止写入。实现 [`db_impl_compaction_flush.cc:6049-6074`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/db/db_impl/db_impl_compaction_flush.cc#L6049-L6074) 也按 wait_for_purge 等待清理。参考在停止写入并显式 Flush 后等待这些任务，**没有强制 CompactRange/全量压到最底层**。 |
| compaction 输出也有同步与安装阶段 | [`compaction_outputs.cc:69-79`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/db/compaction/compaction_outputs.cc#L69-L79) 同步输出；[`compaction_job.cc:1183-1188,1215-1232`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/db/compaction/compaction_job.cc#L1183-L1232) 同步输出目录、验证并安装结果。 |
| Close 不是最后一次 fsync | [`db.h:493-510`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/include/rocksdb/db.h#L493-L510) 明说 Close 不 fsync WAL，调用方须先 SyncWAL/同步空批。关闭可能取消后台工作；[`db_impl.cc:800-848`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/db/db_impl/db_impl.cc#L800-L848) 仅在特定未持久化/直接 blob 等条件下进行关闭 flush。参考不依赖析构隐式补齐，而先 Flush、WaitForCompact、SyncWAL，再检查 Close Status 并销毁对象。 |

**不作的推论**：同步 WAL ≠ SST 已生成；SST flush ≠ 所有层已经完全 compaction；WaitForCompact 结束 ≠ 估计债务指标是一项永久不变量；F_FULLFSYNC 返回成功 ≠ 已证明该 SSD 有硬件掉电保护。与其他数据库比较时应额外匹配实际 OS 同步路径、批大小、键/值内容、最终收尾边界，不能只匹配一个名为 FULL 或 sync 的选项。

## 原生 driver 的契约

集成由主任务提供，所需 target 关系为 `rocks_reference` 链接 `tickshelf_bench_lib` 和 `RocksDB::rocksdb`，使用 C++20，不需要 gflags。特别注意此固定版本 [`db.h:258-259,298-300`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/include/rocksdb/db.h#L258-L300) 的 Open/OpenForReadOnly 输出参数已是 `std::unique_ptr<rocksdb::DB>*`，不是旧版 `DB**`。

```text
rocks_reference --directory NEW --records N [--batch-rows 1..8192]
                [--json-file NEW.json] [--export NEW.jsonl]
```

- `--directory` 必须是尚不存在的目录，且父目录已存在；程序不会删除或复用既有库。
- `--records` 必填且为正数；batch 默认 1024，上限 8192，最后一批允许不足。拒绝重复/未知参数、无效整数及字节计数溢出。
- 每个 key 是从 id=0 开始的 **16 个小写十六进制 ASCII 字节**，补前导零，无 NUL 终止字节；字节序排序就是无符号 id 顺序。不是 symbol/time 索引键，也不是 16 字节随机二进制键。
- 每个 value 是 `encode_record(generate_record(id, {54,5,42,false}))` 的**完整 291 字节**，包含 symbol、time、present/null masks、12 个标量和四组五档数据，不包含 in-process metadata，也没有只存 hash/摘要。
- WAL on、`WriteOptions.sync=true`、`Options.use_fsync=true`；普通和 bottommost compression 都为 none；自动 compaction 开启，`max_background_jobs=2`。其余 RocksDB 参数保持该固定版本默认；没有给原生参考额外调大内存或隐藏 compaction。
- 运行前核对动态 API 返回的库版本和 build-properties SHA；拒绝非上述固定构建来源，避免静默链接到系统里的其他版本。JSON 记录库实际 build properties。
- JSON 默认写到 `NEW.reference.json`，也同时原样输出 stdout；可用 `--json-file` 指定另一个新路径。输出文件必须在 DB 目录之外，以 `O_EXCL` 创建，不能覆盖旧结果。失败时会保留部分产物用于排查，不自动删除数据。
- `--export NEW.jsonl` 可选：只在关库、重新只读打开并逐项校验时导出，每行恰为 `{"symbol":...,"time":...,"hex":...}`；hex 来自 **实际 iterator 读回的 value**，symbol/time 从实际 value 解码，适合交给独立 oracle。

### 计时与后台债务

主分母 `seconds.total` 从创建新目录前开始，直到最终 Close 和 DB 对象销毁后结束，覆盖：

1. 创建目录和 DB::Open。
2. **全部** record 生成、编码、WriteBatch 组装与同步 DB::Write；逐批记录 prepare、write_sync、total 秒数。`write_sync` 不包括编码，批次 total 包括编码。
3. 写完后的后台债务采样。
4. `Flush(wait=true, allow_write_stall=true)`。
5. `WaitForCompact(wait_for_purge=true)`，停止写入后等待自动任务，不强制额外全量压实。
6. `SyncWAL()`。
7. 显式 `Close()`、Status 检查以及对象销毁。

上述阶段时间分别列出，快照与阶段切换开销也进入 total，所以不要强求阶段字段之和逐纳秒等于 total。吞吐仅以 `records / total` 计算；payload MiB/s 仅计 291B value，不把 key、WAL、SST 写放大伪装成业务 payload。

`debt_after_writes`、`debt_after_flush`、`debt_after_wait_for_compact`、`debt_before_close` 各报告：immutable memtable 数、flush pending/running、compaction pending/running、estimated pending compaction bytes、current memtable bytes、total/live SST bytes、background errors。未知属性输出 null，不伪装为零。属性定义可查 [`db.h:1655-1712,1785-1817`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/include/rocksdb/db.h#L1655-L1817)。估计债务不是对物理介质状态的证明；空的 active memtable 仍可能占内存。

不计入数据库工作负载时间：CLI/选项准备、独立输出文件预留，以及**最终关库后**只读 reopen、逐项校验、JSONL 导出、报告格式化与报告文件 I/O。校验/导出单列 `verification_and_export_outside_timing`，不会拿它去稀释/美化写入吞吐。成功 JSON 只在校验完成后输出。验证会检查从 0 到 N-1 的全部 key、完整 value 及没有额外 key，不只抽样或比较条数。

## 已做的功能验证

真实测试二进制 `/tmp/tickshelf-oss-rocksdb-20261114/build/rocks_reference-functional` 使用 Apple clang C++20 编译，开启 `-Wall -Wextra -Wpedantic` 无诊断，链接实际安装的静态 RocksDB、项目现有 `build-bench-release/libtickshelf_bench_lib.a` 和其 record 所需的 simdjson。后续正式项目构建应走主任务集成的 CMake，不必复用这个手工链接的检查二进制。

只运行两组小规模检查：

| records / batch | 功能覆盖 | 结果 |
|---|---|---|
| 257 / 64 | 最后一批 1 行、id 跨 `ff`、多个 symbol/time、五档 payload、关闭/重新打开 | 通过 |
| 1 / 1 | 单行同步批、最终 Flush/等待/同步/关闭、重新打开 | 通过 |

额外用可信工作目录中的 `python3 -I` 临时检查代码，**不导入任何项目/下载源码模块**，只用 Python 标准库 `struct` 和公开生成公式独立组装 291B payload，逐行比较 export 的 symbol/time/hex。258 行全部一致；这不是仅让同一 C++ 编码器自证。

同时通过以下检查：

- stdout JSON 与独立 JSON 文件逐字节一致；batch 行数之和、payload 大小与 reopen 计数正确。
- after-wait/before-close 的 immutable/pending/running/estimated compaction/background errors 均为 0。
- 重复使用同一 DB 路径失败，旧报告内容不变。
- batch=0 及既有 JSON 输出路径在创建新 DB 之前被拒绝。

保留的功能证据：

```text
/tmp/tickshelf-oss-rocksdb-20261114/functional-257/
/tmp/tickshelf-oss-rocksdb-20261114/functional-257.json
/tmp/tickshelf-oss-rocksdb-20261114/functional-257.jsonl
/tmp/tickshelf-oss-rocksdb-20261114/functional-257.stdout.json
/tmp/tickshelf-oss-rocksdb-20261114/functional-1/
/tmp/tickshelf-oss-rocksdb-20261114/functional-1.json
/tmp/tickshelf-oss-rocksdb-20261114/functional-1.jsonl
/tmp/tickshelf-oss-rocksdb-20261114/functional-1.stdout.json
```

这些检查证明初始固定构建与此测试输入可正常同步写、收尾、重新打开并返回全部业务字节；它们没有覆盖随后发现的异常 RTTI 负路径问题，因此不能为旧库的异常处理正确性背书。没有运行上游完整测试套件，没有构建官方 db_bench，没有做大量 SST/持续 compaction 压力测试，没有正式吞吐结果，也不能据此确认突然断电后的行为或硬件 PLP。

## 补充：Apple clang 17 标准异常 RTTI 根因与独立修复

本节是链接正确性检查，**不是性能测试**。全部隔离程序/结果位于新的 `/tmp/tickshelf-oss-followup-20261114/checks/`；未改项目 store、CMake 或原始库文件，未接触共享 Redis。

### 最小复现和符号证据

应用均用同一 `/usr/bin/clang++` 17、C++20、`-O3 -DNDEBUG -frtti`。三类抛出分别是直接构造 `std::filesystem::filesystem_error`、对明确不存在的隔离路径调用 `std::filesystem::canonical`、`std::runtime_error` 对照。分别测试基类 `catch(const std::exception&)` 和精确 `catch(const filesystem_error&)`；`catch(...)` 仅作为诊断观察，不作为产品修复。

| 链接变体 | filesystem_error 被基类 catch 识别 | 精确 filesystem_error catch | runtime_error 基类 catch |
|---|---|---|---|
| 不链接 RocksDB | 两种抛出都正常 | 正常 | 正常 |
| 初始 librocksdb.a，不调用任何 RocksDB API | 两种抛出都失败，落诊断 catch(...) | 正常 | 正常 |
| 初始库 + GetRocksVersion / Env::Default / OpenForReadOnly（三种独立变体） | 均失败 | 正常 | 正常 |
| 完全无 RocksDB，只链接下述 `-fno-rtti` helper 对象 | 两种抛出都失败 | 正常 | 正常 |
| 同一 helper 改用 `-frtti` | 均正常 | 正常 | 正常 |
| 新 RTTI 库：被动 / version / env / read-only open（四种变体） | 均正常 | 正常 | 正常 |

仅一个 helper 对象即可复现，不需要数据库读写：

```cpp
#include <exception>
#include <stdexcept>
void no_rtti_helper() {
    try { throw std::runtime_error("helper only"); }
    catch (const std::exception&) {}
}
```

根因证据链：

1. RocksDB [`CMakeLists.txt:441-464`](https://github.com/meta/rocksdb/blob/17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa/CMakeLists.txt#L441-L464) 的默认 `USE_RTTI=AUTO` 在 Release 加 `-fno-rtti`；初始 `flags.make` 确认实际采用该标志。应用自己加 `-frtti` 不能移除已经存在于 archive 内的符号。
2. 初始 archive 里的 `cache.cc.o` 等对象由 `nm -m` 检出 `weak private external __ZTISt9exception`；最小 `-fno-rtti` helper 同样生成该符号。对应 `-frtti` helper 只有 `undefined external __ZTISt9exception`。
3. 无库应用引用 libc++ 导出的 `std::exception` typeinfo；初始库链接后的最终 executable 内出现私有 `__ZTISt9exception`。所谓“被动链接”不能保证不拉入对象，本例 link map 实际拉入 273 个 archive 成员。初始库没有直接定义 filesystem_error，不应把现象误诊为它替换了 filesystem 实现。
4. `typeinfo_identity.cpp` 直接比较应用 `&typeid(std::exception)` 与 `dlopen("/usr/lib/libc++.1.dylib")` / `dlsym("_ZTISt9exception")`：初始库和 no-RTTI helper 情况下，**两边名字同为 `St9exception`，但地址不同且 `type_info::operator==` 也为 false**；无库、RTTI helper、新 RTTI 库情况下均地址相同、比较为 true。这与跨 libc++ 异常继承链的匹配失败一致。

因此本机已确认是混合 RTTI 编译下标准基类 typeinfo 可见性/身份冲突，不是业务异常类型错误，也不是双方 Apple clang 版本不同；没有据此宣称所有平台或所有 `-fno-rtti` 工程都会失败。捕获精确 filesystem_error、使用 filesystem error_code 重载可以局部规避，但不能根治整个进程的标准异常类型身份。

### 当前应使用的库

获授权后，沿用原固定源码与相同关闭项，只新增 **`-DUSE_RTTI=ON`**，以最多 `--parallel 4` 在新 `build-rtti` / `install-rtti` 中成功编译安装。原 `build` / `install` 不动，初始 archive 的 SHA-256 再次核对未变化。

| 项目 | 修复后的实际值 |
|---|---|
| Source commit | `17b624b5c0d41bd42db5cf88aa867f3b26eaf1fa`，源码工作树干净 |
| 构建目录 | `/tmp/tickshelf-oss-rocksdb-20261114/build-rtti` |
| 安装 prefix | `/tmp/tickshelf-oss-rocksdb-20261114/install-rtti` |
| **新的 RocksDB_DIR** | **`/tmp/tickshelf-oss-rocksdb-20261114/install-rtti/lib/cmake/rocksdb`** |
| Target | `RocksDB::rocksdb`，仍是 STATIC IMPORTED |
| Archive | `/tmp/tickshelf-oss-rocksdb-20261114/install-rtti/lib/librocksdb.a`，约 27 MiB |
| Include | `/tmp/tickshelf-oss-rocksdb-20261114/install-rtti/include` |
| Archive SHA-256 | `1508fc170461d924952d331c106bff0bb731af49b0eb46c1b691de6c7bfaf74b` |
| RTTI 编译证据 | `build-rtti/CMakeCache.txt:349 USE_RTTI:STRING=ON`；`CMakeFiles/rocksdb.dir/flags.make:11` 有 `-DROCKSDB_USE_RTTI`，**没有 `-fno-rtti`** |
| 同步路径证据 | `build-rtti/CMakeCache.txt:657 HAVE_FULLFSYNC:INTERNAL=1`；`flags.make:5` 仍有 `-DHAVE_FULLFSYNC` |

重建配置等于前述完整命令的 `-B` 改为 `build-rtti`、`CMAKE_INSTALL_PREFIX` 改为 `install-rtti` 并增加 `-DUSE_RTTI=ON`；不改源码、不启用额外依赖、不覆盖旧产物。新库的全部负路径异常小测通过，读不存在 DB 的 read-only Open 返回受检查的 IOError Status；没有运行正式性能。正式项目重新配置时必须将 `RocksDB_DIR` 切到新 prefix，重新链接，然后由主任务串行执行其回归/正式测量。

可复查证据文件：`checks/filesystem_exception_probe.cpp`、`no_rtti_helper.cpp`、`typeinfo_identity.cpp`、`probe-results.json`、`helper-results.json`、`identity-results.json`、`rtti-fixed-results.json` 和各链接 map，均相对于 `/tmp/tickshelf-oss-followup-20261114/`。结果 JSON 保存完整编译命令、退出码和输出，避免只凭口述判断修复。

## 补充：ClickHouse MergeTree / InfluxDB TSM-WAL 固定源码设计参考

**以下只审读关键源码，没有构建或运行 ClickHouse/InfluxDB 服务，没有执行性能测试，不得把这些设计证据写成实测吞吐。** 复用上文 RocksDB 的 WAL/数据文件/发布/回收分层，不重复构建依赖。公开 API 和 raw 文件通过本机代理成功取得；未 clone 两个大仓库，未执行下载代码或构建脚本。源文件分别放在新建隔离目录 `.../clickhouse/`、`.../influxdb/`；自行编写的下载脚本在独立 `.../scripts/`，从可信工作目录用 `python3 -I` 执行。

### 来源与取样范围

| 上游 | 固定正式 tag | peeled commit | API 发布时间 |
|---|---|---|---|
| [ClickHouse/ClickHouse](https://github.com/ClickHouse/ClickHouse/releases/tag/v26.10.4.207-stable) | `v26.10.4.207-stable` | `6b3d0460156cbe0bc106c65383848baa7c9e2f43` | `2026-11-13T22:19:54Z` |
| [influxdata/influxdb](https://github.com/influxdata/influxdb/releases/tag/v2.9.1) | **`v2.9.1`（2.x TSM 实现，不是 3.x）** | `d4fa1941fd4adb29a556e774de426dfbab72f346` | `2026-05-12T16:41:06Z` |

两者 release API 均为 `prerelease=false,draft=false`。ClickHouse annotated tag object 为 `2e884f184b0531d786d566b1bd2f4c8a2c57d9a3`，另行剥离到上表 commit；InfluxDB tag ref 直接指向 commit。这里选择官方可验证稳定版本作设计样本，不要求两者产品线相同，也不把 InfluxDB 2.x 误称整个上游的最新大版本。

成功取回的九个关键文件：ClickHouse 的 `MergeTreeDataWriter.cpp`、`MergeTreeSink.cpp`、`MergeTreeData.cpp`、`MergeTreeSettings.cpp`；InfluxDB 的 `tsdb/engine/tsm1/{engine.go,wal.go,file_store.go,compact.go,writer.go}`。目录前缀为 `/tmp/tickshelf-oss-followup-20261114/{clickhouse,influxdb}/`。每个实际文件的固定 raw URL、字节数、SHA-256 和本机路径保存在 `evidence/source-manifest.json` 与 `extra-source-manifest.json`。尝试两个旧的 part-storage 文件名得到 404，已如实记录，**未引用或声称审读这些不存在的路径**；以下引用均为实际下载并读取的文件。

### ClickHouse：临时 part → 完成写入/发布 → 延迟回收旧 part

- **先完成临时输出再提交**：[`MergeTreeSink.cpp:309-317`](https://github.com/ClickHouse/ClickHouse/blob/6b3d0460156cbe0bc106c65383848baa7c9e2f43/src/Storages/MergeTree/MergeTreeSink.cpp#L309-L317) 按顺序 `temp_part->finalize()`、storage `commitTransaction()`、`commitPart()`；[`459-460`](https://github.com/ClickHouse/ClickHouse/blob/6b3d0460156cbe0bc106c65383848baa7c9e2f43/src/Storages/MergeTree/MergeTreeSink.cpp#L459-L460) 在锁内 rename/add 后 transaction commit。不能仅因为“临时文件写完”就宣布新 part 已对查询发布。
- **校验先于重命名**：[`MergeTreeData.cpp:7587-7622`](https://github.com/ClickHouse/ClickHouse/blob/6b3d0460156cbe0bc106c65383848baa7c9e2f43/src/Storages/MergeTree/MergeTreeData.cpp#L7587-L7622) 在 Temporary 状态检查 partition、duplicate、intersect/covered parts、限制，之后才 prepare/rename。源码明确维护“非临时磁盘 part 必须在 data_parts 中”的不变量。
- **发布新/淘汰旧是独立状态转换**：[`MergeTreeData.cpp:12450-12463,12533-12577`](https://github.com/ClickHouse/ClickHouse/blob/6b3d0460156cbe0bc106c65383848baa7c9e2f43/src/Storages/MergeTree/MergeTreeData.cpp#L12450-L12577) 先提交底层 storage transaction，再处理覆盖关系；在 parts 锁下把旧 part 标 Outdated，把新 part 标 Active，而非直接删除旧文件。
- **读者和事务决定何时可回收**：[`MergeTreeData.cpp:4586-4646`](https://github.com/ClickHouse/ClickHouse/blob/6b3d0460156cbe0bc106c65383848baa7c9e2f43/src/Storages/MergeTree/MergeTreeData.cpp#L4586-L4646) 检查事务可移除、shared_ptr 唯一、delete-bitmap pin、old_parts_lifetime 等条件，之后才标 Deleting；还要求先移除被覆盖 part，再移除 covering empty part，防止旧数据复活。
- **同步不是默认等价强保证**：[`MergeTreeSettings.cpp:763-769`](https://github.com/ClickHouse/ClickHouse/blob/6b3d0460156cbe0bc106c65383848baa7c9e2f43/src/Storages/MergeTree/MergeTreeSettings.cpp#L763-L769) 中 `fsync_after_insert` 与 `fsync_part_directory` 默认都为 false。[`MergeTreeDataWriter.cpp:1097-1105,1224-1228`](https://github.com/ClickHouse/ClickHouse/blob/6b3d0460156cbe0bc106c65383848baa7c9e2f43/src/Storages/MergeTree/MergeTreeDataWriter.cpp#L1097-L1228) 分别将目录同步 guard/insert sync 选项传入写入路径。所以不能将默认 MergeTree 的提交语义直接等同本参考每批同步 WAL，更不能类推 replicated/quorum 或所有远端磁盘后端的掉电语义。

**可借鉴的顺序**：临时产物和索引/checksum 完成 → 按明确耐久等级完成文件/目录同步 → 在锁/事务保护下切换可见 generation → 旧 generation 仅标过时 → 等待读者/事务 pin 和回收条件 → 最后物理回收。tickshelf 若增加异步分段/压实，提交点应是“新版本已可恢复且已发布”，而不是开始回收旧缓存的时刻。上述是设计建议，不是已在 tickshelf 实施的声明。

### InfluxDB 2.x：WAL 确认 → snapshot TSM → FileStore 发布 → 清缓存/删 WAL

- **缓存写入不等于 WAL 确认**：[`engine.go:1387-1400`](https://github.com/influxdata/influxdb/blob/d4fa1941fd4adb29a556e774de426dfbab72f346/tsdb/engine/tsm1/engine.go#L1387-L1400) 先 Cache.WriteMulti，再在 WALEnabled 时 WAL.WriteMulti；并非“先持久化 WAL 再让任何缓存可见”的简单模型，不据此替它宣称跨 cache/WAL 的额外原子性。
- **WAL 返回等待 sync 结果**：[`wal.go:506-554`](https://github.com/influxdata/influxdb/blob/d4fa1941fd4adb29a556e774de426dfbab72f346/tsdb/engine/tsm1/wal.go#L506-L554) 写 segment、注册 sync waiter、scheduleSync，最终阻塞接收 syncErr；[`331-381`](https://github.com/influxdata/influxdb/blob/d4fa1941fd4adb29a556e774de426dfbab72f346/tsdb/engine/tsm1/wal.go#L331-L381) 可合并等待者后同步。[`1198-1204`](https://github.com/influxdata/influxdb/blob/d4fa1941fd4adb29a556e774de426dfbab72f346/tsdb/engine/tsm1/wal.go#L1198-L1204) 明确先 flush buffered writer，再对 os.File 调 `Sync()`。这只是 Go 文件 API 证据，没有构建其 Go runtime 或声称其 Mac 实际系统调用与 RocksDB F_FULLFSYNC 相同。
- **建立有边界的快照**：[`engine.go:1909-1956`](https://github.com/influxdata/influxdb/blob/d4fa1941fd4adb29a556e774de426dfbab72f346/tsdb/engine/tsm1/engine.go#L1909-L1956) 在 engine 锁内 close/roll WAL、取得 closed segments 和 Cache.Snapshot，锁外对 snapshot 去重后写出。不能快照整个缓存却随意删除不匹配的活跃 WAL。
- **提交/回收顺序在一个函数内清晰可见**：[`engine.go:2000-2040`](https://github.com/influxdata/influxdb/blob/d4fa1941fd4adb29a556e774de426dfbab72f346/tsdb/engine/tsm1/engine.go#L2000-L2040) 为 `Compactor.WriteSnapshot` → `FileStore.Replace(nil,newFiles)` → `Cache.ClearSnapshot(true)` → `WAL.Remove(closedFiles)`。写出/发布失败会保留可重试 snapshot（defer ClearSnapshot(false)）；FileStore Replace 失败尝试清理新临时文件；WAL 删除失败记录日志，不能误算为提交失败后丢弃全部重试来源。
- **发布新 TSM 与目录同步**：[`file_store.go:930-985`](https://github.com/influxdata/influxdb/blob/d4fa1941fd4adb29a556e774de426dfbab72f346/tsdb/engine/tsm1/file_store.go#L930-L985) 将 `.tsm.tmp` rename 成 `.tsm` 并打开 reader；[`992-1089`](https://github.com/influxdata/influxdb/blob/d4fa1941fd4adb29a556e774de426dfbab72f346/tsdb/engine/tsm1/file_store.go#L992-L1089) 在锁下调整文件集、对目录 `SyncDir`，然后发布 active readers。正在被查询使用的旧文件先改临时名，交给 purger 等待；不再使用的旧文件才 close/remove。[`writer.go:734-765`](https://github.com/influxdata/influxdb/blob/d4fa1941fd4adb29a556e774de426dfbab72f346/tsdb/engine/tsm1/writer.go#L734-L765) 的 TSM writer Close 先 Flush/sync wrapped writer，再关闭，`compact.go:1189-1201` 保留了 wrapped writer 的 Sync 接口。

**可借鉴的顺序**：WAL 同步成功才确认相应持久性等级 → 用确定的 WAL 边界冻结快照 → 临时 TSM/段写完并同步 → 持久化 rename/目录和发布文件集合 → 释放与快照对应的内存 → 最后删除准确覆盖的 closed WAL。失败时宁可保留可重放/可重试的数据，也不能先清 WAL/缓存再尝试提交。对 tickshelf 的 Redis/缓存回收，可借鉴“只有持久化提交和消费者覆盖条件都成立才回收”的顺序；本次**未操作共享 Redis，也未实施新回收协议**。

### 尚未验证的边界

这九个文件不是完整形式化证明：未审遍所有 ClickHouse storage backend、replication/quorum、跨 part 故障路径或 InfluxDB 整个 Go runtime/启动恢复逻辑；未构建两个服务，未运行它们的功能/崩溃测试或 benchmark。仅据可核验固定源码提出提交、发布、延迟回收的设计借鉴，不给跨引擎性能排名，也不以 fsync/Sync 返回成功替代硬件 PLP 验证。
