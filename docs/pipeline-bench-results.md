# 五档 JSON 全链路诊断结果

## 结论与边界

本次确实运行了原始 JSON → TCP → simdjson → 自定义二进制 Record → SHA-256/内容查重 → 私有 Redis → 耐久后端；另测了无 TCP 的文件读取前处理。**没有访问任何生产部署环境、真实行情账号或目标硬件，不能把结果当作既有生产实现的复现。**

最重要的发现：

1. **当前本机原 `SqliteTickStore` 连接实际是 WAL + NORMAL，不是 FULL。** 只读 `connection_info()` / `pipeline_sqlite_probe` 在同一连接上回读，运行期 SQLite 3.43.2，`synchronous=1`、`fullfsync=0`、`checkpoint_fullfsync=1`。生产默认行为未修改。此前未验证同步级别的高吞吐数字不能作为 FULL 耐久容量承诺；其他服务器要重新读实际值。
2. **短跑约 3.1 万条/s，不代表能持续承接 3 万条/s。** 5.4 万条/格的诊断矩阵里 SQLite FULL 中位数 31,109；扩大到每格 30 万条，完整路径完成吞吐降到约 16,220，定速验收没有通过。所有内容完整落库，但发送端被反压拖慢，实际 offered rate 没达到要求。
3. **消融与同轮栈证据一致指向 SQLite 写入/提交/自动 checkpoint，而非 JSON 或 SHA。** 将 checkpoint 阈值由 1000 提到 10000 页提高到约 22,711 条/s，但仍未通过 3 万目标；不能把调参写成已经解决。
4. **追加文件 + SQLite FULL 稀疏索引在三轮 30k/s、10 秒定速观察中通过。** 这只是该合成模型/本机的有限观察。原型包含文件同步、索引提交、symbol/time 范围索引和读回验证，不是只测 `write()`。它与本文全记录 SQLite 的索引物理布局不同，不能推出“文件总比合理设计的 SQLite 快”。

## 环境与记录

- macOS 15.8.1、arm64、Mac14,9、12 核、16 GiB，loopback TCP；不是目标部署硬件。
- Release：`-O3 -DNDEBUG`，实验目标另保留 `-g -fno-omit-frame-pointer`，采样使用同一优化二进制。实际 target 编译命令、库链接和 SHA-256 均留档。
- simdjson 5.0.5、OpenSSL Crypto 4.1.0；SQLite 实际动态运行版本 3.43.2，不用 CMake 发现的 3.51.0 头文件版本代替运行期版本。
- 基线：54 个 symbol、五档、12 个官方 quoter 标量字段、0 重复；源帧/hash/Redis 批各 256，事务上限 1000 条/1 MiB/100ms，WAL、显式 FULL、autocheckpoint=1000、statement 跨事务复用。
- canonical Record 每条 **291B**；这组 JSON 的平均 wire 大小含帧头约 **448.34B/条**。30k/s 的用户载荷仅约 8.73MB/s，但这不能替代真实 WAL/索引写放大、同步和 checkpoint 观测。
- 公开 iQuant 归档手册明确 quoter 的四个五档数组和 12 个标量；JSON ticks 包装、网络 IQB1、二进制 RCD1 为实验定义。字段来源和边界见 [sources](../tests/fixtures/iquant/sources.md)、[coverage](../tests/fixtures/iquant/coverage.md)。

## 三轮诊断矩阵：每格 54,000 条

中位数及 min–max，计时从接收首帧到 store finish；包含排空。它是有界突发，不是稳态容量证明。各个消融保持逻辑输入和 oracle 不变；生成源/二进制入口的变化另有标签，不能直接相减当精确阶段成本。

| 配置 | 完成条/s 中位数 | min–max | 解释 |
|---|---:|---:|---|
| 全链 JSON + SQLite FULL | 31,109 | 30,698–31,614 | 基线 |
| 预解析 Record 重放 | 32,405 | 31,690–32,538 | 仍有 payload 比对、Record 拷贝和编码 |
| 旁路 hash/查重 | 32,398 | 32,327–32,582 | 最终唯一内容对账不变 |
| Redis 仅编码消费 | 33,221 | 33,218–33,682 | 不是持久化短路 |
| 完全旁路 Redis | 34,316 | 32,826–34,505 | 保留真实 SQLite FULL |
| 事务上限 16 条 | 11,737 | 11,731–11,837 | 小事务明显放大边界/同步成本 |
| 附加索引/元数据重事务 | 26,408 | 26,249–26,560 | 明确实际 SQL，不用 sleep 模拟 |
| 追加文件 + FULL 稀疏索引 | 116,164 | 113,148–117,528 | 物理布局改变，须结合下述限制 |
| 二进制入口 | 33,133 | 33,130–33,355 | 不代表已覆盖原始 JSON 解析 |
| C++ 源实际构造/序列化 JSON | 30,795 | 30,282–31,175 | 实际执行 source_generate/source_json，非文件伪装 |

另有 null 消费格，约 13 万条/s 量级，但它没有耐久写入，**绝不列为持久化性能**。所有诊断格完整内容对账通过。

### 文件前处理独立轨道

同一套独立生成的 54,000 条文件，无 TCP/source/Redis，使用相同 codec、hash、null checksum；每格三轮。这是读取 + 帧 CRC + 解码/规范化 + canonical 编码 + hash/可选查重 + 消费，不是裸磁盘或纯 hash 微基准。

| 输入与处理 | 条/s 中位数 | min–max |
|---|---:|---:|
| 原始 JSON + SHA-256，仅算 hash | 221,958 | 219,166–222,439 |
| 原始 JSON + SHA-256 + 查重 | 204,778 | 197,949–207,120 |
| 二进制 Record + SHA-256，仅算 hash | 369,087 | 365,028–371,837 |
| 二进制 Record + SHA-256 + 查重 | 322,936 | 320,163–323,737 |

这些数字不能证明既有生产前处理应有相同速度；它们说明本实验没有将“五档/变长/hash”设成人为 4 万条/s 的固有天花板。生产环境需用同样分段方法核实文件读、分配、数值转换、摘要和查重状态各自成本。

## 30k/s 定速验收：每格 300,000 条

| 后端/配置 | 三轮完成吞吐中位数 | 三轮结论 |
|---|---:|---|
| 全记录 SQLite FULL，1000 页 autocheckpoint | 16,220/s | **未通过**：实际发送约 16.4–16.8k/s，`inconclusive-offer-not-achieved` |
| 相同 SQLite，早期同轮采样开启 | 16,134/s | **未通过**：同样受到背压；不能与通过混称 PASS |
| SQLite FULL，10000 页 autocheckpoint | 22,711/s | **未通过**：实际发送约 22.8–23.6k/s |
| 追加文件 + SQLite FULL symbol/time 稀疏索引 | 约 29,991/s（受 30k 定速限制） | **三轮均通过有限窗口验收** |

这里的未通过并非丢数据：各格 canonical 内容、源/接收/提交/ACK 计数均通过；runner 总退出码仍为 1，因为容量目标没有满足。不能仅凭 source 没达到 offered 就认定接收端极限，需联合以下现场证据。文件后端每轮 source 约 30,013/s，最终 ACK 尾部约 7–10ms，source-to-commit 积压斜率在验收界内；该尾部不是逐记录 P99 延迟。

同轮采样开/关组的完成吞吐中位数相差约 -0.5%，波动区间重叠；这不是采样绝对零开销的证明，也不是统计显著性结论。

## 同一轮现场归因

代表性 SQLite FULL、300,000 条、未采样的一轮：

- SQLite COMMIT 累计摊销约 **46.54µs/输入条**，占该轮总接收完成 wall-time 约 **75%**；store_total 约 53.52µs/条。
- JSON 解析/规范化约 **1.59µs/条**，SHA 计算约 **0.243µs/条**，查重约 **0.681µs/条**。这些 timer 是 wall-time，不能当 CPU 百分比；嵌套项不得相加。
- 源 CPU 约 **0.024 核**，源 send 等待约占源总耗时 **90.5%**；receiver 约 **0.576 核**。源没有因 JSON 生成耗尽单核，低整机 CPU 不等于链路有空余吞吐。
- 实际 424 个写事务，单次 COMMIT 最大约 70.45ms；不能将整个 COMMIT 都叫作 fsync。

早期同轮 `sample` 明确出现：

```text
SqliteStore::write → sqlite3_exec/COMMIT
  → sqlite3WalDefaultHook → sqlite3_wal_checkpoint_v2
  → sqlite3WalCheckpoint → unixSync/fcntl、unixWrite/pwrite
```

进一步用真实低完成率触发而非固定定时器：6 秒 offered-window、180,000 条的一轮，在测量 **3.112 秒**，连续两次不同进度窗口的完成率低于 **27,000/s**（最近约 20,402/s）时自动启动，记录 `sustained-low-completion-rate`，采样工具退出码 0。它捕获的是**同一仍在运行的进程**，不是事后重新复现。

该触发采样中主线程 168 个 wall-stack 样本：COMMIT 子树 116，自动 checkpoint 子树 92，其中可见同步/文件写入。类别嵌套且包含等待，**不能加总，也不能称为 CPU 占比**。阈值提高到 10000 页有约 1.40× 的完成吞吐改善，与 checkpoint 敏感性相符，但仍达不到目标；不能由此断言仅剩磁盘或推导目标磁盘的延迟。

## 优化建议的优先级

1. 先取实际连接的 journal/synchronous、每事务 rows/bytes、提交次数和 COMMIT 分布；保留 FULL，先摊薄小事务。不要通过 NORMAL/OFF 偷换目标。
2. 将解析、规范化、hash 与查重移出写锁持有区间；查重批调用仍须保留逐事件身份和碰撞确认。查看 SHA 字节量，不把“批接口”误说成只计算一次整个批摘要。
3. 对 checkpoint、脏页和索引布局作变量控制。**本实验全记录表以完整 BLOB 为主键**，二级索引复制宽主键，确有写放大；紧凑 row-ID/摘要索引尚未在本次比较中实测。既有生产 schema 不同，不能把 3.7× 的短跑文件收益全部归因为文件本身。
4. 文件原型已含数据 sync → FULL 索引 commit → ACK，以及恢复和范围读验证；迁移生产仍需评估范围读放大、孤儿段回收、分段/轮换、运维和真实掉电。不是把结构体随手 append 后就宣布安全。
5. 不按 CPU 逻辑核数决定开几十个 SQLite writer。目标磁盘的同步和写放大不会按核心数线性扩展；先用本工具在真正目标路径跑矩阵，再决定是否流水化/分片。

## 复跑与证据

完整参数和安全限制见 [pipeline-benchmark.md](pipeline-benchmark.md)。简要命令（结果目录必须不存在）：

```sh
python3 scripts/pipeline_bench.py --build build-bench-release \
  --suite diagnostic --records 54000 --repeat 3 --max-disk-mib 4096 \
  --output /tmp/my-new-diagnostic

python3 scripts/pipeline_bench.py --build build-bench-release \
  --suite target --rate 30000 --duration 10 --repeat 3 --compare-profiles \
  --max-disk-mib 8192 --output /tmp/my-new-target

# 仍在运行时按低吞吐/积压/新同步长尾自动采样
python3 scripts/pipeline_bench.py --build build-bench-release \
  --suite target --rate 30000 --duration 10 --profile trigger \
  --output /tmp/my-new-live-trigger
```

目标机器使用 `--work-dir` 指定目标文件系统上的已有专用父目录，先检查空间、挂载和实际 SQLite 默认值。10 分钟长跑命令已给出，但本次未执行；Linux perf/fdatasync 分支和物理断电均未实测。

精简可审计结果与每轮阶段/环境/配置见 [pipeline-bench-results.json](pipeline-bench-results.json)。原始数据、日志、完整进度、数据库、导出及采样均保留在：

- `/tmp/tickshelf-pipeline-measure-20261113`：33 格正式诊断 + 1 格预热。
- `/tmp/tickshelf-pipeline-target-20261113`：六格目标/采样对照 + 预热；目标失败保留。
- `/tmp/tickshelf-pipeline-target-segment-20261113`：三格文件后端目标观察 + 预热。
- `/tmp/tickshelf-pipeline-target-checkpoint-20261113`：三格 checkpoint 对照 + 预热；目标失败保留。
- `/tmp/tickshelf-pipeline-trigger-20261113`：真实异常触发现场；目标失败保留。
- `/tmp/tickshelf-pipeline-local-20261113`：12 格文件前处理消费对照。

诊断与主要定速实验回读的二进制 SHA-256 均为 `65e259659f046ab39f3dcc0038f39df9d72be6e14806fa35c10276c38fb05753`，与交付时 Release pipeline_bench 一致；新增只读探针未改变该实验二进制或生产默认。脚本/矩阵的实际版本以各轮环境与展开日程为准。`/tmp` 不是长期归档承诺，需长期保留原始 GB 级现场时另行归档；仓库内结果 JSON 已保留各轮配置、计数、阶段、环境及原始位置。

## 交付验收

- Release/Debug：新增实验各 42 用例、6981 断言；既有离线各 75 用例、33564 断言，全部通过。
- ASan+UBSan：42 个实验用例和 32 个 Python 驱动的真实实验程序测试通过；未启用 LeakSanitizer。
- Python：完整实验/oracle/隔离集成 32 项、异常触发与自定义矩阵纯测试 8 项、既有私有四服务 runtime 回归 10 项通过。
- 自定义 JSON axes 矩阵实际跑通 8 格 + 1 格预热，每格 108 条，全字段对账；这是工具验收，不列为性能结果。
- 默认 `TICKSHELF_BUILD_BENCHMARKS=OFF` 的服务构建通过，不引入 simdjson/OpenSSL 实验依赖。
- 未运行 8 个共享 Redis 既有用例，未接真实账号、未做物理断电、未在目标 Linux 环境验证。构建仍保留既有 `tests/test_write_through.cpp:40` 的 unused get_str 警告，新增代码无对应警告；未提交或推送。
