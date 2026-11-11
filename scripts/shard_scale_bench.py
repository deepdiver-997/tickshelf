#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
shard_scale_bench.py — 单机并行 SQLite 分片写的聚合吞吐 vs 分片数 N 规模曲线

目的
====
生产系统用多个 SQLite 文件做行情分片持久化，服务器实测约 8 个并行写最划算。
本脚本在本机（macOS / APFS）复现同款实验，画出"聚合吞吐 vs 分片数 N"曲线，
并自动判定拐点（再加分片聚合吞吐也不再增长的临界 N），作为与服务器曲线的对照。

方法与口径（与 src/libtickshelf/sqlite_store.cpp 生产实现逐项对齐）
================================================================
1. 模型：N 个 SQLite 文件 + N 个并发写者，每写者独占一个文件的连接，
   写者间无任何锁竞争（与生产"每分片一个写者"一致）。
2. Schema 与生产完全一致（sqlite_store.cpp kSchema，表名 ticks）：
   10 列，PRIMARY KEY(symbol, ts_exchange_ms) WITHOUT ROWID。
3. PRAGMA 与生产一致：journal_mode=WAL + busy_timeout=3000；
   不设置 synchronous（SQLite 默认 FULL，生产同样未改）。
4. 写入模式与生产 upsert_flush() 一致：
   BEGIN IMMEDIATE → executemany "INSERT OR IGNORE INTO ticks VALUES(?,...)" 500 行/批 → COMMIT。
5. 跨线程无重复键：每写者独占 symbol 前缀（S{tid:03d}-xx）与 ts 区间
   （base = tid * 1e13 起步），保证 INSERT OR IGNORE 全部命中为真插入。
6. 计时口径：并发起跑同步；每写者先跑 0.5 s 预热（不计），在 t=0.5 s 处
   记 conn.total_changes 基线，跑满 4.0 s 停止，统计窗口内 changes 差分。
   聚合 rows/s = Σrows / (max(写者结束时刻) − min(基线时刻))，即真实墙钟窗口。
7. N ∈ [1, 2, 4, 6, 8, 12, 16, 24]，每个 N 全新临时目录（含 -wal/-shm 跑完清理）。
8. 拐点判定：找最小的 N*，使得从 N*-1 → N* 的聚合吞吐增长 ≤5%，且其后所有
   实测点的增长均 ≤5%（"不再增长 >5%"）。若测试范围内始终 >5% 增长则报告无拐点。

两种并发载体（--mode，默认 thread）
==================================
thread（默认，任务口径）: N 个 Python 线程，每线程独占一个文件连接。
proc:                    N 个独立 fork 子进程，每进程独占一个文件连接；
                         其余口径（schema/PRAGMA/批量/计时/清理）与 thread 完全相同。

已知局限（重要——读数前必看）
============================
CPython 的 GIL 使多线程在 Python 字节码层只能串行；sqlite3 虽在 C 层放锁，
但 executemany 逐行的 GIL 交接在多线程下形成 convoy 效应。本机实测 thread 模式
聚合吞吐随 N 单调塌缩（N=1 即峰值，N≥8 后地板 ≈ 4 万 rows/s），且对照组
（thread + synchronous=OFF）同样塌缩 → 瓶颈是解释器锁而非磁盘 fsync。
thread 模式曲线度量的是 Python 解释器，不是 SQLite 分片写本身的规模特性。
生产写者是 C++（无 GIL），要与服务器曲线对照请用：
    python3 -I scripts/shard_scale_bench.py --mode proc
proc 模式可复现服务器同款形状（本机实测：N=8 聚合 ≈ 190 万 rows/s，拐点 N≈12，
N=24 因超订阅反而回落）。两模式仅并发载体不同，其余完全一致。

运行
====
    python3 -I scripts/shard_scale_bench.py             # thread 模式（任务口径）
    python3 -I scripts/shard_scale_bench.py --mode proc # proc 模式（生产同构对照）

仅用标准库（sqlite3/threading/multiprocessing/tempfile/...），无第三方依赖。
单次完整运行 ≈ 8 × 4.5 s ≈ 40 s。
"""

import argparse
import multiprocessing
import os
import platform
import shutil
import sqlite3
import sys
import tempfile
import threading
import time

# ---------------------------------------------------------------------------
# 可调参数（按任务口径固定）
# ---------------------------------------------------------------------------
N_LIST = [1, 2, 4, 6, 8, 12, 16, 24]   # 分片数取值
WARMUP_S = 0.5                          # 预热时长（不计入统计）
MEASURE_S = 4.0                         # 总时长（含预热），有效统计窗口 ≈ 3.5 s
BATCH_ROWS = 500                        # 每事务批量行数
SYMBOLS_PER_THREAD = 10                 # 每写者模拟的 symbol 数（键空间内连续追加）
INFLECT_THRESH = 0.05                   # 拐点阈值：增长 ≤ 5% 视为"不再增长"

SCHEMA = """
CREATE TABLE IF NOT EXISTS ticks(
  symbol         TEXT    NOT NULL,
  ts_exchange_ms INTEGER NOT NULL,
  ts_receive_ms  INTEGER NOT NULL,
  bid_price      INTEGER NOT NULL,
  ask_price      INTEGER NOT NULL,
  last_price     INTEGER NOT NULL,
  bid_size       INTEGER NOT NULL,
  ask_size       INTEGER NOT NULL,
  last_size      INTEGER NOT NULL,
  flags          INTEGER NOT NULL,
  PRIMARY KEY(symbol, ts_exchange_ms)
) WITHOUT ROWID"""

INSERT_SQL = "INSERT OR IGNORE INTO ticks VALUES(?,?,?,?,?,?,?,?,?,?)"


def build_batch(tid, batch_idx):
    """构造一批 500 行、键必唯一的行情行。

    ts 按 k*10 + (k % NSYM) 推进：相邻 k 的 ts 区间不相交 → (symbol, ts) 全唯一，
    且每个 symbol 的键严格递增（B-tree 顺序追加，无随机写劣化）。
    每写者独立 symbol 前缀 + 独立 ts 区间 → 跨写者零重复。
    """
    nsym = SYMBOLS_PER_THREAD
    ts_base = 1_700_000_000_000_000 + tid * 10_000_000_000_000
    start = ts_base + batch_idx * BATCH_ROWS * 10
    syms = [f"S{tid:03d}-{j:02d}" for j in range(nsym)]
    rows = []
    append = rows.append
    for k in range(BATCH_ROWS):
        ts = start + k * 10 + (k % nsym)
        px = 100_000 + (k & 1023)
        append((syms[k % nsym], ts, ts + 1, px, px + 100, px + 50,
                10 + (k % 90), 12 + (k % 77), 5 + (k % 33), k & 7))
    return rows


def run_workload(tid, db_path):
    """单写者主体：独占一个分片文件，预热后计时写满 MEASURE_S 秒。

    返回 (窗口内插入行数, 基线时刻, 结束时刻)，时刻以本写者 t0 为原点。
    线程模式与进程模式共用此函数，保证两条曲线口径一致。
    """
    conn = None
    try:
        conn = sqlite3.connect(db_path, isolation_level=None)  # 手动控制事务
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=3000")
        conn.execute(SCHEMA)

        t0 = time.perf_counter()
        base_changes = conn.total_changes
        t_base = None
        batch_idx = 0
        while True:
            now = time.perf_counter() - t0
            if t_base is None and now >= WARMUP_S:
                t_base = now                 # 预热结束，记基线
                base_changes = conn.total_changes
            if now >= MEASURE_S:
                break
            rows = build_batch(tid, batch_idx)   # 批数据在事务外构造（与生产一致）
            conn.execute("BEGIN IMMEDIATE")
            conn.executemany(INSERT_SQL, rows)
            conn.execute("COMMIT")
            batch_idx += 1

        t_end = time.perf_counter() - t0
        return (conn.total_changes - base_changes, t_base, t_end)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _thread_worker(tid, db_path, barrier, errors, out):
    """thread 模式的线程体：barrier 对齐起跑，异常如实上报不吞。"""
    try:
        barrier.wait()                       # 全部线程同一起跑线
        out[tid] = run_workload(tid, db_path)
    except Exception as exc:
        errors.append((tid, type(exc).__name__, str(exc)))


def _proc_child(tid, db_path, q):
    """proc 模式的子进程体：fork 后独立运行，结果经队列回传。"""
    try:
        q.put((tid, "ok") + run_workload(tid, db_path))
    except Exception as exc:
        q.put((tid, "err", type(exc).__name__, str(exc), 0.0, 0.0, 0.0))


def _aggregate(results):
    """results: list of (rows, t_base, t_end)。返回 (总行数, 聚合rows/s, 各写者rows/s)。"""
    rows_total = sum(r[0] for r in results)
    window = max(r[2] for r in results) - min(r[1] for r in results)
    agg = rows_total / window if window > 0 else 0.0
    per = [r[0] / (r[2] - r[1]) if r[2] > r[1] else 0.0 for r in results]
    return rows_total, agg, per


def run_one_config(n, mode, parent_dir):
    """测单个分片数 N，返回 (N, 聚合rows/s, 各写者rows/s, 错误列表)。"""
    shard_dir = tempfile.mkdtemp(prefix=f"shard{mode[:1]}{n:02d}_", dir=parent_dir)
    paths = [os.path.join(shard_dir, f"shard_{i:02d}.db") for i in range(n)]
    errors, results = [], []
    try:
        if mode == "thread":
            barrier = threading.Barrier(n)
            out = {}
            threads = [threading.Thread(target=_thread_worker,
                                        args=(i, paths[i], barrier, errors, out),
                                        daemon=True)
                       for i in range(n)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            results = [out[i] for i in range(n) if i in out]
        else:  # proc
            ctx = multiprocessing.get_context("fork")
            q = ctx.Queue()
            procs = [ctx.Process(target=_proc_child, args=(i, paths[i], q), daemon=True)
                     for i in range(n)]
            for p in procs:
                p.start()
            for _ in range(n):               # 带超时收取，防止僵死拖垮总时长
                try:
                    msg = q.get(timeout=MEASURE_S * 3)
                    if msg[1] == "ok":
                        results.append(msg[2:5])
                    else:
                        errors.append((msg[0], msg[2], msg[3]))
                except Exception:
                    pass
            for p in procs:
                p.join(timeout=2.0)
                if p.is_alive():
                    p.terminate()
                if p.exitcode not in (0, None):
                    errors.append((p.pid, "ChildExit", f"exitcode={p.exitcode}"))
    finally:
        shutil.rmtree(shard_dir, ignore_errors=True)   # 含 -wal / -shm

    if errors or len(results) != n:
        return n, None, [], errors or [("?", "MissingResult",
                                        f"只完成 {len(results)}/{n} 个写者")]
    _, agg, per = _aggregate(results)
    return n, agg, per, []


def print_report(results, all_errors, mode):
    if not results:
        print("无成功配置，无法出表。")
        return 1
    base = results[0][1]
    print(f"\n[{mode} 模式] {'N':>3} | {'聚合 rows/s':>15} | {'加速比':>7} | "
          f"{'每写者 rows/s':>14} | {'较上一档增长':>10}")
    print("-" * 70)
    prev = None
    for n, agg in results:
        speedup = agg / base if base else 0.0
        growth = f"{(agg - prev) / prev * 100:+7.1f}%" if prev else "    --"
        print(f"{'':>{len(mode)+9}}{n:>4} | {agg:>15,.0f} | {speedup:>6.2f}x | "
              f"{agg / n:>14,.0f} | {growth:>9}")
        prev = agg

    # ---- 拐点判定：最小的 N*，其后所有档位增长均 ≤5% ----
    inflection = None
    for i in range(1, len(results)):
        g = (results[i][1] - results[i - 1][1]) / results[i - 1][1]
        if g <= INFLECT_THRESH and all(
                (results[j][1] - results[j - 1][1]) / results[j - 1][1] <= INFLECT_THRESH
                for j in range(i + 1, len(results))):
            inflection = results[i][0]
            break
    if inflection is not None:
        best_n, peak = max(results, key=lambda r: r[1])
        print(f"\n[{mode}] 拐点: N={inflection}（此后聚合吞吐增长均 ≤5%）；"
              f"峰值在 N={best_n}（{peak:,.0f} rows/s）")
    else:
        print(f"\n[{mode}] 拐点: 测试范围内未出现（吞吐始终 >5% 增长）——可加大 N 继续探测")

    if all_errors:
        print(f"\n[{mode}][异常] 以下写者出错（未美化，原样上报）:")
        for n, errs in all_errors.items():
            for e in errs:
                print(f"  N={n}: {e}")
        return 2
    return 0


def main():
    ap = argparse.ArgumentParser(description="并行 SQLite 分片写规模曲线基准")
    ap.add_argument("--mode", choices=("thread", "proc"), default="thread",
                    help="thread=Python 线程（默认，任务口径）；proc=独立进程（生产同构对照）")
    args = ap.parse_args()

    py = sys.version.split()[0]
    print(f"环境: Python {py} / sqlite3 {sqlite3.sqlite_version} / "
          f"{platform.system()} {platform.release()} ({platform.machine()}) "
          f"cpu={os.cpu_count()}  模式={args.mode}")
    print(f"口径: 每批 {BATCH_ROWS} 行 BEGIN IMMEDIATE..COMMIT, WAL+busy_timeout=3000"
          f"(synchronous 默认 FULL), 预热 {WARMUP_S}s, 总时长 {MEASURE_S}s, "
          f"N={N_LIST}\n")

    parent = tempfile.mkdtemp(prefix="shard_scale_bench_")
    results, all_errors = [], {}
    try:
        for n in N_LIST:
            t_cfg = time.perf_counter()
            n_, agg, per, errs = run_one_config(n, args.mode, parent)
            wall = time.perf_counter() - t_cfg
            if agg is None:
                print(f"N={n:3d}  失败: {errs}")
                all_errors[n] = errs
                continue
            results.append((n_, agg))
            spread = (max(per) - min(per)) / max(per) * 100 if per and max(per) else 0.0
            print(f"N={n_:3d}  聚合 {agg:>12,.0f} rows/s   "
                  f"(写者间离散度 {spread:4.1f}%, 本档耗时 {wall:.1f}s)")
    finally:
        shutil.rmtree(parent, ignore_errors=True)

    sys.exit(print_report(results, all_errors, args.mode))


if __name__ == "__main__":
    main()
