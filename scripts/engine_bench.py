#!/usr/bin/env python3
"""Indicative old-flush vs engine write-through comparison (private Redis only).

Not a capacity certification: one short synthetic run per mode, same host, same
SQLite shards, same floodgen offer. Reports durable rows / wall time and the
pending count at stop. Use it to decide whether the engine direction is worth
profiling further, not to claim production numbers.

Run: python3 scripts/engine_bench.py --build build-engine-release
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from stack_runtime import (OwnedProcesses, candidate_ports, start_private_redis,
                           wait_until)
from test_engine_runtime import db_rows

ROOT = Path(__file__).resolve().parent.parent
BUILD = ROOT / "build"


def run_case(build: Path, engine: bool, *, duration: float, rate: int, senders: int,
             shards: int) -> dict:
    with tempfile.TemporaryDirectory(prefix="tickshelf-engine-bench-") as temporary:
        directory = Path(temporary)
        processes = OwnedProcesses(directory / "proc")
        redis_port, daemon_port = candidate_ports(2)
        try:
            start_private_redis(processes, shutil.which("redis-server"), redis_port)
            db = directory / "ticks.db"
            config = directory / "tickshelf.ini"
            engine_lines = ""
            if engine:
                engine_lines = (
                    "engine = true\n"
                    "engine_batch_records = 4096\n"
                    "engine_batch_ms = 100\n"
                    "engine_inflight = 8\n"
                    "engine_max_pending_records = 65536\n"
                    "engine_lease_ttl_s = 60\n"
                    "engine_shutdown_ms = 15000\n"
                )
            config.write_text(
                "[redis]\n"
                "host = 127.0.0.1\n"
                f"port = {redis_port}\n"
                "[storage]\n"
                f"db = {db}\n"
                f"shards = {shards}\n"
                "[tickshelfd]\n"
                "bind = 127.0.0.1\n"
                f"port = {daemon_port}\n"
                "persist = sqlite\n"
                "flush_interval_ms = 100\n"
                f"{engine_lines}"
            )
            processes.start("tickshelfd", [build / "tickshelfd", "--config", config])
            wait_until(lambda: "listening " in processes.text("tickshelfd"),
                       timeout=10, description="tickshelfd listening",
                       processes=processes, names=("tickshelfd",))

            logs = []
            children = []
            started = time.monotonic()
            for index in range(senders):
                log_path = directory / f"flood-{index}.log"
                log = log_path.open("wb")
                logs.append((log_path, log))
                children.append(subprocess.Popen(
                    [str(build / "floodgen"), "--host", "127.0.0.1", "--port",
                     str(daemon_port), "--symbols", "64",
                     "--sym-base", str((index + 1) * 100000), "--rate", str(rate),
                     "--duration", str(duration)],
                    stdout=log, stderr=subprocess.STDOUT))
            for child in children:
                child.wait(timeout=duration + 30)
            for _, log in logs:
                log.close()

            sent = 0
            for log_path, _ in logs:
                for line in log_path.read_text(errors="replace").splitlines():
                    if line.startswith("SENT "):
                        sent += int(line.split()[1])

            # Completion is defined by actual SQLite rows, not the 2s stats
            # publish cadence (which aliases a 2s run into 4s).
            deadline = time.monotonic() + 30
            completed = False
            last_total = -1
            stable_since = time.monotonic()
            total = distinct = 0
            while time.monotonic() < deadline:
                total, distinct = db_rows(db, shards)
                if total >= sent and distinct == total:
                    completed = True
                    break
                if total == last_total:
                    if time.monotonic() - stable_since > 2:
                        break
                else:
                    last_total = total
                    stable_since = time.monotonic()
                time.sleep(0.1)
            completed_at = time.monotonic()

            rc = processes.stop("tickshelfd", timeout=30)
            elapsed = completed_at - started
            total, distinct = db_rows(db, shards)
            return {
                "mode": "engine" if engine else "old-flush",
                "status": "ok" if rc == 0 else f"exit-{rc}",
                "completed": completed,
                "offered_records_per_s": rate * senders,
                "duration_s": duration,
                "floodgen_sent": sent,
                "sqlite_rows": total,
                "sqlite_distinct_keys": distinct,
                "wall_s": round(elapsed, 3),
                "durable_equivalent_per_s": round(total / elapsed, 1) if elapsed else None,
            }
        finally:
            processes.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", type=Path, default=BUILD)
    parser.add_argument("--duration", type=float, default=8)
    parser.add_argument("--rate", type=int, default=20000, help="per floodgen process")
    parser.add_argument("--senders", type=int, default=4)
    parser.add_argument("--shards", type=int, default=4)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--json", type=Path, help="write all cases as one JSON array")
    args = parser.parse_args()
    if not shutil.which("redis-server"):
        raise SystemExit("redis-server not installed")
    for path in ("tickshelfd", "floodgen"):
        if not (args.build / path).is_file():
            raise SystemExit(f"missing {args.build / path}")

    results = []
    for repeat in range(args.repeat):
        for engine in (False, True):
            case = run_case(args.build, engine, duration=args.duration, rate=args.rate,
                            senders=args.senders, shards=args.shards)
            case["repeat"] = repeat + 1
            results.append(case)
            print(json.dumps(case, ensure_ascii=False))
    if args.json:
        args.json.write_text(json.dumps(results, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    main()
