#!/usr/bin/env python3
"""Bounded synthetic JSON -> TTAP -> private Redis -> SQLite benchmark.

The optional source lives outside tickshelf. No actual iFinD SDK/account/data is
used. Source-throughput and sampled SQLite visibility are not vendor throughput
or power-loss durability. All logs/results are retained in a fresh run directory.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import platform
import re
import shutil
import socket
import sqlite3
import struct
import sys
import tempfile
import time
import zlib
from pathlib import Path

from stack_runtime import (OwnedProcesses, RedisConnection, RunError, StopFlag,
                           candidate_ports, info_fields, start_private_redis,
                           start_stack)

BASE_TS = 1700000000000
SEED = 1
HEADER = struct.Struct("<4sBBHHHII")
TICK = struct.Struct("<8sQQqqqIIIB3x")


def shard_for(symbol, shards):
    # Match the repository's persisted routing constants, not a library hash.
    value = 1469598103934665603
    for byte in symbol.encode("ascii"):
        value = ((value ^ byte) * 1099511628211) & 0xffffffffffffffff
    return value % shards


def database_paths(base, shards):
    return [base] if shards == 1 else [Path(f"{base}.{i}") for i in range(shards)]


def connect_readonly(path):
    return sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True,
                           timeout=0.5, isolation_level=None)


def write_config(path, ports, mode, shards, flush_ms=100, poll_ms=100):
    redis, ingest, query = ports
    path.write_text(f"""# Generated for this isolated synthetic run; no credentials.
[redis]
host = 127.0.0.1
port = {redis}
[storage]
db = ticks.db
shards = {shards}
[tickshelfd]
bind = 127.0.0.1
port = {ingest}
flush_interval_ms = {flush_ms}
persist = {mode}
[persister]
poll_ms = {poll_ms}
[evictor]
soft_watermark_bytes = 134217728
sample_ms = 100
[queryd]
bind = 127.0.0.1
port = {query}
""")


def percentile(values, p):
    return values[max(0, math.ceil(len(values) * p) - 1)] if values else None


class Observer:
    def __init__(self, db, shards, redis_port, interval_ms):
        self.connections = [connect_readonly(path) for path in database_paths(db, shards)]
        self.sample = self.connections[shard_for("600000", shards)]
        self.redis = RedisConnection("127.0.0.1", redis_port)
        self.seen = set()
        self.latencies = []
        self.interval_ms = interval_ms
        self.last_poll = None
        self.max_gap_ms = 0.0
        self.max_query_ms = 0.0
        self.peak_redis_bytes = 0
        self.samples = []

    def observe(self):
        before = time.monotonic()
        if self.last_poll is not None:
            self.max_gap_ms = max(self.max_gap_ms, (before - self.last_poll) * 1000)
        self.last_poll = before
        # Pending batches may persist out of exchange-time order. A high-water
        # cursor would miss late earlier rows, so retain the bounded sample set.
        rows = self.sample.execute(
            "SELECT ts_exchange_ms,ts_receive_ms FROM ticks "
            "WHERE symbol=? AND ts_exchange_ms>=? ORDER BY ts_exchange_ms",
            ("600000", BASE_TS)).fetchall()
        wall_ms = time.time_ns() / 1000000
        self.max_query_ms = max(self.max_query_ms, (time.monotonic() - before) * 1000)
        for exchange, receive in rows:
            if exchange in self.seen:
                continue
            lag = wall_ms - receive
            if lag < 0:
                raise RunError("wall clock moved backwards; visibility metric invalid")
            self.seen.add(exchange)
            self.latencies.append(lag)
            self.samples.append({"exchange_ms": exchange, "receive_ms": receive,
                                 "first_observed_wall_ms": wall_ms,
                                 "observed_visibility_ms": lag})
        memory = info_fields(self.redis.command("INFO", "memory"))
        self.peak_redis_bytes = max(self.peak_redis_bytes, int(memory["used_memory"]))

    def count(self):
        return sum(conn.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]
                   for conn in self.connections)

    def summary(self):
        values = sorted(self.latencies)
        return {"symbol": "600000", "samples": len(values),
                "poll_interval_ms": self.interval_ms,
                "max_actual_poll_gap_ms": self.max_gap_ms,
                "max_query_ms": self.max_query_ms,
                "p50_ms": percentile(values, .50), "p95_ms": percentile(values, .95),
                "p99_ms": percentile(values, .99), "max_ms": values[-1] if values else None,
                "note": "first observed SELECT visibility, sampled symbol only; includes polling/query/scheduling delay, not fsync durability"}

    def close(self):
        for conn in self.connections:
            conn.close()
        self.redis.close()


def parse_source(text):
    metrics = [line[8:] for line in text.splitlines() if line.startswith("METRICS ")]
    sent = re.findall(r"^SENT (\d+) (\d+)$", text, re.MULTILINE)
    if len(metrics) != 1 or len(sent) != 1:
        raise RunError("source must output exactly one METRICS and SENT record")
    return json.loads(metrics[0]), tuple(map(int, sent[0]))


def receive_exact(sock, size):
    result = bytearray()
    while len(result) < size:
        data = sock.recv(size - len(result))
        if not data:
            raise RunError("queryd closed an incomplete response")
        result.extend(data)
    return bytes(result)


def verify_latest(port, symbol, expected):
    payload = symbol.encode().ljust(8, b"\0")
    request = HEADER.pack(b"TTAP", 1, 3, 0, 0, 0, len(payload), zlib.crc32(payload)) + payload
    with socket.create_connection(("127.0.0.1", port), timeout=3) as sock:
        sock.sendall(request)
        magic, ver, typ, count, flags, reserved, size, crc = HEADER.unpack(receive_exact(sock, 20))
        if (magic, ver, typ, count, size) != (b"TTAP", 1, 3, 1, 64):
            raise RunError("bad latest-price response shape")
        body = receive_exact(sock, size)
        if zlib.crc32(body) != crc:
            raise RunError("latest-price CRC mismatch")
        tick = TICK.unpack(body)
        received = (tick[0].rstrip(b"\0").decode(), *tick[1:])
        if received != expected:
            raise RunError("queryd latest does not match durable sampled tick")


def expected_fields(symbol_index, batch_index, seed=SEED):
    # Independent Python oracle for the synthetic C++ generator (u32 wrap).
    x = seed ^ ((symbol_index * 0x9e3779b9) & 0xffffffff) ^ ((batch_index * 0x85ebca6b) & 0xffffffff)
    x = ((x ^ (x >> 16)) * 0x7feb352d) & 0xffffffff
    x = ((x ^ (x >> 15)) * 0x846ca68b) & 0xffffffff
    x ^= x >> 16
    last = 100000 + x % 1000000
    half = 1 + (x >> 20) % 100
    return (last - half, last + half, last, 10 + x % 5000,
            10 + (x >> 8) % 5000, 0, 0)


def verify_database(observer, symbols, batches):
    counts = {}
    for index, conn in enumerate(observer.connections):
        for symbol, count, low, high in conn.execute(
                "SELECT symbol,COUNT(*),MIN(ts_exchange_ms),MAX(ts_exchange_ms) "
                "FROM ticks GROUP BY symbol"):
            if symbol in counts or shard_for(symbol, len(observer.connections)) != index:
                raise RunError(f"duplicate/misrouted symbol: {symbol}")
            if (count, low, high) != (batches, BASE_TS, BASE_TS + batches - 1):
                raise RunError(f"bad per-symbol key range/count: {symbol}")
            counts[symbol] = count
    if set(counts) != {str(600000 + i) for i in range(symbols)}:
        raise RunError("stored symbol set differs from generated set")
    samples = []
    for symbol in dict.fromkeys(("600000", str(600000 + symbols - 1))):
        conn = observer.connections[shard_for(symbol, len(observer.connections))]
        for ts in dict.fromkeys((BASE_TS, BASE_TS + batches - 1)):
            row = conn.execute("SELECT symbol,ts_exchange_ms,ts_receive_ms,bid_price,ask_price,"
                               "last_price,bid_size,ask_size,last_size,flags FROM ticks "
                               "WHERE symbol=? AND ts_exchange_ms=?", (symbol, ts)).fetchone()
            golden = expected_fields(int(symbol) - 600000, ts - BASE_TS)
            if row is None or row[3:] != golden:
                raise RunError(f"sample payload != deterministic oracle: {row}, expected fields={golden}")
            samples.append(row)
    return samples


def cache_build_type(directory):
    cache = directory / "CMakeCache.txt"
    if cache.exists():
        found = re.search(r"^CMAKE_BUILD_TYPE:STRING=(.*)$", cache.read_text(), re.MULTILINE)
        if found:
            return found[1]
    return "unknown"


def run_case(args, run_dir, symbols, batches, mode, shards, stop):
    run_dir.mkdir()
    ports = candidate_ports(3)
    config = run_dir / "stack.ini"
    write_config(config, ports, mode, shards, args.flush_ms, args.poll_ms)
    expected = symbols * batches
    result = {"status": "FAIL", "symbols": symbols, "batches": batches,
              "expected": expected, "persist": mode, "shards": shards,
              "seed": SEED, "ts_base": BASE_TS, "ports": ports,
              "flush_ms": args.flush_ms, "persister_poll_ms": args.poll_ms,
              "directory": str(run_dir)}
    owner = OwnedProcesses(run_dir)
    observer = None
    try:
        start_private_redis(owner, args.redis_server, ports[0], stop)
        options = start_stack(owner, args.build, config, stop=stop)
        observer = Observer(Path(options["persister"]["db"]), shards, ports[0], args.observe_ms)
        observer.observe()
        env = os.environ.copy()
        for key in list(env):
            if key.startswith("IFIND_"):
                del env[key]
        env["IFIND_USER"] = env["IFIND_PASS"] = "mock"
        command = [args.bridge, "--backend", "mock", "--host", "127.0.0.1", "--port", ports[1],
                   "--mock-symbols", symbols, "--mock-batches", batches, "--mock-seed", SEED,
                   "--mock-ts-base", BASE_TS, "--poll-ms", 0, "--duration-s", args.timeout]
        started = time.monotonic()
        child = owner.start("bridge", command, env=env)
        services = ("redis", "tickshelfd", "persister", "queryd", "evictor")
        while child.poll() is None:
            stop.check()
            owner.require_alive(*services)
            if time.monotonic() - started > args.timeout + 5:
                raise RunError("source exceeded supervisory timeout")
            observer.observe()
            time.sleep(args.observe_ms / 1000)
        ended = time.monotonic()
        if child.returncode:
            raise RunError(f"source exited {child.returncode}; inspect bridge.log")
        source, (sent, frames) = parse_source(owner.text("bridge"))
        result["source"] = source
        if source.get("backend") != "mock":
            raise RunError("source bypassed JSON mock backend")
        for name in ("expected", "generated", "parsed", "sent"):
            if source.get(name) != expected:
                raise RunError(f"source {name}={source.get(name)} != expected {expected}")
        if (source.get("complete") is not True or source.get("symbols") != symbols
                or source.get("batches") != batches or source.get("deduplicated") != 0
                or sent != expected or source.get("frames") != frames):
            raise RunError("incomplete source run or unexpected source drops/SENT/frame mismatch")
        if source.get("elapsed_s", 0) <= 0:
            raise RunError("source did not measure actual elapsed time")
        deadline = ended + args.drain_timeout
        while True:
            stop.check()
            owner.require_alive(*services)
            observer.observe()
            rows = observer.count()
            observed_at = time.monotonic()
            if rows == expected:
                break
            if rows > expected or observed_at > deadline:
                raise RunError(f"durable drain failed: rows={rows} expected={expected}")
            time.sleep(args.observe_ms / 1000)
        # The final commit can land between observe() and count(). Re-observe
        # after count confirms completeness rather than failing that valid race.
        observer.observe()
        if len(observer.latencies) != batches:
            raise RunError("sample observer missed batches")
        result.update({"sent": sent, "frames": frames, "sqlite_rows": rows,
                       "send_elapsed_s": source["elapsed_s"],
                       "send_records_per_s": sent / source["elapsed_s"],
                       "source_process_elapsed_s": ended - started,
                       "drain_observed_ms": (observed_at - ended) * 1000,
                       "end_to_end_elapsed_s": observed_at - started,
                       "durable_completion_records_per_s": rows / (observed_at - started),
                       "visibility": observer.summary(),
                       "peak_observed_redis_bytes": observer.peak_redis_bytes})
        samples = verify_database(observer, symbols, batches)
        result["sampled_ticks"] = samples
        latest = next(row for row in samples if row[0] == "600000" and row[1] == BASE_TS + batches - 1)
        verify_latest(ports[2], "600000", latest)
        result["query_latest_verified"] = True
        (run_dir / "visibility-samples.json").write_text(json.dumps(observer.samples, indent=2) + "\n")
        if owner.stop("tickshelfd") != 0:
            raise RunError("daemon did not stop cleanly")
        lines = [line for line in owner.text("tickshelfd").splitlines()
                 if line.startswith("tickshelfd: ingested=")]
        if not lines:
            raise RunError("missing daemon shutdown counters")
        stats = {k: int(v) for k, v in re.findall(r"(\w+)=(\d+)", lines[-1])}
        result["daemon"] = stats
        if stats.get("ingested") != expected or any(stats.get(k) != 0 for k in
                ("crc_err", "resync", "flush_err", "pending")):
            raise RunError(f"daemon reconciliation failed: {stats}")
        result["status"] = "PASS"
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if observer:
            observer.close()
        owner.close()
        result["child_exit_codes"] = {name: proc.returncode for name, proc in owner.children.items()}
        if owner.forced:
            result["status"] = "FAIL"
            result["error"] = f"forced child termination: {owner.forced}"
        if result["status"] == "PASS" and any(code != 0 for code in result["child_exit_codes"].values()):
            result["status"] = "FAIL"
            result["error"] = "nonzero child exit on cleanup"
        (run_dir / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main():
    root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", type=Path, default=Path(os.getenv("BUILD", root / "build-release")))
    parser.add_argument("--bridge", type=Path, default=Path(os.getenv("BRIDGE_BIN", root.parent / "quant-data-feeds/build-mock-release/ifind_ttap_bridge")))
    parser.add_argument("--redis-server", default=shutil.which("redis-server"))
    parser.add_argument("--suite", choices=("smoke", "stress"), default="smoke")
    parser.add_argument("--symbols", type=int)
    parser.add_argument("--batches", type=int)
    parser.add_argument("--persist", choices=("none", "sqlite"), default="none")
    parser.add_argument("--shards", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--drain-timeout", type=float, default=60)
    parser.add_argument("--observe-ms", type=float, default=20)
    parser.add_argument("--flush-ms", type=int, default=100)
    parser.add_argument("--poll-ms", type=int, default=100)
    parser.add_argument("--output", type=Path, help="new directory; refuses to overwrite")
    args = parser.parse_args()
    if bool(args.symbols is None) != bool(args.batches is None):
        parser.error("--symbols and --batches must be supplied together")
    for name in ("timeout", "drain_timeout", "observe_ms", "flush_ms", "poll_ms", "shards"):
        if not math.isfinite(getattr(args, name)) or getattr(args, name) <= 0:
            parser.error(f"{name} must be positive and finite")
    if args.symbols is not None and (not 1 <= args.symbols <= 10000 or not 1 <= args.batches <= 100000):
        parser.error("symbols must be 1..10000; batches 1..100000")
    if args.shards > 64:
        parser.error("shards must be <=64")
    args.build, args.bridge = args.build.resolve(), args.bridge.resolve()
    if not args.redis_server or not args.bridge.is_file():
        parser.error("Redis or external mock bridge missing; benchmark not run (not a PASS)")
    if args.output:
        args.output = args.output.resolve()
        args.output.mkdir(parents=True, exist_ok=False)
    else:
        args.output = Path(tempfile.mkdtemp(prefix="tickshelf-ifind-"))
    if args.symbols is not None:
        cases = [(args.symbols, args.batches, args.persist, args.shards)]
    elif args.suite == "smoke":
        cases = [(54, 10, args.persist, args.shards)]
    else:
        cases = [(symbols, batches, mode, 1) for symbols, batches in
                 ((54, 2000), (540, 1000), (5400, 200)) for mode in ("none", "sqlite")]
        cases.append((5400, 200, "sqlite", 4))
    metadata = {"platform": platform.platform(), "machine": platform.machine(),
                "logical_cpus": os.cpu_count(), "python": sys.version.split()[0],
                "tickshelf_build": str(args.build), "tickshelf_build_type": cache_build_type(args.build),
                "bridge": str(args.bridge), "bridge_build_type": cache_build_type(args.bridge.parent),
                "method": "synthetic local JSON mock, no real vendor SDK or credentials",
                "results": []}
    print(f"Artifacts: {args.output}", flush=True)
    with StopFlag() as stop:
        for symbols, batches, mode, shards in cases:
            name = f"{mode}-s{symbols}-b{batches}-n{shards}"
            print(f"RUN {name} expected={symbols * batches}", flush=True)
            result = run_case(args, args.output / name, symbols, batches, mode, shards, stop)
            metadata["results"].append(result)
            (args.output / "summary.json").write_text(json.dumps(metadata, indent=2) + "\n")
            if result["status"] != "PASS":
                print(f"RESULT ifind_sim FAIL {result.get('error')}; logs={result['directory']}", flush=True)
                return 1
            lag = result["visibility"]
            print(f"RESULT ifind_sim PASS sent={result['sent']} sqlite={result['sqlite_rows']} "
                  f"send_rps={result['send_records_per_s']:.0f} "
                  f"complete_rps={result['durable_completion_records_per_s']:.0f} "
                  f"drain_ms={result['drain_observed_ms']:.1f} "
                  f"sample_visible_p95_ms={lag['p95_ms']:.1f}", flush=True)
    print(f"Report: {args.output / 'summary.json'}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
