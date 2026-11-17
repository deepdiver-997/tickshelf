#!/usr/bin/env python3
"""Bounded private-Redis Engine experiments with independent full-field checks.

native: 8 in-process real-Engine cells (Redis real/Null, SQLite/Null, FIFO on/off).
This is NOT TTAP network ingress. network: old flush vs Engine using floodgen.
All child PIDs/logs and failed cells remain owned under a new --output directory.
"""
from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import re
import shutil
import sqlite3
import struct
import sys
import time
from contextlib import closing
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from stack_runtime import (OwnedProcesses, RunError, StopFlag, candidate_ports,
                           redis_runtime_snapshot, start_private_redis, wait_until)
# Reuse the existing disk/space guard rather than a second budget framework.
from pipeline_bench import enforce_budget

ROOT = Path(__file__).resolve().parent.parent
BUILD = ROOT / "build"
TICK = struct.Struct("<8sQQqqqIIIB3x")
COLUMNS = ("symbol,ts_exchange_ms,ts_receive_ms,bid_price,ask_price,last_price,"
           "bid_size,ask_size,last_size,flags")
TIME_BASE = 1700000000000
MASK64 = (1 << 64) - 1


def save_json(path, data):
    with Path(path).open("x") as output:
        json.dump(data, output, indent=2, sort_keys=True, allow_nan=False)
        output.write("\n")


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def logical_id(sequence, duplicate_every):
    return sequence - (sequence + 1) // duplicate_every if duplicate_every >= 2 else sequence


def tick_values(record_id, symbols):
    """Independent deterministic input; all ten persisted fields vary."""
    symbol, cycle = record_id % symbols, record_id // symbols
    exchange = TIME_BASE + cycle
    bid = (-1 if record_id % 7 == 0 else 1) * (10000000 + record_id * 17)
    return (f"E{symbol:07d}", exchange, exchange + 1 + record_id % 13,
            bid, bid + 103, bid - 29, (record_id * 31) % (1 << 32),
            (record_id * 43 + 1) % (1 << 32), record_id % 1009, record_id % 256)


def tick_bytes(record_id, symbols):
    values = tick_values(record_id, symbols)
    return TICK.pack(values[0].encode("ascii"), *values[1:])


def tick_hash(raw):
    value = 14695981039346656037
    for byte in raw:
        value = ((value ^ byte) * 1099511628211) & MASK64
    return value


def write_input(path, records, symbols, duplicate_every=5, check=lambda: None):
    unique = logical_id(records - 1, duplicate_every) + 1
    checksum_all = checksum_unique = 0
    previous = None
    digest = hashlib.sha256()
    with Path(path).open("xb") as output:
        for sequence in range(records):
            if sequence % 1024 == 0:
                check()
            record_id = logical_id(sequence, duplicate_every)
            raw = tick_bytes(record_id, symbols)
            output.write(raw)
            digest.update(raw)
            hashed = tick_hash(raw)
            checksum_all = (checksum_all + hashed) & MASK64
            if record_id != previous:
                checksum_unique = (checksum_unique + hashed) & MASK64
            previous = record_id
    return {"path": str(Path(path).resolve()), "bytes": records * TICK.size,
            "records": records, "unique_records": unique, "symbols": symbols,
            "duplicate_every": duplicate_every, "sha256_before": digest.hexdigest(),
            "checksum_all": f"{checksum_all:016x}", "checksum_unique": f"{checksum_unique:016x}",
            "generator": "independent-python-64B-Tick-formula", "source": "synthetic"}


def shard_path(path, index, shards):
    return Path(f"{path}.{index}") if shards > 1 else Path(path)


def symbol_shard(symbol, shards):
    value = 1469598103934665603  # existing routing's deliberately nonstandard basis
    for byte in symbol.encode("ascii"):
        value = ((value ^ byte) * 1099511628211) & MASK64
    return value % shards


def db_connection(path):
    return sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=0.2)


def db_rows(path, shards):
    total = distinct = 0
    for index in range(shards):
        file = shard_path(path, index, shards)
        if not file.is_file():
            continue
        with closing(db_connection(file)) as connection:
            try:
                total += connection.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]
                distinct += connection.execute(
                    "SELECT COUNT(*) FROM (SELECT DISTINCT symbol,ts_exchange_ms FROM ticks)").fetchone()[0]
            except sqlite3.OperationalError as exc:
                if "no such table" not in str(exc):
                    raise
    return total, distinct


def verify_sqlite(path, shards, manifest, check=lambda: None):
    """Exact full-field set equality, not the C++ generator verifying itself."""
    count = manifest["unique_records"]
    seen = bytearray(count)
    physical = 0
    for index in range(shards):
        file = shard_path(path, index, shards)
        if not file.is_file():
            raise RunError(f"missing SQLite shard {file}")
        with closing(db_connection(file)) as connection:
            for row in connection.execute(f"SELECT {COLUMNS} FROM ticks"):
                if physical % 1024 == 0:
                    check()
                symbol, timestamp = row[:2]
                if not isinstance(symbol, str) or not re.fullmatch(r"E[0-9]{7}", symbol):
                    raise RunError("unexpected SQLite symbol")
                symbol_index = int(symbol[1:])
                record_id = (timestamp - TIME_BASE) * manifest["symbols"] + symbol_index
                if not 0 <= symbol_index < manifest["symbols"] or not 0 <= record_id < count:
                    raise RunError("SQLite key outside independent input universe")
                if tuple(row) != tick_values(record_id, manifest["symbols"]):
                    raise RunError(f"SQLite full-field mismatch for record {record_id}")
                if symbol_shard(symbol, shards) != index:
                    raise RunError(f"record stored on wrong shard: {symbol}")
                if seen[record_id]:
                    raise RunError("duplicate natural key across SQLite shards")
                seen[record_id] = 1
                physical += 1
    if physical != count or not all(seen):
        raise RunError(f"SQLite set mismatch: {physical} rows, expected {count}")
    return {"passed": True, "method": "independent-Python-all-ten-fields-and-exact-key-set",
            "physical_rows": physical, "expected_unique": count}


def integer(data, key):
    value = data.get(key)
    if type(value) is not int or value < 0:
        raise RunError(f"missing/noninteger counter {key}")
    return value


def validate_native(data, manifest, redis, storage, fifo):
    if data.get("schema") != 1 or data.get("status") != "PASS":
        raise RunError("native process did not report schema=1/PASS")
    if data.get("label") != "in-process-real-Engine-no-TTAP-ingress":
        raise RunError("native ingress label is missing or misleading")
    parameters = data.get("parameters", {})
    expected_parameters = {"redis": redis, "storage": storage, "fifo": fifo,
                           "records": manifest["records"]}
    if any(parameters.get(key) != value for key, value in expected_parameters.items()):
        raise RunError("native parameters disagree with requested cell")
    forwarded = manifest["unique_records"] if fifo == "on" else manifest["records"]
    expected = {"accepted_records": manifest["records"], "published_records": forwarded,
                "dedup_dropped": manifest["records"] - forwarded,
                "committed_input_records": forwarded if storage == "sqlite" else 0,
                "inserted_unique_records": manifest["unique_records"] if storage == "sqlite" else 0,
                "sink_consumed_records": forwarded if storage == "null" else 0,
                "durable_records": forwarded if storage == "sqlite" else 0,
                "pending_records": 0, "queue_depth": 0, "degraded_records": 0,
                "skipped_records": 0, "finish_pending_records": 0, "publish_only_records": 0, "retries": 0}
    counters = data.get("counters", {})
    for key, wanted in expected.items():
        if integer(counters, key) != wanted:
            raise RunError(f"native counter {key}: got {counters.get(key)}, expected {wanted}")
    for key in ("submitted_records", "rejected_records", "sealed_slices", "queue_peak"):
        integer(counters, key)
    if counters["submitted_records"] != counters["accepted_records"] + counters["rejected_records"]:
        raise RunError("submitted must equal accepted plus backpressure rejection attempts")
    if counters["sealed_slices"] == 0:
        raise RunError("native run did not exercise real Engine slices")
    for key, value in counters.items():
        if type(value) is not int or value < 0:
            raise RunError(f"invalid native counter {key}")
    if data.get("queue_complete") is not True or data.get("storage_complete") is not (storage == "sqlite"):
        raise RunError("queue/storage completion incorrectly reported")
    timing = data.get("timing", {})
    for key in ("elapsed_s", "submit_s", "drain_s"):
        value = timing.get(key)
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise RunError(f"invalid timing {key}")
    if timing["elapsed_s"] <= 0 or not math.isclose(
            timing["elapsed_s"], timing["submit_s"] + timing["drain_s"], rel_tol=1e-8, abs_tol=1e-9):
        raise RunError("native timing denominators disagree")
    observation = data.get("observation", {})
    checksum = manifest["checksum_unique" if fifo == "on" else "checksum_all"]
    observed_count = forwarded if storage == "null" else 0
    observed_checksum = checksum if storage == "null" else "0000000000000000"
    if (observation.get("input_count") != observed_count or observation.get("checksum_sum_fnv1a64") != observed_checksum
            or observation.get("persistent") is not (storage == "sqlite")):
        raise RunError("Null sink count/full-byte checksum or persistent observation scope disagrees")
    infos = data.get("sqlite_connections")
    if not isinstance(infos, list) or len(infos) != (parameters.get("shards") if storage == "sqlite" else 0):
        raise RunError("actual SQLite connection reports missing")
    for index, info in enumerate(infos):
        if (info.get("shard") != index or info.get("journal_mode") != "wal" or not info.get("version")
                or any(type(info.get(key)) is not int or info[key] < 0 for key in
                       ("synchronous", "fullfsync", "checkpoint_fullfsync", "wal_autocheckpoint"))):
            raise RunError("invalid actual SQLite PRAGMA report")
    return {"passed": True, "forwarded_records": forwarded, "checksum_sum_fnv1a64": checksum,
            "null_is_not_durable": storage == "null"}


def wait_child(processes, name, timeout, check=lambda: None, alive=()):
    deadline = time.monotonic() + timeout
    child = processes.children[name]
    while child.poll() is None:
        check()
        processes.require_alive(*alive)
        if time.monotonic() >= deadline:
            raise RunError(f"timeout waiting for owned child {name}")
        time.sleep(0.01)
    if child.returncode != 0:
        raise RunError(f"{name} exited {child.returncode}; see owned log")
    return child.returncode


def parse_sent(text):
    lines = re.findall(r"^SENT ([0-9]+) ([0-9]+)$", text, re.MULTILINE)
    if len(lines) != 1:
        raise RunError("floodgen must report exactly one valid SENT trailer")
    ticks, frames = map(int, lines[0])
    if not ticks or not frames or ticks != frames * 4096:
        raise RunError("zero/impossible floodgen SENT totals")
    return ticks, frames


def verify_flood_sqlite(path, shards, sources, check=lambda: None):
    seen = [bytearray(source["ticks"]) for source in sources]
    total = 0
    for index in range(shards):
        file = shard_path(path, index, shards)
        if not file.is_file():
            raise RunError(f"missing SQLite shard {file}")
        with closing(db_connection(file)) as connection:
            for row in connection.execute(f"SELECT {COLUMNS} FROM ticks"):
                if total % 1024 == 0:
                    check()
                symbol, exchange = row[:2]
                if not isinstance(symbol, str) or not re.fullmatch(r"F[0-9]{7}", symbol):
                    raise RunError("invalid flood symbol")
                source_index = next((i for i, source in enumerate(sources)
                                     if source["base"] <= int(symbol[1:]) < source["base"] + 64), None)
                if source_index is None:
                    raise RunError("flood symbol outside offered universe")
                source = sources[source_index]
                record = (exchange - 1001) * 64 + int(symbol[1:]) - source["base"]
                if not 0 <= record < source["ticks"]:
                    raise RunError("flood key outside successful SENT prefix")
                bid = 10000000 + exchange % 1000
                expected = (symbol, exchange, exchange + 1, bid, bid + 100, bid + 50, 10, 10, 1, 0)
                if tuple(row) != expected or seen[source_index][record]:
                    raise RunError("flood full-field mismatch or duplicate key")
                if symbol_shard(symbol, shards) != index:
                    raise RunError("flood key on wrong shard")
                seen[source_index][record] = 1
                total += 1
    wanted = sum(source["ticks"] for source in sources)
    if total != wanted or any(not all(records) for records in seen):
        raise RunError(f"flood exact set mismatch {total} != {wanted}")
    return {"passed": True, "method": "independent-floodgen-all-ten-fields-and-exact-SENT-key-set",
            "rows": total}


def run_native(build, directory, input_path, manifest, *, redis, storage, fifo,
               args, check=lambda: None):
    directory.mkdir()
    result = {"status": "FAIL", "redis": redis, "storage": storage, "fifo": fifo,
              "label": "in-process-real-Engine-no-TTAP-ingress", "directory": str(directory)}
    processes = OwnedProcesses(directory / "proc")
    try:
        check()
        before = sha256(input_path)
        if before != manifest["sha256_before"]:
            raise RunError("controlled input changed before native run")
        command = [build / "engine_ablation", "--input", input_path, "--output", directory / "native",
                   "--redis", redis, "--storage", storage, "--fifo", fifo,
                   "--shards", args.shards, "--batch-records", args.batch_records,
                   "--inflight", args.inflight, "--max-pending-records", args.max_pending_records,
                   "--dedup-bytes", args.dedup_bytes, "--timeout-ms", int(args.timeout * 1000)]
        if redis == "real":
            port, = candidate_ports(1)
            start_private_redis(processes, shutil.which("redis-server"), port)
            command += ["--redis-port", port]
            result["redis_before"] = redis_runtime_snapshot("127.0.0.1", port, ownership="owned_private")
        processes.start("engine_ablation", command)
        wait_child(processes, "engine_ablation", args.timeout + 10, check,
                   alive=("redis",) if redis == "real" else ())
        lines = [line for line in processes.text("engine_ablation").splitlines() if line.startswith("{")]
        if len(lines) != 1:
            raise RunError("native child must emit exactly one JSON result")
        data = json.loads(lines[0])
        result["native"] = data
        save_json(directory / "native-result.json", data)
        result["counter_oracle"] = validate_native(data, manifest, redis, storage, fifo)
        requested_parameters = {"shards": args.shards, "batch_records": args.batch_records,
                                "inflight": args.inflight, "max_pending_records": args.max_pending_records,
                                "dedup_bytes_per_shard": args.dedup_bytes if fifo == "on" else 0,
                                "timeout_ms": int(args.timeout * 1000), "batch_bytes": 0, "batch_ms": 10}
        if any(data["parameters"].get(key) != value for key, value in requested_parameters.items()):
            raise RunError("native effective configuration differs from requested parameters")
        after = sha256(input_path)
        result["input_sha256_before"] = before
        result["input_sha256_after"] = after
        if before != after:
            raise RunError("controlled input changed during native run")
        if storage == "sqlite":
            result["sqlite_oracle"] = verify_sqlite(directory / "native/ticks.db", args.shards, manifest, check)
        else:
            result["sqlite_oracle"] = {"not_applicable": "Null consumes only; no durable rows claimed"}
        elapsed = data["timing"]["elapsed_s"]
        result["rates_per_s"] = {key: data["counters"][key] / elapsed for key in
                                 ("accepted_records", "published_records", "committed_input_records",
                                  "inserted_unique_records", "sink_consumed_records")}
        if redis == "real":
            result["redis_after"] = redis_runtime_snapshot("127.0.0.1", port, ownership="owned_private")
        result["status"] = "PASS"
    except (Exception, KeyboardInterrupt) as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        try:
            result["input_sha256_before"] = manifest["sha256_before"]
            result["input_sha256_after"] = sha256(input_path)
            if result["input_sha256_after"] != result["input_sha256_before"]:
                result["status"] = "FAIL"
                result["input_error"] = "controlled input changed"
        except OSError as exc:
            result["status"] = "FAIL"
            result["input_error"] = str(exc)
        processes.close()
        result["forced_children"] = processes.forced
        result["child_exit_codes"] = {name: child.returncode for name, child in processes.children.items()}
        if processes.forced or any(code != 0 for code in result["child_exit_codes"].values()):
            result["status"] = "FAIL"
        save_json(directory / "result.json", result)
    return result


def run_network(build, directory, engine, *, args, check=lambda: None):
    directory.mkdir()
    result = {"status": "FAIL", "mode": "engine" if engine else "old-flush",
              "label": "synthetic-TTAP-TCP-floodgen-to-SQLite", "directory": str(directory)}
    processes = OwnedProcesses(directory / "proc")
    try:
        redis_port, daemon_port = candidate_ports(2)
        start_private_redis(processes, shutil.which("redis-server"), redis_port)
        result["redis_before"] = redis_runtime_snapshot("127.0.0.1", redis_port, ownership="owned_private")
        db, config = directory / "ticks.db", directory / "tickshelf.ini"
        config.write_text(
            f"[redis]\nhost=127.0.0.1\nport={redis_port}\n[storage]\ndb={db}\nshards={args.shards}\n"
            f"[tickshelfd]\nbind=127.0.0.1\nport={daemon_port}\npersist=sqlite\nflush_interval_ms=100\n"
            f"engine={'true' if engine else 'false'}\nengine_batch_records={args.batch_records}\n"
            f"engine_batch_ms=10\nengine_inflight={args.inflight}\n"
            f"engine_max_pending_records={args.max_pending_records}\nengine_shutdown_ms=15000\n")
        processes.start("tickshelfd", [build / "tickshelfd", "--config", config])
        wait_until(lambda: "listening " in processes.text("tickshelfd"), timeout=10,
                   processes=processes, names=("tickshelfd", "redis"), stop=None)
        started = time.monotonic()
        sources = []
        for index in range(args.senders):
            base = (index + 1) * 100000
            name = f"flood-{index}"
            processes.start(name, [build / "floodgen", "--host", "127.0.0.1", "--port", daemon_port,
                                  "--symbols", 64, "--sym-base", base, "--rate", args.rate,
                                  "--duration", args.duration])
            sources.append({"name": name, "base": base})
        for source in sources:
            wait_child(processes, source["name"], args.duration + args.timeout, check,
                       alive=("tickshelfd", "redis"))
            source["ticks"], source["frames"] = parse_sent(processes.text(source["name"]))
        senders_finished = time.monotonic()
        sent = sum(source["ticks"] for source in sources)
        result["sources"] = sources
        result["floodgen_sent"] = sent
        deadline = senders_finished + args.timeout
        observed_rows = observed_distinct = 0
        completed = False
        while True:
            check()
            processes.require_alive("tickshelfd", "redis")
            observed_rows, observed_distinct = db_rows(db, args.shards)
            if observed_rows == sent and observed_distinct == sent:
                completed = True
                break
            if observed_rows > sent or time.monotonic() >= deadline:
                break # still record the separate exit-drain outcome; never promote it to pre-exit PASS
            time.sleep(0.025)
        observed_at = time.monotonic()
        # Fixed numerator/denominator pair BEFORE exit. Exit-only drain is separate.
        result["measurement"] = {"completed_before_exit": completed, "observed_rows": observed_rows,
                                 "observed_distinct": observed_distinct, "elapsed_s": observed_at - started,
                                 "rows_per_s": observed_rows / (observed_at - started),
                                 "senders_s": senders_finished - started,
                                 "post_sender_settle_s": observed_at - senders_finished}
        stopping_at = time.monotonic()
        rc = processes.stop("tickshelfd", timeout=25)
        stopped_at = time.monotonic()
        final_rows, final_distinct = db_rows(db, args.shards)
        result["exit_drain"] = {"elapsed_s": stopped_at - stopping_at,
                                "additional_rows": final_rows - observed_rows,
                                "final_rows": final_rows, "final_distinct": final_distinct}
        if not completed:
            raise RunError(f"pre-exit SQLite completion gate failed: rows={observed_rows}, SENT={sent}; exit-only drain is separate")
        if rc or final_rows != sent or final_distinct != sent:
            raise RunError("daemon exit or final exact SQLite row gate failed")
        if "drain incomplete" in processes.text("tickshelfd"):
            raise RunError("daemon reported incomplete shutdown")
        result["sources"] = sources
        result["floodgen_sent"] = sent
        result["sqlite_oracle"] = verify_flood_sqlite(db, args.shards, sources, check)
        result["redis_after"] = redis_runtime_snapshot("127.0.0.1", redis_port, ownership="owned_private")
        result["status"] = "PASS"
    except (Exception, KeyboardInterrupt) as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        processes.close()
        result["forced_children"] = processes.forced
        result["child_exit_codes"] = {name: child.returncode for name, child in processes.children.items()}
        if processes.forced or any(code != 0 for code in result["child_exit_codes"].values()):
            result["status"] = "FAIL"
        save_json(directory / "result.json", result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", choices=("native", "network"), default="native")
    parser.add_argument("--build", type=Path, default=BUILD)
    parser.add_argument("--output", type=Path, required=True, help="new directory; never overwritten or deleted")
    parser.add_argument("--records", type=int, default=10000)
    parser.add_argument("--symbols", type=int, default=64)
    parser.add_argument("--duplicate-every", type=int, default=5)
    parser.add_argument("--warmup-records", type=int, default=1000)
    parser.add_argument("--batch-records", type=int, default=256)
    parser.add_argument("--inflight", type=int, default=4)
    parser.add_argument("--max-pending-records", type=int, default=4096)
    parser.add_argument("--dedup-bytes", type=int, default=1 << 20)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--duration", type=float, default=2)
    parser.add_argument("--rate", type=int, default=20000, help="per network floodgen")
    parser.add_argument("--senders", type=int, default=2)
    parser.add_argument("--shards", type=int, default=2)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--max-disk-mib", type=int, default=12288)
    parser.add_argument("--reserve-free-mib", type=int, default=10240)
    parser.add_argument("--budget-root", type=Path, help="existing containing artifact root, included in disk budget")
    args = parser.parse_args(argv)
    if (not 1 <= args.records <= 1000000 or not 1 <= args.symbols <= 1000000 or
            args.duplicate_every not in (0,) and not 2 <= args.duplicate_every <= 1000000 or
            not 0 <= args.warmup_records <= 1000000 or not 1 <= args.shards <= 64 or
            not 1 <= args.batch_records <= 65536 or not 1 <= args.inflight <= 256 or
            not args.batch_records <= args.max_pending_records <= 1048576 or
            not 128 <= args.dedup_bytes <= 64 << 20 or not 1 <= args.repeat <= 100 or
            not math.isfinite(args.timeout) or not 0 < args.timeout <= 300 or
            not math.isfinite(args.duration) or not 0 < args.duration <= 300 or
            not 1 <= args.rate <= 10000000 or not 1 <= args.senders <= 16 or
            args.max_disk_mib < 16 or args.reserve_free_mib < 0):
        parser.error("arguments outside bounded supported ranges")
    args.build = args.build.resolve()
    args.output = args.output.absolute()
    if args.output.exists() or args.output.is_symlink():
        parser.error("--output must be a new directory")
    required = ("engine_ablation",) if args.suite == "native" else ("tickshelfd", "floodgen")
    for binary in required:
        if not (args.build / binary).is_file():
            parser.error(f"missing {args.build / binary}")
    if not shutil.which("redis-server"):
        parser.error("redis-server not installed")
    paths = [args.output.parent]
    if args.budget_root:
        root = args.budget_root.resolve()
        if not root.is_dir() or not args.output.resolve().is_relative_to(root):
            parser.error("--budget-root must be an existing ancestor of output")
        paths = [root]
    else:
        paths = [args.output]
    # Check free reserve before creating output; count only the requested artifact tree.
    if shutil.disk_usage(args.output.parent).free < args.reserve_free_mib * 1024 ** 2:
        parser.error("free disk reserve unavailable")
    args.output.mkdir()
    results = []
    with StopFlag() as stop:
        next_budget_check = 0.0
        def check():
            nonlocal next_budget_check
            stop.check()
            now = time.monotonic()
            if now >= next_budget_check:
                enforce_budget(paths, args.max_disk_mib * 1024 ** 2, args.reserve_free_mib * 1024 ** 2)
                next_budget_check = now + 0.25
        try:
            check()
            save_json(args.output / "parameters.json", {key: str(value) if isinstance(value, Path) else value
                                                        for key, value in vars(args).items()})
            cache = args.build / "CMakeCache.txt"
            build_type = "unknown"
            if cache.is_file():
                matches = re.findall(r"^CMAKE_BUILD_TYPE:STRING=(.*)$", cache.read_text(), re.MULTILINE)
                if len(matches) == 1:
                    build_type = matches[0]
            save_json(args.output / "build-info.json", {"build_type": build_type, "build_directory": str(args.build),
                      "binaries": {name: {"path": str(args.build / name), "sha256": sha256(args.build / name)}
                                   for name in required}})
            if args.suite == "native":
                input_path = args.output / "input.ticks"
                manifest = write_input(input_path, args.records, args.symbols, args.duplicate_every, check)
                save_json(args.output / "input-manifest.json", manifest)
                warmup = None
                if args.warmup_records:
                    warmup_path = args.output / "warmup.ticks"
                    warmup = write_input(warmup_path, args.warmup_records, args.symbols, args.duplicate_every, check)
                    save_json(args.output / "warmup-manifest.json", warmup)
                for repeat in range(args.repeat):
                    for redis, storage, fifo in itertools.product(("real", "null"), ("sqlite", "null"), ("off", "on")):
                        check()
                        name = f"r{repeat + 1}-{redis}-{storage}-fifo-{fifo}"
                        if warmup:
                            case = run_native(args.build, args.output / (name + "-warmup"), warmup_path, warmup,
                                              redis=redis, storage=storage, fifo=fifo, args=args, check=check)
                            case["phase"] = "independent-warmup"
                            results.append(case)
                            if case["status"] != "PASS":
                                print(json.dumps(case, allow_nan=False))
                                continue
                        case = run_native(args.build, args.output / name, input_path, manifest,
                                          redis=redis, storage=storage, fifo=fifo, args=args, check=check)
                        case.update(phase="measurement", repeat=repeat + 1)
                        results.append(case)
                        print(json.dumps(case, allow_nan=False))
                final_sha = sha256(input_path)
                save_json(args.output / "input-after.json", {"sha256_after": final_sha,
                          "unchanged": final_sha == manifest["sha256_before"]})
                if final_sha != manifest["sha256_before"]:
                    raise RunError("controlled input modified")
            else:
                for repeat in range(args.repeat):
                    for engine in (False, True):
                        check()
                        case = run_network(args.build, args.output / f"r{repeat + 1}-{'engine' if engine else 'old-flush'}",
                                           engine, args=args, check=check)
                        case.update(phase="measurement", repeat=repeat + 1)
                        results.append(case)
                        print(json.dumps(case, allow_nan=False))
            check()
        except (Exception, KeyboardInterrupt) as exc:
            results.append({"status": "FAIL", "error": f"{type(exc).__name__}: {exc}"})
        finally:
            passed = bool(results) and all(case["status"] == "PASS" for case in results)
            save_json(args.output / "summary.json", {"status": "PASS" if passed else "FAIL", "cases": results,
                      "caveat": "synthetic indication only; no bottleneck attribution or power-loss guarantee"})
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
