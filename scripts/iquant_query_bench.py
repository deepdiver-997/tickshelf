"""Measure IQTS ingestion with concurrent hot, cold, and hybrid history queries."""

import argparse
from bisect import bisect_left
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, closing
from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
import sqlite3
import sys
import time

from stack_runtime import OwnedProcesses, RedisConnection, RunError, candidate_ports, info_fields
from stack_runtime import start_private_redis, wait_until

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"
sys.path.insert(0, str(EXAMPLES))
from iquant.sender import Sender
from iquant import protocol as wire

TOKEN = "local-iqts-query-benchmark"
KEY = "tickshelf:iqts:query-benchmark"
SYMBOLS = 256
BASE_MS = 1700000000000
MODES = ("baseline", "hot", "cold", "hybrid")


@dataclass
class HotResult:
    rows: dict
    frames: set
    pages: int
    quotes_read: int
    max_page_quotes: int
    max_page_ms: float
    hit_page_budget: bool


def read_hot(redis, source_stream, symbol, first, last, frame_rows, page_quotes, max_pages):
    """Bound Redis work per command and continue backwards from the last returned ID."""
    limit = page_quotes // frame_rows
    if limit < 1:
        raise ValueError("Redis page must hold at least one frame")
    result = HotResult({}, set(), 0, 0, 0, 0.0, False)
    cursor = "+"
    for _ in range(max_pages):
        start = time.perf_counter()
        page = redis.command("XREVRANGE", KEY, cursor, "-", "COUNT", limit)
        result.max_page_ms = max(result.max_page_ms, (time.perf_counter() - start) * 1000)
        result.pages += 1
        if len(page) > limit:
            raise RunError("Redis exceeded requested page size")
        page_quotes_read = 0
        reached_first = False
        for entry_id, fields in page:
            values = dict(zip(fields[::2], fields[1::2]))
            symbols = [(name, value) for name, value in values.items() if name.startswith(b"symbol:")]
            if len(symbols) != frame_rows:
                raise RunError("Redis frame does not match the fixed benchmark frame size")
            page_quotes_read += len(symbols)
            if values[b"source_stream"] != source_stream:
                continue
            seq = int(values[b"batch"])
            if seq <= first:
                reached_first = True
            if not first <= seq <= last:
                continue
            result.frames.add(seq)
            for name, value in symbols:
                if value == symbol:
                    ordinal = name[len(b"symbol:"):]
                    result.rows[seq, int(ordinal)] = values[b"snapshot:" + ordinal]
        result.quotes_read += page_quotes_read
        result.max_page_quotes = max(result.max_page_quotes, page_quotes_read)
        if reached_first or len(page) < limit:
            return result
        cursor = "(" + page[-1][0].decode("ascii")
    result.hit_page_budget = True
    return result


def missing_ranges(first, last, present):
    start = first
    for seq in sorted(present):
        if seq < start or seq > last:
            continue
        if seq > start:
            yield start, seq - 1
        start = seq + 1
    if start <= last:
        yield start, last


def source_time(index, rate):
    return BASE_MS + index * 1000 // rate


def cold_rows(db, stream, symbol, first, last, frame_rows, rate):
    query = (
        "SELECT batch_seq,ordinal,snapshot FROM quotes WHERE stream=? AND symbol=? "
        "AND source_time_ms BETWEEN ? AND ? AND batch_seq BETWEEN ? AND ?"
    )
    args = (stream, symbol.decode("ascii"), source_time((first - 1) * frame_rows, rate),
            source_time(last * frame_rows - 1, rate), "{:020}".format(first), "{:020}".format(last))
    return {(int(seq), ordinal): snapshot for seq, ordinal, snapshot in db.execute(query, args)}


def query_window(mode, redis, db, stream, symbol, first, last, args):
    frame_rows = args.frame_rows
    hot = None
    if mode != "cold":
        # The newest half of a mixed request targets Redis, the older half SQLite.
        hot_first = first if mode == "hot" else (first + last + 1) // 2
        hot = read_hot(redis, stream.encode("ascii"), symbol, hot_first, last,
                       frame_rows, args.redis_page_quotes, args.max_redis_pages)
    if mode == "hot":
        return hot.rows, hot, 0
    rows = {} if hot is None else hot.rows
    ranges = list(missing_ranges(first, last, hot.frames if hot else set()))
    if ranges:
        db.execute("BEGIN")
        try:
            for lo, hi in ranges:
                rows.update(cold_rows(db, stream, symbol, lo, hi, frame_rows, args.rate))
        finally:
            db.rollback()  # End a read-only snapshot before the next query.
    return rows, hot, len(ranges)


def symbol_at(index):
    return "{:06}.SH".format(600000 + index % SYMBOLS)


def quote_at(index, rate):
    price = 10 + (index % 113) * 0.001
    return {"time": source_time(index, rate), "lastPrice": round(price, 3),
            "askPrice": [round(price + n * 0.001, 3) for n in range(1, 6)],
            "bidVol": [100 * ((index + n) % 197) for n in range(5)]}


def expected_keys(first, last, stock, frame_rows):
    lower = (first - 1) * frame_rows
    start = lower + (stock - lower) % SYMBOLS
    return {(index // frame_rows + 1, index % frame_rows)
            for index in range(start, last * frame_rows, SYMBOLS)}


def feed(sender, start, stop, args, sent):
    batches = int((stop - start) * args.rate / args.frame_rows)
    for batch in range(batches):
        delay = start + batch * args.frame_rows / args.rate - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        if time.monotonic() >= stop:
            break
        lo = batch * args.frame_rows
        records = [[symbol_at(index), quote_at(index, args.rate)]
                   for index in range(lo, lo + args.frame_rows)]
        payload = json.dumps(records, separators=(",", ":")).encode("ascii")
        sent.append(time.monotonic())
        if sender.publish(payload) != len(sent):
            raise RunError("source sequence disagrees with its latency ledger")


def open_history(path):
    db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    db.execute("PRAGMA query_only=ON")
    return db


def run_client(mode, sender, db_path, redis_port, client, start, stop, args):
    stream = sender.stream_id.hex()
    q = client
    due = start + client / args.query_qps
    period = args.query_clients / args.query_qps
    out = []
    with ExitStack() as stack:
        redis = stack.enter_context(RedisConnection("127.0.0.1", redis_port)) if mode != "cold" else None
        db = stack.enter_context(closing(open_history(db_path))) if mode != "hot" else None
        while due < stop:
            delay = due - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            start_call = time.monotonic()
            if start_call >= stop:
                break
            last = sender.stats()["last_batch"]
            first = max(1, last - args.query_frames + 1)
            stock = (q * 67 + 11) % SYMBOLS
            if last >= first:
                rows, hot, cold_queries = query_window(mode, redis, db, stream,
                                                        symbol_at(stock).encode("ascii"), first, last, args)
                elapsed = (time.monotonic() - start_call) * 1000
                expected = expected_keys(first, last, stock, args.frame_rows)
                if rows.keys() - expected:
                    raise RunError("query returned records outside its frozen source window")
                newest_lag_ms = None
                missing = expected - rows.keys()
                missing_before_newest = 0
                if rows:
                    newest, ordinal = max(rows)
                    newest_index = (newest - 1) * args.frame_rows + ordinal
                    expected_batch, expected_ordinal = max(expected)
                    expected_index = (expected_batch - 1) * args.frame_rows + expected_ordinal
                    newest_lag_ms = source_time(expected_index, args.rate) - source_time(newest_index, args.rate)
                    missing_before_newest = sum(key < (newest, ordinal) for key in missing)
                    if json.loads(rows[newest, ordinal]) != quote_at(newest_index, args.rate):
                        raise RunError("newest returned record has unexpected content")
                out.append({
                    "latency_ms": elapsed, "newest_lag_ms": newest_lag_ms,
                    "request_end_seq": last, "newest_seq": max(rows)[0] if rows else None,
                    "expected": len(expected), "returned": len(rows), "missing": len(missing),
                    "missing_before_newest": missing_before_newest,
                    "cold_queries": cold_queries, "redis_pages": hot.pages if hot else 0,
                    "redis_quotes_read": hot.quotes_read if hot else 0,
                    "redis_max_page_quotes": hot.max_page_quotes if hot else 0,
                    "redis_max_page_ms": hot.max_page_ms if hot else 0,
                    "redis_page_budget_hit": hot.hit_page_budget if hot else False,
                })
            q += args.query_clients
            due = max(due + period, time.monotonic())
    return out


def latency(values):
    values = sorted(v for v in values if v is not None)
    if not values:
        return None
    return {"p{}".format(p): round(values[math.ceil(p / 100 * len(values)) - 1], 3)
            for p in (50, 95, 99)}


def summarize(mode, queries, duration):
    if not queries and mode != "baseline":
        raise RunError("no concurrent queries completed")
    hot = [q for q in queries if q["redis_pages"]]
    expected = sum(q["expected"] for q in queries)
    missing = sum(q["missing"] for q in queries)
    return {
        "mode": mode, "queries": len(queries), "actual_qps": len(queries) / duration,
        "latency_ms": latency(q["latency_ms"] for q in queries),
        "newest_returned_lag_ms": latency(q["newest_lag_ms"] for q in queries),
        "expected_rows": expected, "missing_rows": missing,
        "missing_before_newest": sum(q["missing_before_newest"] for q in queries),
        "missing_row_fraction": missing / expected if expected else None,
        "complete_queries": sum(q["missing"] == 0 for q in queries),
        "empty_queries_with_expected_rows": sum(q["expected"] > 0 and q["returned"] == 0 for q in queries),
        "cold_queries": sum(q["cold_queries"] for q in queries),
        "redis_pages_per_query": latency(q["redis_pages"] for q in hot),
        "redis_slowest_page_per_query_ms": latency(q["redis_max_page_ms"] for q in hot),
        "redis_max_page_quotes": max((q["redis_max_page_quotes"] for q in hot), default=0),
        "redis_examined_quotes": sum(q["redis_quotes_read"] for q in hot),
        "redis_page_budget_hits": sum(q["redis_page_budget_hit"] for q in hot),
    }


def committed(db, stream):
    row = db.execute("SELECT last_seq FROM replay_cursor WHERE stream=?", (stream,)).fetchone()
    return int(row[0]) if row else 0


def write_latencies(sent, acked, observations, begin, end):
    first = bisect_left(sent, begin)
    last = bisect_left(sent, end)
    next_index = first
    observed = []
    for at, watermark in observations:
        while next_index < min(last, watermark):
            observed.append((at - sent[next_index]) * 1000)
            next_index += 1
    raw = [(acked[index + 1] - sent[index]) * 1000 for index in range(first, last)
           if index + 1 in acked]
    return {"sent_batches": last - first, "raw_ack_samples": len(raw),
            "raw_ack_ms": latency(raw), "history_samples": len(observed),
            "history_observed_ms": latency(observed)}


def ready(db_path):
    try:
        with closing(open_history(db_path)) as db:
            return db.execute("PRAGMA user_version").fetchone() == (1,)
    except sqlite3.OperationalError:
        return False


def command_stats(redis_port):
    with RedisConnection("127.0.0.1", redis_port) as redis:
        fields = info_fields(redis.command("INFO", "commandstats"))
        result = {}
        for command in ("xrevrange", "xadd"):
            data = fields.get("cmdstat_" + command, "calls=0,usec=0")
            counters = dict(item.split("=", 1) for item in data.split(","))
            calls, usec = int(counters["calls"]), int(counters["usec"])
            result[command] = {"calls": calls, "usec": usec,
                               "mean_server_usec": round(usec / calls, 3) if calls else None}
        return result


def run_mode(mode, path, args):
    path.mkdir()
    root = path / "instance"
    db_path = root / "history.sqlite3"
    port, = candidate_ports(1)
    sent, acked = [], {}

    def on_ack(_stream, kind, first, _last):
        if kind == wire.ACK_DATA:
            acked[first] = time.monotonic()

    with OwnedProcesses(path) as children, Sender(TOKEN.encode("ascii"), port=0, on_ack=on_ack) as sender:
        start_private_redis(children, args.redis_server, port)
        children.start("receiver", [sys.executable, "-B", EXAMPLES / "iquant" / "receiver.py",
                                     "--port", sender.address[1], "--work-dir", root,
                                     "--engine-bin", args.engine_bin, "--redis-port", port,
                                     "--redis-key", KEY], env=dict(os.environ, TS_IQUANT_TOKEN=TOKEN))
        wait_until(lambda: ready(db_path) and sender.stats()["connected"],
                   description="IQTS and history ready", processes=children, names=("redis", "receiver"))
        stream = sender.stream_id.hex()
        with ThreadPoolExecutor(max_workers=args.query_clients + 1) as pool, closing(open_history(db_path)) as db:
            start = time.monotonic() + 0.1
            begin_queries = start + args.warmup
            stop = begin_queries + args.duration
            source = pool.submit(feed, sender, start, stop, args, sent)
            clients = [] if mode == "baseline" else [
                pool.submit(run_client, mode, sender, db_path, port, i, begin_queries, stop, args)
                for i in range(args.query_clients)]
            samples = []
            observations = []

            def observe():
                history = committed(db, stream)
                at = time.monotonic()
                observations.append((at, history))
                return history, at

            while time.monotonic() < stop:
                now = time.monotonic()
                if now >= begin_queries:
                    children.require_alive("redis", "receiver")
                    history, at = observe()
                    stats = sender.stats()
                    samples.append({"t_s": at - begin_queries, "offered": stats["last_batch"],
                                    "raw_acked": stats["data_watermark"], "history": history})
                time.sleep(min(args.sample_ms / 1000, max(0, stop - time.monotonic())))
            source.result()
            offered = sender.stats()["last_batch"]
            drained = False
            while True:
                history, at = observe()
                if history == offered and sender.stats()["data_watermark"] == offered:
                    drained = True
                    break
                children.require_alive("redis", "receiver")
                if at > stop + args.drain_timeout:
                    break
                time.sleep(args.sample_ms / 1000)
            drain_ms = round(max(0, at - stop) * 1000, 3) if drained else None
            queries = [q for client in clients for q in client.result()]
            history_rows = db.execute("SELECT count(*) FROM quotes").fetchone()[0]
            if len(samples) < 2:
                raise RunError("insufficient steady-state ingest samples")
        result = summarize(mode, queries, args.duration)
        seconds = samples[-1]["t_s"] - samples[0]["t_s"]
        result["measured_s"] = round(seconds, 3)
        result["ingest_rps"] = {key: round((samples[-1][key] - samples[0][key]) * args.frame_rows / seconds, 2)
                                for key in ("offered", "raw_acked", "history")}
        result["peak_history_backlog_rows"] = max(sample["offered"] - sample["history"]
                                                  for sample in samples) * args.frame_rows
        result["peak_raw_backlog_rows"] = max(sample["offered"] - sample["raw_acked"]
                                              for sample in samples) * args.frame_rows
        result["history_backlog_growth_rps"] = round(result["ingest_rps"]["offered"]
                                                    - result["ingest_rps"]["history"], 2)
        result["write_latency"] = write_latencies(sent, acked, observations, begin_queries, stop)
        result["drain_ms"] = drain_ms
        result["drained"] = drained
        result["source_rows"] = offered * args.frame_rows
        result["final_history_rows"] = history_rows
        result["audit_pass"] = drained and history_rows == result["source_rows"]
        result["audited_source_rows"] = history_rows if result["audit_pass"] else None
        result["redis_commandstats"] = command_stats(port)
        (path / "queries.jsonl").write_text("".join(json.dumps(row) + "\n" for row in queries), encoding="utf-8")
        (path / "ingest.json").write_text(json.dumps(samples, indent=2) + "\n", encoding="utf-8")
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine-bin", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="new result directory")
    parser.add_argument("--redis-server", default="redis-server")
    parser.add_argument("--modes", nargs="+", choices=MODES, default=MODES)
    parser.add_argument("--duration", type=float, default=15)
    parser.add_argument("--warmup", type=float, default=3)
    parser.add_argument("--rate", type=int, default=10000)
    parser.add_argument("--query-qps", type=float, default=80)
    parser.add_argument("--query-clients", type=int, default=4)
    parser.add_argument("--query-frames", type=int, default=48)
    parser.add_argument("--frame-rows", type=int, default=32)
    parser.add_argument("--redis-page-quotes", type=int, default=256)
    parser.add_argument("--max-redis-pages", type=int, default=64)
    parser.add_argument("--sample-ms", type=float, default=10)
    parser.add_argument("--drain-timeout", type=float, default=45)
    args = parser.parse_args()
    if not (2 <= args.duration <= 600 and 0 <= args.warmup <= 60 and 100 <= args.rate <= 200000
            and 1 <= args.query_clients <= 64 and 0 < args.query_qps <= 2000
            and args.duration * args.query_qps <= 200000
            and 2 <= args.sample_ms <= 200 and 1 <= args.drain_timeout <= 600
            and 1 <= args.query_frames <= 512 and 1 <= args.max_redis_pages <= 512
            and 1 <= args.frame_rows <= args.redis_page_quotes <= 256 and len(set(args.modes)) == len(args.modes)):
        parser.error("invalid duration, rate, query limits, or repeated modes")
    args.engine_bin = args.engine_bin.resolve(strict=True)
    root = args.output.resolve()
    root.mkdir()
    report = {"synthetic": True, "configuration": {k: str(v) if isinstance(v, Path) else v
                                                   for k, v in vars(args).items()}, "runs": []}
    for mode in args.modes:
        result = run_mode(mode, root / mode, args)
        report["runs"].append(result)
        (root / "results.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result, ensure_ascii=False), flush=True)
    return 0 if all(run["audit_pass"] for run in report["runs"]) else 1


if __name__ == "__main__":
    sys.exit(main())
