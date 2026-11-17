"""Independent benchmark records, framing, and post-run correctness oracle.

This implementation deliberately does not call the C++ generator/exporter to
construct expected bytes. The specification is bench/record.hpp, not a vendor
wire protocol. Everything here uses the Python standard library.
"""
from __future__ import annotations

import hashlib
import json
import math
import struct
import zlib
from dataclasses import dataclass
from pathlib import Path

from stack_runtime import RunError

TIME_BASE = 1700000000000
FRAME = struct.Struct("<4sBBHQII")
FNV_OFFSET = 14695981039346656037
FNV_PRIME = 1099511628211
MASK64 = (1 << 64) - 1
SCALARS = ("lastPrice", "amount", "volume", "pvolumn", "openInt", "stockStatus",
           "lastSettlementPrice", "open", "high", "low", "settlementPrice",
           "lastClose", "transactionNum")
SCALED = (True, True, False, False, False, False, True, True, True, True, True, True, False)
BOOKS = ("askPrice", "bidPrice", "askVol", "bidVol")
DEFAULTS = {
    "host": "127.0.0.1", "port": 19900, "redis_host": "127.0.0.1", "redis_port": 6379,
    "input": "json", "input_file": "", "input_manifest": "", "output_file": "", "records": 54000,
    "symbols": 54, "depth": 5, "seed": 42, "duplicate_every": 0,
    "optional_fields": False, "batch_records": 256, "hash_batch_records": 256,
    "redis_batch_records": 256, "rate": 0, "parse": "real",
    "hash": "dedup", "hash_mode": "batch", "hash_algorithm": "sha256",
    "dedup_max_entries": 1000000, "redis": "roundtrip", "backend": "sqlite",
    "synchronous": "FULL", "transaction_model": "light", "reuse_statement": True,
    "sqlite_fullfsync": "default", "sqlite_compact_lookup": "subquery", "store_lookup": "exact",
    "autocheckpoint": 1000, "transaction_rows": 1000, "transaction_bytes": 1048576,
    "transaction_ms": 100, "timeout_s": 120, "failpoint": "",
}


@dataclass(frozen=True)
class Record:
    symbol: str
    time: int
    present: int
    nulls: int
    values: tuple
    book: tuple


def logical_id(sequence, duplicate_every=0):
    if sequence < 0 or duplicate_every == 1 or duplicate_every < 0:
        raise ValueError("negative sequence or invalid duplicate interval")
    return sequence - (sequence + 1) // duplicate_every if duplicate_every >= 2 else sequence


def unique_count(config):
    return logical_id(config["records"] - 1, config.get("duplicate_every", 0)) + 1


def generate_record(record_id, config):
    symbols, depth, seed = config["symbols"], config["depth"], config["seed"]
    symbol_index, cycle = record_id % symbols, record_id // symbols
    x = record_id + seed
    last = 100000 + symbol_index * 17 + (cycle + seed) % 10000
    values = (last, last * (100 + x % 1000), 100 + x % 1000, 99 + x % 1000,
              x % 100, x % 3, last - 2, last - 10, last + 20, last - 20,
              last - 3, last - 1, x % 10000)
    present = ((1 << 17) - 1) & ~(1 << 12)
    nulls = 0
    if config.get("optional_fields", False):
        present |= 1 << 12
        if record_id % 2:
            present &= ~(1 << 4)
        elif record_id % 3 == 0:
            nulls |= 1 << 4
    books = (tuple(last + level + 1 for level in range(depth)),
             tuple(last - level - 1 for level in range(depth)),
             tuple(1000 + x % 97 + level for level in range(depth)),
             tuple(2000 + x % 89 + level for level in range(depth)))
    return Record(f"{600000 + symbol_index}.SH", TIME_BASE + cycle * 1000,
                  present, nulls, values, books)


def encode_record(record):
    symbol = record.symbol.encode("ascii")
    out = bytearray(b"RCD1" + struct.pack("<H", len(symbol)) + symbol)
    out += struct.pack("<QII", record.time, record.present, record.nulls)
    for index, value in enumerate(record.values):
        if record.present & (1 << index) and not record.nulls & (1 << index):
            out += struct.pack("<q", value)
    for index, book in enumerate(record.book, 13):
        if record.present & (1 << index) and not record.nulls & (1 << index):
            out += struct.pack("<B", len(book))
            for value in book:
                out += struct.pack("<q", value)
    return bytes(out)


def decode_record(raw):
    """Strict canonical decoder: rejects truncation, unknown mask bits, and tails."""
    offset = 0

    def take(fmt):
        nonlocal offset
        size = struct.calcsize(fmt)
        if offset + size > len(raw):
            raise RunError("truncated canonical record")
        result = struct.unpack_from(fmt, raw, offset)
        offset += size
        return result

    if len(raw) > 65536 or raw[:4] != b"RCD1":
        raise RunError("invalid canonical magic or size")
    offset = 4
    symbol_size, = take("<H")
    if offset + symbol_size > len(raw):
        raise RunError("invalid canonical symbol size")
    try:
        symbol = raw[offset:offset + symbol_size].decode("utf-8", errors="strict")
    except UnicodeError as exc:
        raise RunError("invalid UTF-8 canonical symbol") from exc
    offset += symbol_size
    timestamp, present, nulls = take("<QII")
    if present >> 17 or nulls & ~present:
        raise RunError("invalid canonical presence/null masks")
    values = []
    for index in range(13):
        values.append(take("<q")[0] if present & (1 << index) and not nulls & (1 << index) else 0)
    books = []
    depth = None
    for index in range(13, 17):
        book = ()
        if present & (1 << index) and not nulls & (1 << index):
            count, = take("<B")
            if count > 10 or (depth is not None and count != depth):
                raise RunError("canonical book exceeds supported depth or non-null depths differ")
            depth = count
            book = tuple(take("<q")[0] for _ in range(count))
        books.append(book)
    if offset != len(raw):
        raise RunError("trailing canonical bytes")
    return Record(symbol, timestamp, present, nulls, tuple(values), tuple(books))


def fixed_decimal(value):
    sign = "-" if value < 0 else ""
    value = abs(value)
    return f"{sign}{value // 10000}.{value % 10000:04d}"


def record_json(record):
    fields = []
    for index, (name, value, scaled) in enumerate(zip(SCALARS, record.values, SCALED)):
        if not record.present & (1 << index):
            continue
        encoded = "null" if record.nulls & (1 << index) else fixed_decimal(value) if scaled else str(value)
        fields.append(json.dumps(name) + ":" + encoded)
    for index, (name, values) in enumerate(zip(BOOKS, record.book), 13):
        if not record.present & (1 << index):
            continue
        if record.nulls & (1 << index):
            encoded = "null"
        else:
            convert = fixed_decimal if index < 15 else str
            encoded = "[" + ",".join(convert(value) for value in values) + "]"
        fields.append(json.dumps(name) + ":" + encoded)
    return ('{"symbol":' + json.dumps(record.symbol) + ',"time":' + str(record.time)
            + ',"quoter":{' + ",".join(fields) + "}}")


def frame(kind, sequence, payload=b""):
    return FRAME.pack(b"IQB1", 1, kind, 0, sequence, len(payload), zlib.crc32(payload) & 0xffffffff) + payload


def fnv_update(value, raw):
    for byte in raw:
        value = ((value ^ byte) * FNV_PRIME) & MASK64
    return value


def write_input(path, config, stop=None):
    """Write one controlled raw input; checksum includes only canonical records."""
    all_checksum = unique_checksum = FNV_OFFSET
    previous = None
    batch = []
    batch_start = 0
    digest = hashlib.sha256()
    size = 0
    raw_payload_bytes = canonical_bytes = 0
    observed_symbols = set()
    full_baseline = 0
    baseline_mask = ((1 << 12) - 1) | (15 << 13)
    with Path(path).open("xb") as output:
        def emit(data):
            nonlocal size
            output.write(data)
            digest.update(data)
            size += len(data)

        def emit_batch():
            nonlocal raw_payload_bytes
            if config["input"] == "json":
                payload = ('{"ticks":[' + ",".join(record_json(r) for r, _ in batch) + "]}").encode()
                kind = 1
            else:
                payload = b"".join(struct.pack("<I", len(raw)) + raw for _, raw in batch)
                kind = 2
            if len(payload) > 4 * 1024 * 1024:
                raise RunError("generated frame exceeds 4MiB; reduce batch_records")
            raw_payload_bytes += len(payload)
            emit(frame(kind, batch_start, payload))

        for sequence in range(config["records"]):
            if stop is not None and sequence % config["batch_records"] == 0:
                stop.check()
            record_id = logical_id(sequence, config["duplicate_every"])
            record = generate_record(record_id, config)
            observed_symbols.add(record.symbol)
            if record.present & baseline_mask == baseline_mask and not record.nulls & baseline_mask and all(len(book) == 5 for book in record.book):
                full_baseline += 1
            raw = encode_record(record)
            canonical_bytes += len(raw)
            all_checksum = fnv_update(all_checksum, raw)
            if record_id != previous:
                # The duplicate formula is monotone, so equal IDs are adjacent.
                unique_checksum = all_checksum if config["duplicate_every"] == 0 else fnv_update(unique_checksum, raw)
            previous = record_id
            if not batch:
                batch_start = sequence
            batch.append((record, raw))
            if len(batch) == config["batch_records"]:
                emit_batch()
                batch.clear()
        if batch:
            emit_batch()
        emit(frame(3, config["records"]))
    return {"path": str(Path(path).resolve()), "sha256": digest.hexdigest(), "bytes": size,
            "records": config["records"], "unique_records": unique_count(config),
            "raw_payload_bytes": raw_payload_bytes, "canonical_bytes": canonical_bytes,
            "checksum_all": f"{all_checksum:016x}", "checksum_unique": f"{unique_checksum:016x}",
            "generator": "independent-python-record.hpp-formula", "input": config["input"],
            "source": {"kind": "synthetic", "provenance": "deterministic record.hpp formula, not production capture", "authorized": False},
            "coverage": {"records": config["records"], "observed_symbols": len(observed_symbols),
                         "full_five_level_baseline_records": full_baseline,
                         "all_records_full_five_level_baseline": full_baseline == config["records"]}}


def read_frames(path):
    """Strict test/debug iterator for our experimental transport, including END."""
    with Path(path).open("rb") as source:
        while True:
            header = source.read(FRAME.size)
            if not header:
                return
            if len(header) != FRAME.size:
                raise RunError("truncated frame header")
            magic, version, kind, reserved, sequence, size, crc = FRAME.unpack(header)
            if magic != b"IQB1" or version != 1 or reserved or kind not in (1, 2, 3, 4) or size > 4 * 1024 * 1024:
                raise RunError("invalid experimental frame header")
            payload = source.read(size)
            if len(payload) != size or zlib.crc32(payload) & 0xffffffff != crc:
                raise RunError("truncated frame or CRC mismatch")
            yield kind, sequence, payload


def verify_export(path, config):
    """Compare the *full* unique canonical BLOB set, without a giant BLOB cache.

    Every row must byte-equal independently generated canonical(id), and every
    expected id must occur. Therefore set equality is exact, not a checksum or
    count-only approximation. Segment physical duplicates are reported separately.
    """
    expected_count = unique_count(config)
    seen = bytearray(expected_count)
    physical = unique = 0
    symbol_counts = {}
    first_time = last_time = None
    with Path(path).open() as rows:
        for line_number, line in enumerate(rows, 1):
            if len(line) > 200000:
                raise RunError(f"export line {line_number} too large")
            try:
                row = json.loads(line)
                if not isinstance(row, dict) or set(row) != {"symbol", "time", "hex"}:
                    raise ValueError("expected symbol,time,hex")
                raw = bytes.fromhex(row["hex"])
                record = decode_record(raw)
                if type(row["time"]) is not int or (row["symbol"], row["time"]) != (record.symbol, record.time):
                    raise ValueError("export metadata disagrees with canonical bytes")
                delta = record.time - TIME_BASE
                if delta < 0 or delta % 1000 or not record.symbol.endswith(".SH"):
                    raise ValueError("unexpected symbol or time")
                symbol_index = int(record.symbol[:-3]) - 600000
                if not 0 <= symbol_index < config["symbols"]:
                    raise ValueError("symbol outside configured universe")
                record_id = delta // 1000 * config["symbols"] + symbol_index
                if not 0 <= record_id < expected_count:
                    raise ValueError("record id outside expected range")
                if raw != encode_record(generate_record(record_id, config)):
                    raise ValueError("full canonical bytes differ from independent oracle")
            except (ValueError, TypeError, KeyError, UnicodeError, struct.error, RunError) as exc:
                raise RunError(f"export line {line_number}: {exc}") from exc
            physical += 1
            if not seen[record_id]:
                seen[record_id] = 1
                unique += 1
                symbol_counts[record.symbol] = symbol_counts.get(record.symbol, 0) + 1
                first_time = record.time if first_time is None else min(first_time, record.time)
                last_time = record.time if last_time is None else max(last_time, record.time)
    if unique != expected_count:
        missing = []
        for i, present in enumerate(seen):
            if not present:
                missing.append(i)
                if len(missing) == 10:
                    break
        raise RunError(f"export missing {expected_count - unique} canonical records; first IDs {missing}")
    if config["backend"] in ("sqlite", "sqlite_compact", "rocksdb") and physical != unique:
        raise RunError("full-record database export contains duplicate logical records")
    return {"passed": True, "method": "full-unique-canonical-blob-set-equality",
            "expected_unique": expected_count, "actual_unique": unique,
            "physical_rows": physical, "physical_duplicates": physical - unique,
            "symbol_counts": symbol_counts, "first_time": first_time, "last_time": last_time}


def counter(metrics, key):
    value = metrics.get("metrics", {}).get("counters", {}).get(key)
    if type(value) is not int or value < 0:
        raise RunError(f"missing or invalid counter {key}")
    return value


def validate_metrics(source, receiver, config, input_info):
    for role, data in (("source", source), ("receiver", receiver)):
        if data.get("role") != role or data.get("status") not in ("PASS", "ok", "success"):
            raise RunError(f"{role} failed or returned invalid metrics status: {data.get('status')}")
        if data.get("error"):
            raise RunError(f"{role} reported error: {data['error']}")
        elapsed = data.get("elapsed_s")
        if type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed <= 0:
            raise RunError(f"invalid {role} elapsed_s")
        for key, value in data.get("metrics", {}).get("counters", {}).items():
            if type(value) is not int or value < 0:
                raise RunError(f"invalid {role} counter {key}")
            if ("error" in key or "fail" in key or key == "rejected") and value:
                raise RunError(f"nonzero {role} error counter {key}={value}")
        for name, stage in data.get("metrics", {}).get("stages", {}).items():
            required = ("count", "sum_ns", "max_ns", "p50_upper_ns", "p95_upper_ns", "p99_upper_ns")
            if not isinstance(stage, dict) or any(type(stage.get(k)) is not int or stage[k] < 0 for k in required):
                raise RunError(f"invalid stage histogram {role}.{name}")
            if not stage["p50_upper_ns"] <= stage["p95_upper_ns"] <= stage["p99_upper_ns"] <= stage["max_ns"]:
                raise RunError(f"unordered stage percentiles {role}.{name}")
    records = config["records"]
    expected_unique = unique_count(config)
    expected_dropped = records - expected_unique if config["hash"] == "dedup" else 0
    expected_forwarded = records - expected_dropped
    checks = {"source_generated": counter(source, "source_generated"),
              "source_sent": counter(source, "source_sent"),
              **{name: counter(receiver, name) for name in
                 ("received", "parsed", "forwarded", "dedup_dropped", "committed_attempts", "sink_consumed")}}
    expected = {"source_generated": records, "source_sent": records, "received": records,
                "parsed": records, "dedup_dropped": expected_dropped,
                "forwarded": expected_forwarded,
                "committed_attempts": 0 if config["backend"] == "null" else expected_forwarded,
                "sink_consumed": expected_forwarded if config["backend"] == "null" else 0}
    for name, value in checks.items():
        if value != expected[name]:
            raise RunError(f"counter mismatch {name}: got {value}, expected {expected[name]}")
    if "bytes" in input_info:
        if counter(source, "source_bytes") != input_info["bytes"]:
            raise RunError("source wire byte count differs from controlled input file")
        receiver_counters = receiver["metrics"]["counters"]
        if "network_bytes" in receiver_counters and receiver_counters["network_bytes"] != input_info["bytes"]:
            raise RunError("receiver wire byte count differs from controlled input file")
    ack = source.get("ack")
    if not isinstance(ack, dict) or type(ack.get("durable")) is not bool:
        raise RunError("missing typed ACK durable flag")
    durable = config["backend"] != "null" and config["synchronous"] == "FULL"
    if ack["durable"] != durable:
        raise RunError(f"ACK durable mismatch for {config['backend']}")
    if "ack" in receiver and receiver["ack"] != ack:
        raise RunError("source and receiver ACK metrics disagree")
    store = receiver.get("store", {})
    if "durable" in store and (type(store["durable"]) is not bool or store["durable"] != durable):
        raise RunError("store durable metadata disagrees with ACK")
    for key in ("received", "forwarded", "committed_attempts"):
        if type(ack.get(key)) is not int or ack[key] != expected[key]:
            raise RunError(f"ACK counter mismatch {key}")
    checksum = input_info["checksum_unique" if config["hash"] == "dedup" else "checksum_all"]
    if config["backend"] == "null" and receiver["metrics"]["counters"].get("ablation_work_schema") == 1:
        if counter(receiver, "null_consumed_rows") != expected_forwarded:
            raise RunError("null sink consumed row count differs from independent forwarded count")
        if counter(receiver, "null_checksum") != int(checksum, 16):
            raise RunError("null sink byte checksum differs from independent canonical stream")
        if store.get("consumed_rows") != expected_forwarded or store.get("checksum") != int(checksum, 16):
            raise RunError("null store metadata differs from independent canonical stream")
    if ack.get("checksum_fnv1a64") != checksum:
        raise RunError(f"ACK canonical checksum mismatch: got {ack.get('checksum_fnv1a64')}, expected {checksum}")
    return {"passed": True, "counters": checks, "ack_durable": durable,
            "checksum_fnv1a64": checksum, "expected_unique": expected_unique}


def linear_slope(points):
    if len(points) < 2:
        return None
    xs, ys = zip(*points)
    xmean, ymean = sum(xs) / len(xs), sum(ys) / len(ys)
    variance = sum((x - xmean) ** 2 for x in xs)
    return sum((x - xmean) * (y - ymean) for x, y in points) / variance if variance else None


def backlog_summary(progress, source_progress=(), backend="sqlite"):
    samples = []
    source_points = []
    for point in source_progress:
        timestamp = point.get("monotonic_ns")
        sent = point.get("metrics", {}).get("counters", {}).get("source_sent")
        if type(timestamp) is not int or type(sent) is not int or timestamp < 0 or sent < 0:
            raise RunError("invalid source progress timestamp/counters")
        if source_points and (timestamp < source_points[-1][0] or sent < source_points[-1][1]):
            raise RunError("source progress went backwards")
        source_points.append((timestamp, sent))
    source_index = 0
    for point in progress:
        counters = point.get("metrics", {}).get("counters", {})
        elapsed = point.get("elapsed_s")
        if type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed < 0:
            raise RunError("invalid progress elapsed_s")
        received, committed = counters.get("received"), counters.get("committed_attempts")
        forwarded, dropped = counters.get("forwarded"), counters.get("dedup_dropped")
        consumed = counters.get("sink_consumed", 0)
        if any(type(value) is not int or value < 0 for value in (received, committed, forwarded, dropped, consumed)):
            raise RunError("missing or invalid progress counters")
        completed = consumed if backend == "null" else committed
        if completed > forwarded or forwarded + dropped > received:
            raise RunError("impossible progress counter ordering")
        if samples and (elapsed < samples[-1]["elapsed_s"] or received < samples[-1]["received"]
                        or committed < samples[-1]["committed_attempts"]):
            raise RunError("progress counters/time went backwards")
        sample = {"elapsed_s": elapsed, "received": received, "forwarded": forwarded,
                  "committed_attempts": committed, "sink_consumed": consumed,
                  "queue_gap": forwarded - completed, "receive_commit_gap": received - dropped - completed}
        timestamp = point.get("monotonic_ns")
        if type(timestamp) is int and source_points:
            # Linear interpolation between bracketing periodic source samples.
            # Sampling skew is disclosed; this is not per-record queue telemetry.
            while source_index + 1 < len(source_points) and source_points[source_index + 1][0] <= timestamp:
                source_index += 1
            before = source_points[source_index]
            if before[0] <= timestamp:
                estimated_sent = float(before[1])
                if source_index + 1 < len(source_points):
                    after = source_points[source_index + 1]
                    if after[0] > before[0]:
                        fraction = (timestamp - before[0]) / (after[0] - before[0])
                        estimated_sent += fraction * (after[1] - before[1])
                sample["source_sent_interpolated"] = estimated_sent
                sample["source_receive_gap_estimated"] = estimated_sent - received
                sample["source_commit_gap_estimated"] = estimated_sent - dropped - completed
        samples.append(sample)
    # Exclude initial warm-up second and the very last (possibly drain-only) point.
    steady = samples[1:-1]
    slope = linear_slope([(point["elapsed_s"], point["receive_commit_gap"]) for point in steady])
    total_points = [(point["elapsed_s"], point["source_commit_gap_estimated"]) for point in steady
                    if "source_commit_gap_estimated" in point]
    return {"samples": samples, "steady_samples": len(steady),
            "gap_slope_records_per_s": slope,
            "source_commit_gap_slope_records_per_s": linear_slope(total_points),
            "source_aligned_steady_samples": len(total_points),
            "max_receive_commit_gap": max((p["receive_commit_gap"] for p in samples), default=None),
            "measurement": "receiver backlog exact at samples; source backlog linearly interpolated by monotonic_ns",
            "sampling_caveat": "periodic samples, not per-record queues; interpolation can yield small negative gaps"}
