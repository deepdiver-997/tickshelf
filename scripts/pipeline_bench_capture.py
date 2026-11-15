"""Bounded, independent raw-JSON capture packer and exact disk-backed oracle.

This is the experimental record.hpp schema, not a vendor wire protocol. A source
label is a declaration, never evidence that a file is genuine production data.
No C++ parser/generator is used to derive expected canonical bytes or FIFO state.
JSONL lines (including their line endings) become unmodified IQB1 payloads.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import sqlite3
import struct
import tempfile
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path

from pipeline_bench_support import (BOOKS, FNV_OFFSET, SCALARS, SCALED, counter,
                                    decode_record, fnv_update, frame)
from stack_runtime import RunError

MAX_PAYLOAD = 4 * 1024 * 1024
MAX_RECORD = 65536
MAX_BATCH = 8192
MAX_FRAMES = 100000
MAX_MANIFEST = 16 * 1024 * 1024
I64_MIN, I64_MAX = -(1 << 63), (1 << 63) - 1
U64_MAX = (1 << 64) - 1
WS = " \t\r\n"


def _guard(check=None, deadline=None, stop=None, budget=None):
    if deadline is not None and time.monotonic() >= deadline:
        raise RunError("capture/oracle deadline exceeded")
    for hook in (check, stop, budget):
        if hook is not None:
            action = hook if callable(hook) else hook.check
            if action() is False:
                raise RunError("capture/oracle budget exhausted")


def _hash_file(path, check=None, deadline=None):
    digest = hashlib.sha256()
    size = 0
    with Path(path).open("rb") as source:
        while True:
            _guard(check, deadline)
            chunk = source.read(1024 * 1024)
            if not chunk:
                return digest.hexdigest(), size
            digest.update(chunk)
            size += len(chunk)


def _private_open(path, mode="wb"):
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    return os.fdopen(os.open(path, flags, 0o600), mode)


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise RunError(f"duplicate JSON key {key!r}")
        # Python accepts lone escaped surrogates; C++ requires Unicode scalars.
        key.encode("utf-8", "strict")
        result[key] = value
    return result


def _number(token):
    # Saturate the exponent, as the bounded C++ grammar does, before Decimal
    # construction. In particular, 0e999...(many digits) is valid exact zero.
    mantissa, sep, exponent = token.lower().partition("e")
    if sep:
        negative = exponent.startswith("-")
        digits = exponent.lstrip("+-").lstrip("0")
        cap = MAX_PAYLOAD + 32
        value = cap if len(digits) > 7 else min(int(digits or "0"), cap)
        token = mantissa + "e" + str(-value if negative else value)
    return Decimal(token)


def _constant(token):
    raise RunError(f"non-JSON numeric constant {token}")


_DECODER = json.JSONDecoder(object_pairs_hook=_pairs, parse_int=_number,
                            parse_float=_number, parse_constant=_constant)


def _integer(value, scaled=False, unsigned=False):
    if not isinstance(value, Decimal) or not value.is_finite():
        raise RunError("expected exact JSON number, not bool/string/null")
    sign, digits, exponent = value.as_tuple()
    if unsigned and sign:
        raise RunError("time must not be negative (including negative zero)")
    first = next((i for i, digit in enumerate(digits) if digit), len(digits))
    if first == len(digits):
        return 0
    last = len(digits)
    while last > first + 1 and digits[last - 1] == 0:
        last -= 1
    shift = exponent + (4 if scaled else 0) + len(digits) - last
    if shift < 0:
        raise RunError("precision loss: expected exact integer or multiple of 1e-4")
    if shift > 20 or last - first + shift > 20:
        raise RunError("numeric overflow")
    result = 0
    for digit in digits[first:last]:
        result = result * 10 + digit
    result *= 10 ** shift
    if sign:
        result = -result
    if not (0 <= result <= U64_MAX if unsigned else I64_MIN <= result <= I64_MAX):
        raise RunError("numeric overflow")
    return result


def _canonical(tick):
    if not isinstance(tick, dict) or set(tick) != {"symbol", "time", "quoter"}:
        raise RunError("tick requires exactly symbol,time,quoter; unknown fields are schema errors")
    if not isinstance(tick["symbol"], str):
        raise RunError("symbol must be a string")
    symbol = tick["symbol"].encode("utf-8", "strict")
    if len(symbol) > 65535:
        raise RunError("symbol exceeds u16 UTF-8 byte length")
    timestamp = _integer(tick["time"], unsigned=True)
    quoter = tick["quoter"]
    if not isinstance(quoter, dict):
        raise RunError("quoter must be an object")
    if "pvolume" in quoter and "pvolumn" in quoter:
        raise RunError("pvolume/pvolumn alias collision")
    names = SCALARS + BOOKS
    present = nulls = 0
    fields = {}
    depth = None
    values = [0] * len(SCALARS)
    books = [()] * len(BOOKS)
    for key, value in quoter.items():
        name = "pvolumn" if key == "pvolume" else key
        if name not in names:
            raise RunError(f"unknown quoter field {key!r}; not silently discarded")
        index = names.index(name)
        present |= 1 << index
        if value is None:
            nulls |= 1 << index
            fields[index] = b""
        elif index < len(SCALARS):
            values[index] = _integer(value, SCALED[index])
            fields[index] = struct.pack("<q", values[index])
        else:
            if not isinstance(value, list) or len(value) > 10:
                raise RunError("book must be an array of depth 0..10 or null")
            if depth is not None and depth != len(value):
                raise RunError("non-null book depths differ")
            depth = len(value)
            book_index = index - len(SCALARS)
            books[book_index] = tuple(_integer(level, book_index < 2) for level in value)
            fields[index] = bytes([len(value)]) + b"".join(struct.pack("<q", n) for n in books[book_index])
    raw = (b"RCD1" + struct.pack("<H", len(symbol)) + symbol
           + struct.pack("<QII", timestamp, present, nulls)
           + b"".join(fields.get(i, b"") for i in range(len(names))))
    if len(raw) > MAX_RECORD:
        raise RunError("canonical record exceeds 64KiB")
    return raw, symbol, timestamp, present, nulls, values, books


def parse_payload(payload, check=None, deadline=None):
    """Yield (canonical bytes, identity, masks, values, books, raw tick size).

    Each tick is decoded separately so its original byte extent, rather than a
    reserialized approximation, is checked against the C++ 64KiB record limit.
    Root syntax is deliberately closed: only one ticks array is accepted.
    """
    if len(payload) > MAX_PAYLOAD:
        raise RunError("JSON frame exceeds 4MiB")
    try:
        text = payload.decode("utf-8", "strict")
        position = 0

        def whitespace():
            nonlocal position
            while position < len(text) and text[position] in WS:
                position += 1

        def expect(char):
            nonlocal position
            whitespace()
            if not text.startswith(char, position):
                raise RunError(f"expected {char!r} in ticks envelope")
            position += len(char)

        expect("{")
        whitespace()
        key, position = _DECODER.raw_decode(text, position)
        if key != "ticks":
            raise RunError("root requires only ticks; unknown fields are schema errors")
        expect(":")
        expect("[")
        count = binary_size = 0
        whitespace()
        if not text.startswith("]", position):
            while True:
                _guard(check, deadline)
                start = position
                # A legal tick cannot have more than MAX_RECORD characters.
                # Bound even the parser's invalid-input object allocation before
                # evaluating the stricter UTF-8 byte extent below.
                tick, relative_end = _DECODER.raw_decode(text[start:start + MAX_RECORD + 1])
                position = start + relative_end
                raw_size = len(text[start:position].encode("utf-8"))
                if raw_size > MAX_RECORD:
                    raise RunError("raw JSON tick exceeds 64KiB")
                count += 1
                if count > MAX_BATCH:
                    raise RunError("JSON batch exceeds 8192 records")
                canonical = _canonical(tick)
                binary_size += 4 + len(canonical[0])
                if binary_size > MAX_PAYLOAD:
                    raise RunError("canonical binary batch exceeds 4MiB")
                yield (*canonical, raw_size)
                whitespace()
                if text.startswith("]", position):
                    break
                expect(",")
                whitespace()
        expect("]")
        expect("}")
        whitespace()
        if position != len(text):
            raise RunError("unknown/duplicate root field or trailing JSON content")
    except (UnicodeError, ValueError, TypeError, RecursionError, InvalidOperation) as exc:
        raise RunError(f"invalid raw JSON capture payload: {exc}") from exc


def _normalize_source(source):
    source = dict(source or {})
    kind = source.get("kind", source.get("source_kind"))
    if kind not in ("synthetic", "production_shape", "production_capture"):
        raise RunError("source kind must explicitly be synthetic, production_shape, or production_capture")
    if "kind" in source and "source_kind" in source and source["kind"] != source["source_kind"]:
        raise RunError("conflicting source kind declarations")
    provenance = source.get("provenance", "")
    authorized = source.get("authorized", False)
    if not isinstance(provenance, str) or type(authorized) is not bool:
        raise RunError("source provenance must be text and authorized must be bool")
    if kind != "synthetic" and (not authorized or not provenance.strip()):
        raise RunError("production labels require an authorized, named source declaration")
    source.pop("source_kind", None)
    source.update(kind=kind, provenance=provenance, authorized=authorized,
                  authenticity_verified=False, production_qualified=False,
                  declaration_caveat="Labels/authorization are declarations; hashes do not prove authenticity.",
                  replay_timing="content-only or fixed-rate replay; no capture arrival-time sidecar verified")
    return source


def _normalize_policy(policy):
    policy = dict(policy or {})
    if "max_entries" not in policy and "dedup_max_entries" in policy:
        policy["max_entries"] = policy["dedup_max_entries"]
    required = ("max_entries", "fixed_charge_bytes", "fifo_limit_bytes")
    if type(policy.get("dedup")) is not bool or any(type(policy.get(k)) is not int or policy[k] < 0 for k in required):
        raise RunError("explicit compiled FIFO policy required: dedup bool and nonnegative max_entries/fixed_charge_bytes/fifo_limit_bytes")
    if policy.get("hash_algorithm", "sha256") not in ("sha256", "fnv1a64"):
        raise RunError("unknown hash_algorithm")
    if policy.get("hash_mode", "batch") not in ("record", "batch"):
        raise RunError("unknown hash_mode")
    policy.setdefault("hash_algorithm", "sha256")
    policy.setdefault("hash_mode", "batch")
    policy["policy_caveat"] = "fixed charge must be supplied from the tested binary's verifiable compiled policy, not inferred from actual_dropped"
    return policy


def _connect(path, check=None, deadline=None):
    db = sqlite3.connect(str(path))
    db.execute("PRAGMA cache_size=-2048")
    db.execute("PRAGMA temp_store=FILE")
    db.execute("PRAGMA mmap_size=0")
    if check is not None or deadline is not None:
        def progress():
            try:
                _guard(check, deadline)
                return 0
            except Exception:
                # sqlite cannot propagate Python callback exceptions; interrupt
                # even a long GROUP BY/external sort instead of waiting for it.
                return 1
        db.set_progress_handler(progress, 1000)
    return db


def _hist(db, kind, value):
    db.execute("INSERT INTO histogram VALUES(?,?,1) ON CONFLICT(kind,value) DO UPDATE SET n=n+1", (kind, value))


def _distribution(db, kind):
    count, total, low, high = db.execute(
        "SELECT coalesce(sum(n),0),coalesce(sum(value*n),0),min(value),max(value) FROM histogram WHERE kind=?", (kind,)).fetchone()
    result = {"count": count, "min": low, "max": high, "mean": total / count if count else None}
    if count:
        targets = {key: max(1, (count * percentile + 99) // 100) for key, percentile in (("p50", 50), ("p95", 95), ("p99", 99))}
        seen = 0
        for value, n in db.execute("SELECT value,n FROM histogram WHERE kind=? ORDER BY value", (kind,)):
            seen += n
            for key, target in targets.items():
                if key not in result and seen >= target:
                    result[key] = value
    return result


@dataclass(frozen=True)
class CaptureDataset:
    raw_path: Path
    packed_path: Path
    input_manifest_path: Path
    oracle_path: Path
    input_info: dict

    @property
    def records(self):
        return self.input_info["records"]

    @property
    def unique_records(self):
        return self.input_info["unique_records"]

    @property
    def frames(self):
        return self.input_info["total_frames"]

    @property
    def expected_forwarded(self):
        return self.input_info["expected_forwarded"]

    @property
    def summary(self):
        return self.input_info

    def verify_inputs(self, check=None, deadline=None):
        for path, sha_key, size_key in ((self.raw_path, "raw_input_sha256", "raw_input_bytes"),
                                        (self.packed_path, "sha256", "bytes"),
                                        (self.input_manifest_path, "manifest_sha256", "manifest_bytes"),
                                        (self.oracle_path, "oracle_sha256", "oracle_bytes")):
            digest, size = _hash_file(path, check, deadline)
            if digest != self.input_info[sha_key] or size != self.input_info[size_key]:
                raise RunError(f"capture/oracle input changed: {path}")
        return {"passed": True, "method": "before/after SHA-256 of raw, packed, manifest and exact oracle"}

    def verify_export(self, path, config=None, **kwargs):
        return verify_export(path, self, config, **kwargs)

    def validate_metrics(self, source, receiver, config, **kwargs):
        return validate_metrics(source, receiver, config, self, **kwargs)

    def iter_expected(self, *, forwarded_only=False, check=None, deadline=None):
        """Ordered full bytes, intentionally not a list-sized memory cache."""
        db = _connect(self.oracle_path, check, deadline)
        try:
            query = "SELECT seq,raw,forwarded FROM ordered" + (" WHERE forwarded=1" if forwarded_only else "") + " ORDER BY seq"
            for sequence, raw, forwarded in db.execute(query):
                _guard(check, deadline)
                yield sequence, raw, bool(forwarded)
        finally:
            db.close()


def prepare_capture(input_path, work_dir, *, source_config, policy, check=None,
                    deadline=None, max_line_bytes=MAX_PAYLOAD, max_records=20000000,
                    stop=None, budget=None):
    """Preflight, pack raw lines, and build an independent exact/FIFO oracle.

    Memory is bounded by one <=4MiB line, one <=64KiB record, small histograms,
    and SQLite's 2MiB cache. All unbounded cardinalities reside in a private
    on-disk index. A failed/aborted preparation removes only its private output.
    """
    if type(max_line_bytes) is not int or not 1 <= max_line_bytes <= MAX_PAYLOAD:
        raise RunError("max_line_bytes must be in 1..4MiB")
    if max_records is not None and (type(max_records) is not int or max_records <= 0):
        raise RunError("max_records must be a positive integer or None")
    source_config, policy = _normalize_source(source_config), _normalize_policy(policy)
    hook = lambda: _guard(check, deadline, stop, budget)
    raw_path = Path(input_path).resolve(strict=True)
    if not raw_path.is_file():
        raise RunError("capture input must be a regular file, not an unbounded stream")
    raw_sha, raw_bytes = _hash_file(raw_path, hook, deadline)
    work_dir = Path(work_dir).resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix="capture-", dir=work_dir))
    os.chmod(directory, 0o700)
    packed, manifest_path, oracle = directory / "input.iqb", directory / "input_manifest.json", directory / "oracle.sqlite3"
    db = None
    try:
        db = _connect(oracle, hook, deadline)
        os.chmod(oracle, 0o600)
        db.executescript("""
            CREATE TABLE canonical(raw BLOB PRIMARY KEY,n_input INTEGER NOT NULL,n_forwarded INTEGER NOT NULL,first_seq INTEGER,last_seq INTEGER) WITHOUT ROWID;
            CREATE TABLE ordered(seq INTEGER PRIMARY KEY,raw BLOB NOT NULL,forwarded INTEGER NOT NULL);
            CREATE TABLE fifo(seq INTEGER PRIMARY KEY,raw BLOB UNIQUE NOT NULL,charge INTEGER NOT NULL);
            CREATE TABLE symbols(symbol BLOB PRIMARY KEY,n INTEGER NOT NULL,max_time BLOB NOT NULL) WITHOUT ROWID;
            CREATE TABLE identities(symbol BLOB,time BLOB,raw BLOB,PRIMARY KEY(symbol,time,raw)) WITHOUT ROWID;
            CREATE TABLE histogram(kind TEXT,value INTEGER,n INTEGER,PRIMARY KEY(kind,value)) WITHOUT ROWID;
            CREATE TABLE frames(number INTEGER PRIMARY KEY,sequence INTEGER,records INTEGER,payload_sha256 TEXT);
        """)
        fields = {name: {"missing": 0, "null": 0, "nonnull": 0, "zero": 0} for name in SCALARS + BOOKS}
        depths = {name: [0] * 11 for name in BOOKS}
        records = frame_count = dropped = evictions = entries = cache_bytes = payload_bytes = full_five = 0
        global_out_of_order = symbol_out_of_order = identity_variants = 0
        last_timestamp = None
        all_checksum = forwarded_checksum = FNV_OFFSET
        wire_digest, streamed_raw = hashlib.sha256(), hashlib.sha256()
        wire_bytes = 0
        with raw_path.open("rb") as source, _private_open(packed) as output:
            def emit(data):
                nonlocal wire_bytes
                output.write(data)
                wire_digest.update(data)
                wire_bytes += len(data)

            while True:
                hook()
                payload = source.readline(max_line_bytes + 1)
                if not payload:
                    break
                if len(payload) > max_line_bytes:
                    raise RunError(f"capture line {frame_count + 1} exceeds bounded frame limit")
                if frame_count >= MAX_FRAMES:
                    raise RunError("capture exceeds 100000 data frames supported by manifest reader")
                streamed_raw.update(payload)
                frame_records = 0
                frame_sequence = records
                for canonical, symbol, timestamp, present, nulls, values, books, raw_size in parse_payload(payload, hook, deadline):
                    hook()
                    if max_records is not None and records >= max_records:
                        raise RunError(f"capture exceeds max_records/record-limit {max_records}")
                    tkey = timestamp.to_bytes(8, "big")
                    old = db.execute("SELECT last_seq FROM canonical WHERE raw=?", (canonical,)).fetchone()
                    if old:
                        _hist(db, "duplicate_distance_records", records - old[0])
                    forwarded = not (policy["dedup"] and db.execute("SELECT 1 FROM fifo WHERE raw=?", (canonical,)).fetchone())
                    all_checksum = fnv_update(all_checksum, canonical)
                    if forwarded:
                        forwarded_checksum = fnv_update(forwarded_checksum, canonical)
                        if policy["dedup"]:
                            charge = len(canonical) + policy["fixed_charge_bytes"]
                            db.execute("INSERT INTO fifo VALUES(?,?,?)", (records, canonical, charge))
                            entries += 1
                            cache_bytes += charge
                            payload_bytes += len(canonical)
                            while entries > policy["max_entries"] or cache_bytes > policy["fifo_limit_bytes"]:
                                hook()
                                sequence, length, charge = db.execute("SELECT seq,length(raw),charge FROM fifo ORDER BY seq LIMIT 1").fetchone()
                                db.execute("DELETE FROM fifo WHERE seq=?", (sequence,))
                                entries -= 1
                                cache_bytes -= charge
                                payload_bytes -= length
                                evictions += 1
                    else:
                        dropped += 1
                    db.execute("INSERT INTO ordered VALUES(?,?,?)", (records, canonical, int(forwarded)))
                    db.execute("INSERT INTO canonical VALUES(?,1,?,?,?) ON CONFLICT(raw) DO UPDATE SET n_input=n_input+1,n_forwarded=n_forwarded+excluded.n_forwarded,last_seq=excluded.last_seq",
                               (canonical, int(forwarded), records, records))
                    previous_symbol = db.execute("SELECT max_time FROM symbols WHERE symbol=?", (symbol,)).fetchone()
                    if previous_symbol and tkey < previous_symbol[0]:
                        symbol_out_of_order += 1
                    db.execute("INSERT INTO symbols VALUES(?,1,?) ON CONFLICT(symbol) DO UPDATE SET n=n+1,max_time=max(max_time,excluded.max_time)", (symbol, tkey))
                    different = db.execute("SELECT 1 FROM identities WHERE symbol=? AND time=? AND raw!=? LIMIT 1", (symbol, tkey, canonical)).fetchone()
                    added = db.execute("INSERT OR IGNORE INTO identities VALUES(?,?,?)", (symbol, tkey, canonical)).rowcount
                    if different and added:
                        identity_variants += 1
                    if last_timestamp is not None and timestamp < last_timestamp:
                        global_out_of_order += 1
                    last_timestamp = timestamp
                    active = present & ~nulls
                    if active & ((1 << 12) - 1) == (1 << 12) - 1 and all(active & (1 << (13 + j)) and len(book) == 5 for j, book in enumerate(books)):
                        full_five += 1
                    for i, name in enumerate(SCALARS + BOOKS):
                        state = "missing" if not present & (1 << i) else "null" if nulls & (1 << i) else "nonnull"
                        fields[name][state] += 1
                        if state == "nonnull":
                            if i < 13 and values[i] == 0:
                                fields[name]["zero"] += 1
                            if i >= 13:
                                depths[name][len(books[i - 13])] += 1
                    for kind, value in (("raw_record_bytes", raw_size), ("canonical_record_bytes", len(canonical)), ("symbol_utf8_bytes", len(symbol))):
                        _hist(db, kind, value)
                    records += 1
                    frame_records += 1
                    if records % 1024 == 0:
                        db.commit()
                db.execute("INSERT INTO frames VALUES(?,?,?,?)", (frame_count, frame_sequence, frame_records, hashlib.sha256(payload).hexdigest()))
                _hist(db, "raw_payload_bytes", len(payload))
                _hist(db, "records_per_frame", frame_records)
                emit(frame(1, frame_sequence, payload))
                frame_count += 1
            if not frame_count or not records:
                raise RunError("capture must contain at least one record; blank lines are not JSON envelopes")
            emit(frame(3, records))
        if streamed_raw.hexdigest() != raw_sha or _hash_file(raw_path, hook, deadline) != (raw_sha, raw_bytes):
            raise RunError("raw capture changed while preparing")
        if _hash_file(packed, hook, deadline) != (wire_digest.hexdigest(), wire_bytes):
            raise RunError("packed capture changed while preparing")
        unique = db.execute("SELECT count(*) FROM canonical").fetchone()[0]
        observed_symbols = db.execute("SELECT count(*) FROM symbols").fetchone()[0]
        top = [{"symbol": symbol.decode("utf-8"), "records": n, "fraction": n / records}
               for symbol, n in db.execute("SELECT symbol,n FROM symbols ORDER BY n DESC,symbol LIMIT 20")]
        conflicting_keys = db.execute("SELECT count(*) FROM (SELECT 1 FROM identities GROUP BY symbol,time HAVING count(*)>1)").fetchone()[0]
        coverage = {"records": records, "observed_symbols": observed_symbols,
                    "full_five_level_baseline_records": full_five,
                    "all_records_full_five_level_baseline": full_five == records,
                    "fields": fields, "book_depth_counts_0_to_10": depths,
                    "symbol_top20": top, "symbol_max_fraction": top[0]["fraction"],
                    "natural_duplicate_records": records - unique,
                    "same_symbol_time_different_content_keys": conflicting_keys,
                    "same_symbol_time_additional_content_variants": identity_variants,
                    "out_of_order_global_adjacent": global_out_of_order,
                    "out_of_order_per_symbol_watermark": symbol_out_of_order,
                    "distributions": {kind: _distribution(db, kind) for kind in
                                      ("raw_record_bytes", "canonical_record_bytes", "symbol_utf8_bytes", "raw_payload_bytes", "records_per_frame", "duplicate_distance_records")}}
        info = {"path": str(packed), "sha256": wire_digest.hexdigest(), "input_sha256": wire_digest.hexdigest(),
                "bytes": wire_bytes, "records": records, "unique_records": unique,
                "total_frames": frame_count, "raw_input_path": str(raw_path),
                "raw_input_sha256": raw_sha, "raw_input_bytes": raw_bytes,
                "input_manifest": str(manifest_path), "oracle_path": str(oracle),
                "input": "json", "generator": "independent-python-raw-json-exact-decimal",
                "source": source_config, "policy": policy, "coverage": coverage,
                "checksum_all": f"{all_checksum:016x}", "checksum_all_forwarded": f"{forwarded_checksum:016x}",
                "checksum_unique": f"{forwarded_checksum:016x}",
                "expected_forwarded": records - dropped, "expected_dedup_dropped": dropped,
                "expected_dedup_evictions": evictions, "expected_dedup_entries": entries,
                "expected_dedup_payload_bytes": payload_bytes, "expected_dedup_cache_bytes": cache_bytes,
                "production_qualified": False}
        # Stream the manifest's frame list: its size must not scale Python RAM.
        with _private_open(manifest_path, "w") as manifest:
            head = {"schema_version": 1, "records": records, "total_frames": frame_count,
                    "input_sha256": info["sha256"], "raw_input_sha256": raw_sha,
                    "source": source_config, "policy": policy, "coverage": coverage}
            manifest.write(json.dumps(head, ensure_ascii=True, allow_nan=False)[:-1] + ',"frames":[')
            for number, sequence, n, digest in db.execute("SELECT * FROM frames ORDER BY number"):
                hook()
                if number:
                    manifest.write(",")
                manifest.write(json.dumps({"sequence": sequence, "records": n, "payload_sha256": digest}))
                if manifest.tell() + 3 > MAX_MANIFEST:
                    raise RunError("capture manifest exceeds supported 16MiB limit")
            manifest.write("]}\n")
        if manifest_path.stat().st_size > MAX_MANIFEST:
            raise RunError("capture manifest exceeds supported 16MiB limit")
        db.commit()
        db.close()
        db = None
        info["manifest_sha256"], info["manifest_bytes"] = _hash_file(manifest_path, hook, deadline)
        info["oracle_sha256"], info["oracle_bytes"] = _hash_file(oracle, hook, deadline)
        dataset = CaptureDataset(raw_path, packed, manifest_path, oracle, info)
        dataset.verify_inputs(hook, deadline)
        return dataset
    except BaseException:
        if db is not None:
            db.close()
        shutil.rmtree(directory)
        raise


def _export_rows(paths, check, deadline):
    if isinstance(paths, (str, os.PathLike)):
        paths = [paths]
    for path in paths:
        with Path(path).open("rb") as source:
            number = 0
            while True:
                _guard(check, deadline)
                # A max-sized symbol may be escaped as six ASCII bytes per byte.
                line = source.readline(8 * MAX_RECORD + 1)
                if not line:
                    break
                number += 1
                try:
                    if len(line) > 8 * MAX_RECORD:
                        raise RunError("export line exceeds bounded size")
                    row = json.loads(line.decode("utf-8", "strict"), object_pairs_hook=_pairs, parse_constant=_constant)
                    if not isinstance(row, dict) or set(row) != {"symbol", "time", "hex"}:
                        raise RunError("export requires exactly symbol,time,hex")
                    if not isinstance(row["hex"], str) or len(row["hex"]) > 2 * MAX_RECORD:
                        raise RunError("export hex exceeds canonical bound")
                    raw = bytes.fromhex(row["hex"])
                    record = decode_record(raw)
                    if type(row["time"]) is not int or row["symbol"] != record.symbol or row["time"] != record.time:
                        raise RunError("export metadata disagrees with full canonical bytes")
                    yield raw
                except (RunError, ValueError, TypeError, UnicodeError, RecursionError) as exc:
                    raise RunError(f"export {path} line {number}: {exc}") from exc


def verify_export(path, dataset, config=None, *, backend=None, check=None, deadline=None):
    """Exact unique physical rows for DBs; exact forwarded multiset for segment.

    path is the existing C++ export JSONL, or a sequence of such export files.
    This does not invoke/trust the C++ parser to construct any expected bytes.
    """
    backend = backend or (config or {}).get("backend")
    if backend not in ("sqlite", "sqlite_compact", "rocksdb", "segment", "segment_rocksdb"):
        raise RunError("capture export verification requires an explicit DB or segment backend")
    is_segment = backend in ("segment", "segment_rocksdb")
    dataset.verify_inputs(check, deadline)
    directory = Path(tempfile.mkdtemp(prefix="export-check-", dir=dataset.oracle_path.parent))
    db = _connect(directory / "observed.sqlite3", check, deadline)
    expected = _connect(dataset.oracle_path, check, deadline)
    physical = unique = 0
    try:
        db.execute("CREATE TABLE observed(raw BLOB PRIMARY KEY,n INTEGER NOT NULL) WITHOUT ROWID")
        for raw in _export_rows(path, check, deadline):
            row = expected.execute("SELECT n_forwarded FROM canonical WHERE raw=?", (raw,)).fetchone()
            if row is None or row[0] == 0:
                raise RunError("export contains unexpected full canonical bytes")
            seen = db.execute("SELECT n FROM observed WHERE raw=?", (raw,)).fetchone()
            n = (seen[0] if seen else 0) + 1
            limit = row[0] if is_segment else 1
            if n > limit:
                raise RunError("export physical multiplicity exceeds independently expected canonical multiset")
            db.execute("INSERT INTO observed VALUES(?,1) ON CONFLICT(raw) DO UPDATE SET n=n+1", (raw,))
            physical += 1
            unique += not bool(seen)
            if physical % 1024 == 0:
                db.commit()
        for raw, n in expected.execute("SELECT raw,n_forwarded FROM canonical WHERE n_forwarded>0"):
            _guard(check, deadline)
            seen = db.execute("SELECT n FROM observed WHERE raw=?", (raw,)).fetchone()
            if seen is None or seen[0] != (n if is_segment else 1):
                raise RunError("export missing expected canonical bytes or physical multiplicity")
        return {"passed": True, "method": "exact-full-canonical-forwarded-multiset" if is_segment else "exact-full-canonical-unique-physical-once",
                "expected_unique": dataset.unique_records, "actual_unique": unique,
                "physical_rows": physical, "physical_duplicates": physical - unique,
                "expected_physical_rows": dataset.expected_forwarded if is_segment else dataset.unique_records}
    finally:
        expected.close()
        db.close()
        shutil.rmtree(directory)


def validate_metrics(source, receiver, config, input_info_or_dataset, *, check=None, deadline=None):
    """Validate measured counters against *precomputed* raw-input FIFO results."""
    _guard(check, deadline)
    info = input_info_or_dataset.input_info if isinstance(input_info_or_dataset, CaptureDataset) else input_info_or_dataset
    policy = info["policy"]
    if policy["dedup"] != (config["hash"] == "dedup"):
        raise RunError("capture oracle/config dedup policy mismatch")
    for key, config_key in (("max_entries", "dedup_max_entries"), ("hash_algorithm", "hash_algorithm"), ("hash_mode", "hash_mode")):
        if config_key in config and config[config_key] != policy[key]:
            raise RunError(f"capture oracle/config {key} mismatch")
    for role, data in (("source", source), ("receiver", receiver)):
        _guard(check, deadline)
        if data.get("role") != role or data.get("status") not in ("PASS", "ok", "success") or data.get("error"):
            raise RunError(f"{role} failed or returned invalid metrics status")
        elapsed = data.get("elapsed_s")
        if type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed <= 0:
            raise RunError(f"invalid {role} elapsed_s")
        for key, value in data.get("metrics", {}).get("counters", {}).items():
            if type(value) is not int or value < 0:
                raise RunError(f"invalid {role} counter {key}")
            if ("error" in key or "fail" in key or key == "rejected") and value:
                raise RunError(f"nonzero {role} error counter {key}")
        for name, stage in data.get("metrics", {}).get("stages", {}).items():
            keys = ("count", "sum_ns", "max_ns", "p50_upper_ns", "p95_upper_ns", "p99_upper_ns")
            if not isinstance(stage, dict) or any(type(stage.get(k)) is not int or stage[k] < 0 for k in keys):
                raise RunError(f"invalid stage histogram {role}.{name}")
            if not stage["p50_upper_ns"] <= stage["p95_upper_ns"] <= stage["p99_upper_ns"] <= stage["max_ns"]:
                raise RunError(f"unordered stage percentiles {role}.{name}")
    n, forwarded = info["records"], info["expected_forwarded"]
    backend = config["backend"]
    expected = {"source_generated": n, "source_sent": n, "received": n, "parsed": n,
                "forwarded": forwarded, "dedup_dropped": info["expected_dedup_dropped"],
                "committed_attempts": 0 if backend == "null" else forwarded,
                "sink_consumed": forwarded if backend == "null" else 0}
    checks = {}
    for key, value in expected.items():
        actual = counter(source if key.startswith("source_") else receiver, key)
        if actual != value:
            raise RunError(f"counter mismatch {key}: got {actual}, expected {value}")
        checks[key] = actual
    if counter(source, "source_bytes") != info["bytes"]:
        raise RunError("source wire byte count differs from raw capture packing")
    if "network_bytes" in receiver["metrics"]["counters"] and counter(receiver, "network_bytes") != info["bytes"]:
        raise RunError("receiver wire byte count differs from raw capture packing")
    if policy["dedup"]:
        for key in ("dedup_evictions", "dedup_entries", "dedup_payload_bytes", "dedup_cache_bytes"):
            # C++ omits untouched zero counters; a missing nonzero is an error.
            actual = receiver["metrics"]["counters"].get(key, 0)
            if type(actual) is not int or actual != info["expected_" + key]:
                raise RunError(f"independent FIFO counter mismatch {key}")
            checks[key] = actual
    ack = source.get("ack")
    durable = backend != "null" and config["synchronous"] == "FULL"
    if not isinstance(ack, dict) or type(ack.get("durable")) is not bool or ack["durable"] != durable:
        raise RunError("missing/invalid ACK durable flag")
    if "ack" in receiver and receiver["ack"] != ack:
        raise RunError("source and receiver ACK metrics disagree")
    store = receiver.get("store", {})
    if "durable" in store and (type(store["durable"]) is not bool or store["durable"] != durable):
        raise RunError("store durable metadata disagrees with ACK")
    for key in ("received", "forwarded", "committed_attempts"):
        if type(ack.get(key)) is not int or ack[key] != expected[key]:
            raise RunError(f"ACK counter mismatch {key}")
    checksum = info["checksum_all_forwarded"]
    if ack.get("checksum_fnv1a64") != checksum:
        raise RunError("ACK ordered all-forwarded canonical FNV checksum mismatch")
    return {"passed": True, "counters": checks, "ack_durable": durable,
            "checksum_fnv1a64": checksum, "expected_unique": info["unique_records"],
            "expected_forwarded": forwarded, "method": "independent-raw-decimal-exact-bytes-and-compiled-policy-FIFO"}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("preflight", "pack", "oracle"))
    parser.add_argument("input", type=Path)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--source-config", type=Path, required=True, help="JSON source declaration, never authenticity proof")
    parser.add_argument("--policy", type=Path, required=True, help="JSON compiled dedup policy from tested binary")
    parser.add_argument("--export", type=Path, action="append")
    parser.add_argument("--backend", choices=("sqlite", "sqlite_compact", "rocksdb", "segment", "segment_rocksdb"))
    parser.add_argument("--timeout-s", type=float, default=120)
    arguments = parser.parse_args(argv)
    if not math.isfinite(arguments.timeout_s) or arguments.timeout_s <= 0:
        parser.error("timeout must be finite and positive")
    deadline = time.monotonic() + arguments.timeout_s
    def declaration(path):
        with path.open("rb") as source:
            payload = source.read(65537)
        if len(payload) > 65536:
            raise RunError("source/policy declaration exceeds 64KiB")
        return json.loads(payload.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_constant)
    dataset = prepare_capture(arguments.input, arguments.work_dir,
                              source_config=declaration(arguments.source_config),
                              policy=declaration(arguments.policy), deadline=deadline)
    result = dict(dataset.input_info)
    if arguments.command == "oracle":
        if not arguments.export or not arguments.backend:
            parser.error("oracle requires --export and --backend")
        result["export_verification"] = verify_export(arguments.export, dataset, backend=arguments.backend, deadline=deadline)
    print(json.dumps(result, ensure_ascii=True, allow_nan=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RunError, OSError, sqlite3.Error, ValueError) as exc:
        raise SystemExit(f"capture error: {exc}")
