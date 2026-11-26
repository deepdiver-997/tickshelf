"""Export, verify and restore sealed iQuant raw + Parquet archive parts."""

import argparse
from collections import namedtuple
from contextlib import closing
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "examples"))
from iquant import protocol as wire
from iquant.receiver import STREAM_NAME, _lock_output, _sync_directory

import pyarrow as pa
import pyarrow.parquet as pq


FORMAT = "iqts-archive/v1"
PART_NAME = re.compile(r"([0-9]{20})-([0-9]{20})")
FILES = ("raw.zst", "quotes.parquet", "frames.parquet")
MIB = 1024 * 1024
MAX_FRAMES = 8192
BUFFER = MIB
Frame = namedtuple("Frame", "first last start end crc status rows detail")
Part = namedtuple("Part", "path meta")
PRICE = pa.decimal128(18, 6)
AMOUNT = pa.decimal128(28, 8)
QUOTES = pa.schema([
    ("stream", pa.string()), ("batch_seq", pa.uint64()), ("ordinal", pa.int32()),
    ("symbol", pa.string()), ("source_time_ms", pa.int64()),
    ("last_price", PRICE), ("ask_price", pa.list_(PRICE)), ("bid_price", pa.list_(PRICE)),
    ("volume", pa.int64()), ("ask_volume", pa.list_(pa.int64())), ("bid_volume", pa.list_(pa.int64())),
    ("amount", AMOUNT), ("projection_errors", pa.list_(pa.string())),
], metadata={b"tickshelf.schema": b"iqts-archive-quotes/v1"})
FRAMES = pa.schema([
    ("stream", pa.string()), ("first_seq", pa.uint64()), ("last_seq", pa.uint64()),
    ("byte_start", pa.int64()), ("byte_end", pa.int64()), ("crc32", pa.uint32()),
    ("status", pa.string()), ("record_count", pa.int32()), ("detail", pa.string()),
], metadata={b"tickshelf.schema": b"iqts-archive-frames/v1"})


def sync_file(path):
    with path.open("r+b") as file:
        os.fsync(file.fileno())


def transfer(source, destination=None):
    sha = hashlib.sha256()
    size = 0
    while True:
        data = source.read(BUFFER)
        if not data:
            return {"bytes": size, "sha256": sha.hexdigest()}
        size += len(data)
        sha.update(data)
        if destination is not None:
            destination.write(data)


def fingerprint(path):
    with path.open("rb") as file:
        return transfer(file)


def seq_key(value):
    return "{:020}".format(value)


def fixed(value, precision, scale):
    if type(value) not in (int, Decimal):
        raise ValueError("expected an exact number")
    decimal = Decimal(value)
    if not decimal.is_finite() or (decimal and decimal.adjusted() >= precision - scale):
        raise ValueError("outside DECIMAL range")
    if decimal and decimal.adjusted() < -scale:
        raise ValueError("outside DECIMAL scale")
    with localcontext() as ctx:
        ctx.prec = precision + 2
        quantized = decimal.quantize(Decimal(1).scaleb(-scale))
    if quantized != decimal:
        raise ValueError("value cannot be represented exactly")
    return quantized


def int64(value):
    if type(value) is int and -(2 ** 63) <= value < 2 ** 63:
        return value
    raise ValueError("outside INT64 or non-integer")


def project(row):
    stream, batch_seq, ordinal, symbol, source_time_ms, snapshot = row
    if type(ordinal) is not int or not 0 <= ordinal < wire.MAX_DATA:
        raise ValueError("invalid archive quote ordinal")
    original = json.loads(snapshot, parse_float=Decimal, parse_constant=Decimal)
    errors = []

    def convert(key, scalar, array=False):
        value = original.get(key)
        if value is None:
            return None
        try:
            if array:
                if not isinstance(value, list):
                    raise ValueError("expected list")
                return [None if item is None else scalar(item) for item in value]
            return scalar(value)
        except (ValueError, InvalidOperation):
            errors.append(key)
            return None

    def price(value):
        return fixed(value, PRICE.precision, PRICE.scale)

    return {
        "stream": stream, "batch_seq": int(batch_seq), "ordinal": ordinal,
        "symbol": symbol, "source_time_ms": source_time_ms,
        "last_price": convert("lastPrice", price),
        "ask_price": convert("askPrice", price, array=True),
        "bid_price": convert("bidPrice", price, array=True),
        "volume": convert("volume", int64),
        "ask_volume": convert("askVol", int64, array=True),
        "bid_volume": convert("bidVol", int64, array=True),
        "amount": convert("amount", lambda v: fixed(v, AMOUNT.precision, AMOUNT.scale)),
        "projection_errors": errors,
    }


def read_catalog(root):
    result = {}
    for stream_dir in sorted(root.iterdir()):
        stream = stream_dir.name
        if not stream_dir.is_dir() or not STREAM_NAME.fullmatch(stream):
            continue
        parts = []
        before, offset = 0, 0
        for path in sorted(stream_dir.iterdir()):
            match = PART_NAME.fullmatch(path.name)
            if not match:
                continue
            first, last = map(int, match.groups())
            with (path / "manifest.json").open(encoding="utf-8") as file:
                meta = json.load(file)
            if (not isinstance(meta, dict) or meta["format"] != FORMAT or meta["stream"] != stream
                    or meta["first_seq"] != first or meta["last_seq"] != last
                    or first != before + 1 or not first <= last < 2 ** 64
                    or meta["raw_start"] != offset or not offset < meta["raw_end"]
                    or meta["raw"]["bytes"] != meta["raw_end"] - offset
                    or not first <= meta["tail_frame_first_seq"] <= last
                    or not 0 <= meta["tail_frame_crc32"] < 2 ** 32
                    or not isinstance(meta["files"], dict) or meta["files"].keys() != set(FILES)):
                raise ValueError("invalid or discontinuous archive manifest: {}".format(path))
            for name in FILES:
                if (path / name).stat().st_size != meta["files"][name]["bytes"]:
                    raise ValueError("archive file length mismatch: {}".format(path / name))
            before, offset = last, meta["raw_end"]
            parts.append(Part(path, meta))
        if parts:
            result[stream] = parts
    return result


def next_group(db, stream, before, limit_seq, max_bytes):
    frames = []
    sql = ("SELECT first_seq,last_seq,byte_start,byte_end,crc32,status,record_count,detail FROM raw_frames "
           "WHERE stream=? AND first_seq>? AND first_seq<=? ORDER BY first_seq LIMIT ?")
    for row in db.execute(sql, (stream, seq_key(before), seq_key(limit_seq), MAX_FRAMES)):
        frame = Frame(int(row[0]), int(row[1]), *row[2:])
        if frames and frame.end - frames[0].start > max_bytes:
            break
        frames.append(frame)
    return frames


def copy_raw(source, out, frames, before, offset):
    sha = hashlib.sha256()
    start = offset
    source.seek(start)
    for expected in frames:
        header = source.read(wire.HEADER.size)
        if len(header) != wire.HEADER.size:
            raise ValueError("raw prefix ends before its history cursor")
        kind, first, last, size, crc = parsed = wire.parse_header(header)
        end = offset + wire.HEADER.size + size
        if ((first, last, offset, end, crc) != expected[:5] or first != before + 1
                or kind not in (wire.DATA, wire.GAP)
                or (kind == wire.GAP) != (expected.status == "gap")):
            raise ValueError("raw frame differs from history at byte {}".format(offset))
        payload = source.read(size)
        wire.parse_frame(header, parsed, payload)
        for data in (header, payload):
            sha.update(data)
            out.write(data)
        offset, before = end, last
    return {"bytes": offset - start, "sha256": sha.hexdigest()}


def write_quotes(path, db, stream, first, last):
    sql = (
        "SELECT stream,batch_seq,ordinal,symbol,source_time_ms,snapshot FROM quotes "
        "WHERE stream=? AND batch_seq BETWEEN ? AND ? "
        "ORDER BY symbol,source_time_ms,batch_seq,ordinal"
    )
    with closing(db.execute(sql, (stream, seq_key(first), seq_key(last)))) as cursor:
        with pq.ParquetWriter(path, QUOTES, compression="zstd", store_decimal_as_integer=True,
                              write_page_checksum=True) as writer:
            rows = errors = 0
            while True:
                group = cursor.fetchmany(4096)
                if not group:
                    return rows, errors
                projected = [project(row) for row in group]
                rows += len(projected)
                errors += sum(bool(row["projection_errors"]) for row in projected)
                writer.write_table(pa.Table.from_pylist(projected, schema=QUOTES))


def publish_part(db, work, target, stream, frames, before, offset):
    first, last = frames[0].first, frames[-1].last
    final = target / (seq_key(first) + "-" + seq_key(last))
    if final.exists():
        raise ValueError("archive part already exists: {}".format(final))
    with tempfile.TemporaryDirectory(prefix=".pending-", dir=str(target)) as temporary:
        stage = Path(temporary) / "part"
        stage.mkdir()
        with (work / "raw" / (stream + ".raw")).open("rb") as source:
            with pa.output_stream(stage / "raw.zst", compression="zstd") as out:
                raw = copy_raw(source, out, frames, before, offset)
        rows, errors = write_quotes(stage / "quotes.parquet", db, stream, first, last)
        if rows != sum(frame.rows for frame in frames):
            raise ValueError("history quote count differs from its committed frames")
        frame_rows = [
            dict(zip(FRAMES.names, (stream, *frame)))
            for frame in frames
        ]
        pq.write_table(pa.Table.from_pylist(frame_rows, schema=FRAMES), stage / "frames.parquet",
                       compression="zstd", write_page_checksum=True)
        meta = {
            "format": FORMAT, "stream": stream, "first_seq": first, "last_seq": last,
            "raw_start": offset, "raw_end": frames[-1].end, "raw": raw,
            "tail_frame_first_seq": frames[-1].first, "tail_frame_crc32": frames[-1].crc,
            "frame_count": len(frames), "quote_count": rows, "projection_error_rows": errors,
            "gap_frames": sum(f.status == "gap" for f in frames),
            "quarantined_frames": sum(f.status == "quarantine" for f in frames),
            "files": {},
        }
        for name in FILES:
            path = stage / name
            sync_file(path)
            meta["files"][name] = fingerprint(path)
        if pq.read_metadata(stage / "quotes.parquet").num_rows != rows:
            raise ValueError("sealed quotes Parquet row count mismatch")
        manifest = stage / "manifest.json"
        manifest.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        sync_file(manifest)
        _sync_directory(stage)
        stage.rename(final)
        _sync_directory(target)
    return meta


def export(work, root, max_bytes):
    catalog = read_catalog(root)
    db_path = work / "history.sqlite3"
    with closing(sqlite3.connect(db_path.as_uri() + "?mode=ro", uri=True)) as db:
        if (db.execute("PRAGMA application_id").fetchone()[0] != 0x54534951
                or db.execute("PRAGMA user_version").fetchone()[0] != 1
                or db.execute("SELECT * FROM record_schema").fetchall() != [("iqts-snapshot", 1)]):
            raise ValueError("unsupported iQuant history schema")
        cursors = {stream: (raw_end, int(last)) for stream, raw_end, last in
                   db.execute("SELECT stream,raw_end,last_seq FROM replay_cursor")}
        if catalog.keys() - cursors.keys():
            raise ValueError("source history is missing previously archived streams")
        for stream, (raw_end, limit_seq) in sorted(cursors.items()):
            if not STREAM_NAME.fullmatch(stream):
                raise ValueError("invalid history stream ID")
            existing = catalog.get(stream, [])
            before, offset = 0, 0
            if existing:
                previous = existing[-1].meta
                before, offset = previous["last_seq"], previous["raw_end"]
                boundary = db.execute(
                    "SELECT last_seq,byte_end,crc32 FROM raw_frames WHERE stream=? AND first_seq=?",
                    (stream, seq_key(previous["tail_frame_first_seq"])),
                ).fetchone()
                if boundary != (seq_key(before), offset, previous["tail_frame_crc32"]):
                    raise ValueError("source history does not match the archived boundary")
            if before > limit_seq or offset > raw_end or (before == limit_seq) != (offset == raw_end):
                raise ValueError("source history cursor is behind or conflicts with archive")
            target = root / stream
            if not target.exists():
                target.mkdir()
                _sync_directory(root)
            while before < limit_seq:
                frames = next_group(db, stream, before, limit_seq, max_bytes)
                if (not frames or frames[-1].last > limit_seq or frames[-1].end > raw_end
                        or (frames[-1].last == limit_seq) != (frames[-1].end == raw_end)):
                    raise ValueError("source history does not cover its replay cursor")
                meta = publish_part(db, work, target, stream, frames, before, offset)
                before, offset = meta["last_seq"], meta["raw_end"]
                print(json.dumps(meta, sort_keys=True), flush=True)


def check_part(part, restored=None):
    meta = part.meta
    for name in FILES:
        if fingerprint(part.path / name) != meta["files"][name]:
            raise ValueError("archive checksum mismatch: {}".format(part.path / name))
    with pa.input_stream(part.path / "raw.zst", compression="zstd") as source:
        if transfer(source, restored) != meta["raw"]:
            raise ValueError("decoded raw checksum mismatch: {}".format(part.path))
    for name, expected, schema in (("quotes.parquet", meta["quote_count"], QUOTES),
                                   ("frames.parquet", meta["frame_count"], FRAMES)):
        path = part.path / name
        found = pq.read_schema(path)
        if (pq.read_metadata(path).num_rows != expected or not found.equals(schema)
                or found.metadata != schema.metadata):
            raise ValueError("archive Parquet schema or row count mismatch: {}".format(path))


def restore(catalog, destination):
    if destination.exists():
        raise ValueError("restore requires a new workspace path")
    if not catalog:
        raise ValueError("archive contains no published parts")
    with tempfile.TemporaryDirectory(prefix=".restore-", dir=str(destination.parent)) as temporary:
        stage = Path(temporary) / "workspace"
        raw = stage / "raw"
        raw.mkdir(parents=True)
        for stream, parts in catalog.items():
            with (raw / (stream + ".raw")).open("xb") as out:
                for part in parts:
                    check_part(part, out)
                out.flush()
                os.fsync(out.fileno())
        _sync_directory(raw)
        _sync_directory(stage)
        stage.rename(destination)
        _sync_directory(destination.parent)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    write = commands.add_parser("export", help="export the archived prefix up to the current history cursor")
    write.add_argument("--work-dir", type=Path, required=True)
    write.add_argument("--archive-dir", type=Path)
    write.add_argument("--max-part-mib", type=int, default=64, choices=range(4, 257),
                       metavar="4..256")
    verify = commands.add_parser("verify", help="check archive hashes and Parquet metadata")
    verify.add_argument("--archive-dir", type=Path, required=True)
    recover = commands.add_parser("restore-raw", help="restore verified raw into a new workspace")
    recover.add_argument("--archive-dir", type=Path, required=True)
    recover.add_argument("--work-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    work = args.work_dir.expanduser().resolve() if hasattr(args, "work_dir") else None
    root = args.archive_dir.expanduser().resolve() if args.archive_dir else work / "archive"
    try:
        if args.action == "export":
            if not (work / "history.sqlite3").is_file():
                raise ValueError("iQuant workspace has no history.sqlite3")
            root.mkdir(exist_ok=True)
            _sync_directory(root.parent)
        with _lock_output(root / ".archive.lock"):
            if args.action == "export":
                export(work, root, args.max_part_mib * MIB)
            else:
                catalog = read_catalog(root)
                if not catalog:
                    raise ValueError("archive contains no published parts")
                if args.action == "restore-raw":
                    restore(catalog, work)
                else:
                    for parts in catalog.values():
                        for part in parts:
                            check_part(part)
                print("Verified {} archive parts".format(sum(map(len, catalog.values()))))
    except (OSError, ValueError, RuntimeError, sqlite3.Error, KeyError, InvalidOperation) as exc:
        print("iquant_archive: {}".format(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
