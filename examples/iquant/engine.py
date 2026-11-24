"""Feed committed IQTS archive ranges to the variable-record Engine worker."""

from collections import namedtuple
import hashlib
import json
import logging
import queue
import socket
import subprocess
import threading

from iquant import protocol as wire

LOG = logging.getLogger(__name__)
MAX_GROUP_BYTES = wire.MAX_DATA + wire.HEADER.size
MAX_GROUP_FRAMES = 32
Range = namedtuple("Range", "start before end last")
HotRange = namedtuple("HotRange", "extent packets")
Record = namedtuple("Record", "symbol timestamp snapshot")
Frame = namedtuple("Frame", "first last begin end crc status detail records")


def parse_quotes(payload):
    """Parse the SDK envelope and keep each original snapshot's JSON bytes."""
    text = payload.decode("utf-8")
    decoder = json.JSONDecoder()
    whitespace = " \t\r\n"
    size = len(text)

    def skip(at):
        while at < size and text[at] in whitespace:
            at += 1
        return at

    def expect(at, char):
        at = skip(at)
        if at == size or text[at] != char:
            raise ValueError("expected {!r} in iQuant snapshot batch".format(char))
        return at + 1

    at = skip(expect(0, "["))
    records = []
    if at < size and text[at] != "]":
        while True:
            start = skip(expect(at, "["))
            symbol, at = decoder.raw_decode(text, start)
            start = skip(expect(at, ","))
            snapshot, at = decoder.raw_decode(text, start)
            if not isinstance(symbol, str) or not symbol or "\0" in symbol or not isinstance(snapshot, dict):
                raise ValueError("expected a nonempty security code and a snapshot object")
            timestamp = snapshot.get("time")
            if type(timestamp) is not int or not 0 <= timestamp < 2 ** 63:
                timestamp = -1
            records.append(Record(symbol.encode("utf-8"), timestamp, text[start:at].encode("utf-8")))
            at = skip(expect(at, "]"))
            if at >= size or text[at] != ",":
                break
            at += 1
    if skip(expect(at, "]")) != size:
        raise ValueError("trailing data in iQuant snapshot batch")
    return records


def decode_frame(begin, header, parsed, payload):
    kind, first, last, size, crc = parsed
    frame = wire.parse_frame(header, parsed, payload)
    records, detail = [], b""
    status = "gap" if kind == wire.GAP else "data"
    if kind == wire.DATA:
        try:
            records = parse_quotes(frame.payload)
        except (ValueError, RecursionError) as exc:
            status = "quarantine"
            detail = str(exc).encode("utf-8", errors="replace")[:1024] or b"invalid snapshot"
    return Frame(first, last, begin, begin + wire.HEADER.size + size, crc, status, detail, records)


def read_group(path, start, before, limit):
    frames = []
    with path.open("rb") as source:
        source.seek(start)
        while len(frames) < MAX_GROUP_FRAMES and source.tell() < limit:
            begin = source.tell()
            if limit - begin < wire.HEADER.size:
                raise ValueError("durable iQuant boundary splits a header")
            header = source.read(wire.HEADER.size)
            if len(header) != wire.HEADER.size:
                raise ValueError("iQuant raw is shorter than its durable boundary")
            kind, first, last, size, _ = parsed = wire.parse_header(header)
            end = begin + wire.HEADER.size + size
            if kind not in (wire.DATA, wire.GAP) or first != before + 1 or end > limit:
                raise ValueError("iQuant raw sequence or durable boundary is inconsistent")
            if frames and end - start > MAX_GROUP_BYTES:
                break
            frames.append(decode_frame(begin, header, parsed, source.read(size)))
            before = last
    return frames


class Worker:
    def __init__(self, binary, root):
        self.process = subprocess.Popen([binary, "--worker", "."], cwd=str(root), stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE)
        try:
            if self.readline() != b"READY":
                raise RuntimeError("iQuant Engine failed to initialize")
        except BaseException:
            self.close()
            raise

    def readline(self):
        reply = self.process.stdout.readline(128)
        if not reply.endswith(b"\n"):
            raise RuntimeError("iQuant Engine closed its result pipe")
        return reply.rstrip(b"\n")

    def cursor(self, key):
        self.process.stdin.write(("P {}\n".format(key)).encode("ascii"))
        self.process.stdin.flush()
        return tuple(map(int, self.readline().split()))

    def write(self, key, frames, sweep):
        dest = self.process.stdin
        dest.write(("W {} {} {}\n".format(key, int(sweep), len(frames))).encode("ascii"))
        for frame in frames:
            dest.write(("{} {} {} {} {} {} {} {}\n".format(
                frame.first, frame.last, frame.begin, frame.end, frame.crc, frame.status,
                len(frame.records), len(frame.detail))).encode("ascii"))
            dest.write(frame.detail)
            for record in frame.records:
                dest.write(("{} {} {}\n".format(len(record.symbol), record.timestamp,
                                                len(record.snapshot))).encode("ascii"))
                dest.write(record.symbol)
                dest.write(record.snapshot)
        dest.flush()
        if self.readline() != b"OK":
            raise RuntimeError("iQuant Engine returned an invalid commit response")

    def close(self):
        try:
            self.process.stdin.close()
        except OSError:
            pass
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        finally:
            self.process.stdout.close()


def redis_command(parts):
    chunks = [b"*%d\r\n" % len(parts)]
    for part in parts:
        chunks.extend((b"$%d\r\n" % len(part), part, b"\r\n"))
    return b"".join(chunks)


class RedisPublisher:
    def __init__(self, port, key):
        self.port, self.key = port, key.encode("utf-8")
        self.queue = queue.Queue(maxsize=4)
        self.stopping = threading.Event()
        self.thread = threading.Thread(target=self.run, name="iqts-redis")
        self.dropped = 0
        self.thread.start()

    def offer(self, key, frames):
        try:
            self.queue.put_nowait((key, frames))
        except queue.Full:
            self.dropped += len(frames)
            if self.dropped == len(frames) or self.dropped % 256 < len(frames):
                LOG.warning("Redis hot queue full, skipped %s IQTS frames in total", self.dropped)

    def run(self):
        conn = replies = None
        failures = 0
        try:
            while not self.stopping.is_set():
                try:
                    key, frames = self.queue.get(timeout=0.1)
                except queue.Empty:
                    continue
                commands = []
                for frame in frames:
                    if frame.status != "data" or not frame.records:
                        continue
                    args = [b"XADD", self.key, b"MAXLEN", b"~", b"10000", b"*",
                            b"source_stream", key.encode("ascii"), b"batch", str(frame.first).encode("ascii"),
                            b"schema", b"iqts-snapshot/v1"]
                    for index, record in enumerate(frame.records):
                        suffix = str(index).encode("ascii")
                        args.extend((b"symbol:" + suffix, record.symbol, b"snapshot:" + suffix, record.snapshot))
                    commands.append(redis_command(args))
                if not commands:
                    continue
                try:
                    if conn is None:
                        conn = socket.create_connection(("127.0.0.1", self.port), timeout=0.25)
                        replies = conn.makefile("rb")
                    conn.sendall(b"".join(commands))
                    for _ in commands:
                        header = replies.readline(256)
                        if not header.startswith(b"$") or not header.endswith(b"\r\n"):
                            raise OSError("Redis rejected IQTS XADD")
                        length = int(header[1:-2])
                        if not 1 <= length <= 64 or len(replies.read(length + 2)) != length + 2:
                            raise OSError("invalid Redis stream ID")
                    failures = 0
                except (OSError, ValueError) as exc:
                    failures += 1
                    if failures == 1 or failures % 100 == 0:
                        LOG.warning("Redis hot publication failed (%s consecutive groups): %s", failures, exc)
                    if replies is not None:
                        replies.close()
                    if conn is not None:
                        conn.close()
                    conn = replies = None
        finally:
            if replies is not None:
                replies.close()
            if conn is not None:
                conn.close()

    def close(self):
        self.stopping.set()
        self.thread.join()


class ArchiveEngine:
    def __init__(self, archive, binary, redis_port=None, redis_key=None):
        self.archive, self.binary = archive, binary
        self.stop = threading.Event()
        self.worker = None
        self.redis = None
        try:
            namespace = hashlib.sha256(str(archive.root.parent).encode("utf-8")).hexdigest()[:24]
            self.redis = RedisPublisher(redis_port, redis_key or "tickshelf:iqts:" + namespace) if redis_port else None
            self.thread = threading.Thread(target=self.run, name="iqts-engine")
            self.thread.start()
        except BaseException:
            if self.redis is not None:
                self.redis.close()
            raise

    def consume(self, key, extent, sweep, packets=()):
        if packets:
            frames, begin = [], extent.start
            for packet in packets:
                header = packet[:wire.HEADER.size]
                frame = decode_frame(begin, header, wire.parse_header(header), packet[wire.HEADER.size:])
                frames.append(frame)
                begin = frame.end
        else:
            frames = read_group(self.archive.root / (key + ".raw"), extent.start, extent.before, extent.end)
        if not frames:
            raise ValueError("empty IQTS range submitted for replay")
        if frames[-1].end == extent.end and frames[-1].last != extent.last:
            raise ValueError("IQTS range ends at a different source sequence")
        self.worker.write(key, frames, sweep)
        for frame in frames:
            if frame.status == "quarantine":
                LOG.warning("quarantined IQTS stream=%s batch=%s offset=%s: %s",
                            key, frame.first, frame.begin, frame.detail.decode("utf-8", errors="replace"))
        return frames

    def run(self):
        cursors, live_seen = {}, {}
        turn = 0
        while not self.stop.is_set():
            try:
                if self.worker is None:
                    self.worker = Worker(self.binary, self.archive.root.parent)
                with self.archive.changed:
                    frontiers = dict(self.archive.frontiers)
                keys = sorted(frontiers)
                worked = False
                if keys:
                    # One recent group, then one historical group in round-robin order.
                    for key in reversed(keys):
                        _, _, recent = frontiers[key]
                        if recent is None or recent.extent.end == live_seen.get(key):
                            continue
                        hot = recent.extent
                        if key not in cursors:
                            cursors[key] = self.worker.cursor(key)
                        offset, seq = cursors[key]
                        if offset >= hot.end:
                            live_seen[key] = hot.end
                            continue
                        start, before = max(offset, hot.start), seq if offset >= hot.start else hot.before
                        sweep = start == offset
                        frames = self.consume(key, Range(start, before, hot.end, hot.last), sweep,
                                              recent.packets if start == hot.start else ())
                        if sweep:
                            cursors[key] = frames[-1].end, frames[-1].last
                        # A large receiver group can be partially imported here.
                        # The rest remains in the historical sweep.
                        live_seen[key] = hot.end
                        if self.redis is not None:
                            self.redis.offer(key, frames)
                        worked = True
                        break
                    for index in range(len(keys)):
                        key = keys[(turn + index) % len(keys)]
                        end, last, _ = frontiers[key]
                        if key not in cursors:
                            cursors[key] = self.worker.cursor(key)
                        offset, seq = cursors[key]
                        if not 0 <= offset <= end or not 0 <= seq <= last or (offset == end) != (seq == last):
                            raise ValueError("iQuant Engine cursor is ahead of or inconsistent with raw")
                        if offset == end:
                            continue
                        frames = self.consume(key, Range(offset, seq, end, last), True)
                        cursors[key] = frames[-1].end, frames[-1].last
                        turn = (turn + index + 1) % len(keys)
                        worked = True
                        break
                if not worked:
                    with self.archive.changed:
                        self.archive.changed.wait(timeout=0.1)
            except (OSError, RuntimeError, ValueError) as exc:
                LOG.error("iQuant Engine paused; raw retained for retry: %s", exc)
                if self.worker is not None:
                    self.worker.close()
                self.worker = None
                cursors.clear()
                self.stop.wait(1)

    def close(self):
        self.stop.set()
        with self.archive.changed:
            self.archive.changed.notify_all()
        self.thread.join()
        if self.worker is not None:
            self.worker.close()
        if self.redis is not None:
            self.redis.close()
