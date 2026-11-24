"""Reconnect to IQTS, append raw records, and ACK only after fsync."""

import argparse
import json
import logging
import math
import os
from pathlib import Path
import re
import socket
import sys
import tempfile
import threading
import time
import uuid

if not __package__:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from iquant import protocol as wire

LOG = logging.getLogger(__name__)
STREAM_NAME = re.compile(r"[0-9a-f]{32}")


class ArchiveError(RuntimeError):
    pass


def _sync_directory(path):
    # Windows has no portable directory fsync in the Python standard library.
    if os.name == "nt":
        return
    fd = os.open(str(path), os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _lock_output(path):
    lock = path.open("a+b")
    try:
        if os.name == "nt":
            import msvcrt
            lock.seek(0)
            msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        lock.close()
        raise ArchiveError("cannot lock capture directory {}: {}".format(path.parent, exc)) from exc
    return lock


def _progress_mark(value):
    if not isinstance(value, dict):
        raise ValueError("missing progress entry")
    data, resolved = value["data_watermark"], value["resume_watermark"]
    if type(data) is not int or type(resolved) is not int or not 0 <= data <= resolved < 2 ** 64:
        raise ValueError("invalid progress watermarks")
    return data, resolved


def _recover(path, saved):
    mark = wire.Watermark()
    try:
        with path.open("r+b") as raw:
            while True:
                offset = raw.tell()
                header = raw.read(wire.HEADER.size)
                if len(header) != wire.HEADER.size:
                    torn = bool(header)
                    break
                parsed = wire.parse_header(header)
                kind, first, last, size, _ = parsed
                if kind not in (wire.DATA, wire.GAP) or first != mark.resolved + 1:
                    raise wire.ProtocolError("raw records do not form a contiguous resolved prefix")
                payload = raw.read(size)
                if len(payload) != size:
                    torn = True
                    break
                wire.parse_frame(header, parsed, payload)
                mark.complete(kind, first, last)
            if saved is not None and (saved[0] > mark.data or saved[1] > mark.resolved):
                raise ArchiveError("progress is ahead of verified raw data in {}".format(path))
            if torn:
                raw.truncate(offset)
                LOG.warning("Truncated an incomplete raw tail at %s:%s", path, offset)
            # Valid records can survive a process crash in the OS cache. Sync before resuming them.
            os.fsync(raw.fileno())
    except (OSError, wire.ProtocolError) as exc:
        raise ArchiveError("cannot recover {}: {}".format(path, exc)) from exc
    return mark


class RawArchive:
    def __init__(self, root, checkpoint_seconds=2.0):
        if not math.isfinite(checkpoint_seconds) or checkpoint_seconds <= 0:
            raise ValueError("checkpoint interval must be positive and finite")
        self.root = Path(root).expanduser().resolve()
        self.checkpoint_seconds = checkpoint_seconds
        self.progress_path = self.root / "progress.json"
        self.streams = {}
        self.current_stream = None
        self._last_checkpoint = float("-inf")
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._lock = _lock_output(self.root / ".capture.lock")
        try:
            # Another process may have created the path and died before syncing its entries.
            for parent in self.root.parents:
                _sync_directory(parent)
            saved = self._read_progress()
            for path in self.root.glob("*.raw"):
                if STREAM_NAME.fullmatch(path.stem):
                    self.streams[path.stem] = _recover(path, saved.get(path.stem))
            if saved.keys() - self.streams.keys():
                raise ArchiveError("progress refers to a missing raw file")
            _sync_directory(self.root)
        except BaseException:
            self.close()
            raise

    def _read_progress(self):
        try:
            source = self.progress_path.open(encoding="utf-8")
        except FileNotFoundError:
            return {}
        try:
            with source:
                saved = json.load(source)
            if not isinstance(saved, dict) or saved.get("version") != 1:
                raise ValueError("unsupported progress version")
            entries = saved["streams"]
            if not isinstance(entries, dict) or any(not STREAM_NAME.fullmatch(k) for k in entries):
                raise ValueError("invalid progress stream IDs")
            current = saved.get("current_stream")
            if current is not None and (not isinstance(current, str) or current not in entries):
                raise ValueError("invalid current stream ID")
            self.current_stream = current
            return {key: _progress_mark(value) for key, value in entries.items()}
        except (OSError, ValueError, KeyError) as exc:
            raise ArchiveError("invalid progress file {}: {}".format(self.progress_path, exc)) from exc

    def resume(self, stream_id):
        mark = self.streams.get(stream_id.hex())
        return (mark.data, mark.resolved) if mark is not None else (0, 0)

    def open_stream(self, stream_id):
        key = stream_id.hex()
        path = self.root / (key + ".raw")
        mark = self.streams.get(key)
        try:
            if mark is None:
                raw = path.open("xb", buffering=0)
                try:
                    _sync_directory(self.root)
                except BaseException:
                    raw.close()
                    raise
                mark = self.streams[key] = wire.Watermark()
            else:
                raw = path.open("r+b", buffering=0)
                try:
                    raw.seek(0, os.SEEK_END)
                except BaseException:
                    raw.close()
                    raise
        except OSError as exc:
            raise ArchiveError("cannot open raw file {}: {}".format(path, exc)) from exc
        self.current_stream = key
        return RawStream(key, raw, mark)

    def checkpoint(self, force=False):
        now = time.monotonic()
        if not force and now - self._last_checkpoint < self.checkpoint_seconds:
            return
        state = {
            "version": 1,
            "updated_unix_ms": int(time.time() * 1000),
            "current_stream": self.current_stream,
            "streams": {key: {"data_watermark": mark.data, "resume_watermark": mark.resolved}
                        for key, mark in self.streams.items()},
        }
        tmp = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=str(self.root),
                                             prefix=".progress-", suffix=".tmp", delete=False) as dest:
                tmp = dest.name
                json.dump(state, dest, sort_keys=True, indent=2)
                dest.write("\n")
                dest.flush()
                os.fsync(dest.fileno())
            os.replace(tmp, str(self.progress_path))
            tmp = None
            _sync_directory(self.root)
        except OSError as exc:
            raise ArchiveError("cannot checkpoint {}: {}".format(self.progress_path, exc)) from exc
        finally:
            if tmp is not None:
                try:
                    os.unlink(tmp)
                except OSError:
                    LOG.warning("Could not remove temporary progress file %s", tmp)
        self._last_checkpoint = now

    def close(self):
        self._lock.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class RawStream:
    def __init__(self, key, raw, mark):
        self.key = key
        self.raw = raw
        self.mark = mark
        self.pending = []
        self.pending_bytes = 0
        self.pending_since = None
        self.failed = False

    def stage(self, frame):
        first = (self.pending[-1][2] if self.pending else self.mark.resolved) + 1
        if frame.kind not in (wire.DATA, wire.GAP) or frame.first != first:
            raise wire.ProtocolError("unexpected raw record; wanted batch/range starting at {}".format(first))
        record = wire.pack(frame.kind, frame.first, frame.last, frame.payload)
        if not self.pending:
            self.pending_since = time.monotonic()
        self.pending.append((frame.kind, frame.first, frame.last, record))
        self.pending_bytes += len(record)

    def commit(self):
        if not self.pending or self.failed:
            return b""
        self.failed = True  # Never retry an uncertain append on the same open file.
        try:
            for _, _, _, record in self.pending:
                rest = memoryview(record)
                while rest:
                    size = self.raw.write(rest)
                    if not size:
                        raise OSError("short raw write")
                    rest = rest[size:]
            os.fsync(self.raw.fileno())
        except OSError as exc:
            raise ArchiveError("cannot sync raw stream {}: {}".format(self.key, exc)) from exc
        acks = []
        for kind, first, last, _ in self.pending:
            self.mark.complete(kind, first, last)
            acks.append(wire.pack(wire.ack_kind(kind), first, last))
            if kind == wire.GAP:
                LOG.warning("Recorded unavailable IQTS batches %s:%s..%s", self.key, first, last)
        self.pending.clear()
        self.pending_bytes = 0
        self.pending_since = None
        self.failed = False
        return b"".join(acks)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.raw.close()


class _Stopped(Exception):
    pass


class _Superseded(Exception):
    pass


class Receiver:
    def __init__(self, token, archive, host="127.0.0.1", port=wire.DEFAULT_PORT,
                 sync_batches=32, sync_bytes=4 * 1024 * 1024, sync_seconds=0.010,
                 retry_seconds=1.0, stop=None, on_durable=None):
        if not isinstance(token, bytes) or not 1 <= len(token) <= wire.MAX_TOKEN:
            raise ValueError("invalid IQTS token length")
        if min(sync_batches, sync_bytes) < 1:
            raise ValueError("sync batch and byte limits must be positive")
        if any(not math.isfinite(t) or t <= 0 for t in (sync_seconds, retry_seconds)):
            raise ValueError("sync and retry intervals must be positive and finite")
        self.token = token
        self.archive = archive
        self.address = (wire.loopback(host), port)
        self.sync_batches = sync_batches
        self.sync_bytes = sync_bytes
        self.sync_seconds = sync_seconds
        self.retry_seconds = retry_seconds
        self.stop = stop if stop is not None else threading.Event()
        if on_durable is not None and not callable(on_durable):
            raise ValueError("on_durable must be callable")
        self.on_durable = on_durable
        self._owner = uuid.uuid4().bytes
        self._attached_stream = None

    def receive(self, sock):
        sock.settimeout(0.25)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        deadline = time.monotonic() + 3.0
        sock.sendall(wire.pack(wire.AUTH, payload=self.token))
        welcome = wire.read_frame(sock, deadline=deadline, kinds=(wire.WELCOME,))
        data, resolved = self.archive.resume(welcome.payload)
        mode = wire.RECONNECT if self._attached_stream == welcome.payload else wire.TAKEOVER
        sock.sendall(wire.pack(wire.RESUME, data, resolved, bytes((mode,)) + self._owner))
        response = wire.read_frame(sock, deadline=deadline, kinds=(wire.READY, wire.SUPERSEDED))
        if response.kind == wire.SUPERSEDED:
            raise _Superseded()
        self._attached_stream = welcome.payload
        LOG.info("IQTS connected: stream=%s data=%s resume=%s", welcome.payload.hex(), data, resolved)
        sock.settimeout(min(0.25, self.sync_seconds, self.archive.checkpoint_seconds))
        with self.archive.open_stream(welcome.payload) as stream:
            def commit_and_notify(send_ack):
                before = stream.mark.resolved
                acks = stream.commit()
                try:
                    if send_ack and acks:
                        sock.sendall(acks)
                finally:
                    # This is a wakeup hint, not a durable queue. Consumers must
                    # scan the archive on startup and on missed notifications.
                    if stream.mark.resolved != before and self.on_durable is not None:
                        try:
                            self.on_durable(stream.key, stream.mark.resolved)
                        except Exception:
                            LOG.exception("durable raw notification failed; consumer must rescan")

            def service():
                if self.stop.is_set():
                    raise _Stopped()
                if stream.pending_since is not None and time.monotonic() - stream.pending_since >= self.sync_seconds:
                    commit_and_notify(True)
                self.archive.checkpoint()

            try:
                while True:
                    service()
                    frame = wire.read_frame(sock, on_timeout=service, kinds=(wire.DATA, wire.GAP))
                    stream.stage(frame)
                    if len(stream.pending) >= self.sync_batches or stream.pending_bytes >= self.sync_bytes:
                        commit_and_notify(True)
            finally:
                # A valid received batch can still be kept if its connection vanished before ACK.
                commit_and_notify(False)

    def run(self):
        while not self.stop.is_set():
            self.archive.checkpoint()
            try:
                with socket.create_connection(self.address, timeout=3.0) as sock:
                    self.receive(sock)
            except _Stopped:
                return
            except _Superseded:
                LOG.info("IQTS receiver was superseded; stopping automatic reconnect")
                return
            except (EOFError, OSError, wire.ProtocolError) as exc:
                LOG.warning("IQTS connection ended: %s; retrying in %gs", exc, self.retry_seconds)
            self.stop.wait(self.retry_seconds)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=os.environ.get("TS_IQUANT_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=os.environ.get("TS_IQUANT_PORT", wire.DEFAULT_PORT))
    parser.add_argument("--output", required=True, help="raw and progress directory (one writer)")
    parser.add_argument("--sync-batches", type=int, default=32)
    parser.add_argument("--sync-bytes", type=int, default=4 * 1024 * 1024)
    parser.add_argument("--sync-ms", type=float, default=10)
    parser.add_argument("--checkpoint-seconds", type=float, default=2)
    parser.add_argument("--retry-seconds", type=float, default=1)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        token = wire.read_token()
        with RawArchive(args.output, args.checkpoint_seconds) as archive:
            receiver = Receiver(token, archive, args.host, args.port, args.sync_batches, args.sync_bytes,
                                args.sync_ms / 1000, args.retry_seconds)
            try:
                receiver.run()
            except KeyboardInterrupt:
                pass
            archive.checkpoint(force=True)
    except (ArchiveError, OSError, ValueError) as exc:
        LOG.error("IQTS receiver stopped: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
