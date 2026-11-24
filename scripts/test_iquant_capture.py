"""Local TCP and durability checks for the independent iQuant capture path."""

from contextlib import contextmanager, redirect_stdout
import io
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "examples"))
from iquant import protocol as wire, receiver, strategy
from iquant.sender import Sender

TOKEN = b"local-test-token"


def wait_for(predicate, seconds=3):
    until = time.monotonic() + seconds
    while not predicate():
        if time.monotonic() >= until:
            raise AssertionError("timed out waiting for IQTS state")
        time.sleep(0.005)


def tcp(sender):
    return socket.create_connection(sender.address, timeout=2)


def attach(sock, mark=(0, 0), owner=None, mode=wire.TAKEOVER):
    sock.sendall(wire.pack(wire.AUTH, payload=TOKEN))
    welcome = wire.read_frame(sock, kinds=(wire.WELCOME,))
    sock.sendall(wire.pack(wire.RESUME, *mark, bytes((mode,)) + (owner or uuid.uuid4().bytes)))
    ready = wire.read_frame(sock)
    return welcome.payload, ready.kind


def ack(sock, frame):
    sock.sendall(wire.pack(wire.ack_kind(frame.kind), frame.first, frame.last))


def raw_records(path):
    with path.open("rb") as file:
        records = []
        while True:
            header = file.read(wire.HEADER.size)
            if not header:
                return records
            parsed = wire.parse_header(header)
            records.append(wire.parse_frame(header, parsed, file.read(parsed[3])))


def new_receiver(sender, archive, **kwargs):
    return receiver.Receiver(TOKEN, archive, port=sender.address[1], retry_seconds=0.03, **kwargs)


@contextmanager
def running(client):
    errors = []

    def run():
        try:
            client.run()
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        yield thread
    finally:
        client.stop.set()
        thread.join(3)
        if thread.is_alive():
            raise AssertionError("receiver did not stop")
        if errors:
            raise errors[0]


class ProtocolTests(unittest.TestCase):
    def test_split_tcp_reads_and_checksum_covers_header_and_payload(self):
        packet = wire.pack(wire.DATA, 9, 9, b"a\x00b")
        sock = mock.Mock()
        sock.recv.side_effect = iter([bytes((byte,)) for byte in packet[:12]] + [socket.timeout()]
                                     + [bytes((byte,)) for byte in packet[12:]])
        tick = mock.Mock()
        self.assertEqual(wire.read_frame(sock, on_timeout=tick), wire.Frame(wire.DATA, 9, 9, b"a\x00b"))
        tick.assert_called_once_with()
        for offset in (15, len(packet) - 1):
            changed = bytearray(packet)
            changed[offset] ^= 1
            if offset == 15:
                changed[23] ^= 1  # Keep the DATA range valid while changing its identity.
            header = changed[:wire.HEADER.size]
            with self.assertRaisesRegex(wire.ProtocolError, "checksum"):
                wire.parse_frame(header, wire.parse_header(header), changed[wire.HEADER.size:])

    def test_unexpected_type_rejected_before_reading_payload(self):
        packet = wire.pack(wire.DATA, 1, 1, b"body")
        sock = mock.Mock()
        sock.recv.return_value = packet[:wire.HEADER.size]
        with self.assertRaisesRegex(wire.ProtocolError, "unexpected"):
            wire.read_frame(sock, kinds=(wire.AUTH,))
        sock.recv.assert_called_once_with(wire.HEADER.size)


class SenderTests(unittest.TestCase):
    def test_out_of_order_ack_does_not_move_window_or_drop_batches(self):
        with Sender(TOKEN, port=0, window=4) as sender, tcp(sender) as sock:
            self.assertEqual(attach(sock), (sender.stream_id, wire.READY))
            for seq in range(1, 6):
                self.assertEqual(sender.publish(bytes((seq,))), seq)
            got = {expected: wire.read_frame(sock) for expected in range(1, 5)}
            self.assertEqual([frame.first for frame in got.values()], [1, 2, 3, 4])
            for seq in (2, 4, 3):
                ack(sock, got[seq])
            sock.settimeout(0.10)
            with self.assertRaises(socket.timeout):
                wire.read_frame(sock)
            self.assertEqual(sender.stats()["data_watermark"], 0)
            self.assertEqual(sender.stats()["buffered_batches"], 5)
            sock.settimeout(2)
            ack(sock, got[1])
            fifth = wire.read_frame(sock)
            self.assertEqual(fifth.first, 5)
            self.assertEqual(sender.stats()["data_watermark"], 4)
            self.assertEqual(sender.stats()["first_retained"], 5)
            ack(sock, fifth)
            wait_for(lambda: sender.stats()["buffered_batches"] == 0)

    def test_bad_token_bad_resume_and_partial_handshake_do_not_take_over(self):
        with Sender(TOKEN, port=0, handshake_seconds=0.15) as sender, tcp(sender) as old:
            self.assertEqual(attach(old)[1], wire.READY)
            with tcp(sender) as bad:
                bad.sendall(wire.pack(wire.AUTH, payload=b"incorrect"))
                with self.assertRaises((EOFError, ConnectionResetError)):
                    wire.read_frame(bad)
            with tcp(sender) as bad:
                with self.assertRaises((EOFError, ConnectionResetError)):
                    attach(bad, (3, 3))
            with tcp(sender) as stuck:
                stuck.sendall(wire.pack(wire.AUTH, payload=TOKEN))
                self.assertEqual(wire.read_frame(stuck).kind, wire.WELCOME)
                with self.assertRaises((EOFError, ConnectionResetError)):
                    wire.read_frame(stuck)
            seq = sender.publish(b"still old")
            self.assertEqual(wire.read_frame(old).first, seq)
            with tcp(sender) as replacement:
                self.assertEqual(attach(replacement)[1], wire.READY)
                with self.assertRaises((EOFError, ConnectionResetError)):
                    wire.read_frame(old)
                replay = wire.read_frame(replacement)
                self.assertEqual(replay.payload, b"still old")
                ack(replacement, replay)
                wait_for(lambda: sender.stats()["data_watermark"] == seq)

    def test_runtime_overflow_evicts_oldest_and_retains_inflight_ack(self):
        with Sender(TOKEN, port=0, window=1, buffer_batches=4,
                    buffer_bytes=2 * (wire.HEADER.size + 1)) as sender, tcp(sender) as sock:
            attach(sock)
            sender.publish(b"1")
            first = wire.read_frame(sock)
            for seq in range(2, 6):
                sender.publish(str(seq).encode())
            self.assertEqual(sender.stats()["first_retained"], 4)
            self.assertEqual(sender.stats()["evicted_unresolved_batches"], 3)
            ack(sock, first)
            missing = wire.read_frame(sock)
            self.assertEqual(missing, wire.Frame(wire.GAP, 2, 3, b""))
            ack(sock, missing)
            for seq in (4, 5):
                frame = wire.read_frame(sock)
                self.assertEqual(frame, wire.Frame(wire.DATA, seq, seq, str(seq).encode()))
                ack(sock, frame)
            wait_for(lambda: sender.stats()["resume_watermark"] == 5)
            self.assertEqual(sender.stats()["data_watermark"], 1)

    def test_ack_must_match_an_offered_frame(self):
        with Sender(TOKEN, port=0) as sender, tcp(sender) as sock:
            attach(sock)
            sender.publish(b"data")
            self.assertEqual(wire.read_frame(sock).first, 1)
            sock.sendall(wire.pack(wire.ACK_DATA, 2, 2))
            with self.assertRaises((EOFError, ConnectionResetError)):
                wire.read_frame(sock)
            self.assertEqual(sender.stats()["data_watermark"], 0)
            self.assertEqual(sender.stats()["buffered_batches"], 1)


class ArchiveTests(unittest.TestCase):
    def test_ack_loss_resume_uses_synced_raw_not_stale_progress(self):
        with tempfile.TemporaryDirectory() as tmp, Sender(TOKEN, port=0) as sender:
            stream_id = sender.stream_id
            key = stream_id.hex()
            payloads = [b"raw\x00bytes", b"\xff\xfe", b"next"]
            with receiver.RawArchive(tmp) as archive, tcp(sender) as sock:
                self.assertEqual(attach(sock), (stream_id, wire.READY))
                with archive.open_stream(stream_id) as raw:
                    archive.checkpoint(force=True)
                    for payload in payloads[:2]:
                        sender.publish(payload)
                        raw.stage(wire.read_frame(sock))
                    self.assertEqual(len(raw.commit()), 2 * wire.HEADER.size)
                    # Intentionally lose both ACKs and leave the older checkpoint on disk.
            self.assertEqual(sender.stats()["data_watermark"], 0)
            sender.publish(payloads[2])
            with receiver.RawArchive(tmp, checkpoint_seconds=0.025) as archive:
                self.assertEqual(archive.resume(stream_id), (2, 2))
                with running(new_receiver(sender, archive, sync_batches=32)):
                    wait_for(lambda: sender.stats()["data_watermark"] == 3)
                    wait_for(lambda: json.loads(archive.progress_path.read_text())["streams"][key]
                             ["data_watermark"] == 3)
                records = raw_records(Path(tmp) / (key + ".raw"))
                self.assertEqual([frame.first for frame in records], [1, 2, 3])
                self.assertEqual([frame.payload for frame in records], payloads)

    def test_same_receiver_reconnects_after_a_transport_failure(self):
        with tempfile.TemporaryDirectory() as tmp, Sender(TOKEN, port=0) as sender:
            with receiver.RawArchive(tmp) as archive, running(new_receiver(sender, archive)):
                sender.publish(b"before disconnect")
                wait_for(lambda: sender.stats()["data_watermark"] == 1)
                with sender._cv:
                    sender._active.sock.shutdown(socket.SHUT_RDWR)
                sender.publish(b"after reconnect")
                wait_for(lambda: sender.stats()["data_watermark"] == 2)
            records = raw_records(Path(tmp) / (sender.stream_id.hex() + ".raw"))
            self.assertEqual([frame.payload for frame in records], [b"before disconnect", b"after reconnect"])

    def test_gap_is_persisted_without_faking_data_watermark(self):
        with tempfile.TemporaryDirectory() as tmp, Sender(TOKEN, port=0, buffer_batches=2) as sender:
            for payload in (b"first", b"second", b"third", b"fourth"):
                sender.publish(payload)
            with receiver.RawArchive(tmp) as archive:
                with running(new_receiver(sender, archive, sync_batches=3)):
                    wait_for(lambda: sender.stats()["resume_watermark"] == 4)
                archive.checkpoint(force=True)
            with receiver.RawArchive(tmp) as archive:
                key = sender.stream_id.hex()
                self.assertEqual(archive.resume(sender.stream_id), (0, 4))
                self.assertEqual(raw_records(Path(tmp) / (key + ".raw")), [
                    wire.Frame(wire.GAP, 1, 2, b""),
                    wire.Frame(wire.DATA, 3, 3, b"third"),
                    wire.Frame(wire.DATA, 4, 4, b"fourth"),
                ])
                self.assertEqual(json.loads(archive.progress_path.read_text())["streams"][key],
                                 {"data_watermark": 0, "resume_watermark": 4})

    def test_raw_fsync_failure_produces_no_ack_or_progress(self):
        with tempfile.TemporaryDirectory() as tmp, Sender(TOKEN, port=0) as sender:
            with receiver.RawArchive(tmp) as archive:
                with archive.open_stream(sender.stream_id) as raw:
                    raw_info = os.fstat(raw.raw.fileno())
                fsync = os.fsync

                def fail_raw(fd):
                    info = os.fstat(fd)
                    if (info.st_dev, info.st_ino) == (raw_info.st_dev, raw_info.st_ino):
                        raise OSError("injected raw fsync failure")
                    return fsync(fd)

                sender.publish(b"unsynced")
                with mock.patch.object(receiver.os, "fsync", side_effect=fail_raw), tcp(sender) as sock:
                    with self.assertRaisesRegex(receiver.ArchiveError, "injected raw fsync failure"):
                        new_receiver(sender, archive, sync_batches=1).receive(sock)
                self.assertEqual(archive.resume(sender.stream_id), (0, 0))
                self.assertEqual(sender.stats()["buffered_batches"], 1)
                self.assertEqual(sender.stats()["resume_watermark"], 0)

    def test_authorized_replacement_does_not_fight_with_reconnecting_receiver(self):
        with tempfile.TemporaryDirectory() as tmp, Sender(TOKEN, port=0) as sender:
            with receiver.RawArchive(Path(tmp) / "old") as old, receiver.RawArchive(Path(tmp) / "new") as new:
                with running(new_receiver(sender, old)) as old_thread:
                    sender.publish(b"old receiver")
                    wait_for(lambda: sender.stats()["data_watermark"] == 1)
                    with running(new_receiver(sender, new)):
                        wait_for(lambda: not old_thread.is_alive())
                        sender.publish(b"new receiver")
                        wait_for(lambda: sender.stats()["resume_watermark"] == 2)
                self.assertEqual(old.resume(sender.stream_id), (1, 1))
                self.assertEqual(new.resume(sender.stream_id), (0, 2))

    def test_fsck_trims_uncommitted_tail_but_rejects_lost_confirmed_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            stream_id = uuid.uuid4().bytes
            path = Path(tmp) / (stream_id.hex() + ".raw")
            first = wire.pack(wire.DATA, 1, 1, b"first")
            with receiver.RawArchive(tmp) as archive:
                with archive.open_stream(stream_id) as raw:
                    raw.stage(wire.Frame(wire.DATA, 1, 1, b"first"))
                    raw.commit()
                archive.checkpoint(force=True)
            torn = wire.pack(wire.DATA, 2, 2, b"partial")[:-1]
            path.write_bytes(first + torn)
            with receiver.RawArchive(tmp) as archive:
                self.assertEqual(path.read_bytes(), first)
                with archive.open_stream(stream_id) as raw:
                    raw.stage(wire.Frame(wire.DATA, 2, 2, b"partial"))
                    raw.commit()
                archive.checkpoint(force=True)
            path.write_bytes(first + torn)
            with self.assertRaisesRegex(receiver.ArchiveError, "ahead"):
                receiver.RawArchive(tmp)
            self.assertEqual(path.read_bytes(), first + torn)
            bad_crc = wire.pack(wire.DATA, 2, 2, b"partial")[:-1] + b"x"
            path.write_bytes(first + bad_crc)
            with self.assertRaisesRegex(receiver.ArchiveError, "checksum"):
                receiver.RawArchive(tmp)
            self.assertEqual(path.read_bytes(), first + bad_crc)

    def test_atomic_progress_replace_and_independent_stream_watermarks(self):
        with tempfile.TemporaryDirectory() as tmp, receiver.RawArchive(tmp) as archive:
            with self.assertRaisesRegex(receiver.ArchiveError, "lock"):
                receiver.RawArchive(tmp)
            archive.checkpoint(force=True)
            before = archive.progress_path.read_bytes()
            one, two = uuid.uuid4().bytes, uuid.uuid4().bytes
            for stream_id, count in ((one, 2), (two, 1)):
                with archive.open_stream(stream_id) as raw:
                    for seq in range(1, count + 1):
                        raw.stage(wire.Frame(wire.DATA, seq, seq, b"payload"))
                    raw.commit()
            with mock.patch.object(receiver.os, "replace", side_effect=OSError("rename injected")) as replace:
                with self.assertRaisesRegex(receiver.ArchiveError, "rename injected"):
                    archive.checkpoint(force=True)
                self.assertEqual(Path(replace.call_args[0][0]).parent, archive.root)
            self.assertEqual(archive.progress_path.read_bytes(), before)
            self.assertEqual(list(archive.root.glob(".progress-*.tmp")), [])
            archive.checkpoint(force=True)
            state = json.loads(archive.progress_path.read_text())
            self.assertEqual(state["current_stream"], two.hex())
            self.assertEqual(state["streams"][one.hex()]["data_watermark"], 2)
            self.assertEqual(state["streams"][two.hex()]["data_watermark"], 1)


class FakeContext:
    def __init__(self, failing=()):
        self.callbacks = {}
        self.unsubscribed = []
        self.failing = failing

    def subscribe_whole_quote(self, codes, callback):
        if codes[0] in self.failing:
            return -1
        self.callbacks[tuple(codes)] = callback
        return len(self.callbacks)

    def unsubscribe_quote(self, sub_id):
        self.unsubscribed.append(sub_id)


class StrategyTests(unittest.TestCase):
    def test_strategy_callback_to_disk_preserves_full_codes_and_snapshot_fields(self):
        context = FakeContext()
        env = {"TS_IQUANT_TOKEN": TOKEN.decode(), "TS_IQUANT_PORT": "0", "TS_IQUANT_BATCH_TICKS": "1"}
        with mock.patch.dict(os.environ, env, clear=True), redirect_stdout(io.StringIO()):
            strategy.init(context)
            try:
                self.assertEqual(list(context.callbacks), [(m,) for _, m in strategy.MARKETS])
                sender = strategy._CAPTURE.sender
                examples = {"600000.SH": {"askPrice": [1, 2, 3, 4, 5], "openInt": 7},
                            "900901.SH": {"name": "\u6d66\u53d1"}}
                context.callbacks[("SH",)](examples)
                options = {"IO2712-C-6000.IF": {"pos": 3, "value": None}}
                context.callbacks[("IF",)](options)
                with tempfile.TemporaryDirectory() as tmp, receiver.RawArchive(tmp) as archive:
                    with running(new_receiver(sender, archive)):
                        wait_for(lambda: sender.stats()["data_watermark"] == 3)
                    frames = raw_records(Path(tmp) / (sender.stream_id.hex() + ".raw"))
                    self.assertEqual([json.loads(frame.payload) for frame in frames],
                                     [[[code, snapshot]] for code, snapshot in dict(examples, **options).items()])
            finally:
                strategy.stop(context)
        self.assertEqual(context.unsubscribed, list(range(1, 11)))

    def test_broker_api_missing_or_failed_routes_are_reported(self):
        env = {"TS_IQUANT_TOKEN": TOKEN.decode(), "TS_IQUANT_PORT": "0"}
        context = FakeContext(failing=("GF",))
        with mock.patch.dict(os.environ, env, clear=True), redirect_stdout(io.StringIO()) as log:
            with self.assertRaisesRegex(RuntimeError, "lacks ContextInfo"):
                strategy.init(object())
            with self.assertRaisesRegex(RuntimeError, "GFEX_DERIVATIVES"):
                strategy.init(context)
        self.assertIn("FAILED GFEX_DERIVATIVES", log.getvalue())
        self.assertIsNone(strategy._CAPTURE)
        self.assertEqual(len(context.unsubscribed), 9)

    def test_sector_and_symbols_subscription_configuration(self):
        context = FakeContext()
        context.get_stock_list_in_sector = lambda name, ts: ["600000.SH", "600000.SH", "900901.SH"]
        config = {"SSE_EQUITIES": {"sectors": ["my SH A/B"]},
                  "SZSE_OPTIONS": {"symbols": ["90000001.SZO"]}}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "subscriptions.json"
            path.write_text(json.dumps(config), encoding="utf-8")
            routes = strategy.load_routes(str(path))
        self.assertEqual(strategy.subscription_codes(context, routes["SSE_EQUITIES"]),
                         ["600000.SH", "900901.SH"])
        self.assertEqual(strategy.subscription_codes(context, routes["SZSE_OPTIONS"]), ["90000001.SZO"])


if __name__ == "__main__":
    unittest.main()
