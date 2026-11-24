"""IQTS worker integration using the normal receiver entry point."""

import argparse
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from test_iquant_capture import TOKEN, Sender, wait_for
from iquant.engine import Worker, read_group
from iquant import protocol as wire

BINARY = None
RECEIVER = Path(__file__).resolve().parents[1] / "examples" / "iquant" / "receiver.py"
SNAPSHOT = b'{"time":1700000000000,"askPrice":[1,2,3,4,5],"mark":NaN,"value":0.1234567890123456789}'
QUOTE = b'[["IO2712-C-6000.IF",' + SNAPSHOT + b']]'


def command(sender, root, *flags):
    return [sys.executable, "-B", str(RECEIVER), "--work-dir", str(root), "--engine-bin", BINARY,
            "--port", str(sender.address[1]), "--retry-seconds", "0.03", "--sync-batches", "1", *flags]


def environment():
    return dict(os.environ, TS_IQUANT_TOKEN=TOKEN.decode())


def start(sender, root, *flags):
    return subprocess.Popen(command(sender, root, *flags), env=environment())


def stop(process):
    if process.poll() is None:
        process.kill()
    process.wait(timeout=5)


def rows(root, sql):
    with sqlite3.connect((root / "history.sqlite3").as_uri() + "?mode=ro", uri=True) as conn:
        return conn.execute(sql).fetchall()


def ready(root):
    try:
        return rows(root, "PRAGMA user_version") == [(1,)]
    except sqlite3.OperationalError:
        return False


class EngineTests(unittest.TestCase):
    def test_hot_commit_can_precede_history_without_advancing_cursor(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "raw").mkdir()
            key = "0123456789abcdef" * 2
            path = root / "raw" / (key + ".raw")
            first = wire.pack(wire.DATA, 1, 1, QUOTE)
            second = wire.pack(wire.DATA, 2, 2, QUOTE)
            path.write_bytes(first + second)
            worker = Worker(BINARY, root)
            try:
                worker.write(key, read_group(path, len(first), 1, len(first + second)), False)
                self.assertEqual(worker.cursor(key), (0, 0))
                self.assertEqual(rows(root, "select batch_seq from quotes"), [("00000000000000000002",)])
            finally:
                worker.close()
            worker = Worker(BINARY, root)
            try:
                worker.write(key, read_group(path, 0, 0, len(first + second)), True)
                self.assertEqual(worker.cursor(key), (len(first + second), 2))
                self.assertEqual(rows(root, "select count(*) from quotes"), [(2,)])
            finally:
                worker.close()

    def test_two_receiver_kills_while_storage_blocked_then_catch_up_and_lock(self):
        with tempfile.TemporaryDirectory() as directory, Sender(TOKEN, port=0) as sender:
            root = Path(directory) / "workspace"
            process = start(sender, root)
            try:
                wait_for(lambda: (root / "raw" / (sender.stream_id.hex() + ".raw")).exists())
                wait_for(lambda: ready(root))
                second = subprocess.run(command(sender, root), env=environment(), capture_output=True, timeout=5)
                self.assertEqual(second.returncode, 1)
                self.assertIn(b"cannot lock capture directory", second.stderr)
                with sqlite3.connect(str(root / "history.sqlite3")) as blocker:
                    blocker.execute("BEGIN IMMEDIATE")
                    for seq in (1, 2):
                        if seq == 2:
                            process = start(sender, root)
                        sender.publish(QUOTE)
                        wait_for(lambda: sender.stats()["data_watermark"] == seq)
                        self.assertEqual(rows(root, "select count(*) from quotes"), [(0,)])
                        stop(process)
                    blocker.rollback()
                process = start(sender, root)
                sender.publish(b'invalid json')
                sender.publish(QUOTE)
                wait_for(lambda: rows(root, "select last_seq from replay_cursor") == [("00000000000000000004",)], 8)
                self.assertEqual(rows(root, "select status from raw_frames order by first_seq"),
                                 [("data",), ("data",), ("quarantine",), ("data",)])
                self.assertEqual(rows(root, "select symbol,source_time_ms,snapshot from quotes"),
                                 [("IO2712-C-6000.IF", 1700000000000, SNAPSHOT)] * 3)
            finally:
                stop(process)

    def test_redis_hot_failure_does_not_hold_raw_ack_or_history(self):
        with tempfile.TemporaryDirectory() as directory, Sender(TOKEN, port=0) as sender:
            root = Path(directory)
            with socket.socket() as server:
                server.bind(("127.0.0.1", 0))
                server.listen()
                server.settimeout(3)
                process = start(sender, root, "--redis-port", str(server.getsockname()[1]))
                try:
                    wait_for(lambda: (root / "raw" / (sender.stream_id.hex() + ".raw")).exists())
                    sender.publish(QUOTE)
                    with server.accept()[0] as client:
                        client.settimeout(1)
                        self.assertIn(b"XADD", client.recv(4096))
                        # Hold this response and continue to ingest.
                        for _ in range(12):
                            sender.publish(QUOTE)
                        wait_for(lambda: sender.stats()["data_watermark"] == 13)
                        wait_for(lambda: rows(root, "select count(*) from quotes") == [(13,)])
                finally:
                    stop(process)

    def test_storage_hole_and_raw_gap_are_distinct(self):
        with tempfile.TemporaryDirectory() as directory, Sender(TOKEN, port=0, buffer_batches=2) as sender:
            root = Path(directory)
            for _ in range(4):
                sender.publish(QUOTE)
            process = start(sender, root)
            try:
                wait_for(lambda: sender.stats()["resume_watermark"] == 4)
                wait_for(lambda: ready(root))
                wait_for(lambda: rows(root, "select count(*) from quotes") == [(2,)])
                self.assertEqual(sender.stats()["data_watermark"], 0)
                self.assertEqual(rows(root, "select first_seq,last_seq,status from raw_frames order by first_seq"), [
                    ("00000000000000000001", "00000000000000000002", "gap"),
                    ("00000000000000000003", "00000000000000000003", "data"),
                    ("00000000000000000004", "00000000000000000004", "data"),
                ])
            finally:
                stop(process)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine-bin", required=True)
    args, remaining = parser.parse_known_args()
    BINARY = str(Path(args.engine_bin).resolve())
    unittest.main(argv=[sys.argv[0], *remaining])
