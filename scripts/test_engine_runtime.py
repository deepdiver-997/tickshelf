#!/usr/bin/env python3
"""Engine end-to-end checks on a private Redis and private data directory.

Never connects to 127.0.0.1:6379 or touches a shared Redis. The write-through
case proves the engine's own path; the kill -9 case proves the pending state is
recoverable by the existing persister after the lease TTL.

Run: python3 scripts/test_engine_runtime.py --build build
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import socket
import sqlite3
import struct
import subprocess
import zlib
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from stack_runtime import (OwnedProcesses, RedisConnection, candidate_ports,
                           start_private_redis, wait_until)
from engine_bench import (COLUMNS, TICK, shard_path, tick_bytes, tick_values,
                          verify_sqlite, write_input)

ROOT = Path(__file__).resolve().parent.parent
BUILD = ROOT / "build"


def db_rows(path: Path, shards: int) -> tuple[int, int]:
    total = distinct = 0
    for shard in range(shards):
        file = Path(f"{path}.{shard}") if shards > 1 else path
        if not file.exists():
            continue
        connection = sqlite3.connect(file)
        try:
            total += connection.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]
            distinct += connection.execute(
                "SELECT COUNT(*) FROM (SELECT DISTINCT symbol, ts_exchange_ms FROM ticks)"
            ).fetchone()[0]
        finally:
            connection.close()
    return total, distinct


def pending_state(port: int) -> int:
    with RedisConnection("127.0.0.1", port) as redis:
        values = redis.command("HVALS", "state") or []
        return sum(1 for value in values if value == b"pending")


def send_ticks(port, records):
    payload = b"".join(records)
    if not records or len(records) > 4096:
        raise ValueError("one bounded TTAP frame required")
    header = struct.pack("<4sBBHHHII", b"TTAP", 1, 0, len(records), 0, 0,
                         len(payload), zlib.crc32(payload) & 0xffffffff)
    with socket.create_connection(("127.0.0.1", port), timeout=5) as connection:
        connection.sendall(header + payload)


class FloodgenInputTests(unittest.TestCase):
    def test_rejects_missing_malformed_and_out_of_range_before_connect(self):
        if not (BUILD / "floodgen").is_file():
            self.skipTest(f"floodgen not built under {BUILD}")
        invalid = [["--symbols"], ["--symbols", "-1"], ["--symbols", "abc"],
                   ["--sym-base", "-1"], ["--sym-base", "9999999", "--symbols", "2"],
                   ["--port", "65536"], ["--port", "0"], ["--rate", "nan"],
                   ["--rate", "inf"], ["--rate", "-1"], ["--duration", "-1"],
                   ["--ticks-per-frame", "4097"], ["--ts-base", "-1"],
                   ["--ts-base", "18446744073709551616"], ["--symbols", "1000001"]]
        with tempfile.TemporaryDirectory(prefix="tickshelf-floodgen-input-") as temporary:
            with OwnedProcesses(Path(temporary)) as owner:
                for index, arguments in enumerate(invalid):
                    with self.subTest(arguments=arguments):
                        name = f"invalid-{index}"
                        child = owner.start(name, [BUILD / "floodgen", *arguments])
                        self.assertEqual(child.wait(timeout=3), 2, owner.text(name))

    def test_largest_eight_byte_symbol_encodes_without_terminator_overwrite(self):
        if not (BUILD / "floodgen").is_file():
            self.skipTest(f"floodgen not built under {BUILD}")
        with tempfile.TemporaryDirectory(prefix="tickshelf-floodgen-boundary-") as temporary:
            with socket.socket() as listener, OwnedProcesses(Path(temporary)) as owner:
                listener.bind(("127.0.0.1", 0))
                listener.listen(1)
                listener.settimeout(3)
                port = listener.getsockname()[1]
                child = owner.start("boundary", [BUILD / "floodgen", "--port", port,
                    "--symbols", 1, "--sym-base", 9999999, "--rate", 1, "--duration", .01])
                connection, _ = listener.accept()
                with connection:
                    connection.settimeout(3)
                    raw = bytearray()
                    while True:
                        block = connection.recv(65536)
                        if not block:
                            break
                        raw.extend(block)
                self.assertEqual(child.wait(timeout=3), 0, owner.text("boundary"))
                self.assertEqual(len(raw), 20 + 4096 * 64)
                self.assertEqual(raw[20:28], b"F9999999")
                self.assertEqual(raw[20 + 4095 * 64:28 + 4095 * 64], b"F9999999")
                self.assertRegex(owner.text("boundary"), r"SENT 4096 1")


class EngineRuntimeTests(unittest.TestCase):
    shards = 2

    def setUp(self):
        if not shutil.which("redis-server"):
            self.skipTest("redis-server not installed")
        if not (BUILD / "tickshelfd").is_file():
            self.skipTest(f"binaries not built under {BUILD}")
        self.tmp = tempfile.TemporaryDirectory(prefix="tickshelf-engine-runtime-")
        self.directory = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        self.processes = OwnedProcesses(self.directory / "proc")
        self.addCleanup(self.processes.close)
        self.redis_port, self.daemon_port = candidate_ports(2)
        start_private_redis(self.processes, shutil.which("redis-server"), self.redis_port)
        self.db = self.directory / "ticks.db"
        self.config = self.directory / "tickshelf.ini"
        self.config.write_text(
            "[redis]\n"
            "host = 127.0.0.1\n"
            f"port = {self.redis_port}\n"
            "[storage]\n"
            f"db = {self.db}\n"
            f"shards = {self.shards}\n"
            "[tickshelfd]\n"
            "bind = 127.0.0.1\n"
            f"port = {self.daemon_port}\n"
            "persist = sqlite\n"
            "engine = true\n"
            "engine_batch_records = 64\n"
            "engine_batch_ms = 10\n"
            "engine_inflight = 4\n"
            "engine_max_pending_records = 4096\n"
            "engine_lease_ttl_s = 1\n"
            "engine_shutdown_ms = 5000\n"
            "[persister]\n"
            "worker = engine-test\n"
            "poll_ms = 100\n"
        )

    def start_daemon(self):
        self.processes.start("tickshelfd", [BUILD / "tickshelfd", "--config", self.config])
        wait_until(lambda: "listening " in self.processes.text("tickshelfd"),
                   timeout=10, description="tickshelfd listening",
                   processes=self.processes, names=("tickshelfd",))

    def test_write_through_persists_exact_rows_and_clears_pending(self):
        self.start_daemon()
        manifest = write_input(self.directory / "controlled.ticks", 200, 8, 0)
        send_ticks(self.daemon_port, [tick_bytes(i, 8) for i in range(200)])
        sent = manifest["records"]

        def settled():
            total, distinct = db_rows(self.db, self.shards)
            return total >= sent and distinct == total and pending_state(self.redis_port) == 0

        wait_until(settled, timeout=15, description="engine durable rows",
                   processes=self.processes, names=("tickshelfd",))
        rc = self.processes.stop("tickshelfd", timeout=10)
        self.assertEqual(rc, 0, self.processes.text("tickshelfd"))

        total, distinct = db_rows(self.db, self.shards)
        self.assertEqual(total, sent)
        self.assertEqual(distinct, sent)
        self.assertEqual(pending_state(self.redis_port), 0)
        self.assertTrue(verify_sqlite(self.db, self.shards, manifest)["passed"])
        for shard in range(self.shards):
            self.assertTrue(Path(f"{self.db}.{shard}").exists())

    def lock_storage(self):
        locks = []
        try:
            for index in range(self.shards):
                connection = sqlite3.connect(shard_path(self.db, index, self.shards), timeout=2)
                locks.append(connection)
                connection.execute("BEGIN IMMEDIATE")
            return locks
        except BaseException:
            self.unlock_storage(locks)
            raise

    @staticmethod
    def unlock_storage(locks):
        for connection in locks:
            connection.rollback()
            connection.close()

    def kill_owned(self, name):
        child = self.processes.children[name]
        child.kill()
        child.wait(timeout=5)
        self.processes.stop(name, timeout=5)

    def test_storage_only_persists_without_any_redis_connection(self):
        # persist=storage（ADR-026）不建 Redis 连接：把 [redis] 指向一个确定无人
        # 监听的端口，daemon 仍须完成全部落库——证明零 Redis 依赖。
        unused_port, = candidate_ports(1)
        text = self.config.read_text().replace("persist = sqlite", "persist = storage")
        text = text.replace(f"port = {self.redis_port}", f"port = {unused_port}", 1)
        self.config.write_text(text)
        self.start_daemon()
        manifest = write_input(self.directory / "storage-only.ticks", 64, 8, 0)
        send_ticks(self.daemon_port, [tick_bytes(i, 8) for i in range(64)])

        wait_until(lambda: db_rows(self.db, self.shards) == (64, 64), timeout=10,
                   description="storage-only durable rows without Redis",
                   processes=self.processes, names=("tickshelfd",))
        self.assertEqual(self.processes.stop("tickshelfd", timeout=10), 0,
                         self.processes.text("tickshelfd"))
        log = self.processes.text("tickshelfd")
        self.assertNotIn("drain incomplete", log)
        self.assertRegex(log, r"engine ingested=64 .*accepted=64 ")
        self.assertTrue(verify_sqlite(self.db, self.shards, manifest)["passed"])

    def test_storage_mode_without_engine_is_rejected(self):
        text = self.config.read_text().replace("persist = sqlite", "persist = storage")
        text = text.replace("engine = true", "engine = false")
        self.config.write_text(text)
        child = self.processes.start("reject", [BUILD / "tickshelfd", "--config", self.config])
        self.assertEqual(child.wait(timeout=5), 1, self.processes.text("reject"))
        self.assertIn("--persist storage requires --engine", self.processes.text("reject"))

    def test_kill9_leaves_recoverable_pending_for_persister(self):
        if not (BUILD / "persister").is_file():
            self.skipTest("persister not built")
        self.start_daemon()
        manifest = write_input(self.directory / "recover.ticks", 64, 8, 0)
        locks = self.lock_storage()
        try:
            send_ticks(self.daemon_port, [tick_bytes(i, 8) for i in range(64)])
            wait_until(lambda: pending_state(self.redis_port) == 8, timeout=3,
                       description="all eight symbol batches published pending while SQLite locked",
                       processes=self.processes, names=("tickshelfd",))
            self.kill_owned("tickshelfd")
        finally:
            self.unlock_storage(locks)
        time.sleep(1.3)  # private 1s leases expire
        self.processes.start("persister", [BUILD / "persister", "--config", self.config])
        wait_until(lambda: pending_state(self.redis_port) == 0 and db_rows(self.db, self.shards)[0] == 64,
                   timeout=20, description="persister recovers exact pending rows", processes=self.processes,
                   names=("persister",))
        self.assertTrue(verify_sqlite(self.db, self.shards, manifest)["passed"])
        self.assertEqual(self.processes.stop("persister", timeout=10), 0)

    def test_retry_tail_precedes_new_connection_input(self):
        text = self.config.read_text().replace("engine_batch_records = 64", "engine_batch_records = 1")
        text = text.replace("engine_max_pending_records = 4096", "engine_max_pending_records = 1")
        self.config.write_text(text)
        self.start_daemon()
        locks = self.lock_storage()
        try:
            old = [tick_bytes(i, 1) for i in range(16)]
            send_ticks(self.daemon_port, old)
            wait_until(lambda: pending_state(self.redis_port) > 0, timeout=3,
                       description="old first slice blocked, old tail retained",
                       processes=self.processes, names=("tickshelfd",))
            changed = list(tick_values(1, 1))
            changed[3] += 99999
            new = TICK.pack(changed[0].encode(), *changed[1:])
            send_ticks(self.daemon_port, [new])
        finally:
            self.unlock_storage(locks)
        wait_until(lambda: db_rows(self.db, self.shards)[0] == 16, timeout=10,
                   description="retry tail drains before conflicting new key", processes=self.processes,
                   names=("tickshelfd",))
        time.sleep(0.1) # let the final conflicting input be decoded, not merely left in TCP
        self.assertEqual(self.processes.stop("tickshelfd", timeout=10), 0)
        manifest = {"records": 16, "unique_records": 16, "symbols": 1}
        self.assertTrue(verify_sqlite(self.db, self.shards, manifest)["passed"])
        log = self.processes.text("tickshelfd")
        self.assertRegex(log, r"engine ingested=17 .*accepted=17 ")
        self.assertNotIn("drain incomplete", log)
        sizes = [int(value) for value in re.findall(r"retry_buf=([0-9]+)", log)]
        self.assertTrue(all(size <= 65536 for size in sizes))

    def test_private_non_aof_redis_loss_does_not_erase_committed_fields(self):
        self.start_daemon()
        manifest = write_input(self.directory / "baseline.ticks", 64, 8, 0)
        send_ticks(self.daemon_port, [tick_bytes(i, 8) for i in range(64)])
        wait_until(lambda: db_rows(self.db, self.shards)[0] == 64 and pending_state(self.redis_port) == 0,
                   timeout=10, description="baseline committed", processes=self.processes,
                   names=("tickshelfd", "redis"))
        self.assertTrue(verify_sqlite(self.db, self.shards, manifest)["passed"])
        locks = self.lock_storage()
        try:
            send_ticks(self.daemon_port, [tick_bytes(i, 8) for i in range(64, 128)])
            wait_until(lambda: pending_state(self.redis_port) > 0, timeout=3,
                       description="new noncommitted pending segment", processes=self.processes,
                       names=("tickshelfd", "redis"))
            self.kill_owned("tickshelfd")
            self.kill_owned("redis")  # owned PID only; no AOF/RDB on this private instance
        finally:
            self.unlock_storage(locks)
        restarted = OwnedProcesses(self.directory / "restart-proc")
        self.addCleanup(restarted.close)
        port, = candidate_ports(1)
        start_private_redis(restarted, shutil.which("redis-server"), port)
        with RedisConnection("127.0.0.1", port) as redis:
            self.assertEqual(redis.command("HLEN", "state"), 0)
        # Only the previously committed baseline is required. Uncommitted loss is
        # permitted, never asserted as necessary or promoted to a healthy result.
        baseline = {}
        extra = 0
        for index in range(self.shards):
            with sqlite3.connect(shard_path(self.db, index, self.shards)) as connection:
                for row in connection.execute(f"SELECT {COLUMNS} FROM ticks"):
                    symbol_index = int(row[0][1:])
                    record_id = (row[1] - 1700000000000) * 8 + symbol_index
                    self.assertTrue(0 <= record_id < 128)
                    self.assertEqual(tuple(row), tick_values(record_id, 8))
                    if record_id < 64:
                        baseline[record_id] = tuple(row)
                    else:
                        extra += 1
        self.assertEqual(set(baseline), set(range(64)))
        observation = {"scenario": "owned-private-non-AOF-Redis-and-daemon-SIGKILL",
                       "healthy_completion": False, "committed_baseline_verified": 64,
                       "uncommitted_observed_after_loss": extra,
                       "uncommitted_loss_permitted_not_required": True}
        (self.directory / "loss-observation.json").write_text(json.dumps(observation, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--build", type=Path, default=BUILD)
    options, remaining = parser.parse_known_args()
    BUILD = options.build
    unittest.main(argv=[sys.argv[0], *remaining])
