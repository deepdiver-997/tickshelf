#!/usr/bin/env python3
"""Engine end-to-end checks on a private Redis and private data directory.

Never connects to 127.0.0.1:6379 or touches a shared Redis. The write-through
case proves the engine's own path; the kill -9 case proves the pending state is
recoverable by the existing persister after the lease TTL.

Run: python3 scripts/test_engine_runtime.py --build build
"""
from __future__ import annotations

import argparse
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from stack_runtime import (OwnedProcesses, RedisConnection, candidate_ports,
                           start_private_redis, wait_until)

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


class EngineRuntimeTests(unittest.TestCase):
    shards = 2

    def setUp(self):
        if not shutil.which("redis-server"):
            self.skipTest("redis-server not installed")
        if not (BUILD / "tickshelfd").is_file() or not (BUILD / "feedgen").is_file():
            self.skipTest(f"binaries not built under {BUILD}")
        self.tmp = tempfile.TemporaryDirectory(prefix="tickshelf-engine-runtime-")
        self.directory = Path(self.tmp.name)
        self.processes = OwnedProcesses(self.directory / "proc")
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

    def tearDown(self):
        self.processes.close()
        self.tmp.cleanup()

    def start_daemon(self):
        self.processes.start("tickshelfd", [BUILD / "tickshelfd", "--config", self.config])
        wait_until(lambda: "listening " in self.processes.text("tickshelfd"),
                   timeout=10, description="tickshelfd listening",
                   processes=self.processes, names=("tickshelfd",))

    def test_write_through_persists_exact_rows_and_clears_pending(self):
        self.start_daemon()
        generator = subprocess.run(
            [str(BUILD / "feedgen"), "--host", "127.0.0.1", "--port", str(self.daemon_port),
             "--symbols", "8", "--rate", "500", "--duration-s", "2"],
            capture_output=True, text=True, timeout=20)
        self.assertEqual(generator.returncode, 0, generator.stderr)
        sent = int(generator.stdout.strip().splitlines()[-1].split()[1])

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
        for shard in range(self.shards):
            self.assertTrue(Path(f"{self.db}.{shard}").exists())

    def test_kill9_leaves_recoverable_pending_for_persister(self):
        self.start_daemon()
        generator = subprocess.Popen(
            [str(BUILD / "feedgen"), "--host", "127.0.0.1", "--port", str(self.daemon_port),
             "--symbols", "8", "--rate", "500", "--duration-s", "5"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        try:
            wait_until(lambda: pending_state(self.redis_port) > 0, timeout=10,
                       description="published pending state",
                       processes=self.processes, names=("tickshelfd",))
            daemon = self.processes.children["tickshelfd"]
            daemon.kill()
            daemon.wait(timeout=5)
            self.processes.stop("tickshelfd", timeout=5)
        finally:
            generator.terminate()
            try:
                generator.wait(timeout=5)
            except subprocess.TimeoutExpired:
                generator.kill()
                generator.wait()
            if generator.stdout is not None:
                generator.stdout.close()

        # The daemon's write-through leases are 1s in this config; let them expire.
        time.sleep(1.3)
        self.processes.start("persister", [BUILD / "persister", "--config", self.config])
        wait_until(lambda: pending_state(self.redis_port) == 0, timeout=20,
                   description="persister drains pending", processes=self.processes,
                   names=("persister",))

        total, distinct = db_rows(self.db, self.shards)
        self.assertGreater(total, 0)
        self.assertEqual(total, distinct)
        self.processes.stop("persister", timeout=10)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--build", type=Path, default=BUILD)
    options, remaining = parser.parse_known_args()
    BUILD = options.build
    unittest.main(argv=[sys.argv[0], *remaining])
