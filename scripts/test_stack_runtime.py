#!/usr/bin/env python3
"""Isolated startup/ownership checks; never connects to the default Redis."""
import argparse
import json
import os
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from ifind_sim_bench import BASE_TS, Observer, parse_source, percentile, shard_for, write_config
from stack_runtime import (OwnedProcesses, RedisConnection, RunError, candidate_ports,
                           start_private_redis, wait_until)

ROOT = Path(__file__).resolve().parent.parent
BUILD = ROOT / "build-release"
RUNTIME_ONLY = False


class RuntimeTests(unittest.TestCase):
    def test_metrics_require_anchors(self):
        with self.assertRaises(RunError):
            parse_source("bridge: SENT 1 1\n")
        metrics, sent = parse_source('METRICS {"backend":"mock"}\nSENT 1 1\n')
        self.assertEqual(metrics["backend"], "mock")
        self.assertEqual(sent, (1, 1))
        self.assertEqual(percentile([1, 2, 3, 4], .95), 4)
        self.assertIsNone(percentile([], .95))
        self.assertTrue(0 <= shard_for("600000", 4) < 4)

    def test_visibility_observes_late_earlier_rows_once(self):
        class FakeRedis:
            def command(self, *_):
                return b"used_memory:0\r\n"

            def close(self):
                pass

        with tempfile.TemporaryDirectory(prefix="tickshelf-observer-test-") as directory:
            db = Path(directory) / "sample.db"
            with sqlite3.connect(db) as writer:
                writer.execute("CREATE TABLE ticks(symbol TEXT, ts_exchange_ms INTEGER, ts_receive_ms INTEGER)")
                writer.execute("INSERT INTO ticks VALUES(?,?,?)",
                               ("600000", BASE_TS + 1, int(time.time() * 1000) - 10))
                writer.commit()
                with patch("ifind_sim_bench.RedisConnection", return_value=FakeRedis()):
                    observer = Observer(db, 1, 0, 20)
                try:
                    observer.observe()
                    writer.execute("INSERT INTO ticks VALUES(?,?,?)",
                                   ("600000", BASE_TS, int(time.time() * 1000) - 10))
                    writer.commit()
                    observer.observe()
                    observer.observe()
                    self.assertEqual(observer.seen, {BASE_TS, BASE_TS + 1})
                    self.assertEqual(len(observer.latencies), 2)
                finally:
                    observer.close()

    def test_owned_child_stopped_and_waited(self):
        with tempfile.TemporaryDirectory(prefix="tickshelf-owner-test-") as directory:
            owner = OwnedProcesses(Path(directory))
            with owner:
                child = owner.start("sleep", [sys.executable, "-I", "-c",
                    "import signal,time; signal.signal(signal.SIGINT, lambda *a: exit(0)); print('ready',flush=True); time.sleep(60)"])
                wait_until(lambda: "ready" in owner.text("sleep"), processes=owner, names=("sleep",))
            self.assertEqual(child.returncode, 0)
            self.assertFalse(owner.forced)

    @unittest.skipUnless(shutil.which("redis-server"), "redis-server not installed")
    def test_occupied_redis_port_is_never_reused_or_stopped(self):
        with tempfile.TemporaryDirectory(prefix="tickshelf-port-test-") as directory:
            port, = candidate_ports(1)
            with OwnedProcesses(Path(directory) / "first") as first:
                start_private_redis(first, shutil.which("redis-server"), port)
                with OwnedProcesses(Path(directory) / "second") as second:
                    with self.assertRaises(RunError):
                        start_private_redis(second, shutil.which("redis-server"), port)
                first.require_alive("redis")
                with RedisConnection("127.0.0.1", port) as rc:
                    self.assertEqual(rc.command("PING"), b"PONG")


class StartupTests(unittest.TestCase):
    def setUp(self):
        if RUNTIME_ONLY:
            self.skipTest("runtime-only requested; C++ startup not tested")
        self.temp = tempfile.TemporaryDirectory(prefix="tickshelf-startup-test-")
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.owner = OwnedProcesses(self.directory / "infra")
        self.addCleanup(self.owner.close)
        self.ports = candidate_ports(3)
        self.config = self.directory / "stack.ini"
        write_config(self.config, self.ports, "none", 1)
        start_private_redis(self.owner, shutil.which("redis-server"), self.ports[0])

    def test_configuration_validation_has_no_db_side_effect(self):
        for app in ("tickshelfd", "persister", "evictor", "queryd"):
            done = subprocess.run([BUILD / app, "--config", self.config, "--print-config"],
                                  capture_output=True, text=True, timeout=5)
            self.assertEqual(done.returncode, 0, done.stderr)
            parsed = json.loads(done.stdout)
            self.assertEqual(parsed["redis_port"], self.ports[0])
            self.assertFalse((self.directory / "ticks.db").exists())
        self.config.write_text("[storage]\nshards = 0\n")
        bad = subprocess.run([BUILD / "persister", "--config", self.config],
                             capture_output=True, text=True, timeout=5)
        self.assertEqual(bad.returncode, 2)
        self.assertFalse((self.directory / "ticks.db").exists())

    def test_launcher_readiness_and_signal_cleanup(self):
        log_dir = self.directory / "services"
        child = self.owner.start("launcher", [sys.executable, ROOT / "scripts/run_stack.py",
                                 "--build", BUILD, "--config", self.config, "--log-dir", log_dir])
        wait_until(lambda: "STACK_READY " in self.owner.text("launcher"), timeout=15,
                   processes=self.owner, names=("launcher", "redis"))
        line = next(line for line in self.owner.text("launcher").splitlines()
                    if line.startswith("STACK_READY "))
        pids = json.loads(line[len("STACK_READY "):])["pids"]
        self.assertEqual(set(pids), {"tickshelfd", "persister", "evictor", "queryd"})
        child.send_signal(signal.SIGINT)
        time.sleep(0.025)
        child.send_signal(signal.SIGTERM)  # repeated interruption during teardown
        self.assertEqual(self.owner.stop("launcher"), 0, self.owner.text("launcher"))
        for pid in pids.values():
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)
        self.owner.require_alive("redis")

    def test_old_cli_service_startup(self):
        common = ["--redis-host", "127.0.0.1", "--redis-port", self.ports[0]]
        db = self.directory / "old-cli.db"
        with OwnedProcesses(self.directory / "old-cli") as owner:
            owner.start("persister", [BUILD / "persister", *common, "--db", db, "--poll-ms", 100])
            owner.start("tickshelfd", [BUILD / "tickshelfd", *common, "--port", self.ports[1],
                                      "--flush-interval-ms", 100])
            owner.start("queryd", [BUILD / "queryd", *common, "--db", db, "--port", self.ports[2]])
            owner.start("evictor", [BUILD / "evictor", *common, "--soft-watermark-bytes", 134217728])
            wait_until(lambda: db.exists() and "listening " in owner.text("tickshelfd")
                       and "listening " in owner.text("queryd") and "soft=" in owner.text("evictor"),
                       processes=owner, names=tuple(owner.children))
        self.assertFalse(owner.forced)
        self.assertTrue(all(child.returncode == 0 for child in owner.children.values()))

    def test_failed_component_cleans_up_without_touching_port_owner(self):
        # Occupy queryd's port with our own listener; the launcher must fail rather
        # than treat an unrelated listening socket as a healthy child.
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", self.ports[2]))
            listener.listen()
            child = self.owner.start("launcher", [sys.executable, ROOT / "scripts/run_stack.py",
                                     "--build", BUILD, "--config", self.config,
                                     "--log-dir", self.directory / "failed-services"])
            self.assertEqual(child.wait(timeout=15), 1, self.owner.text("launcher"))
            self.owner.require_alive("redis")
            with socket.create_connection(("127.0.0.1", self.ports[2]), timeout=1):
                pass
            daemon_log = self.directory / "failed-services/tickshelfd.log"
            self.assertFalse(daemon_log.exists(), "launch should stop at queryd failure")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", type=Path, default=BUILD)
    parser.add_argument("--runtime-only", action="store_true")
    args, remaining = parser.parse_known_args()
    BUILD = args.build.resolve()
    RUNTIME_ONLY = args.runtime_only
    unittest.main(argv=[sys.argv[0], *remaining])
