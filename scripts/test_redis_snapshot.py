#!/usr/bin/env python3
"""Read-only Redis evidence tests. Default: fakes only; optional owned Redis.

No build step, default endpoint, shared-Redis connection, or Redis mutation.
"""
import argparse
import json
import shutil
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from stack_runtime import (OwnedProcesses, REDIS_SNAPSHOT_CONFIG_KEYS,
                           REDIS_SNAPSHOT_INFO_SECTIONS, RunError,
                           candidate_ports, redis_runtime_snapshot,
                           start_private_redis)

PRIVATE_REDIS = False
HOST, PORT = "declared-redis.invalid", 16381
COMMANDS = ([('CONFIG', 'GET', key) for key in REDIS_SNAPSHOT_CONFIG_KEYS] +
            [('INFO', section) for section in REDIS_SNAPSHOT_INFO_SECTIONS])
CONFIG = {"appendonly": "no", "save": "", "appendfsync": "everysec",
          "maxmemory": "536870912", "maxmemory-policy": "noeviction"}


class FakeConnection:
    def __init__(self, overrides=None):
        self.overrides = overrides or {}
        self.commands = []
        self.closed = False

    def command(self, *args):
        self.commands.append(args)
        if args not in COMMANDS:
            raise AssertionError(f"non-allowlisted command: {args}")
        if args in self.overrides:
            value = self.overrides[args]
            if isinstance(value, Exception):
                raise value
            return value
        if args[:2] == ('CONFIG', 'GET'):
            return [args[2].encode(), CONFIG[args[2]].encode()]
        return {
            'server': b"# Server\r\nredis_version:7.2.5\r\nprocess_id:4321\r\n",
            'persistence': b"# Persistence\r\naof_enabled:0\r\nrdb_changes_since_last_save:0\r\n",
            'memory': b"# Memory\r\nused_memory:1024\r\nmaxmemory:536870912\r\n",
        }[args[1]]

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.closed = True


class RedisSnapshotTests(unittest.TestCase):
    def snapshot(self, fake=None, **kwargs):
        fake = fake if fake is not None else FakeConnection()
        with patch("stack_runtime.RedisConnection", return_value=fake) as factory:
            result = redis_runtime_snapshot(HOST, PORT, ownership="external", **kwargs)
        factory.assert_called_once_with(HOST, PORT, timeout=kwargs.get("timeout", 1.0))
        self.assertEqual(fake.commands, COMMANDS)
        self.assertTrue(fake.closed)
        json.dumps(result)  # all evidence must be machine serializable
        return result

    def test_complete_no_aof_no_rdb_and_request_observation_separation(self):
        requested = {"appendonly": "yes", "save": "60 1", "launch_pid": 9999}
        result = self.snapshot(requested=requested, timeout=0.25)
        self.assertEqual(result["schema_version"], 1)
        self.assertEqual(result["status"], "observed")
        self.assertEqual(result["ownership"], "external")
        self.assertEqual(result["endpoint"], {"host": HOST, "port": PORT})
        self.assertEqual(result["requested"], requested)
        self.assertIsNot(result["requested"], requested)
        observed = result["observed"]
        self.assertEqual(observed["config"]["appendonly"]["value"], "no")
        self.assertEqual(observed["config"]["save"], {"status": "observed", "value": ""})
        self.assertEqual(observed["config"]["appendfsync"]["value"], "everysec")
        self.assertEqual(observed["appendfsync_activity"]["value"], "inactive")
        self.assertEqual(observed["redis_version"]["value"], "7.2.5")
        self.assertEqual(observed["process_id"]["value"], 4321)
        self.assertEqual(observed["info"]["memory"]["value"]["used_memory"], "1024")
        start = datetime.fromisoformat(result["observed_at"])
        end = datetime.fromisoformat(result["completed_at"])
        self.assertIsNotNone(start.tzinfo)
        self.assertGreaterEqual(end, start)

    def test_aof_activity_does_not_replace_observed_appendfsync_policy(self):
        result = self.snapshot(FakeConnection({
            ('CONFIG', 'GET', 'appendonly'): [b'appendonly', b'yes'],
            ('CONFIG', 'GET', 'appendfsync'): [b'appendfsync', b'no'],
        }))
        self.assertEqual(result["observed"]["appendfsync_activity"]["value"], "active")
        self.assertEqual(result["observed"]["config"]["appendfsync"]["value"], "no")

    def test_denied_config_does_not_infer_default_from_info_or_request(self):
        denied = {command: RunError("Redis: NOPERM CONFIG GET denied")
                  for command in COMMANDS if command[0] == 'CONFIG'}
        result = self.snapshot(FakeConnection(denied), requested=CONFIG)
        self.assertEqual(result["status"], "partial")
        for item in result["observed"]["config"].values():
            self.assertEqual(item["status"], "denied")
            self.assertNotIn("value", item)
        self.assertEqual(result["observed"]["appendfsync_activity"]["status"], "denied")
        self.assertEqual(result["observed"]["process_id"]["value"], 4321)

    def test_all_commands_denied(self):
        denied = {command: RunError("Redis: NOPERM blocked") for command in COMMANDS}
        result = self.snapshot(FakeConnection(denied))
        self.assertEqual(result["status"], "denied")
        self.assertEqual(result["observed"]["process_id"]["status"], "denied")

    def test_per_field_unknown_error_and_denied_continue_observation(self):
        result = self.snapshot(FakeConnection({
            ('CONFIG', 'GET', 'save'): [],
            ('CONFIG', 'GET', 'maxmemory'): RunError("Redis: ERR unknown command CONFIG"),
            ('INFO', 'persistence'): RunError("Redis: NOPERM INFO persistence denied"),
            ('INFO', 'memory'): b"# Memory\r\n",
        }))
        self.assertEqual(result["status"], "partial")
        observed = result["observed"]
        self.assertEqual(observed["config"]["save"]["status"], "unknown")
        self.assertEqual(observed["config"]["maxmemory"]["status"], "error")
        self.assertEqual(observed["info"]["persistence"]["status"], "denied")
        self.assertEqual(observed["info"]["memory"]["status"], "unknown")
        self.assertEqual(observed["redis_version"]["value"], "7.2.5")

    def test_malformed_config_is_not_accepted(self):
        for reply in ([b'save', b'no'], [b'appendonly'], b'no',
                      [b'appendonly', b'\xff']):
            with self.subTest(reply=reply):
                result = self.snapshot(FakeConnection({('CONFIG', 'GET', 'appendonly'): reply}))
                self.assertEqual(result["status"], "partial")
                self.assertEqual(result["observed"]["config"]["appendonly"]["status"], "error")
                self.assertNotIn("value", result["observed"]["appendfsync_activity"])

    def test_server_identity_absent_or_invalid_remains_explicit(self):
        result = self.snapshot(FakeConnection({('INFO', 'server'): b"uptime_in_seconds:1\r\n"}))
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["observed"]["redis_version"]["status"], "unknown")
        self.assertEqual(result["observed"]["process_id"]["status"], "unknown")
        for pid in ("invalid", "0", "-1"):
            with self.subTest(pid=pid):
                result = self.snapshot(FakeConnection({
                    ('INFO', 'server'): f"redis_version:7.2\r\nprocess_id:{pid}\r\n".encode(),
                }))
                self.assertEqual(result["observed"]["process_id"]["status"], "error")

    def test_connection_failure_never_falls_back_to_another_endpoint(self):
        for exc, status in ((OSError("connection refused"), "error"),
                            (RunError("Redis: NOAUTH Authentication required"), "denied"),
                            (RunError("Redis: WRONGPASS invalid credentials"), "denied")):
            with self.subTest(status=status, message=str(exc)):
                with patch("stack_runtime.RedisConnection", side_effect=exc) as factory:
                    result = redis_runtime_snapshot(HOST, PORT, ownership="external")
                factory.assert_called_once_with(HOST, PORT, timeout=1.0)
                self.assertEqual(result["status"], status)
                self.assertEqual(result["requested"], {})
                for item in result["observed"]["config"].values():
                    self.assertEqual(item["status"], status)
                    self.assertNotIn("value", item)

    def test_explicit_factory_receives_only_declared_endpoint(self):
        fake = FakeConnection()
        with patch("stack_runtime.RedisConnection", side_effect=AssertionError("unexpected default connection")):
            def factory(host, port, *, timeout):
                self.assertEqual((host, port, timeout), (HOST, PORT, 0.5))
                return fake
            result = redis_runtime_snapshot(HOST, PORT, ownership="owned_private",
                                            timeout=0.5, connection_factory=factory)
        self.assertEqual(result["ownership"], "owned_private")
        self.assertEqual(result["status"], "observed")
        self.assertEqual(fake.commands, COMMANDS)

    def test_invalid_declarations_make_no_connection(self):
        for host, port, ownership, timeout in (
                ("", PORT, "external", 1), (None, PORT, "external", 1),
                (HOST, 0, "external", 1), (HOST, 65536, "external", 1),
                (HOST, True, "external", 1), (HOST, "6379", "external", 1),
                (HOST, PORT, "shared", 1), (HOST, PORT, "external", 0),
                (HOST, PORT, "external", float("nan"))):
            with self.subTest(host=host, port=port, ownership=ownership, timeout=timeout):
                with patch("stack_runtime.RedisConnection") as factory:
                    with self.assertRaises(ValueError):
                        redis_runtime_snapshot(host, port, ownership=ownership, timeout=timeout)
                factory.assert_not_called()


class PrivateRedisSnapshotTests(unittest.TestCase):
    def test_owned_no_aof_no_rdb_snapshot_matches_own_pid_without_mutation(self):
        if not PRIVATE_REDIS:
            self.skipTest("opt in with --private-redis; defaults to fake connections only")
        binary = shutil.which("redis-server")
        if binary is None:
            self.skipTest("redis-server not installed; no downloads")
        with tempfile.TemporaryDirectory(prefix="tickshelf-snapshot-test-") as directory:
            with OwnedProcesses(Path(directory)) as owner:
                port, = candidate_ports(1)
                start_private_redis(owner, binary, port)
                requested = {"appendonly": "no", "save": "", "maxmemory": "512mb",
                             "maxmemory-policy": "noeviction"}
                before = redis_runtime_snapshot("127.0.0.1", port, ownership="owned_private",
                                                requested=requested)
                after = redis_runtime_snapshot("127.0.0.1", port, ownership="owned_private")
                self.assertEqual(before["status"], "observed")
                self.assertEqual(before["observed"]["process_id"]["value"], owner.children["redis"].pid)
                self.assertEqual(before["observed"]["config"]["appendonly"]["value"], "no")
                self.assertEqual(before["observed"]["config"]["save"]["value"], "")
                self.assertEqual(before["observed"]["appendfsync_activity"]["value"], "inactive")
                self.assertEqual(before["observed"]["config"], after["observed"]["config"])
                owner.require_alive("redis")
                self.assertFalse((Path(directory) / "redis/dump.rdb").exists())
                self.assertFalse((Path(directory) / "redis/appendonlydir").exists())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--private-redis", action="store_true",
                        help="also observe a newly started, uniquely owned private Redis")
    args, remaining = parser.parse_known_args()
    PRIVATE_REDIS = args.private_redis
    unittest.main(argv=[sys.argv[0], *remaining])
