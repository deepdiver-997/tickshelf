#!/usr/bin/env python3
"""Small offline Engine-runner oracles; optional native checks use Null Redis only."""
from __future__ import annotations

import argparse
import copy
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import engine_bench as bench
from stack_runtime import OwnedProcesses, RunError

BUILD = bench.BUILD
SCHEMA = "CREATE TABLE ticks(symbol TEXT,ts_exchange_ms INTEGER,ts_receive_ms INTEGER,bid_price INTEGER,ask_price INTEGER,last_price INTEGER,bid_size INTEGER,ask_size INTEGER,last_size INTEGER,flags INTEGER,PRIMARY KEY(symbol,ts_exchange_ms)) WITHOUT ROWID"


def populate(path, shards, rows):
    connections = []
    try:
        for index in range(shards):
            connection = sqlite3.connect(bench.shard_path(path, index, shards))
            connection.execute(SCHEMA)
            connections.append(connection)
        for row in rows:
            index = bench.symbol_shard(row[0], shards)
            connections[index].execute("INSERT OR IGNORE INTO ticks VALUES(?,?,?,?,?,?,?,?,?,?)", row)
        for connection in connections:
            connection.commit()
    finally:
        for connection in connections:
            connection.close()


class EngineBenchOracles(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="tickshelf-engine-oracle-")
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name)
        self.manifest = bench.write_input(self.directory / "input.ticks", 25, 3, 5)

    def metrics(self, storage="sqlite", fifo="on"):
        forwarded = self.manifest["unique_records"] if fifo == "on" else self.manifest["records"]
        persistent = storage == "sqlite"
        return {"schema": 1, "status": "PASS", "label": "in-process-real-Engine-no-TTAP-ingress",
                "parameters": {"redis": "null", "storage": storage, "fifo": fifo,
                               "pipeline": "off", "records": 25, "shards": 2},
                "timing": {"elapsed_s": .3, "submit_s": .1, "drain_s": .2},
                "counters": {"submitted_records": 26, "accepted_records": 25, "rejected_records": 1,
                             "sealed_slices": 4, "queue_peak": 2, "retries": 0, "published_records": forwarded,
                             "dedup_dropped": 25 - forwarded,
                             "committed_input_records": forwarded if persistent else 0,
                             "inserted_unique_records": self.manifest["unique_records"] if persistent else 0,
                             "sink_consumed_records": 0 if persistent else forwarded,
                             "durable_records": forwarded if persistent else 0,
                             "pending_records": 0, "queue_depth": 0, "degraded_records": 0,
                             "skipped_records": 0, "finish_pending_records": 0, "publish_only_records": 0},
                "queue_complete": True, "storage_complete": persistent,
                "observation": {"input_count": 0 if persistent else forwarded, "persistent": persistent,
                                "checksum_sum_fnv1a64": "0000000000000000" if persistent else
                                self.manifest["checksum_unique" if fifo == "on" else "checksum_all"]},
                "sqlite_connections": [{"shard": i, "version": "test", "journal_mode": "wal", "synchronous": 1,
                                        "fullfsync": 0, "checkpoint_fullfsync": 0, "wal_autocheckpoint": 1000}
                                       for i in range(2)] if persistent else []}

    def test_input_is_deterministic_canonical_and_contains_real_duplicates(self):
        other = bench.write_input(self.directory / "other.ticks", 25, 3, 5)
        self.assertEqual(bench.sha256(other["path"]), self.manifest["sha256_before"])
        raw = Path(self.manifest["path"]).read_bytes()
        self.assertEqual(len(raw), 25 * 64)
        self.assertEqual(raw[3 * 64:4 * 64], raw[4 * 64:5 * 64])
        self.assertEqual(self.manifest["unique_records"], 20)
        self.assertNotEqual(self.manifest["checksum_all"], self.manifest["checksum_unique"])

    def test_all_native_null_sqlite_fifo_counter_cells(self):
        for storage in ("sqlite", "null"):
            for fifo in ("off", "on"):
                with self.subTest(storage=storage, fifo=fifo):
                    data = self.metrics(storage, fifo)
                    self.assertTrue(bench.validate_native(data, self.manifest, "null", storage, fifo, "off")["passed"])

    def test_null_cannot_claim_commit_insert_or_storage_completion(self):
        for field, value in (("committed_input_records", 20), ("inserted_unique_records", 20), ("durable_records", 20)):
            data = self.metrics("null")
            data["counters"][field] = value
            with self.subTest(field=field), self.assertRaises(RunError):
                bench.validate_native(data, self.manifest, "null", "null", "on", "off")
        data = self.metrics("null")
        data["storage_complete"] = True
        with self.assertRaises(RunError):
            bench.validate_native(data, self.manifest, "null", "null", "on", "off")

    def test_native_timing_checksum_and_pragma_gates(self):
        corruptions = [lambda d: d["timing"].update(elapsed_s=.4),
                       lambda d: d["timing"].update(elapsed_s=float("nan")),
                       lambda d: d["observation"].update(checksum_sum_fnv1a64="ffffffffffffffff"),
                       lambda d: d.update(sqlite_connections=[]),
                       lambda d: d["counters"].update(skipped_records=1),
                       lambda d: d["counters"].update(finish_pending_records=1),
                       lambda d: d["counters"].update(accepted_records=True)]
        for corrupt in corruptions:
            data = self.metrics()
            corrupt(data)
            with self.assertRaises(RunError):
                bench.validate_native(data, self.manifest, "null", "sqlite", "on", "off")

    def test_independent_sqlite_oracle_checks_every_field(self):
        rows = [bench.tick_values(i, 3) for i in range(20)]
        for field in range(2, 10):
            path = self.directory / f"corrupt-{field}.db"
            corrupted = list(rows)
            row = list(corrupted[3])
            row[field] += 1
            corrupted[3] = tuple(row)
            populate(path, 2, corrupted)
            with self.subTest(field=field), self.assertRaisesRegex(RunError, "full-field"):
                bench.verify_sqlite(path, 2, self.manifest)
        path = self.directory / "correct.db"
        populate(path, 2, rows)
        self.assertEqual(bench.verify_sqlite(path, 2, self.manifest)["physical_rows"], 20)

    def test_sqlite_missing_extra_and_wrong_shard_fail(self):
        rows = [bench.tick_values(i, 3) for i in range(20)]
        for name, altered in (("missing", rows[:-1]), ("extra", rows + [bench.tick_values(20, 3)])):
            path = self.directory / (name + ".db")
            populate(path, 2, altered)
            with self.subTest(name=name), self.assertRaises(RunError):
                bench.verify_sqlite(path, 2, self.manifest)
        path = self.directory / "wrong-shard.db"
        populate(path, 2, [])
        row = rows[0]
        index = 1 - bench.symbol_shard(row[0], 2)
        with sqlite3.connect(bench.shard_path(path, index, 2)) as connection:
            connection.execute("INSERT INTO ticks VALUES(?,?,?,?,?,?,?,?,?,?)", row)
        with self.assertRaisesRegex(RunError, "wrong shard"):
            bench.verify_sqlite(path, 2, self.manifest)

    def test_sent_requires_exact_one_positive_consistent_trailer(self):
        self.assertEqual(bench.parse_sent("RATE sent=1\nSENT 8192 2\n"), (8192, 2))
        for text in ("", "SENT 0 0\n", "SENT 1 1\n", "SENT 4096 1\nSENT 4096 1\n", "SENT x 1\n"):
            with self.subTest(text=text), self.assertRaises(RunError):
                bench.parse_sent(text)

    def test_flood_independent_fullfield_oracle(self):
        sources = [{"base": 100000, "ticks": 8}]
        rows = []
        for i in range(8):
            exchange = 1001
            bid = 10000000 + exchange % 1000
            rows.append((f"F{100000 + i:07d}", exchange, exchange + 1, bid, bid + 100, bid + 50, 10, 10, 1, 0))
        correct = self.directory / "flood.db"
        populate(correct, 2, rows)
        self.assertTrue(bench.verify_flood_sqlite(correct, 2, sources)["passed"])
        with sqlite3.connect(bench.shard_path(correct, bench.symbol_shard(rows[0][0], 2), 2)) as connection:
            connection.execute("UPDATE ticks SET ask_size=123 WHERE symbol=?", (rows[0][0],))
        with self.assertRaisesRegex(RunError, "full-field"):
            bench.verify_flood_sqlite(correct, 2, sources)

    def test_new_output_rejects_existing_before_any_child(self):
        with patch.object(bench, "OwnedProcesses") as owner:
            with self.assertRaises(SystemExit):
                bench.main(["--output", str(self.directory)])
            owner.assert_not_called()

    def test_native_failure_keeps_logs_result_and_input_hash(self):
        args = SimpleNamespace(shards=2, batch_records=4, inflight=2,
                               max_pending_records=8, dedup_bytes=1024, timeout=1)
        with patch.object(bench, "wait_child", side_effect=RunError("synthetic timeout")), \
                patch.object(OwnedProcesses, "start", return_value=None):
            result = bench.run_native(self.directory, self.directory / "failed", Path(self.manifest["path"]),
                                      self.manifest, redis="null", storage="null", fifo="on", pipeline="off", args=args)
        self.assertEqual(result["status"], "FAIL")
        self.assertIn("synthetic timeout", result["error"])
        self.assertTrue((self.directory / "failed/result.json").is_file())
        self.assertEqual(result["input_sha256_before"], result["input_sha256_after"])
        self.assertTrue(Path(self.manifest["path"]).is_file())


class NativeEngineSmoke(unittest.TestCase):
    def test_small_native_null_redis_all_storage_fifo_cells(self):
        if not (BUILD / "engine_ablation").is_file():
            self.skipTest(f"optional engine_ablation not built under {BUILD}")
        with tempfile.TemporaryDirectory(prefix="tickshelf-native-smoke-") as temporary:
            directory = Path(temporary)
            manifest = bench.write_input(directory / "input.ticks", 41, 4, 5)
            args = SimpleNamespace(shards=2, batch_records=4, inflight=2,
                                   max_pending_records=8, dedup_bytes=1024, timeout=3)
            for storage in ("sqlite", "null"):
                for fifo in ("off", "on"):
                    for pipeline in ("off", "on"):
                        with self.subTest(storage=storage, fifo=fifo, pipeline=pipeline):
                            result = bench.run_native(
                                BUILD, directory / f"{storage}-{fifo}-pipe-{pipeline}",
                                directory / "input.ticks", manifest, redis="null",
                                storage=storage, fifo=fifo, pipeline=pipeline, args=args)
                            self.assertEqual(result["status"], "PASS", json.dumps(result, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--build", type=Path, default=BUILD)
    options, remaining = parser.parse_known_args()
    BUILD = options.build.resolve()
    unittest.main(argv=[sys.argv[0], *remaining])
