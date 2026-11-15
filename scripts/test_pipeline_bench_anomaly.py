#!/usr/bin/env python3
"""Deterministic tests for in-run profiling gates, without starting a profiler."""
import tempfile
import unittest
from pathlib import Path

from pipeline_bench_anomaly import AnomalyTrigger
from pipeline_bench import tail_progress, load_matrix
import json


def point(second, committed, peak=0, sent=None):
    counters = {"committed_attempts": committed, "sink_consumed": committed, "dedup_dropped": 0}
    if sent is not None:
        counters["source_sent"] = sent
    return {"monotonic_ns": int(second * 1e9), "metrics": {"counters": counters,
            "stages": {"sqlite_commit": {"max_ns": peak}}}}


class AnomalyTests(unittest.TestCase):
    def test_elapsed_alone_does_not_trigger_healthy_run(self):
        gate = AnomalyTrigger(minimum_records_per_s=90, maximum_backlog_records=50)
        for second in range(1, 8):
            self.assertIsNone(gate.observe(second, point(second, second * 100), point(second, 0, sent=second * 100)))

    def test_distinct_sustained_low_rate_windows(self):
        gate = AnomalyTrigger(minimum_elapsed_s=0, minimum_records_per_s=100, sustained_windows=2)
        self.assertIsNone(gate.observe(1, point(1, 10)))
        self.assertIsNone(gate.observe(2, point(2, 20)))
        self.assertIsNone(gate.observe(2.5, point(2, 20)))
        result = gate.observe(3, point(3, 30))
        self.assertIn("sustained-low-completion-rate", result["reasons"])
        self.assertEqual(result["low_rate_windows"], 2)

    def test_backlog_uses_past_source_snapshot_and_recovers(self):
        gate = AnomalyTrigger(minimum_elapsed_s=0, maximum_backlog_records=50)
        self.assertIsNone(gate.observe(1, point(1, 10), point(1, 0, sent=100)))
        # A newer source snapshot must not inflate an old receiver's queue.
        self.assertIsNone(gate.observe(2, point(2, 20), point(3, 0, sent=500)))
        self.assertIsNone(gate.observe(3, point(3, 30), point(3, 0, sent=200)))
        result = gate.observe(4, point(4, 40), point(4, 0, sent=300))
        self.assertIn("sustained-source-to-commit-backlog", result["reasons"])
        self.assertIsNone(gate.observe(5, point(5, 500), point(5, 0, sent=500)))

    def test_new_sync_peak_not_stale_warmup_peak(self):
        gate = AnomalyTrigger(minimum_elapsed_s=2, maximum_commit_ms=10)
        self.assertIsNone(gate.observe(1, point(1, 100, 20_000_000)))
        self.assertIsNone(gate.observe(2, point(2, 200, 20_000_000)))
        result = gate.observe(3, point(3, 300, 30_000_000))
        self.assertIn("new-commit-or-file-sync-latency-peak", result["reasons"])
        self.assertIsNone(gate.observe(4, point(4, 400, 30_000_000)))

    def test_live_partial_line_is_ignored_and_read_is_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "progress.jsonl"
            path.write_bytes(b'{"monotonic_ns":1}\n{"monotonic_ns":')
            self.assertEqual(tail_progress(path), [{"monotonic_ns": 1}])
            path.write_bytes((b'{"monotonic_ns":2}\n' * 20000) + b'{"monotonic_ns":3}\n')
            result = tail_progress(path)
            self.assertLess(len(result), 4000)
            self.assertEqual(result[-1]["monotonic_ns"], 3)


class MatrixTests(unittest.TestCase):
    def matrix(self, value):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "matrix.json"
            path.write_text(json.dumps(value))
            return load_matrix(path)

    def test_axes_expand_config_values_without_changing_code(self):
        cases = self.matrix({"axes": {"transaction_rows": [16, 1000], "redis": ["roundtrip", "off"]}})
        self.assertEqual(len(cases), 4)
        self.assertEqual(cases[0], ("matrix-001", {"transaction_rows": 16, "redis": "roundtrip"}))
        self.assertEqual(cases[-1][1], {"transaction_rows": 1000, "redis": "off"})

    def test_named_cases_validate_types_paths_and_duplicates(self):
        self.assertEqual(self.matrix({"cases": [{"name": "full", "set": {}}]}), [("full", {})])
        for cases in ([{"name": "../bad", "set": {}}],
                      [{"name": "a", "set": {}}, {"name": "a", "set": {}}],
                      [{"name": "a", "set": {"directory": "/tmp/unsafe"}}],
                      [{"name": "a", "set": {"transaction_rows": True}}]):
            with self.assertRaises(ValueError):
                self.matrix({"cases": cases})

    def test_empty_unknown_and_excessive_products_rejected(self):
        for spec in ({}, {"axes": {}}, {"axes": {"redis": []}},
                     {"axes": {"unknown": [1]}}, {"cases": []},
                     {"axes": {"transaction_rows": list(range(65))}}):
            with self.assertRaises(ValueError):
                self.matrix(spec)


if __name__ == "__main__":
    unittest.main()
