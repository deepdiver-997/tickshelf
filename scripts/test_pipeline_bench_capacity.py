#!/usr/bin/env python3
"""Acceptance gates and default target warmup regression; no throughput claim."""
import argparse
import csv
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from pipeline_bench import (ROOT, aggregate, binary_path, full_chain_contract,
                            performance_summary, save_results_csv, stage_summary)
from pipeline_bench_support import DEFAULTS

BUILD = ROOT / "build-opt-release"


def fixture(overrides=None, records=1_200_000, source_elapsed=10.1, receiver_elapsed=10):
    config = {**DEFAULTS, "records": records, "rate": records // 10, **(overrides or {})}
    source = {"elapsed_s": source_elapsed, "send_elapsed_s": 10,
              "metrics": {"counters": {"source_sent": records}, "stages": {}}}
    receiver = {"elapsed_s": receiver_elapsed,
                "metrics": {"counters": {"received": records}, "stages": {}}}
    progress, source_progress = [], []
    for second in range(11):
        count = second * records // 10
        progress.append({"elapsed_s": second, "monotonic_ns": second * 1_000_000_000,
                         "metrics": {"counters": {"received": count, "forwarded": count,
                                      "dedup_dropped": 0, "committed_attempts": count, "sink_consumed": 0}}})
        source_progress.append({"elapsed_s": second, "monotonic_ns": second * 1_000_000_000,
                                "metrics": {"counters": {"source_sent": count}}})
    return config, source, receiver, progress, source_progress


def result(overrides=None, minimum=100_000, **kwargs):
    config, source, receiver, progress, source_progress = fixture(overrides, **kwargs)
    return performance_summary(source, receiver, progress, [], config, 10, "target", source_progress, minimum)


class CapacityTests(unittest.TestCase):
    def test_hard_floor_is_after_ack_and_final_completion_not_95_percent(self):
        self.assertEqual(result()["verdict"], "pass-full-chain-capacity")
        measured = result(records=1_000_000)
        self.assertEqual(measured["durable_unique_records_per_s_including_drain"], 100_000)
        self.assertLess(measured["end_to_end_durable_unique_records_per_s"], 100_000)
        self.assertEqual(measured["verdict"], "fail-minimum-durable-throughput")
        self.assertFalse(measured["minimum_durable_rate_passed"])

    def test_requested_duration_cannot_disguise_a_short_actual_offer(self):
        config, source, receiver, progress, source_progress = fixture(records=1_200_000, source_elapsed=9.91)
        source["send_elapsed_s"] = 9.9
        measured = performance_summary(source, receiver, progress, [], config, 10, "target", source_progress, 100_000)
        self.assertEqual(measured["verdict"], "inconclusive-capacity-window-under-ten-seconds")

    def test_ablations_cannot_pass_complete_chain_contract(self):
        for change in ({"input": "binary"}, {"parse": "replay"}, {"depth": 1}, {"hash": "off"},
                       {"hash_algorithm": "fnv1a64"}, {"redis": "off"}, {"redis": "encode"},
                       {"backend": "null"}, {"synchronous": "NORMAL"}, {"symbols": 1},
                       {"duplicate_every": 7}, {"optional_fields": True}):
            with self.subTest(change=change):
                self.assertFalse(full_chain_contract({**DEFAULTS, **change})["eligible"])
                self.assertEqual(result(change)["verdict"], "fail-full-chain-contract")

    def test_backend_names_are_not_interchanged(self):
        for backend in ("sqlite", "sqlite_compact", "segment", "segment_rocksdb"):
            self.assertEqual(result({"backend": backend})["verdict"], "pass-full-chain-capacity")
            self.assertTrue(result({"backend": backend})["full_chain_contract"]["eligible"])

    def test_generic_target_remains_separate_from_hard_capacity_gate(self):
        self.assertEqual(result(records=300_000, minimum=0)["verdict"], "pass-bounded-target-observation")
        self.assertFalse(result({"redis": "off"}, minimum=0)["full_chain_contract"]["eligible"])

    def test_reports_preserve_hard_floor_and_single_owner_cache_semantics(self):
        measured = result()
        self.assertIn("held once", measured["dedup_cache"]["semantics"])
        rows = [{"case": "compact", "repeat": 1, "status": "passed", "config": {"records": 1_200_000},
                 "performance": measured, "receiver": {"elapsed_s": 10}}]
        groups = aggregate(rows)
        self.assertEqual(groups["compact"]["full_chain_eligible_runs"], 1)
        self.assertEqual(groups["compact"]["end_to_end_durable_unique_rates_summary"]["median"],
                         measured["end_to_end_durable_unique_records_per_s"])
        with tempfile.TemporaryDirectory(prefix="tickshelf-capacity-csv-") as directory:
            path = Path(directory) / "results.csv"
            save_results_csv(path, rows)
            with path.open() as source:
                exported = next(csv.DictReader(source))
            self.assertEqual(exported["minimum_durable_records_per_s"], "100000")
            self.assertEqual(exported["full_chain_eligible"], "True")
            self.assertIn("dedup_cache_bytes", exported)
            self.assertEqual(float(exported["end_to_end_durable_unique_records_per_s"]),
                             measured["end_to_end_durable_unique_records_per_s"])

    def test_queue_age_observations_are_not_reported_as_work_or_cpu_fraction(self):
        _, source, receiver, _, _ = fixture()
        receiver["metrics"]["stages"] = {"store_queue_oldest_age": {"sum_ns": 1000, "count": 2},
                                               "store_total": {"sum_ns": 500, "count": 2}}
        stages = stage_summary(source, receiver)["receiver"]
        self.assertEqual(stages["store_queue_oldest_age"]["measurement_kind"], "observed_queue_age")
        self.assertEqual(stages["store_queue_oldest_age"]["mean_ns_per_call"], 500)
        self.assertIsNone(stages["store_queue_oldest_age"]["wall_fraction_of_role_elapsed"])
        self.assertIsNone(stages["store_queue_oldest_age"]["amortized_ns_per_input_record"])
        self.assertEqual(stages["store_total"]["measurement_kind"], "timed_work")
        self.assertIsNotNone(stages["store_total"]["wall_fraction_of_role_elapsed"])

    def test_default_target_warmup_is_not_rejected_for_zero_rate(self):
        binary = binary_path(BUILD)
        if not binary.is_file():
            self.skipTest(f"executable not built: {binary}")
        with tempfile.TemporaryDirectory(prefix="tickshelf-capacity-test-") as directory:
            for floor in (0, 1000):
                with self.subTest(minimum=floor):
                    output = Path(directory) / f"new-output-{floor}"
                    command = [sys.executable, str(ROOT / "scripts/pipeline_bench.py"), "--build", str(binary),
                               "--suite", "target", "--records", "108", "--rate", "1000", "--duration", "1",
                               "--min-durable-rps", str(floor),
                               "--config", '{"backend":"null","redis":"off"}', "--max-disk-mib", "32",
                               "--output", str(output)]
                    done = subprocess.run(command, capture_output=True, text=True, timeout=20)
                    self.assertEqual(done.returncode, 1)  # Neither bypass is a durable capacity pass.
                    summary = json.loads((output / "summary.json").read_text())
                    self.assertNotIn("error", summary)
                    self.assertEqual(len(summary["results"]), 2)
                    warmup, measured = summary["results"]
                    self.assertTrue(warmup["warmup"])
                    self.assertEqual(warmup["status"], "passed")
                    self.assertEqual(warmup["config"]["rate"], 0)
                    self.assertEqual(measured["status"], "passed")
                    self.assertEqual(measured["performance"]["verdict"],
                                     "fail-full-chain-contract" if floor else "consume-only-not-durable")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--build", type=Path, default=BUILD)
    options, remaining = parser.parse_known_args()
    BUILD = options.build
    unittest.main(argv=[sys.argv[0], *remaining])
