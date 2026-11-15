#!/usr/bin/env python3
"""Qualification regressions. All fixtures are synthetic, never production data."""
import argparse
import json
import os
import shutil
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from pipeline_bench import full_chain_contract, performance_summary, source_evidence, stage_summary
from pipeline_bench_support import DEFAULTS, generate_record, record_json
from test_pipeline_bench_capacity import fixture

BUILD = Path(__file__).resolve().parent.parent / "build-record-release"


def synthetic_info(config):
    return {"records": config["records"], "unique_records": config["records"],
            "source": {"kind": "synthetic", "provenance": "explicit test fixture", "authorized": False},
            "coverage": {"records": config["records"], "observed_symbols": 54,
                         "full_five_level_baseline_records": config["records"],
                         "all_records_full_five_level_baseline": True}}


class QualificationTests(unittest.TestCase):
    def test_synthetic_pass_is_not_production_validation(self):
        evidence = source_evidence(synthetic_info(DEFAULTS))
        self.assertFalse(evidence["production_capture_supplied"])
        self.assertIn("not-performed", evidence["production_validation"])
        self.assertIn("integrity", evidence["authenticity_scope"])

    def test_actual_input_coverage_overrides_declared_universe(self):
        config = {**DEFAULTS, "symbols": 5400}
        info = synthetic_info(config)
        info["coverage"]["observed_symbols"] = 1
        self.assertFalse(full_chain_contract(config, info)["eligible"])
        info["coverage"]["observed_symbols"] = 5400
        info["coverage"]["all_records_full_five_level_baseline"] = False
        self.assertFalse(full_chain_contract(config, info)["eligible"])
        info.pop("coverage")
        self.assertFalse(full_chain_contract(config, info)["eligible"])

    def test_authorized_capture_still_only_has_attested_provenance(self):
        config = dict(DEFAULTS)
        info = synthetic_info(config)
        info["source"].update(kind="production_capture", authorized=False)
        self.assertFalse(full_chain_contract(config, info)["eligible"])
        info["source"]["authorized"] = True
        contract = full_chain_contract(config, info)
        self.assertTrue(contract["eligible"])
        self.assertIn("user-attested", contract["source"]["production_validation"])
        self.assertIn("not live production", contract["source"]["production_validation"])

    def test_natural_capture_duplicates_are_not_a_generator_flag(self):
        config = {**DEFAULTS, "duplicate_every": 7, "optional_fields": True}
        info = synthetic_info(config)
        info["source"].update(kind="production_capture", authorized=True)
        self.assertTrue(full_chain_contract(config, info)["eligible"])
        info["unique_records"] = config["records"] - 1
        self.assertTrue(full_chain_contract(config, info)["eligible"])

    def test_rocksdb_is_a_durable_full_record_backend(self):
        self.assertTrue(full_chain_contract({**DEFAULTS, "backend": "rocksdb"})["eligible"])
        self.assertFalse(full_chain_contract({**DEFAULTS, "backend": "rocksdb", "synchronous": "NORMAL"})["eligible"])

    def test_integer_timing_cannot_round_a_floor_miss_into_pass(self):
        config, source, receiver, progress, source_progress = fixture(records=1_000_000, source_elapsed=10)
        source.update(elapsed_ns=10_000_000_001, send_elapsed_ns=10_000_000_000)
        receiver["elapsed_ns"] = 10_000_000_000
        measured = performance_summary(source, receiver, progress, [], config, 10, "target", source_progress, 100_000)
        self.assertEqual(measured["verdict"], "fail-minimum-durable-throughput")

    def test_actual_unique_count_not_generator_formula_sets_capture_rate(self):
        config, source, receiver, progress, source_progress = fixture()
        info = synthetic_info(config)
        info["unique_records"] = 100_000
        measured = performance_summary(source, receiver, progress, [], config, 10, "diagnostic", source_progress,
                                       input_info=info)
        self.assertEqual(measured["durable_unique_count"], 100_000)
        self.assertAlmostEqual(measured["end_to_end_durable_unique_records_per_s"], 100_000 / source["elapsed_s"])

    def test_missing_source_plan_telemetry_cannot_pass_new_target(self):
        config, source, receiver, progress, source_progress = fixture()
        measured = performance_summary(source, receiver, progress, [], config, 10, "target", source_progress,
                                       require_source_schedule=True)
        self.assertEqual(measured["verdict"], "inconclusive-missing-source-schedule")

    def scheduled_result(self, *, lag_ns=10_000_000, gap_step=0):
        config, source, receiver, progress, source_progress = fixture()
        source["metrics"]["counters"].update(source_planned_records=config["records"],
                                               source_send_complete_lag_max_ns=lag_ns,
                                               source_planned_minus_sent_peak=gap_step * 9)
        for second, point in enumerate(source_progress):
            c = point["metrics"]["counters"]
            c["source_planned_records"] = min(config["records"], c["source_sent"] + second * gap_step)
        return performance_summary(source, receiver, progress, [], config, 10, "target", source_progress,
                                   require_source_schedule=True)

    def test_delayed_source_is_not_hidden_by_final_drain(self):
        self.assertEqual(self.scheduled_result(lag_ns=2_000_000_000)["verdict"], "fail-source-schedule-lag")
        self.assertEqual(self.scheduled_result(gap_step=2000)["verdict"], "fail-growing-unsent-source-plan")
        self.assertEqual(self.scheduled_result()["verdict"], "pass-bounded-target-observation")

    def test_source_lag_is_not_reported_as_work_fraction(self):
        _, source, receiver, _, _ = fixture()
        source["metrics"]["stages"]["source_send_start_lag"] = {"count": 2, "sum_ns": 999_000_000}
        stage = stage_summary(source, receiver)["source"]["source_send_start_lag"]
        self.assertIsNone(stage["wall_fraction_of_role_elapsed"])
        self.assertIsNone(stage["amortized_ns_per_input_record"])


class CaptureRunnerTests(unittest.TestCase):
    def setUp(self):
        self.binary = BUILD / "pipeline_bench" if BUILD.is_dir() else BUILD
        if not self.binary.is_file():
            self.skipTest("new benchmark executable not built")
        self.temp = tempfile.TemporaryDirectory(prefix="tickshelf-capture-runner-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.raw = self.root / "synthetic.jsonl"
        a, b = (record_json(generate_record(i, DEFAULTS)) for i in (0, 1))
        self.raw.write_text('{"ticks":[]}\n{"ticks":[' + a + ',' + b + ']}\r\n{"ticks":[' + a + ']}\n{"ticks":[]}\n')

    def invoke(self, *extra):
        output = self.root / "new-results"
        command = [sys.executable, str(Path(__file__).with_name("pipeline_bench.py")), "--build", str(self.binary),
                   "--suite", "smoke", "--capture", str(self.raw), "--capture-source-kind", "synthetic",
                   "--capture-provenance", "artificial integration fixture; not production", "--timeout", "20",
                   "--max-disk-mib", "32", "--record-limit", "10", "--output", str(output), *extra]
        done = subprocess.run(command, capture_output=True, text=True, timeout=45)
        return done, output

    def test_original_variable_frames_remote_duplicate_and_no_production_claim(self):
        before = self.raw.read_bytes()
        done, output = self.invoke("--config", '{"backend":"sqlite_compact","redis":"off","dedup_max_entries":1}')
        self.assertEqual(done.returncode, 0, done.stderr + done.stdout)
        summary = json.loads((output / "summary.json").read_text())
        self.assertEqual(len(summary["results"]), 1)
        result = summary["results"][0]
        self.assertEqual(result["input"]["source_mode"], "capture_original_json")
        self.assertEqual(result["config"]["records"], 3)
        self.assertEqual(result["oracle"]["actual_unique"], 2)
        self.assertEqual(result["receiver"]["metrics"]["counters"]["forwarded"], 3)
        self.assertIn("not-performed", summary["production_validation"])
        self.assertEqual(self.raw.read_bytes(), before)
        self.assertTrue(result["correctness"]["passed"])

    def test_capture_full_record_rocks_and_real_private_redis(self):
        if not shutil.which("redis-server"):
            self.skipTest("real private Redis unavailable; do not replace it with bypass")
        probe = self.root / "rocks-probe.json"
        probe.write_text(json.dumps({"directory": str(self.root / "unused"), "backend": "rocksdb"}))
        checked = subprocess.run([self.binary, "--config", probe, "--check-config"], capture_output=True, text=True, timeout=5)
        if checked.returncode and "requires TICKSHELF_BENCH_ROCKSDB=ON" in checked.stderr:
            self.skipTest("optional full-record RocksDB is not built")
        self.assertEqual(checked.returncode, 0, checked.stderr)
        done, output = self.invoke("--config", '{"backend":"rocksdb","redis":"roundtrip","dedup_max_entries":1}')
        self.assertEqual(done.returncode, 0, done.stderr + done.stdout)
        summary = json.loads((output / "summary.json").read_text())
        result = summary["results"][0]
        self.assertEqual(result["receiver"]["store"]["backend"], "rocksdb")
        self.assertEqual(result["oracle"]["actual_unique"], 2)
        self.assertGreater(result["receiver"]["metrics"]["counters"]["redis_commands"], 0)
        self.assertFalse(summary["production_qualified"])
        self.assertEqual(list(Path(result["directory"]).rglob("*.segments")), [])

    def test_record_count_mismatch_is_not_silent_truncation(self):
        done, output = self.invoke("--records", "2", "--config", '{"backend":"null","redis":"off"}')
        self.assertEqual(done.returncode, 1)
        self.assertIn("silently truncating", json.loads((output / "summary.json").read_text())["error"])

    def test_production_capture_without_authorization_is_rejected(self):
        done, output = self.invoke("--capture-source-kind", "production_capture")
        self.assertEqual(done.returncode, 2)
        self.assertIn("authorization", done.stderr)
        self.assertFalse(output.exists())

    def test_capture_replay_bypass_is_rejected_before_prepare(self):
        done, output = self.invoke("--config", '{"parse":"replay"}')
        self.assertEqual(done.returncode, 2)
        self.assertFalse(output.exists())

    def test_non_linux_trace_cannot_silently_run_untraced(self):
        if sys.platform.startswith("linux"):
            self.skipTest("platform-specific no-fallback assertion")
        done, output = self.invoke("--syscalls", "trace")
        self.assertEqual(done.returncode, 2)
        self.assertIn("unavailable", done.stderr)
        self.assertFalse(output.exists())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--build", type=Path, default=BUILD)
    options, remaining = parser.parse_known_args()
    BUILD = options.build
    unittest.main(argv=[sys.argv[0], *remaining])
