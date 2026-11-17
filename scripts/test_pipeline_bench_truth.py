#!/usr/bin/env python3
"""Pure ablation mapping/evidence tests; no Redis or storage is started."""
import copy
import csv
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from pipeline_bench import full_chain_contract, performance_summary
from pipeline_bench_support import DEFAULTS
from pipeline_bench_truth import (build_truth_report, describe_run, effective_work,
                                  truth_cells, truth_specs, write_truth_report)

ROOT = Path(__file__).resolve().parent.parent


def observed(config, received=108):
    digest = config["hash"] != "off"
    dedup = config["hash"] == "dedup"
    network = config["redis"] in ("roundtrip", "publish")
    backend = config["backend"]
    stored_digest = backend in ("sqlite_compact", "rocksdb")
    counters = {
        "ablation_work_schema": 1, "received": received, "forwarded": received, "dedup_dropped": 0,
        "record_encoded_records": received,
        "hash_invocations": received if digest else 0,
        "hash_bypassed_records": 0 if digest else received,
        "dedup_lookup_records": received if dedup else 0,
        "dedup_candidate_comparisons": 0,
        "redis_build_records": received if config["redis"] != "off" else 0,
        "redis_publish_records": received if network else 0,
        "redis_readback_records": received if config["redis"] == "roundtrip" else 0,
        "redis_network_execs": 3 if network else 0,
        "redis_commands": 4 if network else 0,
        "committed_attempts": received if backend != "null" else 0,
        "sink_consumed": received if backend == "null" else 0,
        "backend_digest_recomputed": received if stored_digest and not digest else 0,
        "backend_digest_cached": received if stored_digest and digest else 0,
        "backend_lookup_calls": received if stored_digest and config.get("store_lookup", "exact") == "exact" else 0,
        "backend_lookup_exact": int(config.get("store_lookup", "exact") == "exact"),
        "backend_lookup_assume_unique": int(config.get("store_lookup", "exact") == "assume_unique"),
        "canonical_bytes": received * 291,
    }
    stage = "json_parse_normalize" if config["parse"] == "real" else "replay_copy"
    return {"metrics": {"counters": counters, "stages": {stage: {"count": 1}}}}


def run_for(cell, base, repeat=1):
    config = {**base, **cell["overrides"]}
    receiver = observed(config)
    return {
        "case": cell["case"], "repeat": repeat, "status": "passed", "config": config,
        "correctness": {"passed": True}, "ablation": describe_run(config, receiver),
        "performance": {
            "end_to_end_input_records_per_s": 108.0,
            "end_to_end_durable_unique_records_per_s": 108.0 if cell["S"] else None,
            "full_chain_contract": full_chain_contract(config),
        },
        "input_source": {"kind": "synthetic"},
        "input": {"sha256": "a" * 64, "records": base["records"]}, "receiver": receiver,
    }


class TruthTableTests(unittest.TestCase):
    def setUp(self):
        self.base = {**DEFAULTS, "records": 108, "backend": "sqlite_compact", "sqlite_compact_lookup": "separate"}

    def test_all_32_rows_24_legal_and_8_dependency_invalid(self):
        cells = truth_cells(self.base)
        self.assertEqual(len(cells), 32)
        self.assertEqual(len({cell["case"] for cell in cells}), 32)
        self.assertEqual(sum(cell["valid"] for cell in cells), 24)
        invalid = [cell for cell in cells if not cell["valid"]]
        self.assertEqual(len(invalid), 8)
        self.assertTrue(all(cell["D"] and not cell["H"] and cell["overrides"] is None for cell in invalid))
        self.assertEqual(len(truth_specs(self.base)), 24)

    def test_existing_native_hash_modes_map_without_fabricating_records(self):
        for cell in truth_cells(self.base):
            if not cell["valid"]:
                continue
            config = cell["overrides"]
            self.assertEqual(config["hash"], "dedup" if cell["D"] else "only" if cell["H"] else "off")
            self.assertEqual(config["parse"], "real" if cell["P"] else "replay")
            self.assertEqual(config["redis"], "roundtrip" if cell["R"] else "off")
            self.assertEqual(config["backend"], "sqlite_compact" if cell["S"] else "null")
            if not cell["S"]:
                self.assertEqual(config["sqlite_compact_lookup"], "subquery")

    def test_noncomparably_configured_profiles_rejected(self):
        for change in ({"input": "binary"}, {"hash_algorithm": "fnv1a64"}, {"backend": "null"},
                       {"synchronous": "NORMAL"}, {"store_lookup": "assume_unique"}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                truth_cells({**self.base, **change})

    def test_every_legal_row_has_verified_actual_work(self):
        for cell in truth_cells(self.base):
            if cell["valid"]:
                config = {**self.base, **cell["overrides"]}
                with self.subTest(case=cell["case"]):
                    self.assertEqual(effective_work(config, observed(config))["status"], "verified")

    def test_hash_off_does_not_mean_backend_hash_off(self):
        config = {**self.base, "hash": "off"}
        work = effective_work(config, observed(config))
        self.assertFalse(work["configured"]["ingress_hash"])
        self.assertTrue(work["configured"]["backend_digest_required"])
        self.assertEqual(work["observed"]["backend_digest_recomputed"], 108)
        self.assertEqual(work["status"], "verified")

    def test_missing_evidence_is_unknown_not_free(self):
        work = effective_work(self.base, {})
        self.assertTrue(work["status"].startswith("unverified"))
        self.assertIsNone(work["observed"]["redis_network_execs"])
        metrics = observed(self.base)
        del metrics["metrics"]["counters"]["record_encoded_records"]
        self.assertEqual(effective_work(self.base, metrics)["status"], "failed")

    def test_missing_native_core_counters_never_verify(self):
        for key in ("received", "forwarded", "dedup_dropped"):
            for value in (None, "108", -1, True):
                with self.subTest(key=key, value=value):
                    metrics = observed(self.base)
                    metrics["metrics"]["counters"][key] = value
                    self.assertEqual(effective_work(self.base, metrics)["status"], "failed")
        metrics = observed(self.base)
        metrics["metrics"]["counters"]["dedup_dropped"] = 1
        self.assertEqual(effective_work(self.base, metrics)["status"], "failed")

    def test_real_redis_operations_cannot_hide_in_off_row(self):
        config = {**self.base, "redis": "off"}
        metrics = observed(config)
        metrics["metrics"]["counters"]["redis_network_execs"] = 1
        self.assertEqual(effective_work(config, metrics)["status"], "failed")

    def test_exact_lookup_cannot_be_omitted_or_relabelled(self):
        for backend in ("sqlite_compact", "rocksdb"):
            config = {**self.base, "backend": backend}
            for change in ({"backend_lookup_calls": 0}, {"backend_lookup_calls": None},
                           {"backend_lookup_exact": 0}, {"backend_lookup_assume_unique": 1}):
                with self.subTest(backend=backend, change=change):
                    metrics = observed(config)
                    metrics["metrics"]["counters"].update(change)
                    self.assertEqual(effective_work(config, metrics)["status"], "failed")

    def test_null_has_no_durable_unique_count(self):
        config = {**self.base, "backend": "null", "sqlite_compact_lookup": "subquery"}
        receiver = {**observed(config), "elapsed_s": 1.0}
        source = {"elapsed_s": 1.0, "send_elapsed_s": 1.0}
        performance = performance_summary(source, receiver, [], [], config, 1, "diagnostic")
        self.assertIsNone(performance["durable_unique_count"])
        self.assertIsNone(performance["end_to_end_durable_unique_records_per_s"])
        self.assertEqual(performance["validated_input_unique_count"], 108)

    def test_assume_unique_still_hashes_and_is_never_full_contract(self):
        config = {**self.base, "store_lookup": "assume_unique"}
        metrics = observed(config)
        self.assertEqual(effective_work(config, metrics)["status"], "verified")
        metrics["metrics"]["counters"]["backend_lookup_calls"] = 1
        self.assertEqual(effective_work(config, metrics)["status"], "failed")
        self.assertFalse(full_chain_contract(config)["eligible"])
        self.assertEqual(describe_run(config, metrics)["measurement_class"], "ablation")

    def test_sqlite_internal_candidates_are_unknown_when_not_observable(self):
        metrics = observed(self.base)
        metrics["metrics"]["counters"].update(backend_candidates_observable=0, backend_candidates=0)
        work = effective_work(self.base, metrics)
        self.assertIsNone(work["observed"]["backend_candidates"])
        self.assertEqual(work["observed"]["backend_candidates_observable"], 0)

    def test_backend_digest_fallback_cannot_disappear(self):
        config = {**self.base, "hash": "off"}
        metrics = observed(config)
        metrics["metrics"]["counters"]["backend_digest_recomputed"] = 0
        self.assertEqual(effective_work(config, metrics)["status"], "failed")

    def test_parse_replay_is_not_actual_json_parse(self):
        config = {**self.base, "parse": "replay"}
        metrics = observed(config)
        self.assertEqual(effective_work(config, metrics)["status"], "verified")
        metrics["metrics"]["stages"]["json_parse_normalize"] = {"count": 1}
        self.assertEqual(effective_work(config, metrics)["status"], "failed")

    def test_unexecuted_and_invalid_rows_stay_in_report(self):
        report = build_truth_report(self.base, [], 1)
        self.assertFalse(report["all_legal_rows_passed"])
        self.assertEqual(sum(row["status"] == "invalid" for row in report["rows"]), 8)
        self.assertEqual(sum(row["status"] == "not-run" for row in report["rows"]), 24)
        self.assertTrue(all(row["completed_input_records_per_s"] is None for row in report["rows"]))

    def test_success_requires_exact_repeat_count_and_verified_work(self):
        cell = next(cell for cell in truth_cells(self.base) if cell["valid"])
        run = run_for(cell, self.base)
        report = build_truth_report(self.base, [run], 2)
        row = next(row for row in report["rows"] if row["case"] == cell["case"])
        self.assertEqual(row["status"], "failed-or-partial")
        another = copy.deepcopy(run)
        another["repeat"] = 2
        report = build_truth_report(self.base, [run, another], 2)
        row = next(row for row in report["rows"] if row["case"] == cell["case"])
        self.assertEqual(row["status"], "passed")
        self.assertFalse(row["capacity_certified"])
        self.assertIsNone(row["committed_unique_records_per_s"])

    def test_report_rechecks_native_core_despite_saved_verified_status(self):
        cell = next(cell for cell in truth_cells(self.base) if cell["valid"])
        for name in ("received", "forwarded", "dedup_dropped"):
            run = run_for(cell, self.base)
            del run["receiver"]["metrics"]["counters"][name]
            report = build_truth_report(self.base, [run], 1)
            row = next(row for row in report["rows"] if row["case"] == cell["case"])
            self.assertEqual(row["status"], "failed-or-partial")
            self.assertTrue(row["evidence_errors"])

    def test_mixed_inputs_or_mislabeled_config_cannot_form_a_pass(self):
        cells = [cell for cell in truth_cells(self.base) if cell["valid"]]
        first, second = run_for(cells[0], self.base), run_for(cells[1], self.base)
        second["input"]["sha256"] = "b" * 64
        report = build_truth_report(self.base, [first, second], 1)
        self.assertTrue(report["input_coherence_errors"])
        self.assertFalse(any(row["status"] == "passed" for row in report["rows"]))
        first = run_for(cells[0], self.base)
        first["config"]["symbols"] += 1
        report = build_truth_report(self.base, [first], 1)
        self.assertFalse(any(row["status"] == "passed" for row in report["rows"]))

    def test_duplicate_repeat_numbers_do_not_satisfy_requested_repeats(self):
        cell = next(cell for cell in truth_cells(self.base) if cell["valid"])
        first = run_for(cell, self.base)
        report = build_truth_report(self.base, [first, copy.deepcopy(first)], 2)
        row = next(row for row in report["rows"] if row["case"] == cell["case"])
        self.assertEqual(row["status"], "failed-or-partial")

    def test_complete_matrix_still_is_not_capacity_certification(self):
        runs = [run_for(cell, self.base) for cell in truth_cells(self.base) if cell["valid"]]
        report = build_truth_report(self.base, runs, 1)
        self.assertTrue(report["all_legal_rows_passed"])
        self.assertEqual(sum(row["full_path_eligible"] for row in report["rows"]), 1)
        self.assertFalse(any(row["capacity_certified"] for row in report["rows"]))
        self.assertEqual(report["dcrb_compatibility"], "not-adapted")

    def test_json_csv_markdown_share_the_same_32_rows(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            write_truth_report(directory, self.base, [], 1)
            report = json.loads((directory / "truth-table.json").read_text())
            with (directory / "truth-table.csv").open() as source:
                rows = list(csv.DictReader(source))
            self.assertEqual([row["case"] for row in rows], [row["case"] for row in report["rows"]])
            text = (directory / "truth-table.md").read_text()
            self.assertEqual(sum(line.startswith("| 0 |") or line.startswith("| 1 |") for line in text.splitlines()), 32)
            self.assertIn("不是理论极限", text)

    def test_describe_does_not_require_binary_or_create_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "never-created"
            done = subprocess.run([sys.executable, str(ROOT / "scripts/pipeline_bench.py"),
                                   "--describe-truth-table", "--build", "/nonexistent/binary",
                                   "--output", str(output)], capture_output=True, text=True, timeout=10)
            self.assertEqual(done.returncode, 0, done.stderr)
            self.assertEqual(len(json.loads(done.stdout)["cells"]), 32)
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
