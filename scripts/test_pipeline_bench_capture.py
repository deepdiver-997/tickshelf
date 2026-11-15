"""Only explicitly synthetic fixtures; no production captures or credentials."""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import struct
import tempfile
import time
import unittest
from decimal import localcontext
from pathlib import Path

import pipeline_bench_capture as capture
from pipeline_bench_support import BOOKS, FNV_OFFSET, SCALARS, decode_record, fnv_update, read_frames
from stack_runtime import RunError


SOURCE = {"kind": "synthetic", "provenance": "explicitly synthetic unittest fixture", "authorized": False}
POLICY = {"dedup": True, "max_entries": 2, "fixed_charge_bytes": 160,
          "fifo_limit_bytes": 256 * 1024 * 1024, "hash_algorithm": "sha256", "hash_mode": "batch"}


def tick(symbol="SYNTH", timestamp=1, quoter=None):
    return {"symbol": symbol, "time": timestamp, "quoter": {} if quoter is None else quoter}


def payload(ticks):
    return json.dumps({"ticks": ticks}, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def blobs(raw):
    return [item[0] for item in capture.parse_payload(raw)]


def export(path, rows):
    with path.open("w") as output:
        for raw in rows:
            record = decode_record(raw)
            output.write(json.dumps({"symbol": record.symbol, "time": record.time, "hex": raw.hex()}) + "\n")
    return path


class CaptureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="synthetic-capture-test-")
        self.root = Path(self.temp.name)
        self.input = self.root / "synthetic-input.jsonl"
        self.work = self.root / "work"

    def tearDown(self):
        self.temp.cleanup()

    def prepare(self, lines, policy=None, **kwargs):
        self.input.write_bytes(b"".join(lines))
        return capture.prepare_capture(self.input, self.work, source_config=SOURCE,
                                       policy=POLICY if policy is None else policy, **kwargs)

    def fixture(self):
        a = tick("合成\x00A", 100, {"volume": 0})
        b = tick("合成-" + "长" * 100, 90)
        c = tick("合成\x00A", 100, {"volume": 1})
        lines = [b"  " + payload([a, b]) + b"\r\n", payload([a, c, a]) + b"\n", payload([b])]
        return self.prepare(lines), lines

    def test_variable_raw_frames_exact_fifo_and_coverage(self):
        dataset, lines = self.fixture()
        self.assertEqual(dataset.records, 6)
        self.assertEqual(dataset.unique_records, 3)
        self.assertEqual(dataset.frames, 3)
        self.assertEqual(dataset.expected_forwarded, 5)
        framed = list(read_frames(dataset.packed_path))
        self.assertEqual([(kind, seq) for kind, seq, _ in framed], [(1, 0), (1, 2), (1, 5), (3, 6)])
        self.assertEqual([raw for _, _, raw in framed[:-1]], lines)
        manifest = json.loads(dataset.input_manifest_path.read_text())
        self.assertEqual(manifest["schema_version"], 1)
        self.assertEqual(manifest["records"], 6)
        self.assertEqual(manifest["total_frames"], 3)
        self.assertEqual([f["records"] for f in manifest["frames"]], [2, 3, 1])
        for line, frame in zip(lines, manifest["frames"]):
            self.assertEqual(frame["payload_sha256"], hashlib.sha256(line).hexdigest())
        self.assertEqual(manifest["input_sha256"], hashlib.sha256(dataset.packed_path.read_bytes()).hexdigest())
        self.assertEqual(dataset.input_info["raw_input_sha256"], hashlib.sha256(b"".join(lines)).hexdigest())
        ordered = list(dataset.iter_expected())
        self.assertEqual([forward for _, _, forward in ordered], [True, True, False, True, True, True])
        self.assertEqual([raw for _, raw, _ in ordered], sum((blobs(line) for line in lines), []))
        checksum = FNV_OFFSET
        for _, raw, _ in dataset.iter_expected(forwarded_only=True):
            checksum = fnv_update(checksum, raw)
        self.assertEqual(dataset.input_info["checksum_all_forwarded"], f"{checksum:016x}")
        self.assertEqual(dataset.input_info["expected_dedup_evictions"], 3)
        coverage = dataset.input_info["coverage"]
        self.assertEqual(coverage["records"], 6)
        self.assertEqual(coverage["observed_symbols"], 2)
        self.assertEqual(coverage["natural_duplicate_records"], 3)
        self.assertEqual(coverage["same_symbol_time_different_content_keys"], 1)
        self.assertEqual(coverage["out_of_order_global_adjacent"], 2)
        self.assertEqual(coverage["fields"]["volume"], {"missing": 2, "null": 0, "nonnull": 4, "zero": 3})
        self.assertFalse(coverage["all_records_full_five_level_baseline"])
        self.assertEqual(dataset.input_info["source"]["kind"], "synthetic")
        self.assertFalse(dataset.input_info["production_qualified"])
        self.assertFalse(dataset.input_info["source"]["authenticity_verified"])
        self.assertEqual(os.stat(dataset.oracle_path.parent).st_mode & 0o777, 0o700)
        for path in (dataset.oracle_path, dataset.packed_path, dataset.input_manifest_path):
            self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)
        self.assertTrue(dataset.verify_inputs()["passed"])

    def test_full_exact_multiset_not_count_plus_set(self):
        dataset, _ = self.fixture()
        rows = [raw for _, raw, _ in dataset.iter_expected(forwarded_only=True)]
        path = export(self.root / "synthetic-export.jsonl", rows)
        result = dataset.verify_export(path, backend="segment")
        self.assertEqual(result["physical_rows"], 5)
        self.assertTrue(dataset.verify_export(path, backend="segment_rocksdb")["passed"])
        # Keep the set and row count identical, while corrupting multiplicities.
        bad = rows.copy()
        bad[-1] = bad[0]
        export(path, bad)
        with self.assertRaisesRegex(RunError, "multiplicity"):
            dataset.verify_export(path, backend="segment")
        unique = list(dict.fromkeys(rows))
        export(path, unique)
        for backend in ("sqlite", "sqlite_compact", "rocksdb"):
            self.assertTrue(dataset.verify_export(path, backend=backend)["passed"])
        export(path, unique + [unique[0]])
        with self.assertRaisesRegex(RunError, "multiplicity"):
            dataset.verify_export(path, backend="sqlite")
        export(path, unique[:-1])
        with self.assertRaisesRegex(RunError, "missing"):
            dataset.verify_export(path, backend="sqlite")
        export(path, unique + blobs(payload([tick("unexpected synthetic", 2)])))
        with self.assertRaisesRegex(RunError, "unexpected"):
            dataset.verify_export(path, backend="segment")

    def test_multiple_export_files_use_one_multiset(self):
        dataset, _ = self.fixture()
        rows = [raw for _, raw, _ in dataset.iter_expected(forwarded_only=True)]
        paths = [export(self.root / "part1.jsonl", rows[:2]), export(self.root / "part2.jsonl", rows[2:])]
        self.assertTrue(capture.verify_export(paths, dataset, backend="segment")["passed"])

    def test_byte_budget_and_no_dedup_model(self):
        same = payload([tick(), tick(), tick()])
        for changed in ({"fifo_limit_bytes": 1}, {"max_entries": 0}):
            with self.subTest(changed=changed):
                dataset = self.prepare([same], policy={**POLICY, **changed})
                self.assertEqual(dataset.expected_forwarded, 3)
                self.assertEqual(dataset.input_info["expected_dedup_evictions"], 3)
                self.assertEqual(dataset.input_info["expected_dedup_entries"], 0)
                self.assertEqual(dataset.input_info["expected_dedup_cache_bytes"], 0)
        dataset = self.prepare([same], policy={**POLICY, "dedup": False})
        self.assertEqual(dataset.expected_forwarded, 3)
        self.assertEqual(dataset.input_info["expected_dedup_evictions"], 0)
        self.assertEqual(dataset.input_info["checksum_all"], dataset.input_info["checksum_all_forwarded"])

    def test_variable_size_byte_fifo_evicts_after_insert_without_refresh(self):
        a, b, c = tick("A"), tick("B" * 300), tick("C")
        a_bytes = blobs(payload([a]))[0]
        # Two small entries fit; one large entry evicts itself after evicting A.
        byte_limit = 2 * (len(a_bytes) + POLICY["fixed_charge_bytes"])
        dataset = self.prepare([payload([a, b]) + b"\n", payload([a, c, a])],
                               policy={**POLICY, "max_entries": 100, "fifo_limit_bytes": byte_limit})
        self.assertEqual([forward for _, _, forward in dataset.iter_expected()], [True, True, True, True, False])
        self.assertEqual(dataset.input_info["expected_dedup_evictions"], 2)
        self.assertEqual(dataset.input_info["expected_dedup_cache_bytes"], byte_limit)
        self.assertEqual(dataset.input_info["expected_dedup_payload_bytes"], len(a_bytes) * 2)
        # A touched on a duplicate must remain FIFO oldest, not become LRU newest.
        a, b, c = tick("A"), tick("B"), tick("C")
        dataset = self.prepare([payload([a, b, a, c, a])])
        self.assertEqual([forward for _, _, forward in dataset.iter_expected()], [True, True, False, True, True])

    def test_explicit_record_limit_is_checked_before_oracle_updates(self):
        dataset = self.prepare([payload([tick(), tick()])], max_records=2)
        self.assertEqual(dataset.records, 2)
        existing = set(self.work.glob("capture-*"))
        with self.assertRaisesRegex(RunError, "record-limit 2"):
            self.prepare([payload([tick(), tick(), tick()])], max_records=2)
        self.assertEqual(set(self.work.glob("capture-*")), existing)
        # Reject at the third record, without reaching a later invalid line.
        with self.assertRaisesRegex(RunError, "record-limit 2"):
            self.prepare([payload([tick(), tick(), tick()]) + b"\n", b"not-json\n"], max_records=2)
        self.assertEqual(self.prepare([payload([tick()] * 3)], max_records=None).records, 3)
        for limit in (0, -1, True, 1.5, "2"):
            with self.subTest(limit=limit), self.assertRaisesRegex(RunError, "max_records"):
                self.prepare([payload([tick()])], max_records=limit)

    def test_manifest_reader_hard_limits_cleanup(self):
        from unittest.mock import patch
        with patch.object(capture, "MAX_FRAMES", 2):
            with self.assertRaisesRegex(RunError, "frames"):
                self.prepare([payload([tick()]) + b"\n"] * 3)
        with patch.object(capture, "MAX_MANIFEST", 100):
            with self.assertRaisesRegex(RunError, "16MiB"):
                self.prepare([payload([tick()])])
        self.assertFalse(list(self.work.glob("capture-*")))

    def test_full_five_actual_coverage_and_per_symbol_out_of_order(self):
        quoter = {name: 0 for name in SCALARS[:12]}
        quoter.update({name: [0] * 5 for name in BOOKS})
        dataset = self.prepare([payload([tick("SYNTH", 9, quoter), tick("SYNTH", 8, quoter)])])
        coverage = dataset.input_info["coverage"]
        self.assertEqual(coverage["full_five_level_baseline_records"], 2)
        self.assertTrue(coverage["all_records_full_five_level_baseline"])
        self.assertEqual(coverage["out_of_order_per_symbol_watermark"], 1)
        self.assertEqual(coverage["book_depth_counts_0_to_10"]["bidVol"][5], 2)

    def test_single_plain_json_and_empty_frame(self):
        dataset = self.prepare([payload([tick()])])
        self.assertEqual(dataset.frames, 1)
        lines = [b'{"ticks":[]}\n', payload([tick()]) + b"\n", b' {"ticks":[]}\r\n']
        dataset = self.prepare(lines)
        manifest = json.loads(dataset.input_manifest_path.read_text())
        self.assertEqual([f["sequence"] for f in manifest["frames"]], [0, 0, 1])
        self.assertEqual([f["records"] for f in manifest["frames"]], [0, 1, 0])
        framed = list(read_frames(dataset.packed_path))
        self.assertEqual([row[2] for row in framed[:-1]], lines)
        self.assertEqual([(kind, seq) for kind, seq, _ in framed], [(1, 0), (1, 0), (1, 1), (3, 1)])

    def test_source_authorization_is_required_not_authenticity(self):
        for source in ({"kind": "production_capture"}, {"kind": "production_shape", "authorized": True}, {"kind": "synthetic", "authorized": "yes"}):
            with self.subTest(source=source), self.assertRaises(RunError):
                capture._normalize_source(source)
        source = capture._normalize_source({"source_kind": "production_capture", "authorized": True, "provenance": "declared source, not proof"})
        self.assertEqual(source["kind"], "production_capture")
        self.assertFalse(source["authenticity_verified"])
        self.assertFalse(source["production_qualified"])
        self.assertIn("no capture arrival", source["replay_timing"])

    def test_policy_must_be_explicit_not_inferred(self):
        for policy in ({}, {**POLICY, "fixed_charge_bytes": None}, {**POLICY, "max_entries": True}, {**POLICY, "dedup": 1}):
            with self.subTest(policy=policy), self.assertRaises(RunError):
                self.prepare([payload([tick()])], policy=policy)

    def test_input_hash_verification_all_artifacts(self):
        for attribute in ("raw_path", "packed_path", "input_manifest_path", "oracle_path"):
            with self.subTest(attribute=attribute):
                dataset, _ = self.fixture()
                path = getattr(dataset, attribute)
                with path.open("ab") as output:
                    output.write(b" ")
                with self.assertRaisesRegex(RunError, "changed"):
                    dataset.verify_inputs()

    def test_cancellation_deadline_budget_and_cleanup(self):
        calls = 0
        def stop():
            nonlocal calls
            calls += 1
            if calls >= 4:
                raise RunError("synthetic cancellation")
        with self.assertRaisesRegex(RunError, "cancellation"):
            self.prepare([payload([tick(), tick()])], check=stop)
        self.assertFalse(list(self.work.glob("capture-*")))
        with self.assertRaisesRegex(RunError, "deadline"):
            self.prepare([payload([tick()])], deadline=time.monotonic() - 1)
        with self.assertRaisesRegex(RunError, "budget"):
            self.prepare([payload([tick()])], budget=lambda: False)

    def test_raw_mutation_during_prepare_is_detected(self):
        calls = 0
        def mutate():
            nonlocal calls
            calls += 1
            if calls == 4:
                self.input.write_bytes(payload([tick("OTHER")]))
        with self.assertRaisesRegex(RunError, "changed"):
            self.prepare([payload([tick()])], check=mutate)
        self.assertFalse(list(self.work.glob("capture-*")))

    def test_bounded_lines_and_export_rows(self):
        with self.assertRaisesRegex(RunError, "bounded"):
            self.prepare([b"x" * (capture.MAX_PAYLOAD + 1)])
        with self.assertRaises(RunError):
            self.prepare([b"\n"])
        dataset = self.prepare([payload([tick()])])
        path = self.root / "oversized-export.jsonl"
        path.write_bytes(b"x" * (8 * capture.MAX_RECORD + 1))
        with self.assertRaisesRegex(RunError, "bounded"):
            dataset.verify_export(path, backend="sqlite")

    def metrics(self, dataset, backend="segment"):
        info = dataset.input_info
        count = info["records"]
        forwarded = dataset.expected_forwarded
        counters = {"received": count, "parsed": count, "forwarded": forwarded,
                    "dedup_dropped": info["expected_dedup_dropped"],
                    "committed_attempts": 0 if backend == "null" else forwarded,
                    "sink_consumed": forwarded if backend == "null" else 0,
                    "network_bytes": info["bytes"]}
        for key in ("dedup_evictions", "dedup_entries", "dedup_payload_bytes", "dedup_cache_bytes"):
            counters[key] = info["expected_" + key]
        ack = {key: counters[key] for key in ("received", "forwarded", "committed_attempts")}
        ack.update(durable=backend != "null", checksum_fnv1a64=info["checksum_all_forwarded"])
        source = {"role": "source", "status": "PASS", "elapsed_s": 1,
                  "metrics": {"counters": {"source_generated": count, "source_sent": count, "source_bytes": info["bytes"]}}, "ack": ack.copy()}
        receiver = {"role": "receiver", "status": "PASS", "elapsed_s": 1,
                    "metrics": {"counters": counters}, "ack": ack.copy()}
        config = {"backend": backend, "hash": "dedup", "synchronous": "FULL", "dedup_max_entries": POLICY["max_entries"]}
        return source, receiver, config

    def test_metric_checks_use_expected_fifo_not_actual_drop(self):
        dataset, _ = self.fixture()
        source, receiver, config = self.metrics(dataset)
        self.assertTrue(dataset.validate_metrics(source, receiver, config)["passed"])
        receiver["metrics"]["counters"]["dedup_dropped"] = 0
        receiver["metrics"]["counters"]["forwarded"] = 6
        with self.assertRaisesRegex(RunError, "counter mismatch"):
            dataset.validate_metrics(source, receiver, config)
        source, receiver, config = self.metrics(dataset)
        receiver["metrics"]["counters"]["dedup_cache_bytes"] += 1
        with self.assertRaisesRegex(RunError, "FIFO"):
            dataset.validate_metrics(source, receiver, config)
        source, receiver, config = self.metrics(dataset)
        source["ack"]["checksum_fnv1a64"] = dataset.input_info["checksum_all"]
        receiver["ack"] = source["ack"].copy()
        with self.assertRaisesRegex(RunError, "FNV"):
            dataset.validate_metrics(source, receiver, config)
        source, receiver, config = self.metrics(dataset, "null")
        self.assertTrue(dataset.validate_metrics(source, receiver, config)["passed"])

    def test_cli_preflight(self):
        self.input.write_bytes(payload([tick()]))
        source = self.root / "synthetic-source.json"
        policy = self.root / "compiled-policy-fixture.json"
        source.write_text(json.dumps(SOURCE))
        policy.write_text(json.dumps(POLICY))
        from contextlib import redirect_stdout
        from io import StringIO
        output = StringIO()
        with redirect_stdout(output):
            self.assertEqual(capture.main(["preflight", str(self.input), "--work-dir", str(self.work), "--source-config", str(source), "--policy", str(policy)]), 0)
        self.assertFalse(json.loads(output.getvalue())["production_qualified"])


class ExactSchemaTests(unittest.TestCase):
    def one(self, quoter="{}", timestamp="0", symbol='"SYNTH"'):
        return b'{"ticks":[{"symbol":' + symbol.encode() + b',"time":' + timestamp.encode() + b',"quoter":' + quoter.encode() + b"}]}"

    def test_exact_golden_canonical_missing_null_zero_alias(self):
        expected = b"RCD1\x00\x00" + struct.pack("<QII", (1 << 64) - 1, 0b111, 0b010) + struct.pack("<qq", 12345, -(1 << 63))
        raw = blobs(self.one('{"lastPrice":1.2345000,"amount":null,"volume":-9223372036854775808}', "18446744073709551615", '""'))[0]
        self.assertEqual(raw, expected)
        self.assertEqual(blobs(self.one('{"pvolume":3}')), blobs(self.one('{"pvolumn":3.0}')))
        distinct = [blobs(self.one(q))[0] for q in ('{}', '{"volume":null}', '{"volume":0}')]
        self.assertEqual(len(set(distinct)), 3)

    def test_decimal_context_cannot_round_and_extreme_exponents(self):
        with localcontext() as context:
            context.prec = 2
            raw = blobs(self.one('{"lastPrice":922337203685477.5807,"volume":9.223372036854775807e18}', "1.8446744073709551615e19"))[0]
            record = decode_record(raw)
            self.assertEqual(record.values[:3:2], ((1 << 63) - 1,) * 2)
            self.assertEqual(record.time, (1 << 64) - 1)
        for number in ("0e" + "9" * 1000, "-0e-" + "9" * 1000, "10000e-4", "1.00000"):
            self.assertEqual(decode_record(blobs(self.one('{"volume":' + number + '}'))[0]).values[2], 0 if number.startswith(("0e", "-0e")) else 1)

    def test_invalid_precisions_and_integer_bounds(self):
        bad = [('{}', '-0'), ('{}', '18446744073709551616'), ('{}', '1e-1'), ('{}', 'true'),
               ('{"volume":9223372036854775808}', '0'), ('{"volume":-9223372036854775809}', '0'),
               ('{"lastPrice":922337203685477.5808}', '0'), ('{"lastPrice":-922337203685477.5809}', '0'),
               ('{"volume":1.00001}', '0'), ('{"lastPrice":0.00001}', '0'), ('{"volume":true}', '0'),
               ('{"volume":"1"}', '0'), ('{"volume":1e999999999}', '0'), ('{"volume":NaN}', '0')]
        for quoter, timestamp in bad:
            with self.subTest(quoter=quoter, timestamp=timestamp), self.assertRaises(RunError):
                blobs(self.one(quoter, timestamp))

    def test_unknown_duplicate_and_malformed_closed_schema(self):
        bad = [b'{"ticks":[],"unknown":0}', b'{"ticks":[],"ticks":[]}', b'{"unknown":[]}',
               b'{"ticks":[{"symbol":"S","symbol":"S","time":0,"quoter":{}}]}',
               b'{"ticks":[{"symbol":"S","time":0,"quoter":{},"extra":null}]}',
               self.one('{"volume":1,"volume":2}'), self.one('{"pvolume":1,"pvolumn":1}'),
               self.one('{"volume":1,"vol\\u0075me":2}'), self.one('{"unknown":null}'),
               b'{"ticks":[]} true', b'{"ticks":[null]}', b'{"ticks":[true]}', b'{"ticks":null}',
               b'{"ticks":[{"symbol":"S","time":0}]}', self.one('null'), self.one('[]'),
               b'{"ticks":[{"symbol":"S","time":0,"quoter":{}},]}']
        for raw in bad:
            with self.subTest(raw=raw[:100]), self.assertRaises(RunError):
                blobs(raw)

    def test_utf8_nul_large_symbols_and_record_boundaries(self):
        for symbol in ("", "A\x00B", "证券" * 100, "S" * 65000, "\U0001f600"):
            raw = blobs(payload([tick(symbol)]))[0]
            self.assertEqual(decode_record(raw).symbol, symbol)
        for symbol in ('"\\ud800"', '"\\udfff"'):
            with self.assertRaises(RunError):
                blobs(self.one(symbol=symbol))
        for symbol in (b'"\xc0\x80"', b'"\xed\xa0\x80"', b'"\xff"'):
            with self.assertRaises(RunError):
                blobs(self.one().replace(b'"SYNTH"', symbol))
        for symbol in ("x" * 65515, "x" * 65536, "x" * 65510):
            with self.assertRaises(RunError):
                blobs(payload([tick(symbol)]))
        # Binary has a different bound from JSON syntax: exactly 65536 bytes OK.
        canonical = b"RCD1" + struct.pack("<H", 65514) + b"x" * 65514 + struct.pack("<QII", 0, 0, 0)
        self.assertEqual(len(canonical), 65536)
        self.assertEqual(len(decode_record(canonical).symbol), 65514)
        with self.assertRaises(RunError):
            decode_record(canonical + b"x")

    def test_books_depths_null_empty_masks(self):
        for depth in range(11):
            raw = blobs(payload([tick(quoter={name: [0] * depth for name in BOOKS})]))[0]
            self.assertTrue(all(len(book) == depth for book in decode_record(raw).book))
        for quoter in ('{"askPrice":[],"bidPrice":[0]}', '{"askVol":[' + ','.join(['0'] * 11) + ']}', '{"askVol":[null]}', '{"bidVol":0}'):
            with self.assertRaises(RunError):
                blobs(self.one(quoter))
        raw = blobs(self.one('{"askPrice":null,"bidPrice":[],"askVol":[]}'))[0]
        self.assertEqual(decode_record(raw).nulls, 1 << 13)
        malformed = b"RCD1\x00\x00" + struct.pack("<QII", 0, (1 << 13) | (1 << 14), 0) + b"\x00\x01" + struct.pack("<q", 0)
        with self.assertRaises(RunError):
            decode_record(malformed)
        for present, nulls in ((1 << 17, 0), (0, 1)):
            with self.assertRaises(RunError):
                decode_record(b"RCD1\x00\x00" + struct.pack("<QII", 0, present, nulls))

    def test_max_batch_record_count(self):
        raw = payload([tick()] * 8192)
        self.assertEqual(len(blobs(raw)), 8192)
        with self.assertRaisesRegex(RunError, "8192"):
            blobs(payload([tick()] * 8193))


if __name__ == "__main__":
    unittest.main()
