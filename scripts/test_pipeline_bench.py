#!/usr/bin/env python3
"""Independent oracle tests plus isolated real-pipeline integration checks.

Like test_stack_runtime.py, accepts --build and --runtime-only before unittest
arguments. Integration tests never connect to a default/shared Redis.
"""
from __future__ import annotations

import argparse
import copy
import json
import os
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import unittest
import zlib
from decimal import Decimal
from pathlib import Path

from pipeline_bench import (binary_path, config_overrides, performance_summary,
                            read_json, save_json, sha256_file)
from pipeline_bench_support import (DEFAULTS, FRAME, FNV_OFFSET, TIME_BASE,
    backlog_summary, decode_record, encode_record, fnv_update, frame,
    generate_record, logical_id, read_frames, record_json, unique_count,
    validate_metrics, verify_export, write_input)
from stack_runtime import (OwnedProcesses, RedisConnection, RunError,
                           candidate_ports, start_private_redis, wait_until)

ROOT = Path(__file__).resolve().parent.parent
BUILD = ROOT / "build"
RUNTIME_ONLY = False


def base_config(**overrides):
    return {**DEFAULTS, "records": 108, **overrides}


def export_line(record):
    return json.dumps({"symbol": record.symbol, "time": record.time,
                       "hex": encode_record(record).hex()}) + "\n"


def fake_metrics(config, checksum="0123456789abcdef"):
    records, unique = config["records"], unique_count(config)
    forwarded = unique if config["hash"] == "dedup" else records
    committed = 0 if config["backend"] == "null" else forwarded
    counters = {"received": records, "parsed": records, "forwarded": forwarded,
                "dedup_dropped": records - forwarded, "committed_attempts": committed,
                "sink_consumed": forwarded if config["backend"] == "null" else 0}
    ack = {"durable": config["backend"] != "null" and config["synchronous"] == "FULL",
           "received": records, "forwarded": forwarded, "committed_attempts": committed,
           "checksum_fnv1a64": checksum}
    source = {"role": "source", "status": "ok", "elapsed_s": 10.1, "send_elapsed_s": 10,
              "metrics": {"counters": {"source_generated": records, "source_sent": records}, "stages": {}},
              "ack": ack}
    receiver = {"role": "receiver", "status": "ok", "elapsed_s": 10,
                "metrics": {"counters": counters, "stages": {}}, "store": {}}
    return source, receiver


class OracleTests(unittest.TestCase):
    def test_first_five_depth_record_matches_explicit_scalar_and_book_specification(self):
        config = base_config()
        first = generate_record(0, config)
        self.assertEqual((first.symbol, first.time, first.present, first.nulls),
                         ("600000.SH", TIME_BASE, 0x1efff, 0))
        self.assertEqual(first.values, (100042, 14205964, 142, 141, 42, 0,
                                       100040, 100032, 100062, 100022, 100039, 100041, 42))
        self.assertEqual(first.book, ((100043, 100044, 100045, 100046, 100047),
                                     (100041, 100040, 100039, 100038, 100037),
                                     (1042, 1043, 1044, 1045, 1046),
                                     (2042, 2043, 2044, 2045, 2046)))
        # Explicit layout anchor, not C++ output or the Python encode/decode roundtrip.
        expected = b"RCD1\x09\x00600000.SH" + struct.pack("<QII", TIME_BASE, 0x1efff, 0)
        expected += struct.pack("<12q", 100042, 14205964, 142, 141, 42, 0,
                                100040, 100032, 100062, 100022, 100039, 100041)
        for values in first.book:
            expected += b"\x05" + struct.pack("<5q", *values)
        self.assertEqual(encode_record(first), expected)
        self.assertEqual(decode_record(expected),
                         type(first)(first.symbol, first.time, first.present, 0, first.values[:12] + (0,), first.book))

    def test_optional_absent_null_and_transaction_extension(self):
        config = base_config(optional_fields=True)
        zero = generate_record(0, config)
        one = generate_record(1, config)
        self.assertEqual(zero.present, 0x1ffff)
        self.assertEqual(zero.nulls, 1 << 4)
        self.assertFalse(one.present & (1 << 4))
        self.assertFalse(one.nulls)
        qzero = json.loads(record_json(zero), parse_float=Decimal)["quoter"]
        qone = json.loads(record_json(one), parse_float=Decimal)["quoter"]
        self.assertIsNone(qzero["openInt"])
        self.assertNotIn("openInt", qone)
        self.assertEqual(qzero["transactionNum"], 42)
        self.assertEqual(qzero["lastPrice"], Decimal("10.0042"))
        self.assertEqual(qzero["amount"], Decimal("1420.5964"))
        self.assertNotEqual(encode_record(zero), encode_record(one))

    def test_duplicate_formula_crosses_batch_boundary_without_new_id(self):
        self.assertEqual([logical_id(i, 7) for i in range(9)], [0, 1, 2, 3, 4, 5, 5, 6, 7])
        self.assertEqual([logical_id(i, 2) for i in range(6)], [0, 0, 1, 1, 2, 2])
        self.assertEqual(unique_count(base_config(records=14, duplicate_every=7)), 12)
        with self.assertRaises(ValueError):
            logical_id(0, 1)

    def test_fnv_known_vector(self):
        self.assertEqual(f"{fnv_update(FNV_OFFSET, b'hello'):016x}", "a430d84680aabd0b")

    def test_independent_raw_files_have_correct_sequence_crc_and_canonical_payload(self):
        with tempfile.TemporaryDirectory(prefix="pipeline-oracle-") as temp:
            for kind in ("json", "binary"):
                config = base_config(records=15, batch_records=7, input=kind, optional_fields=True, duplicate_every=7)
                path = Path(temp) / (kind + ".iqb")
                info = write_input(path, config)
                self.assertEqual(info["sha256"], sha256_file(path))
                frames = list(read_frames(path))
                self.assertEqual([sequence for _, sequence, _ in frames], [0, 7, 14, 15])
                self.assertEqual(frames[-1], (3, 15, b""))
                if kind == "json":
                    self.assertEqual(len(json.loads(frames[0][2])["ticks"]), 7)
                else:
                    payload = frames[0][2]
                    size, = struct.unpack_from("<I", payload)
                    self.assertEqual(payload[4:4 + size], encode_record(generate_record(0, config)))
                damaged = bytearray(path.read_bytes())
                damaged[FRAME.size + 1] ^= 1
                bad = Path(temp) / (kind + "-bad.iqb")
                bad.write_bytes(damaged)
                with self.assertRaises(RunError):
                    list(read_frames(bad))

    def test_canonical_decoder_rejects_truncation_tail_masks_and_wrong_magic(self):
        raw = encode_record(generate_record(0, base_config()))
        for damaged in (raw[:-1], raw + b"\0", b"BAD!" + raw[4:]):
            with self.assertRaises(RunError):
                decode_record(damaged)
        damaged = bytearray(raw)
        struct.pack_into("<I", damaged, 4 + 2 + 9 + 8, 1 << 25)
        with self.assertRaises(RunError):
            decode_record(damaged)

    def test_export_compares_full_blobs_not_only_count_symbol_or_time(self):
        config = base_config(records=3)
        with tempfile.TemporaryDirectory(prefix="pipeline-export-") as temp:
            path = Path(temp) / "rows.jsonl"
            lines = [export_line(generate_record(i, config)) for i in range(3)]
            path.write_text("".join(reversed(lines)))
            self.assertEqual(verify_export(path, config)["actual_unique"], 3)
            row = json.loads(lines[1])
            raw = bytearray.fromhex(row["hex"])
            raw[-1] ^= 1
            row["hex"] = raw.hex()
            path.write_text(lines[0] + json.dumps(row) + "\n" + lines[2])
            with self.assertRaisesRegex(RunError, "full canonical bytes"):
                verify_export(path, config)
            path.write_text(lines[0] + lines[1])
            with self.assertRaisesRegex(RunError, "missing"):
                verify_export(path, config)
            path.write_text("".join(lines) + lines[0])
            with self.assertRaisesRegex(RunError, "duplicate"):
                verify_export(path, config)
            physical = verify_export(path, {**config, "backend": "segment"})
            self.assertEqual((physical["physical_rows"], physical["physical_duplicates"]), (4, 1))

    def test_metrics_distinguish_null_normal_and_full_and_fail_errors(self):
        for overrides in ({}, {"backend": "null"}, {"synchronous": "NORMAL"}, {"hash": "off", "duplicate_every": 7}):
            config = base_config(**overrides)
            source, receiver = fake_metrics(config)
            inputs = {"checksum_unique": "0123456789abcdef", "checksum_all": "0123456789abcdef"}
            self.assertTrue(validate_metrics(source, receiver, config, inputs)["passed"])
            receiver["metrics"]["counters"]["parse_errors"] = 1
            with self.assertRaisesRegex(RunError, "error counter"):
                validate_metrics(source, receiver, config, inputs)
            receiver["metrics"]["counters"].pop("parse_errors")
            source["ack"]["checksum_fnv1a64"] = "bad"
            with self.assertRaisesRegex(RunError, "checksum"):
                validate_metrics(source, receiver, config, inputs)

    def test_strict_runner_config_keys_and_types(self):
        self.assertEqual(config_overrides('{"depth":5,"optional_fields":true}')["depth"], 5)
        for raw in ('{"unknown":1}', '{"depth":5,"depth":2}', '{"depth":true}',
                    '{"directory":"/tmp/shared"}', '{"input_file":"x"}', '[]'):
            with self.assertRaises((ValueError, OSError)):
                config_overrides(raw)

    def test_source_aligned_backlog_and_target_require_sustained_evidence(self):
        config = base_config(records=300000, rate=30000)
        source, receiver = fake_metrics(config)
        receiver_progress, source_progress = [], []
        for second in range(11):
            sent = second * 30000
            counters = {"received": sent, "forwarded": sent, "dedup_dropped": 0,
                        "committed_attempts": sent, "sink_consumed": 0}
            receiver_progress.append({"elapsed_s": second, "monotonic_ns": second * 1000000000,
                                      "metrics": {"counters": counters}})
            source_progress.append({"elapsed_s": second, "monotonic_ns": second * 1000000000,
                                    "metrics": {"counters": {"source_sent": sent}}})
        summary = performance_summary(source, receiver, receiver_progress, [], config, 10, "target", source_progress)
        self.assertEqual(summary["verdict"], "pass-bounded-target-observation")
        fast = performance_summary(source, receiver, receiver_progress[:1], [], config, 10, "target", source_progress[:1])
        self.assertTrue(fast["verdict"].startswith("inconclusive"))
        for index, point in enumerate(receiver_progress):
            point["metrics"]["counters"]["committed_attempts"] -= index * 1000
        growing = performance_summary(source, receiver, receiver_progress, [], config, 10, "target", source_progress)
        self.assertEqual(growing["verdict"], "fail-growing-backlog")
        source["send_elapsed_s"] = 20
        source["elapsed_s"] = 20.1
        bound = performance_summary(source, receiver, receiver_progress, [], config, 10, "target", source_progress)
        self.assertEqual(bound["verdict"], "inconclusive-offer-not-achieved")
        self.assertIsNotNone(bound["source_bound_warning"])


class PipelineIntegrationTests(unittest.TestCase):
    def setUp(self):
        if RUNTIME_ONLY:
            self.skipTest("runtime-only: actual pipeline executable not exercised")
        self.binary = binary_path(BUILD)
        if not self.binary.is_file():
            self.skipTest(f"actual pipeline executable not built: {self.binary}")
        self.temp = tempfile.TemporaryDirectory(prefix="tickshelf-pipeline-test-")
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.owner = OwnedProcesses(self.directory / "processes")
        self.addCleanup(self.owner.close)

    def config(self, **overrides):
        redis_port, port = candidate_ports(2)
        directory = self.directory / ("store-" + str(len(list(self.directory.glob("config-*.json")))))
        directory.mkdir()
        config = base_config(**{"directory": str(directory), "redis_port": redis_port,
                                "port": port, "timeout_s": 10, "redis": "off", **overrides})
        path = self.directory / ("config-" + directory.name + ".json")
        save_json(path, config)
        return config, path

    def invoke(self, role, path, *extra, timeout=15):
        return subprocess.run([self.binary, "--role", role, "--config", path, *extra],
                              capture_output=True, text=True, timeout=timeout)

    def run_pipeline(self, payload_fixture=None, generated_source=False, **overrides):
        config, path = self.config(**overrides)
        raw = self.directory / "controlled-input.iqb"
        info = write_input(raw, config)
        if payload_fixture:
            payload = Path(payload_fixture).read_bytes()
            self.assertEqual(len(json.loads(payload)["ticks"]), config["records"])
            raw.write_bytes(frame(1, 0, payload) + frame(3, config["records"]))
            info["sha256"] = sha256_file(raw)
            info["bytes"] = raw.stat().st_size
        config["input_file"] = "" if generated_source else str(raw)
        if generated_source:
            info = {key: value for key, value in info.items() if key not in ("path", "bytes", "sha256")}
        save_json(path, config)
        if config["redis"] in ("roundtrip", "publish"):
            if not shutil.which("redis-server"):
                self.skipTest("redis-server absent; full pipeline may not be replaced with bypass")
            start_private_redis(self.owner, shutil.which("redis-server"), config["redis_port"])
        child = self.owner.start("receiver", [self.binary, "--role", "receiver", "--config", path])
        wait_until(lambda: f"BENCH_READY pid={child.pid}" in self.owner.text("receiver"),
                   timeout=15, processes=self.owner, names=("receiver",))
        source = self.invoke("source", path)
        self.assertEqual(source.returncode, 0, source.stderr + "\n" + self.owner.text("receiver"))
        self.assertEqual(child.wait(timeout=15), 0, self.owner.text("receiver"))
        directory = Path(config["directory"])
        source_metrics = read_json(directory / "source.json")
        receiver_metrics = read_json(directory / "receiver.json")
        checked = validate_metrics(source_metrics, receiver_metrics, config, info)
        self.assertTrue(checked["passed"])
        if config["backend"] != "null":
            exported = self.invoke("export", path)
            self.assertEqual(exported.returncode, 0, exported.stderr)
            oracle = verify_export(directory / "rows.jsonl", config)
            self.assertEqual(oracle["actual_unique"], unique_count(config))
        else:
            self.assertFalse((directory / "rows.jsonl").exists())
        return config, info, source_metrics, receiver_metrics

    def test_config_print_check_are_side_effect_free_unknown_keys_strict(self):
        config, path = self.config()
        unused = self.directory / "must-not-be-created"
        config["directory"] = str(unused)
        save_json(path, config)
        for role in ("receiver", "source", "export", "generate"):
            for flag in ("--check-config", "--print-config"):
                done = self.invoke(role, path, flag)
                self.assertEqual(done.returncode, 0, done.stderr)
                if flag == "--print-config":
                    self.assertEqual(json.loads(done.stdout)["depth"], 5)
                self.assertFalse(unused.exists())
        config["not_a_key"] = 1
        save_json(path, config)
        self.assertNotEqual(self.invoke("receiver", path, "--check-config").returncode, 0)
        self.assertFalse(unused.exists())
        path.write_text('{"directory":"' + str(unused) + '","depth":5,"depth":1}')
        self.assertNotEqual(self.invoke("receiver", path, "--check-config").returncode, 0)
        self.assertFalse(unused.exists())

    def test_primary_five_depth_json_full_private_redis_to_store_oracle(self):
        self.run_pipeline(redis="roundtrip", optional_fields=True, duplicate_every=7)

    def test_real_cpp_generated_json_source_full_pipeline_independent_oracle(self):
        _, _, source, receiver = self.run_pipeline(generated_source=True, redis="roundtrip",
                                                    optional_fields=True, duplicate_every=7)
        stages = source["metrics"]["stages"]
        self.assertGreater(stages["source_generate"]["count"], 0)
        self.assertGreater(stages["source_json"]["count"], 0)
        self.assertGreater(stages["source_frame_crc"]["count"], 0)
        self.assertGreater(receiver["metrics"]["stages"]["json_parse_normalize"]["count"], 0)

    def test_example_root_five_level_json_full_private_redis_fixture(self):
        self.run_pipeline(ROOT / "examples/bench/five-level.json", records=1, batch_records=1,
                          redis="roundtrip")

    def test_example_optional_null_and_transaction_extension_fixture(self):
        self.run_pipeline(ROOT / "examples/bench/optional-null.json", records=1, batch_records=1,
                          optional_fields=True)

    def test_regular_file_binary_replay_to_store_oracle(self):
        self.run_pipeline(input="binary", optional_fields=True, duplicate_every=7)

    def test_synthetic_parse_bypass_same_raw_json(self):
        self.run_pipeline(parse="replay", optional_fields=True, duplicate_every=7)

    def test_hash_bypass_preserves_unique_full_canonical_set(self):
        self.run_pipeline(hash="off", optional_fields=True, duplicate_every=7)

    def test_bounded_fifo_evictions_preserve_adjacent_duplicate_oracle(self):
        _, _, _, receiver = self.run_pipeline(dedup_max_entries=1, optional_fields=True, duplicate_every=7)
        self.assertGreater(receiver["metrics"]["counters"].get("dedup_evictions", 0), 0)

    def test_segment_reports_physical_duplicates_without_losing_unique_rows(self):
        self.run_pipeline(backend="segment", hash="off", duplicate_every=7, optional_fields=True)

    def test_compact_full_private_redis_digest_reuse_oracle(self):
        self.run_pipeline(backend="sqlite_compact", redis="roundtrip", optional_fields=True, duplicate_every=7)

    def test_compact_hash_off_oracle(self):
        self.run_pipeline(backend="sqlite_compact", hash="off", optional_fields=True, duplicate_every=7)

    def require_rocks(self):
        _, config_path = self.config(backend="segment_rocksdb")
        checked = self.invoke("receiver", config_path, "--check-config")
        if checked.returncode and "requires TICKSHELF_BENCH_ROCKSDB=ON" in checked.stderr:
            self.skipTest("optional RocksDB backend not built")
        self.assertEqual(checked.returncode, 0, checked.stderr)

    def test_rocks_segment_private_redis_oracle(self):
        self.require_rocks()
        self.run_pipeline(backend="segment_rocksdb", redis="roundtrip", optional_fields=True, duplicate_every=7)

    def test_full_record_rocks_private_redis_oracle(self):
        self.require_rocks()
        config, _, _, receiver = self.run_pipeline(backend="rocksdb", redis="roundtrip", optional_fields=True, duplicate_every=7)
        self.assertEqual(receiver["store"]["backend"], "rocksdb")
        self.assertEqual(list(Path(config["directory"]).rglob("*.segments")), [])

    def test_full_record_rocks_hash_off_still_exact_deduplicates(self):
        self.require_rocks()
        self.run_pipeline(backend="rocksdb", hash="off", optional_fields=True, duplicate_every=7)

    def test_rocks_segment_physical_duplicates_oracle(self):
        self.require_rocks()
        self.run_pipeline(backend="segment_rocksdb", hash="off", optional_fields=True, duplicate_every=7)

    def test_sqlite_fullfsync_roundtrips_and_invalid_enum_rejected(self):
        for setting in ("default", "on", "off"):
            _, path = self.config(sqlite_fullfsync=setting)
            done = self.invoke("receiver", path, "--print-config")
            self.assertEqual(done.returncode, 0, done.stderr)
            self.assertEqual(json.loads(done.stdout)["sqlite_fullfsync"], setting)
        _, path = self.config(sqlite_fullfsync="maybe")
        self.assertNotEqual(self.invoke("receiver", path, "--check-config").returncode, 0)

    def test_null_checksum_and_consumed_not_durable(self):
        _, _, source, receiver = self.run_pipeline(backend="null", optional_fields=True, duplicate_every=7)
        self.assertFalse(source["ack"]["durable"])
        self.assertEqual(receiver["metrics"]["counters"]["committed_attempts"], 0)

    def run_local_file_hash_only_null(self, kind):
        config, path = self.config(input=kind, backend="null", hash="only",
                                   optional_fields=True, duplicate_every=7)
        raw = self.directory / ("local-" + kind + ".iqb")
        info = write_input(raw, config)
        config["input_file"] = str(raw)
        save_json(path, config)
        done = self.invoke("local", path)
        self.assertEqual(done.returncode, 0, done.stderr)
        metrics = read_json(Path(config["directory"]) / "local.json")
        self.assertEqual(metrics["role"], "local")
        self.assertEqual(metrics["status"], "PASS")
        self.assertEqual(metrics["transport"], "local_file")
        self.assertGreater(metrics["elapsed_s"], 0)
        counters = metrics["metrics"]["counters"]
        for key in ("received", "parsed", "forwarded", "sink_consumed"):
            self.assertEqual(counters[key], config["records"], key)
        self.assertEqual(counters["dedup_dropped"], 0)
        self.assertEqual(counters["committed_attempts"], 0)
        self.assertEqual(metrics["checksum_fnv1a64"], info["checksum_all"])
        self.assertFalse((Path(config["directory"]) / "rows.jsonl").exists())

    def test_local_json_file_hash_only_null_independent_checksum(self):
        self.run_local_file_hash_only_null("json")

    def test_local_binary_file_hash_only_null_independent_checksum(self):
        self.run_local_file_hash_only_null("binary")

    def test_cpp_generate_payloads_match_independent_canonical_records(self):
        config, path = self.config(input="binary", records=19, batch_records=7, optional_fields=True, duplicate_every=7)
        output = self.directory / "cpp-generated.iqb"
        config["output_file"] = str(output)
        save_json(path, config)
        generated = self.invoke("generate", path)
        self.assertEqual(generated.returncode, 0, generated.stderr)
        actual, sequence = [], 0
        for kind, first, payload in read_frames(output):
            if kind == 3:
                self.assertEqual(first, config["records"])
                continue
            self.assertEqual(kind, 2)
            self.assertEqual(first, sequence)
            offset = 0
            while offset < len(payload):
                size, = struct.unpack_from("<I", payload, offset)
                offset += 4
                actual.append(payload[offset:offset + size])
                offset += size
                sequence += 1
        expected = [encode_record(generate_record(logical_id(i, config["duplicate_every"]), config))
                    for i in range(config["records"])]
        self.assertEqual(actual, expected)

    def test_cpp_generate_json_matches_independent_logical_fields(self):
        config, path = self.config(input="json", records=19, batch_records=7, optional_fields=True, duplicate_every=7)
        output = self.directory / "cpp-generated-json.iqb"
        config["output_file"] = str(output)
        save_json(path, config)
        generated = self.invoke("generate", path)
        self.assertEqual(generated.returncode, 0, generated.stderr)
        position = 0
        for kind, first, payload in read_frames(output):
            if kind == 3:
                self.assertEqual(first, config["records"])
                continue
            self.assertEqual((kind, first), (1, position))
            ticks = json.loads(payload, parse_float=Decimal)["ticks"]
            for tick in ticks:
                expected = generate_record(logical_id(position, config["duplicate_every"]), config)
                self.assertEqual(tick, json.loads(record_json(expected), parse_float=Decimal))
                position += 1
        self.assertEqual(position, config["records"])

    def start_raw_receiver(self, name, config, path):
        child = self.owner.start(name, [self.binary, "--role", "receiver", "--config", path])
        wait_until(lambda: f"BENCH_READY pid={child.pid}" in self.owner.text(name),
                   timeout=10, processes=self.owner, names=(name,))
        return child

    def test_python_wire_end_half_close_gets_independently_verified_ack(self):
        config, path = self.config(records=1, batch_records=1, backend="null")
        child = self.start_raw_receiver("receiver", config, path)
        record = generate_record(0, config)
        payload = ('{"ticks":[' + record_json(record) + ']}').encode()
        with socket.create_connection(("127.0.0.1", config["port"]), timeout=2) as source:
            source.sendall(frame(1, 0, payload) + frame(3, 1))
            source.shutdown(socket.SHUT_WR)
            response = source.makefile("rb")
            header = response.read(FRAME.size)
            self.assertEqual(len(header), FRAME.size)
            magic, version, kind, reserved, sequence, size, crc = FRAME.unpack(header)
            self.assertEqual((magic, version, kind, reserved), (b"IQB1", 1, 4, 0))
            self.assertLess(size, 65536)
            ack_payload = response.read(size)
            self.assertEqual(len(ack_payload), size)
            self.assertEqual(zlib.crc32(ack_payload) & 0xffffffff, crc)
            ack = json.loads(ack_payload)
            self.assertFalse(ack["durable"])
            self.assertEqual(ack["received"], 1)
            self.assertEqual(ack["checksum_fnv1a64"], f"{fnv_update(FNV_OFFSET, encode_record(record)):016x}")
            response.close()
        self.assertEqual(child.wait(timeout=5), 0, self.owner.text("receiver"))

    def test_end_coalesced_or_delayed_trailing_frames_are_rejected(self):
        for delayed in (False, True):
            with self.subTest(delayed=delayed):
                config, path = self.config(records=1, batch_records=1, backend="null")
                name = "delayed" if delayed else "coalesced"
                child = self.start_raw_receiver(name, config, path)
                payload = ('{"ticks":[' + record_json(generate_record(0, config)) + ']}').encode()
                normal = frame(1, 0, payload) + frame(3, 1)
                extra = frame(1, 1, payload)
                with socket.create_connection(("127.0.0.1", config["port"]), timeout=2) as source:
                    source.sendall(normal if delayed else normal + extra)
                    if delayed:
                        time.sleep(.05)
                        source.sendall(extra)
                    source.shutdown(socket.SHUT_WR)
                self.assertNotEqual(child.wait(timeout=5), 0, self.owner.text(name))
                metrics_path = Path(config["directory"]) / "receiver.json"
                if metrics_path.exists():
                    self.assertNotEqual(read_json(metrics_path).get("status"), "PASS")

    def test_reset_peer_never_leaves_receiver_pass_metrics(self):
        config, path = self.config(records=1, batch_records=1, backend="null")
        child = self.start_raw_receiver("receiver", config, path)
        payload = ('{"ticks":[' + record_json(generate_record(0, config)) + ']}').encode()
        source = socket.create_connection(("127.0.0.1", config["port"]), timeout=2)
        source.sendall(frame(1, 0, payload) + frame(3, 1))
        source.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        source.close()
        self.assertNotEqual(child.wait(timeout=5), 0, self.owner.text("receiver"))
        metrics_path = Path(config["directory"]) / "receiver.json"
        if metrics_path.exists():
            self.assertNotEqual(read_json(metrics_path).get("status"), "PASS")

    def test_receiver_occupied_port_does_not_accept_or_stop_other_listener(self):
        config, path = self.config()
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", config["port"]))
            listener.listen()
            done = self.invoke("receiver", path, timeout=5)
            self.assertNotEqual(done.returncode, 0)
            with socket.create_connection(("127.0.0.1", config["port"]), timeout=1):
                pass

    def assert_recorded_children_gone(self, summary):
        for result in summary["results"]:
            for pid in result.get("pids", {}).values():
                with self.assertRaises(ProcessLookupError, msg=f"recorded child {pid} survived cleanup"):
                    os.kill(pid, 0)

    def test_runner_timeout_retains_failure_json_logs_and_reaps_children(self):
        output = self.directory / "timeout-artifacts"
        child = self.owner.start("runner", [sys.executable, ROOT / "scripts/pipeline_bench.py",
            "--build", self.binary, "--suite", "target", "--output", output,
            "--records", "108", "--rate", "1", "--timeout", "1", "--no-warmup",
            "--config", '{"batch_records":1}',
            "--max-disk-mib", "32"])
        self.assertEqual(child.wait(timeout=20), 1, self.owner.text("runner"))
        summary = read_json(output / "summary.json")
        self.assertEqual(summary["status"], "failed")
        self.assertEqual(len(summary["results"]), 1)
        failed = summary["results"][0]
        self.assertEqual(failed["status"], "failed")
        case = Path(failed["directory"])
        self.assertTrue((case / "result.json").is_file())
        self.assertTrue((case / "processes/receiver.log").is_file())
        self.assertTrue((case / "processes/source.log").is_file())
        self.assert_recorded_children_gone(summary)

    def test_runner_signal_retains_failure_json_and_reaps_owned_private_redis(self):
        if not shutil.which("redis-server"):
            self.skipTest("redis-server absent")
        output = self.directory / "signal-artifacts"
        child = self.owner.start("runner", [sys.executable, ROOT / "scripts/pipeline_bench.py",
            "--build", self.binary, "--suite", "target", "--output", output,
            "--records", "108", "--rate", "1", "--timeout", "15", "--no-warmup",
            "--config", '{"batch_records":1}',
            "--max-disk-mib", "32"])

        def source_started():
            for case_json in output.glob("cases/*/case.json"):
                try:
                    if "source" in read_json(case_json).get("pids", {}):
                        return True
                except (ValueError, FileNotFoundError):
                    pass
            return False

        wait_until(source_started, timeout=10, processes=self.owner, names=("runner",))
        child.send_signal(signal.SIGINT)
        self.assertEqual(child.wait(timeout=20), 1, self.owner.text("runner"))
        summary = read_json(output / "summary.json")
        self.assertEqual(summary["status"], "failed")
        self.assertTrue(summary["results"][0]["interrupted"])
        self.assert_recorded_children_gone(summary)

    def test_runner_refuses_existing_output_and_preserves_logs(self):
        output = self.directory / "already-exists"
        output.mkdir()
        sentinel = output / "sentinel"
        sentinel.write_text("keep")
        done = subprocess.run([sys.executable, ROOT / "scripts/pipeline_bench.py", "--build", self.binary,
                               "--output", output, "--records", "10"], capture_output=True, text=True, timeout=5)
        self.assertNotEqual(done.returncode, 0)
        self.assertEqual(sentinel.read_text(), "keep")
        self.assertFalse((output / "summary.json").exists())


if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument("--build", type=Path, default=BUILD)
    argument_parser.add_argument("--runtime-only", action="store_true")
    args, remaining = argument_parser.parse_known_args()
    BUILD = args.build.resolve()
    RUNTIME_ONLY = args.runtime_only
    if "--build" in sys.argv and not binary_path(BUILD).is_file() and not RUNTIME_ONLY:
        argument_parser.error(f"actual pipeline_bench executable not found: {binary_path(BUILD)}")
    unittest.main(argv=[sys.argv[0], *remaining])
