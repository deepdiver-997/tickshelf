#!/usr/bin/env python3
"""Owned, Redis-free C++ source/manifest integration regressions.

Runs only small correctness diagnostics, not a performance benchmark. Every test
owns its temporary files, child processes and loopback listeners independently
of test_pipeline_bench.py. Use --build DIR or --binary FILE before unittest args.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BINARY = ROOT / "build" / "pipeline_bench"
HEADER = struct.Struct("<4sBBHQII")
MAX_PAYLOAD = 4 * 1024 * 1024


def frame(kind, sequence, payload=b""):
    return HEADER.pack(b"IQB1", 1, kind, 0, sequence, len(payload), zlib.crc32(payload)) + payload


def raw_payload(position, count, padding=0):
    # Deliberately not the generator or C++ serializer: original whitespace,
    # wrapper order, exponent spelling and UINT64_MAX must survive unchanged.
    ticks = [('{"quoter":{"lastPrice":1.2300e+1,"volume":null},'
              '"time":18446744073709551615,"symbol":"RAW%04d.SH"}' % (position + i))
             for i in range(count)]
    return (" \n { \"ticks\" : [ " + ", \n".join(ticks) + " ] }\n").encode() + b" " * padding


def capture_files(root, counts=(2, 0, 1, 3, 0), *, padding=0):
    root.mkdir(parents=True, exist_ok=True)
    position, packed, frames = 0, [], []
    for count in counts:
        payload = raw_payload(position, count, padding)
        packed.append(frame(1, position, payload))
        frames.append({"sequence": position, "records": count,
                       "payload_sha256": hashlib.sha256(payload).hexdigest()})
        position += count
    packed.append(frame(3, position))
    wire = b"".join(packed)
    manifest = {"schema_version": 1, "records": position, "total_frames": len(frames),
                "frames": frames, "input_sha256": hashlib.sha256(wire).hexdigest(),
                "raw_input_sha256": "a" * 64, "source": {"kind": "regression"},
                "coverage": {"time": "UINT64_MAX"}, "policy": "preserve_raw"}
    input_path, manifest_path = root / "capture.iqb", root / "capture.manifest.json"
    input_path.write_bytes(wire)
    manifest_path.write_text(json.dumps(manifest))
    return input_path, manifest_path, manifest, packed


class Peer:
    """A single owned peer; validates END plus half-close before sending ACK."""
    def __init__(self, records, *, ack="good", pause=0, receive_buffer=None):
        self.records, self.ack, self.pause = records, ack, pause
        self.listener = socket.socket()
        if receive_buffer:
            self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, receive_buffer)
        self.listener.bind(("127.0.0.1", 0))
        self.port = self.listener.getsockname()[1]
        self.listener.listen(1)
        self.listener.settimeout(0.1)
        self.stopped = threading.Event()
        self.connection = None
        self.observed, self.error = [], None
        self.half_closed = False
        self.thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stopped.set()
        self.listener.close()
        if self.connection:
            with contextlib.suppress(OSError):
                self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
        self.thread.join(3)
        if self.thread.is_alive():
            raise AssertionError("owned test peer did not terminate")

    def _read(self, connection, count):
        out = bytearray()
        while len(out) < count:
            chunk = connection.recv(count - len(out))
            if not chunk:
                if not out:
                    return None
                raise AssertionError("partial wire frame")
            out.extend(chunk)
        return bytes(out)

    def _run(self):
        try:
            while not self.stopped.is_set():
                try:
                    self.connection, _ = self.listener.accept()
                    break
                except socket.timeout:
                    continue
            if self.connection is None:
                return
            connection = self.connection
            connection.settimeout(8)
            if self.pause:
                time.sleep(self.pause)
            while not self.stopped.is_set():
                header = self._read(connection, HEADER.size)
                if header is None:
                    return  # Expected when source rejects a malformed fixture.
                magic, version, kind, reserved, sequence, length, crc = HEADER.unpack(header)
                if (magic, version, reserved) != (b"IQB1", 1, 0) or length > MAX_PAYLOAD:
                    raise AssertionError("bad source wire header")
                payload = self._read(connection, length) if length else b""
                if payload is None or zlib.crc32(payload) != crc:
                    raise AssertionError("bad source payload/CRC")
                self.observed.append(header + payload)
                if kind != 3:
                    continue
                self.half_closed = connection.recv(1) == b""
                if not self.half_closed or sequence != self.records:
                    raise AssertionError("source did not strictly END then half-close")
                if self.ack == "drop":
                    connection.close()
                    return
                value = {"received": self.records, "durable": False}
                if self.ack == "count":
                    value["received"] += 1
                elif self.ack == "durable":
                    value["durable"] = True
                payload = json.dumps(value).encode() if self.ack != "malformed" else b"{"
                connection.sendall(frame(4, self.records, payload))
                return
        except Exception as exc:
            if not self.stopped.is_set():
                self.error = exc


class SourceIntegrationTests(unittest.TestCase):
    def setUp(self):
        if not BINARY.is_file():
            self.skipTest(f"build pipeline_bench first: {BINARY}")
        self.temp = tempfile.TemporaryDirectory(prefix="tickshelf-source-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.case = 0

    def config(self, *, directory=None, **overrides):
        self.case += 1
        return {"directory": str(directory or self.root / f"out-{self.case}"),
                "redis": "off", "hash": "off", "backend": "null", "records": 6,
                "batch_records": 1, "timeout_s": 8, **overrides}

    def invoke(self, config, role="source", *arguments):
        self.case += 1
        path = self.root / f"config-{self.case}.json"
        path.write_text(json.dumps(config))
        return subprocess.run([str(BINARY), "--config", str(path), "--role", role, *arguments],
                              cwd=self.root, capture_output=True, text=True, timeout=12)

    def capture_config(self, *, counts=(2, 0, 1, 3, 0), **overrides):
        self.case += 1
        fixture = self.root / f"fixture-{self.case}"
        input_path, manifest_path, manifest, packed = capture_files(fixture, counts)
        config = self.config(records=sum(counts), input_file=str(input_path),
                             input_manifest=str(manifest_path), **overrides)
        return config, input_path, manifest_path, manifest, packed

    def assert_failure(self, config, expected):
        result = self.invoke(config)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(expected, result.stderr)
        path = Path(config["directory"]) / "source.json"
        if "\0" not in config["directory"] and path.exists():
            self.assertEqual(json.loads(path.read_text())["status"], "FAIL")
        return result

    @contextlib.contextmanager
    def receiver(self, config):
        self.case += 1
        config_path, stderr_path = self.root / f"receiver-{self.case}.json", self.root / f"receiver-{self.case}.log"
        config_path.write_text(json.dumps(config))
        with stderr_path.open("w") as stderr:
            child = subprocess.Popen([str(BINARY), "--config", str(config_path), "--role", "receiver"],
                                     cwd=self.root, stdout=subprocess.DEVNULL, stderr=stderr)
            try:
                deadline = time.monotonic() + 5
                while "BENCH_READY" not in stderr_path.read_text():
                    if child.poll() is not None or time.monotonic() > deadline:
                        self.fail("receiver did not become ready: " + stderr_path.read_text())
                    time.sleep(0.01)
                yield child
            finally:
                if child.poll() is None:
                    child.terminate()
                    try:
                        child.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait(timeout=2)

    def free_port(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            return listener.getsockname()[1]

    def test_raw_variable_frames_empty_middle_tail_u64_and_exact_wire_preserved(self):
        config, input_path, _, _, packed = self.capture_config(rate=20)
        original_hash = hashlib.sha256(input_path.read_bytes()).digest()
        with Peer(config["records"]) as peer:
            config["port"] = peer.port
            result = self.invoke(config)
            peer.thread.join(2)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIsNone(peer.error)
            self.assertTrue(peer.half_closed)
            self.assertEqual(peer.observed, packed)
        self.assertEqual(hashlib.sha256(input_path.read_bytes()).digest(), original_hash)
        output = json.loads((Path(config["directory"]) / "source.json").read_text())
        counters, stages = output["metrics"]["counters"], output["metrics"]["stages"]
        self.assertEqual(output["status"], "PASS")
        self.assertEqual((counters["source_sent"], counters["source_generated"]), (6, 6))
        self.assertEqual(counters["source_frames_sent"], 5)
        self.assertEqual(stages["source_count_validate"]["count"], 5)
        self.assertEqual(stages["source_send_start_lag"]["count"], 5)
        self.assertEqual(stages["source_send_complete_lag"]["count"], 5)
        self.assertGreaterEqual(output["send_elapsed_ns"], 300_000_000)
        self.assertEqual(counters["source_planned_records"], 6)
        self.assertEqual(counters["source_planned_minus_sent"], 0)
        self.assertAlmostEqual(output["elapsed_ns"] / 1e9, output["elapsed_s"], places=15)
        progress = [json.loads(line) for line in (Path(config["directory"]) / "source-progress.jsonl").read_text().splitlines()]
        self.assertEqual(progress[-1]["metrics"]["counters"]["source_sent"], 6)
        self.assertIn("source_planned_minus_sent_peak", progress[-1]["metrics"]["counters"])
        self.assertTrue(output["source_schedule"]["lag_is_observation_not_work"])

    def test_real_receiver_parses_capture_larger_than_config_batch_and_empty_frames(self):
        config, _, _, _, _ = self.capture_config(port=self.free_port())
        with self.receiver(config) as child:
            result = self.invoke(config)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(child.wait(timeout=5), 0)
        output = json.loads((Path(config["directory"]) / "receiver.json").read_text())
        counters = output["metrics"]["counters"]
        self.assertEqual((counters["received"], counters["parsed"], counters["received_frames"]), (6, 6, 5))
        self.assertEqual(output["status"], "PASS")

    def test_local_capture_also_preserves_empty_frame_contract(self):
        config, _, _, _, _ = self.capture_config()
        result = self.invoke(config, "local")
        self.assertEqual(result.returncode, 0, result.stderr)
        output = json.loads((Path(config["directory"]) / "local.json").read_text())
        self.assertEqual(output["metrics"]["counters"]["received_frames"], 5)
        self.assertEqual(output["metrics"]["counters"]["received"], 6)

    def test_sha_tamper_fails_even_with_recomputed_wire_crc(self):
        config, input_path, _, _, packed = self.capture_config(counts=(2, 1))
        payload = packed[0][HEADER.size:].replace(b"1.2300e+1", b"1.2400e+1")
        input_path.write_bytes(frame(1, 0, payload) + b"".join(packed[1:]))
        with Peer(3) as peer:
            config["port"] = peer.port
            self.assert_failure(config, "SHA256 mismatch")
            peer.thread.join(2)
            self.assertFalse(peer.observed)

    def test_wire_sequence_is_physical_record_offset_not_frame_number(self):
        config, input_path, _, _, packed = self.capture_config(counts=(2, 1))
        input_path.write_bytes(packed[0] + frame(1, 1, packed[1][HEADER.size:]) + packed[2])
        with Peer(3) as peer:
            config["port"] = peer.port
            self.assert_failure(config, "frame type/sequence mismatch")

    def test_payload_count_is_actually_parsed_not_just_trusted_manifest(self):
        config, _, manifest_path, manifest, _ = self.capture_config(counts=(1, 2))
        manifest["frames"][0]["records"] = 2
        manifest["frames"][1].update(sequence=2, records=1)
        manifest_path.write_text(json.dumps(manifest))
        with Peer(3) as peer:
            config["port"] = peer.port
            self.assert_failure(config, "record count differs from manifest")
            peer.thread.join(2)
            self.assertFalse(peer.observed)

    def test_receiver_independently_rejects_actual_count_different_from_manifest(self):
        config, _, manifest_path, manifest, packed = self.capture_config(counts=(1, 2), port=self.free_port())
        manifest["frames"][0]["records"] = 2
        manifest["frames"][1].update(sequence=2, records=1)
        manifest_path.write_text(json.dumps(manifest))
        with self.receiver(config) as child:
            with socket.create_connection(("127.0.0.1", config["port"]), timeout=2) as connection:
                connection.sendall(packed[0])
                connection.shutdown(socket.SHUT_WR)
                self.assertNotEqual(child.wait(timeout=5), 0)
        output = json.loads((Path(config["directory"]) / "receiver.json").read_text())
        self.assertEqual(output["status"], "FAIL")
        self.assertIn("count differs from capture manifest", output["error"])

    def test_manifest_schema_missing_unknown_duplicate_totals_ranges_and_overflow(self):
        changes = [
            (lambda m: m.update(schema_version=2), "schema_version"),
            (lambda m: m.pop("records"), "missing required key"),
            (lambda m: m.pop("input_sha256"), "missing required key"),
            (lambda m: m["frames"][0].pop("records"), "missing required fields"),
            (lambda m: m.update(total_frames=99), "total_frames"),
            (lambda m: m.update(records=4), "records differs"),
            (lambda m: m.update(extra="ignored?"), "unknown manifest key"),
            (lambda m: m["frames"][0].update(extra=1), "unknown manifest frame key"),
            (lambda m: m["frames"][0].update(sequence=1), "cumulative physical record offset"),
            (lambda m: m["frames"][1].update(sequence=1), "cumulative physical record offset"),
            (lambda m: m["frames"][0].update(records=8193), "records must be 0..8192"),
            (lambda m: m["frames"][0].update(records=-1), "integer type/range"),
            (lambda m: m["frames"][0].update(records=True), "integer type/range"),
            # simdjson rejects >u64 JSON numbers before DOM field extraction.
            (lambda m: m["frames"][0].update(sequence=2**64), "manifest must be a JSON object"),
            (lambda m: m.update(records=2**64), "manifest must be a JSON object"),
            (lambda m: m["frames"][0].update(payload_sha256="z" * 64), "non-hex"),
            (lambda m: m["frames"][1].update(records=2), "total exceeds"),
        ]
        for change, expected in changes:
            with self.subTest(expected=expected):
                config, _, manifest_path, manifest, _ = self.capture_config(counts=(2, 1))
                change(manifest)
                manifest_path.write_text(json.dumps(manifest))
                self.assert_failure(config, expected)
        config, _, manifest_path, manifest, _ = self.capture_config(counts=(2, 1))
        text = json.dumps(manifest)
        manifest_path.write_text('{"schema_version":1,' + text[1:])
        self.assert_failure(config, "duplicate manifest key")

    def test_manifest_frame_and_byte_limits_and_nonregular_inputs_are_bounded(self):
        config, _, manifest_path, _, _ = self.capture_config(counts=(1,))
        with manifest_path.open("wb") as output:
            output.truncate(16 * 1024 * 1024 + 1)
        self.assert_failure(config, "exceeds 16MiB")
        config, _, manifest_path, manifest, _ = self.capture_config(counts=(1,))
        manifest["frames"] = [manifest["frames"][0]] * 100001
        manifest["total_frames"] = 100001
        manifest_path.write_text(json.dumps(manifest, separators=(",", ":")))
        self.assertLess(manifest_path.stat().st_size, 16 * 1024 * 1024)
        self.assert_failure(config, "exceeds 100000 frames")
        for target in ("input_manifest", "input_file"):
            config, _, _, _, _ = self.capture_config(counts=(1,))
            fifo = self.root / f"fifo-{target}"
            os.mkfifo(fifo)
            config[target] = str(fifo)
            before = time.monotonic()
            self.assert_failure(config, "regular file")
            self.assertLess(time.monotonic() - before, 2)
        config, _, _, _, _ = self.capture_config(counts=(1,))
        config["input_manifest"] = str(self.root / "missing-manifest")
        self.assert_failure(config, "cannot open input")

    def test_end_eof_and_terminal_integrity_failures_never_pass(self):
        for suffix, expected in ((frame(3, 1) + b"tail", "invalid input END/trailing bytes"),
                                 (b"", "truncated input frame header"),
                                 (frame(3, 2), "invalid input END/trailing bytes")):
            with self.subTest(expected=expected, suffix=suffix):
                config, input_path, _, _, packed = self.capture_config(counts=(1,))
                input_path.write_bytes(packed[0] + suffix)
                with Peer(1) as peer:
                    config["port"] = peer.port
                    self.assert_failure(config, expected)
        config, input_path, _, _, packed = self.capture_config(counts=(1, 1))
        input_path.write_bytes(packed[0] + packed[-1])
        with Peer(2) as peer:
            config["port"] = peer.port
            self.assert_failure(config, "frame type/sequence mismatch")

    def test_ack_disconnect_bad_count_bad_durability_and_bad_json_never_pass(self):
        for mode in ("drop", "count", "durable", "malformed"):
            with self.subTest(mode=mode):
                config, _, _, _, _ = self.capture_config(counts=(1,))
                with Peer(1, ack=mode) as peer:
                    config["port"] = peer.port
                    result = self.invoke(config)
                    self.assertNotEqual(result.returncode, 0)
                    output = json.loads((Path(config["directory"]) / "source.json").read_text())
                    self.assertEqual(output["status"], "FAIL")

    def test_backpressure_exposes_clock_planned_unsent_records_and_lag(self):
        # Large legal whitespace creates transport pressure, not more records.
        # The peer intentionally waits, with a private small TCP receive buffer.
        fixture = self.root / "backpressure"
        input_path, manifest_path, _, _ = capture_files(fixture, (1,) * 8, padding=3 * 1024 * 1024)
        config = self.config(records=8, rate=80, timeout_s=10,
                             input_file=str(input_path), input_manifest=str(manifest_path))
        with Peer(8, pause=0.45, receive_buffer=16384) as peer:
            config["port"] = peer.port
            result = self.invoke(config)
            peer.thread.join(2)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIsNone(peer.error)
        output = json.loads((Path(config["directory"]) / "source.json").read_text())
        counters, stages = output["metrics"]["counters"], output["metrics"]["stages"]
        self.assertEqual(counters["source_planned_records"], 8)
        self.assertGreaterEqual(counters["source_planned_minus_sent_peak"], 1)
        self.assertEqual(counters["source_planned_minus_sent"], 0)
        self.assertGreater(stages["source_send_complete_lag"]["max_ns"], 100_000_000)
        self.assertEqual(stages["source_send_complete_lag"]["max_ns"], counters["source_send_complete_lag_max_ns"])
        self.assertIn("no_samples_inside_send", output["source_schedule"]["sampling"])

    def test_blocked_send_timeout_keeps_planned_unsent_backlog_and_fail_status(self):
        fixture = self.root / "blocked-send"
        input_path, manifest_path, _, _ = capture_files(fixture, (1,) * 8, padding=3 * 1024 * 1024)
        config = self.config(records=8, rate=80, timeout_s=1,
                             input_file=str(input_path), input_manifest=str(manifest_path))
        with Peer(8, pause=1.3, receive_buffer=16384) as peer:
            config["port"] = peer.port
            result = self.invoke(config)
            self.assertNotEqual(result.returncode, 0, result.stderr)
        output = json.loads((Path(config["directory"]) / "source.json").read_text())
        counters = output["metrics"]["counters"]
        self.assertEqual(output["status"], "FAIL")
        self.assertEqual(counters["source_planned_records"], 8)
        self.assertGreater(counters["source_planned_minus_sent"], 0)
        self.assertGreaterEqual(counters["source_planned_minus_sent_peak"], counters["source_planned_minus_sent"])
        self.assertEqual(counters["source_incomplete_frame_sends"], 1)
        progress = [json.loads(line) for line in (Path(config["directory"]) / "source-progress.jsonl").read_text().splitlines()]
        self.assertGreater(progress[-1]["metrics"]["counters"]["source_planned_minus_sent"], 0)

    def test_missing_capture_manifest_does_not_silently_assume_variable_batches(self):
        config, _, _, _, _ = self.capture_config(counts=(2, 1))
        config.pop("input_manifest")
        with Peer(3) as peer:
            config["port"] = peer.port
            self.assert_failure(config, "frame type/sequence mismatch")

    def test_original_synthetic_mode_remains_compatible(self):
        config = self.config(records=5, batch_records=2)
        with Peer(5) as peer:
            config["port"] = peer.port
            result = self.invoke(config)
            peer.thread.join(2)
            self.assertEqual(result.returncode, 0, result.stderr)
            sequences = [HEADER.unpack(wire[:HEADER.size])[4] for wire in peer.observed]
            self.assertEqual(sequences, [0, 2, 4, 5])
        output = json.loads((Path(config["directory"]) / "source.json").read_text())
        self.assertEqual(output["metrics"]["counters"]["source_sent"], 5)

    def test_config_resolves_manifest_and_nul_paths_overflow_replay_are_side_effect_free(self):
        config = self.config(input_file="input.iqb", input_manifest="sidecar.json")
        result = self.invoke(config, "source", "--print-config")
        self.assertEqual(result.returncode, 0, result.stderr)
        printed = json.loads(result.stdout)
        self.assertEqual(printed["input_manifest"], str(self.root / "sidecar.json"))
        self.assertFalse(Path(config["directory"]).exists())
        for key in ("directory", "input_file", "input_manifest", "output_file"):
            invalid = {**config, key: "name\0suffix"}
            self.assert_failure(invalid, "paths must not contain NUL")
            self.assertFalse(Path(config["directory"]).exists())
        for key in ("records", "batch_records", "dedup_max_entries"):
            self.assert_failure({**config, key: 2**64}, "invalid config JSON")
        self.assert_failure({**config, "parse": "replay"}, "does not support parse=replay")
        self.assert_failure({**config, "input": "binary"}, "requires JSON input_file")
        self.assert_failure({**config, "input_file": ""}, "requires JSON input_file")

    def test_sqlite_compact_lookup_enum_backend_scope_and_config_checks_have_no_side_effects(self):
        base = self.config(backend="sqlite_compact")
        for override, expected in (({}, "subquery"), ({"sqlite_compact_lookup": "subquery"}, "subquery"),
                                   ({"sqlite_compact_lookup": "separate"}, "separate")):
            with self.subTest(override=override):
                config = {**base, **override}
                printed = self.invoke(config, "source", "--print-config")
                self.assertEqual(printed.returncode, 0, printed.stderr)
                roundtrip = json.loads(printed.stdout)
                self.assertEqual(roundtrip["sqlite_compact_lookup"], expected)
                checked = self.invoke(roundtrip, "source", "--check-config")
                self.assertEqual(checked.returncode, 0, checked.stderr)
                self.assertFalse(Path(base["directory"]).exists())
        for value in ("", "auto", "SEPARATE"):
            self.assert_failure({**base, "sqlite_compact_lookup": value}, "invalid sqlite_compact_lookup")
            self.assertFalse(Path(base["directory"]).exists())
        for value in (None, True, 1):
            self.assert_failure({**base, "sqlite_compact_lookup": value}, "wrong type or overflow: sqlite_compact_lookup")
            self.assertFalse(Path(base["directory"]).exists())
        for backend in ("sqlite", "null", "segment", "segment_rocksdb", "rocksdb"):
            self.assert_failure({**base, "backend": backend, "sqlite_compact_lookup": "separate"},
                                "sqlite_compact_lookup=separate requires backend=sqlite_compact")
            self.assertFalse(Path(base["directory"]).exists())
        default = self.invoke(self.config(), "source", "--print-config")
        self.assertEqual(default.returncode, 0, default.stderr)
        self.assertEqual(json.loads(default.stdout)["sqlite_compact_lookup"], "subquery")

    def test_backend_failpoint_whitelists_and_rocksdb_full_contract(self):
        base = self.config(backend="rocksdb")
        probe = self.invoke(base, "source", "--check-config")
        if probe.returncode:
            self.assertIn("TICKSHELF_BENCH_ROCKSDB=ON", probe.stderr)
        else:
            for failpoint in ("before_write", "after_write"):
                result = self.invoke({**base, "failpoint": failpoint}, "source", "--check-config")
                self.assertEqual(result.returncode, 0, result.stderr)
            for failpoint in ("after_append", "after_sync", "after_index"):
                self.assert_failure({**base, "failpoint": failpoint}, "not supported by this backend")
        self.assert_failure({**base, "synchronous": "NORMAL"}, "require synchronous=FULL")
        for backend in ("sqlite", "null", "segment"):
            for failpoint in ("before_write", "after_write"):
                self.assert_failure({**base, "backend": backend, "failpoint": failpoint}, "not supported by this backend")
        self.assertFalse(Path(base["directory"]).exists())

    def test_dedup_policy_needs_no_config_and_has_no_filesystem_side_effects(self):
        before = set(self.root.iterdir())
        result = subprocess.run([str(BINARY), "--dedup-policy"], cwd=self.root,
                                capture_output=True, text=True, timeout=3)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(set(self.root.iterdir()), before)
        policy = json.loads(result.stdout)
        self.assertEqual(policy["schema_version"], 1)
        self.assertEqual(policy["policy"], "fifo")
        self.assertEqual(policy["fifo_limit_bytes"], 256 * 1024 * 1024)
        self.assertGreater(policy["fixed_charge_bytes"], 0)
        self.assertIn("dedup_max_entries", policy["max_entry_policy"])
        self.assertIn("does_not_refresh_fifo", policy["duplicate_policy"])
        self.assertEqual(policy["entry_charge"], "canonical_bytes+fixed_charge_bytes")
        result = subprocess.run([str(BINARY), "--dedup-policy", "--config", "missing.json"],
                                cwd=self.root, capture_output=True, text=True, timeout=3)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must be used alone", result.stderr)
        self.assertEqual(set(self.root.iterdir()), before)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(add_help=False)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--build", type=Path)
    group.add_argument("--binary", type=Path)
    options, remaining = parser.parse_known_args()
    if options.binary:
        BINARY = options.binary.resolve()
    elif options.build:
        BINARY = (options.build / "pipeline_bench").resolve()
    if not BINARY.is_file():
        parser.error(f"pipeline_bench does not exist: {BINARY}")
    unittest.main(argv=[sys.argv[0], *remaining])
