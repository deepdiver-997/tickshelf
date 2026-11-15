#!/usr/bin/env python3
"""Isolated, bounded pipeline experiment runner; no shared Redis or raw devices."""
from __future__ import annotations

import argparse
import csv
from dataclasses import asdict
import hashlib
import itertools
import re
import json
import math
import os
import platform
import random
import shutil
import signal
import sqlite3
import statistics
import subprocess
import sys
import time
import uuid
from pathlib import Path

from pipeline_bench_anomaly import AnomalyTrigger
from pipeline_bench_processes import BenchmarkProcesses
from pipeline_bench_syscalls import analyze_trace, preflight as syscall_preflight, prepare_launch
from pipeline_bench_support import (DEFAULTS, backlog_summary, counter, unique_count,
                                    validate_metrics, verify_export, write_input)
from stack_runtime import (OwnedProcesses, RunError, StopFlag, candidate_ports,
                           start_private_redis, wait_until)

ROOT = Path(__file__).resolve().parent.parent
DIAGNOSTIC = (
    ("full", {}), ("parse_replay", {"parse": "replay"}),
    ("hash_off", {"hash": "off"}), ("redis_encode", {"redis": "encode"}),
    ("redis_off", {"redis": "off"}), ("null", {"backend": "null"}),
    ("smalltxn", {"transaction_rows": 16}),
    ("heavy", {"transaction_model": "heavy"}),
    ("segment", {"backend": "segment"}), ("binary", {"input": "binary"}),
    ("generated_json_source", {"input": "json", "parse": "real"}),
)
SMOKE = (DIAGNOSTIC[0], ("duplicates", {"duplicate_every": 7, "optional_fields": True}),
         ("duplicates_hash_off", {"duplicate_every": 7, "optional_fields": True, "hash": "off"}),
         DIAGNOSTIC[5], DIAGNOSTIC[8], DIAGNOSTIC[9])
RESERVED = {"directory", "host", "port", "redis_host", "redis_port", "input_file", "input_manifest", "output_file", "failpoint"}


def save_json(path, data):
    Path(path).write_text(json.dumps(data, indent=2, sort_keys=True, allow_nan=False) + "\n")


def sync_created_directory(path):
    """Persist the new directory and its parent before starting a real sink."""
    evidence = []
    for directory in (Path(path), Path(path).parent):
        fd = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        evidence.append({"path": str(directory.resolve()), "fsync": True})
    return evidence


def read_json(path):
    with Path(path).open() as source:
        return json.load(source)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def config_overrides(value):
    if not value:
        return {}
    text = value if value.lstrip().startswith("{") else Path(value).read_text()
    config = json.loads(text, object_pairs_hook=unique_object)
    if not isinstance(config, dict):
        raise ValueError("--config must be a flat JSON object or its filename")
    unknown = set(config) - set(DEFAULTS) - {"directory"}
    if unknown:
        raise ValueError(f"unknown config keys: {sorted(unknown)}")
    reserved = set(config) & RESERVED
    if reserved:
        raise ValueError(f"runner owns these config keys; remove {sorted(reserved)}")
    for key, value in config.items():
        default = DEFAULTS[key]
        if type(value) is not type(default):
            raise ValueError(f"wrong config type for {key}")
    return config


def load_matrix(path):
    """A bounded data-defined matrix: axes product OR explicit named cases."""
    with Path(path).open() as source:
        text = source.read(65537)
    if len(text.encode()) > 65536:
        raise ValueError("matrix exceeds 64KiB")
    spec = json.loads(text, object_pairs_hook=unique_object)
    if not isinstance(spec, dict) or set(spec) not in ({"axes"}, {"cases"}):
        raise ValueError("matrix requires exactly one of axes or cases")
    if "axes" in spec:
        axes = spec["axes"]
        if not isinstance(axes, dict) or not axes or any(not isinstance(v, list) or not v for v in axes.values()):
            raise ValueError("axes must map config keys to nonempty value lists")
        if math.prod(len(values) for values in axes.values()) > 64:
            raise ValueError("matrix exceeds 64 cells")
        keys = list(axes)
        cases = [{"name": f"matrix-{i + 1:03d}", "set": dict(zip(keys, values))}
                 for i, values in enumerate(itertools.product(*(axes[key] for key in keys)))]
    else:
        cases = spec["cases"]
    if not isinstance(cases, list) or not 1 <= len(cases) <= 64:
        raise ValueError("matrix cases must contain 1..64 entries")
    result, names = [], set()
    for case in cases:
        if not isinstance(case, dict) or set(case) != {"name", "set"}:
            raise ValueError("each matrix case requires name and set")
        name = case["name"]
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", name) or name in names:
            raise ValueError("invalid or duplicate matrix case name")
        if not isinstance(case["set"], dict):
            raise ValueError("matrix set must be config overrides")
        overrides = config_overrides(json.dumps(case["set"]))
        names.add(name)
        result.append((name, overrides))
    return result


def binary_path(build):
    path = build.expanduser().resolve()
    return path / "pipeline_bench" if path.is_dir() else path


def command_capture(argv, timeout=5):
    started = time.time()
    try:
        done = subprocess.run([str(a) for a in argv], capture_output=True, text=True,
                              timeout=timeout, errors="replace")
        return {"argv": [str(a) for a in argv], "started_unix_s": started,
                "returncode": done.returncode, "stdout": done.stdout[:200000],
                "stderr": done.stderr[:20000]}
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"argv": [str(a) for a in argv], "started_unix_s": started, "error": str(exc)}


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def environment(binary, output, work):
    result = {"captured_unix_s": time.time(), "platform": platform.platform(),
              "uname": list(platform.uname()), "python": sys.version, "cpu_count": os.cpu_count(),
              "binary": str(binary), "binary_sha256": sha256_file(binary),
              "sqlite_python_runtime_version": sqlite3.sqlite_version,
              "sqlite_version_caveat": "Python SQLite is not proof of the executable's linked SQLite version",
              "git_head": command_capture(["git", "-C", ROOT, "rev-parse", "HEAD"]),
              "git_status": command_capture(["git", "-C", ROOT, "status", "--porcelain=v1"]),
              "git_diff_stat": command_capture(["git", "-C", ROOT, "diff", "--stat"]),
              "filesystem": command_capture(["df", "-Pk", output, work]),
              "linked_libraries": command_capture(["otool", "-L", binary] if sys.platform == "darwin" else ["ldd", binary]),
              "disk_paths": {str(p): dict(zip(("total", "used", "free"), shutil.disk_usage(p))) for p in {output, work}}}
    # The working tree can contain new, uncommitted benchmark files: HEAD alone
    # does not identify the experiment. Record their exact hashes alongside binary.
    source_paths = list((ROOT / "bench").glob("*")) + list((ROOT / "scripts").glob("*pipeline_bench*.py"))
    source_paths += [ROOT / "scripts/pipeline_bench_anomaly.py", ROOT / "scripts/stack_runtime.py", ROOT / "CMakeLists.txt"]
    result["source_sha256"] = {str(path.relative_to(ROOT)): sha256_file(path)
                               for path in sorted(set(source_paths)) if path.is_file()}
    cache = binary.parent / "CMakeCache.txt"
    if cache.is_file():
        wanted = ("CMAKE_BUILD_TYPE:", "CMAKE_CXX_COMPILER:", "CMAKE_CXX_FLAGS", "SQLite", "SQLITE", "simdjson")
        result["cmake_cache_build_flags"] = [line for line in cache.read_text(errors="replace").splitlines()
                                             if any(line.startswith(prefix) for prefix in wanted)]
    else:
        result["cmake_cache_build_flags"] = {"unavailable": str(cache)}
    target_flags = {}
    for flags in sorted((binary.parent / "CMakeFiles").glob("*bench*.dir/flags.make")):
        target_flags[str(flags)] = flags.read_text(errors="replace")[:100000]
    compile_commands = binary.parent / "compile_commands.json"
    if compile_commands.is_file():
        try:
            commands = json.loads(compile_commands.read_text())
            result["bench_compile_commands"] = [entry for entry in commands
                                                if "/bench/" in str(entry.get("file", "")) or str(entry.get("file", "")).startswith("bench/")]
        except (ValueError, TypeError) as exc:
            result["bench_compile_commands_error"] = str(exc)
    result["target_compile_flags"] = target_flags or {"unavailable": "no bench flags.make; inspect bench_compile_commands if supplied"}
    for name in ("cpuinfo", "meminfo", "version"):
        path = Path("/proc") / name
        if path.is_file():
            result["proc_" + name] = path.read_text(errors="replace")[:16000]
    if hasattr(os, "sched_getaffinity"):
        result["cpu_affinity"] = sorted(os.sched_getaffinity(0))
    if sys.platform == "darwin":
        result["hardware"] = command_capture(["sysctl", "hw.model", "hw.memsize", "hw.ncpu", "hw.physicalcpu"])
    return result


def cpu_seconds(text):
    # ps TIME is [[days-]hh:]mm:ss[.fraction].
    days = 0
    if "-" in text:
        prefix, text = text.split("-", 1)
        days = int(prefix)
    value = 0.0
    for part in text.split(":"):
        value = value * 60 + float(part)
    return value + days * 86400


def observe(processes, elapsed):
    alive = {name: processes.process_pid(name) if hasattr(processes, "process_pid") else child.pid
             for name, child in processes.children.items()
             if name in ("source", "receiver", "redis") and child.poll() is None}
    result = {"wall_elapsed_s": elapsed, "captured_unix_s": time.time(), "processes": []}
    if alive:
        ps = command_capture(["ps", "-p", ",".join(map(str, alive.values())),
                              "-o", "pid=,pcpu=,rss=,time=,command="], timeout=2)
        result["ps"] = ps
        for line in ps.get("stdout", "").splitlines():
            parts = line.split(None, 4)
            try:
                pid = int(parts[0])
                name = next(name for name, candidate in alive.items() if candidate == pid)
                point = {"name": name, "pid": pid, "cpu_pct_lifetime": float(parts[1]),
                         "rss_kib": int(parts[2]), "cpu_time_s": cpu_seconds(parts[3]), "command": parts[4]}
            except (ValueError, IndexError, StopIteration):
                continue
            for filename in ("io", "status"):
                path = Path(f"/proc/{pid}/{filename}")
                try:
                    point["proc_" + filename] = path.read_text(errors="replace")[:8000]
                except OSError:
                    pass
            result["processes"].append(point)
    try:
        result["diskstats"] = Path("/proc/diskstats").read_text()[:64000]
    except OSError:
        pass
    return result


def tree_bytes(paths):
    total = 0
    roots = []
    for path in sorted(set(p.resolve() for p in paths), key=lambda p: len(p.parts)):
        if any(path.is_relative_to(previous) for previous in roots):
            continue
        roots.append(path)
        for directory, _, files in os.walk(path):
            for name in files:
                try:
                    total += (Path(directory) / name).stat().st_size
                except FileNotFoundError:
                    pass
    return total


def enforce_budget(paths, max_bytes, reserve_bytes):
    size = tree_bytes(paths)
    if size > max_bytes:
        raise RunError(f"artifact budget exceeded: {size} > {max_bytes} bytes")
    for path in set(paths):
        if shutil.disk_usage(path).free < reserve_bytes:
            raise RunError(f"free disk reserve exhausted at {path}")
    return size


class Profiler:
    """Bounded sampling of exact owned receiver PID, while ingestion is running."""
    def __init__(self, mode, directory, trigger_s, *, config=None, thresholds=None, expected_forwarded=None):
        self.mode, self.directory, self.trigger_s = mode, directory, trigger_s
        self.started = False
        self.next_gate_poll = 0.0
        config = config or DEFAULTS
        expected_rate = config["rate"]
        if config["hash"] == "dedup":
            forwarded = unique_count(config) if expected_forwarded is None else expected_forwarded
            expected_rate *= forwarded / config["records"]
        self.trigger = AnomalyTrigger(
            minimum_elapsed_s=trigger_s,
            minimum_records_per_s=getattr(thresholds, "profile_min_rps", 0) or expected_rate * .9,
            maximum_backlog_records=getattr(thresholds, "profile_backlog_records", 0) or max(4 * config["transaction_rows"], config["rate"], 4000),
            maximum_commit_ms=getattr(thresholds, "profile_commit_ms", 100),
            sustained_windows=getattr(thresholds, "profile_windows", 2), backend=config["backend"])
        self.info = {"mode": mode, "requested": mode != "off", "started": False,
                     "interpretation": "sampling artifacts only; no CPU hotspot claim without inspecting them"}

    def maybe_start(self, owner, elapsed):
        if self.mode == "off" or self.started or (self.mode == "after" and elapsed < self.trigger_s):
            return
        if self.mode == "trigger":
            if elapsed < self.next_gate_poll:
                return
            self.next_gate_poll = elapsed + .5
            receiver_points = tail_progress(self.directory / "progress.jsonl")
            if not receiver_points:
                return
            point = receiver_points[-1]
            source_points = tail_progress(self.directory / "source-progress.jsonl")
            aligned = next((p for p in reversed(source_points) if p["monotonic_ns"] <= point["monotonic_ns"]), None)
            evidence = self.trigger.observe(elapsed, point, aligned)
            self.info["last_gate_observation"] = self.trigger.last_evidence
            if not evidence:
                return
            self.info["trigger_evidence"] = evidence
        receiver = owner.children["receiver"]
        source = owner.children.get("source")
        if receiver.poll() is not None or source is None or source.poll() is not None:
            return
        self.started = True
        target_pid = owner.process_pid("receiver") if hasattr(owner, "process_pid") else receiver.pid
        if sys.platform == "darwin":
            executable = shutil.which("sample")
            argv = [executable, target_pid, "2", "10", "-file", self.directory / "receiver.sample.txt"] if executable else []
        elif sys.platform.startswith("linux"):
            executable = shutil.which("perf")
            argv = [executable, "record", "-F", "99", "-g", "-p", target_pid,
                    "-o", self.directory / "receiver.perf.data", "--", "sleep", "2"] if executable else []
        else:
            argv = []
        self.info.update({"target_pid": target_pid, "started_unix_s": time.time(),
                          "measurement_elapsed_s": elapsed, "argv": [str(a) for a in argv]})
        if not argv:
            self.info["error"] = "no supported sample/perf executable; profiling unavailable"
            return
        try:
            child = owner.start("profiler", argv)
            self.info.update({"started": True, "profiler_pid": child.pid})
        except OSError as exc:
            self.info["error"] = str(exc)

    def finish(self, owner):
        if "profiler" in owner.children:
            child = owner.children["profiler"]
            try:
                child.wait(timeout=4)
            except subprocess.TimeoutExpired:
                owner.stop("profiler", timeout=1)
            self.info.update({"ended_unix_s": time.time(), "returncode": child.returncode,
                              "log": str(owner.directory / "profiler.log")})
            text = owner.text("profiler")
            if child.returncode or any(word in text.lower() for word in ("permission", "not permitted", "denied")):
                self.info["permission_or_tool_error"] = text[-10000:]
        elif self.mode != "off" and not self.started:
            self.info["not_started_reason"] = "run completed before in-measurement trigger/sampling opportunity"
        save_json(self.directory / "profiling.json", self.info)


def tail_progress(path):
    """Read bounded complete live snapshots; a concurrent partial line is not an error."""
    try:
        with path.open("rb") as source:
            source.seek(0, os.SEEK_END)
            size = source.tell()
            offset = max(0, size - 65536)
            source.seek(offset)
            lines = source.read(65536).splitlines(keepends=True)
        if offset and lines:
            lines = lines[1:]
        return [json.loads(line) for line in lines if line.endswith(b"\n")]
    except (OSError, ValueError):
        return []  # Final, strict read_progress still verifies every line after exit.


def read_progress(path):
    if not path.exists():
        return []
    result = []
    with path.open() as source:
        for line_number, line in enumerate(source, 1):
            try:
                result.append(json.loads(line))
            except ValueError as exc:
                raise RunError(f"invalid progress JSON line {line_number}") from exc
    return result


def process_cpu_summary(observations):
    result = {}
    for name in ("source", "receiver", "redis"):
        points = [(sample["wall_elapsed_s"], process["cpu_time_s"])
                  for sample in observations for process in sample["processes"] if process["name"] == name]
        slope = None
        if len(points) >= 2 and points[-1][0] > points[0][0]:
            slope = (points[-1][1] - points[0][1]) / (points[-1][0] - points[0][0])
        rss = [process["rss_kib"] for sample in observations for process in sample["processes"] if process["name"] == name]
        result[name] = {"sample_count": len(points), "cpu_cores_over_observed_interval": slope,
                        "max_rss_kib": max(rss, default=None)}
    return result


def source_evidence(input_info=None):
    info = input_info or {}
    source = info.get("source", {"kind": "synthetic", "provenance": "deterministic generated benchmark"})
    kind = source.get("kind", "unknown")
    return {"kind": kind, "provenance": source.get("provenance"), "authorization_declared": source.get("authorized", False),
            "production_capture_supplied": kind == "production_capture", "production_qualified": False,
            "production_validation": ("user-attested-capture-replay-only; not live production certification" if kind == "production_capture"
                                      else "not-performed; synthetic or production-shape input is not production capture"),
            "authenticity_scope": "file hashes establish integrity, not independent proof of historical source authenticity"}


def full_chain_contract(config, input_info=None):
    required = {"input": "json", "parse": "real", "hash": "dedup",
                "hash_algorithm": "sha256", "redis": "roundtrip", "synchronous": "FULL"}
    evidence = source_evidence(input_info)
    if evidence["kind"] == "synthetic":
        required.update(depth=5, duplicate_every=0, optional_fields=False)
    violations = {key: {"required": value, "actual": config.get(key)}
                  for key, value in required.items() if config.get(key) != value}
    if config.get("backend") not in ("sqlite", "sqlite_compact", "rocksdb", "segment", "segment_rocksdb"):
        violations["backend"] = {"required": "durable record or segment backend", "actual": config.get("backend")}
    coverage = (input_info or {}).get("coverage")
    symbols = coverage.get("observed_symbols", 0) if coverage is not None else config.get("symbols", 0)
    if symbols < 54:
        violations["symbols"] = {"required": ">=54 actual input symbols", "actual": symbols}
    if input_info is not None:
        if not coverage or coverage.get("records") != config["records"] or not coverage.get("all_records_full_five_level_baseline"):
            violations["input_coverage"] = {"required": "all actual records contain the twelve baseline scalars and four five-level arrays",
                                             "actual": coverage}
        if evidence["kind"] not in ("synthetic", "production_shape", "production_capture"):
            violations["source_kind"] = {"required": "explicit supported input origin", "actual": evidence["kind"]}
        if evidence["kind"] in ("production_shape", "production_capture") and not (evidence["authorization_declared"] and evidence["provenance"]):
            violations["provenance"] = {"required": "explicit authorization and source declaration", "actual": evidence}
    return {"eligible": not violations, "violations": violations, "source": evidence,
            "coverage_basis": "independently inspected input records" if coverage is not None else "declared synthetic config only; no input inspected",
            "scope": "complete five-level JSON/TCP/SHA/dedup/Redis/FULL under the stated input; no production certification implied"}


def source_schedule_summary(source, progress):
    counters = source.get("metrics", {}).get("counters", {})
    points = []
    for point in progress:
        c = point.get("metrics", {}).get("counters", {})
        if "source_planned_records" in c:
            points.append((point["elapsed_s"], max(0, c["source_planned_records"] - c.get("source_sent", 0))))
    interior = points[1:-1]
    slope = None
    if len(interior) >= 3:
        mean_t = statistics.mean(t for t, _ in interior)
        mean_gap = statistics.mean(gap for _, gap in interior)
        denominator = sum((t - mean_t) ** 2 for t, _ in interior)
        if denominator:
            slope = sum((t - mean_t) * (gap - mean_gap) for t, gap in interior) / denominator
    return {"available": "source_planned_records" in counters,
            "send_start_lag_max_s": counters.get("source_send_start_lag_max_ns", 0) / 1e9,
            "send_complete_lag_max_s": counters.get("source_send_complete_lag_max_ns", 0) / 1e9,
            "planned_minus_sent_peak": counters.get("source_planned_minus_sent_peak"),
            "planned_minus_sent_slope_records_per_s": slope,
            "steady_samples": len(interior), "samples": points,
            "scope": "planned source arrivals minus socket-accepted sends; synchronous send intervals are sampled before/after, not per record"}


def performance_summary(source, receiver, progress, observations, config, duration, suite, source_progress=(), minimum_durable_rps=0,
                        input_info=None, max_source_lag_s=1.0, require_source_schedule=False):
    send_s = source["send_elapsed_ns"] / 1e9 if "send_elapsed_ns" in source else source.get("send_elapsed_s")
    if type(send_s) not in (int, float) or not math.isfinite(send_s) or send_s <= 0:
        raise RunError("missing or invalid source send_elapsed_s")
    actual = config["records"] / send_s
    unique_records = (input_info or {}).get("unique_records", unique_count(config))
    source_s = source["elapsed_ns"] / 1e9 if "elapsed_ns" in source else source["elapsed_s"]
    receiver_s = receiver["elapsed_ns"] / 1e9 if "elapsed_ns" in receiver else receiver["elapsed_s"]
    durable_rate = unique_records / receiver_s if config["backend"] != "null" and config["synchronous"] == "FULL" else None
    backlog = backlog_summary(progress, source_progress, config["backend"])
    schedule = source_schedule_summary(source, source_progress)
    cpu = process_cpu_summary(observations)
    target = config["rate"]
    offered = target if target else None
    contract = full_chain_contract(config, input_info)
    end_to_end_rate = unique_records / max(source_s, receiver_s) if durable_rate is not None else None
    result = {"offered_records_per_s": offered, "actual_source_records_per_s": actual,
              "full_chain_contract": contract, "minimum_durable_records_per_s": minimum_durable_rps,
              "end_to_end_durable_unique_records_per_s": end_to_end_rate,
              "source_offer_ratio": actual / target if target else None,
              "receiver_records_per_s_including_drain": config["records"] / receiver_s,
              "durable_unique_records_per_s_including_drain": durable_rate,
              "durable_unique_count": unique_records, "input_source": source_evidence(input_info),
              "source_send_elapsed_s": send_s, "source_ack_elapsed_s": source_s,
              "ack_tail_after_send_s": max(0.0, source_s - send_s),
              "source_schedule": schedule,
              "source_schedule_acceptance": {"required": require_source_schedule, "max_lag_s": max_source_lag_s},
              "backlog": backlog, "cpu": cpu,
              "dedup_cache": {"dedup_evictions": receiver["metrics"]["counters"].get("dedup_evictions", 0),
                              "dedup_entries": receiver["metrics"]["counters"].get("dedup_entries"),
                              "dedup_payload_bytes": receiver["metrics"]["counters"].get("dedup_payload_bytes"),
                              "dedup_cache_bytes": receiver["metrics"]["counters"].get("dedup_cache_bytes"),
                              "semantics": "bounded FIFO; payload is held once; cache bytes include conservative entry/key/link accounting, not RSS; evictions are not durability failures"},
              "verdict": "not-a-capacity-test",
              "source_bound_warning": None,
              "notes": ["Receiver rate includes drain and alone never establishes sustainable capacity.",
                        "Progress cannot observe kernel/socket/source queues; timestamps are receiver-relative.",
                        "CPU stage timers measure wall time, not inspected CPU hotspots."]}
    source_cores = cpu["source"]["cpu_cores_over_observed_interval"]
    if target and actual < target * .95:
        result["source_bound_warning"] = ("source appears CPU-bound; do not label receiver capacity" if source_cores is not None and source_cores >= .8
                                            else "offered rate not achieved; source, transport, or backpressure bound is unresolved")
    if suite != "target":
        return result
    if minimum_durable_rps > 0 and not contract["eligible"]:
        result["verdict"] = "fail-full-chain-contract"
        return result
    if config["backend"] == "null" or config["synchronous"] != "FULL":
        result["verdict"] = "consume-only-not-durable" if config["backend"] == "null" else "weaker-durability-not-full-target"
        return result
    steady_samples = backlog["steady_samples"]
    slope = backlog["gap_slope_records_per_s"]
    total_slope = backlog["source_commit_gap_slope_records_per_s"]
    # A short, drained burst is deliberately inconclusive. Target runs need a
    # sustained interval plus at least five receiver time-series observations.
    if duration < 5 or steady_samples < 3 or send_s < duration * .8:
        result["verdict"] = "inconclusive-insufficient-steady-time-series"
    elif actual < target * .95:
        result["verdict"] = "inconclusive-offer-not-achieved"
    elif require_source_schedule and (not schedule["available"] or schedule["steady_samples"] < 3):
        result["verdict"] = "inconclusive-missing-source-schedule"
    elif require_source_schedule and schedule["send_complete_lag_max_s"] > max_source_lag_s:
        result["verdict"] = "fail-source-schedule-lag"
    elif require_source_schedule and (schedule["planned_minus_sent_slope_records_per_s"] is None or
                                      schedule["planned_minus_sent_slope_records_per_s"] > max(100, target * .01)):
        result["verdict"] = "fail-growing-unsent-source-plan"
    elif total_slope is None or backlog["source_aligned_steady_samples"] < 3:
        result["verdict"] = "inconclusive-missing-source-aligned-time-series"
    elif slope is None or max(slope, total_slope) > max(100, target * .01):
        result["verdict"] = "fail-growing-backlog"
    elif source_s - send_s > max(1.0, duration * .1):
        result["verdict"] = "fail-excessive-ack-tail"
    else:
        result["verdict"] = "pass-bounded-target-observation"
    if minimum_durable_rps > 0:
        result["minimum_durable_rate_passed"] = end_to_end_rate is not None and end_to_end_rate >= minimum_durable_rps
        if result["verdict"] == "pass-bounded-target-observation":
            if duration < 10 or send_s < 10:
                result["verdict"] = "inconclusive-capacity-window-under-ten-seconds"
            elif actual < minimum_durable_rps:
                result["verdict"] = "inconclusive-source-below-capacity-floor"
            elif not result["minimum_durable_rate_passed"]:
                result["verdict"] = "fail-minimum-durable-throughput"
            else:
                result["verdict"] = "pass-full-chain-capacity"
    result["target_acceptance"] = {"minimum_duration_s": 10 if minimum_durable_rps > 0 else 5, "minimum_steady_samples": 3,
                                    "minimum_offer_ratio": .95, "max_gap_slope_records_per_s": max(100, target * .01),
                                    "max_ack_tail_s": max(1.0, duration * .1),
                                    "scope": "this finite run and hardware only, not an unbounded capacity proof"}
    return result


def run_case(binary, case, config, case_dir, input_info, *, stop, args, output, work, dataset=None):
    case_dir.mkdir(mode=0o700)
    config = {**config, "directory": str(case_dir)}
    redis_port, port = candidate_ports(2)
    config.update(redis_port=redis_port, port=port, host="127.0.0.1", redis_host="127.0.0.1")
    config_path = case_dir / "config.json"
    save_json(config_path, config)
    result = {"case": case["name"], "repeat": case["repeat"], "warmup": case.get("warmup", False),
              "directory": str(case_dir), "config": config, "input": input_info,
              "labels": ["synthetic_parse_bypass; predecoded same raw bytes; replay_copy is not zero work"] if config["parse"] == "replay" else [],
              "started_unix_s": time.time(), "binary_sha256": sha256_file(binary),
              "correctness": {"passed": False}}
    if input_info.get("source_mode") == "generated_cpp":
        result["labels"].append("C++ source constructs records/JSON/frames during measurement; not a receiver-parse-only comparison")
    save_json(case_dir / "case.json", result)
    owner = BenchmarkProcesses(case_dir / "processes")
    trace_enabled = getattr(args, "syscalls", "off") == "trace" and not case.get("warmup")
    result["syscalls"] = {"status": "not-requested"}
    result["input_source"] = source_evidence(input_info)
    profiler = Profiler(case.get("profile", args.profile), case_dir, args.profile_trigger_s,
                        config=config, thresholds=args, expected_forwarded=input_info.get("expected_forwarded"))
    observations = []
    try:
        result["startup_directory_sync"] = sync_created_directory(case_dir)
        if dataset is not None:
            dataset.verify_inputs(check=stop.check, deadline=time.monotonic() + args.timeout)
        check = command_capture([binary, "--role", "receiver", "--config", config_path, "--print-config"])
        save_json(case_dir / "config-check.json", check)
        if check.get("returncode") != 0:
            raise RunError(f"receiver config rejected: {check.get('stderr', check.get('error'))}")
        result["resolved_config"] = json.loads(check["stdout"])
        if config["redis"] in ("roundtrip", "publish"):
            redis_binary = shutil.which("redis-server")
            if not redis_binary:
                raise RunError("redis-server not found; full pipeline cannot be silently skipped")
            start_private_redis(owner, redis_binary, redis_port, stop=stop)
        receiver_command = [binary, "--role", "receiver", "--config", config_path]
        if trace_enabled:
            launch = prepare_launch(receiver_command, trace_path=case_dir / "receiver.strace", owned_binary=binary,
                                    enabled=True, capability=getattr(args, "syscall_capability", None))
            receiver_command = launch.command
            result["syscalls"] = {"status": "requested", "trace": str(launch.trace_path),
                                  "capability": asdict(launch.capability), "observer": "strace; not an untraced baseline"}
        receiver = owner.start("receiver", receiver_command)
        if trace_enabled:
            owner.traced.add("receiver")
        # A traced receiver's Popen PID belongs to strace, not the application.
        # No TCP readiness probe: that would consume its single source connection.
        wait_until(lambda: owner.receiver_ready(binary, traced=trace_enabled), timeout=min(args.timeout, 60),
                   description="owned receiver BENCH_READY stderr marker", processes=owner,
                   names=tuple(owner.children), stop=stop)
        started = time.monotonic()
        source = owner.start("source", [binary, "--role", "source", "--config", config_path])
        result["pids"] = {name: owner.process_pid(name) for name in owner.children}
        result["supervisor_pids"] = {name: child.pid for name, child in owner.children.items()}
        result["measurement_started_unix_s"] = time.time()
        save_json(case_dir / "case.json", result)
        next_observe = next_budget = 0.0
        with (case_dir / "observations.jsonl").open("x") as samples:
            while source.poll() is None or receiver.poll() is None:
                stop.check()
                elapsed = time.monotonic() - started
                if elapsed > args.timeout:
                    raise RunError(f"case measurement timeout after {args.timeout}s")
                for name in ("source", "receiver"):
                    rc = owner.children[name].poll()
                    if rc not in (None, 0):
                        raise RunError(f"{name} exited {rc}; see owned process log")
                if "redis" in owner.children:
                    owner.require_alive("redis")
                profiler.maybe_start(owner, elapsed)
                if elapsed >= next_observe:
                    sample = observe(owner, elapsed)
                    observations.append(sample)
                    samples.write(json.dumps(sample, allow_nan=False) + "\n")
                    samples.flush()
                    next_observe = elapsed + .5
                if elapsed >= next_budget:
                    enforce_budget([output, work], args.max_disk_mib * 1024 * 1024, args.reserve_free_mib * 1024 * 1024)
                    next_budget = elapsed + 2
                time.sleep(.025)
        profiler.finish(owner)
        source_metrics, receiver_metrics = read_json(case_dir / "source.json"), read_json(case_dir / "receiver.json")
        result["source"] = source_metrics
        result["receiver"] = receiver_metrics
        result["correctness"] = (dataset.validate_metrics(source_metrics, receiver_metrics, config, check=stop.check)
                                 if dataset is not None else validate_metrics(source_metrics, receiver_metrics, config, input_info))
        result["stage_summary"] = stage_summary(source_metrics, receiver_metrics)
        if config["backend"] == "null":
            result["oracle"] = {"passed": True, "method": "independent-forwarded-canonical-checksum-and-consume-ACK",
                                "exports_expected": False, "durability_proved": False}
            if (case_dir / "rows.jsonl").exists():
                raise RunError("null sink unexpectedly exported durable rows")
        else:
            export = owner.start("export", [binary, "--role", "export", "--config", config_path])
            deadline = time.monotonic() + args.timeout
            while export.poll() is None:
                stop.check()
                if time.monotonic() > deadline:
                    raise RunError("export timeout")
                enforce_budget([output, work], args.max_disk_mib * 1024 * 1024, args.reserve_free_mib * 1024 * 1024)
                time.sleep(.1)
            if export.returncode:
                raise RunError(f"export exited {export.returncode}")
            result["oracle"] = (dataset.verify_export(case_dir / "rows.jsonl", config, check=stop.check,
                                                      deadline=time.monotonic() + args.timeout)
                                if dataset is not None else verify_export(case_dir / "rows.jsonl", config))
            if config["backend"] in ("segment", "segment_rocksdb"):
                expected_physical = counter(receiver_metrics, "forwarded")
                if result["oracle"]["physical_rows"] != expected_physical:
                    raise RunError("segment physical count disagrees with forwarded records")
        if dataset is not None:
            dataset.verify_inputs(check=stop.check, deadline=time.monotonic() + args.timeout)
        progress = read_progress(case_dir / "progress.jsonl")
        source_progress = read_progress(case_dir / "source-progress.jsonl")
        result["performance"] = performance_summary(source_metrics, receiver_metrics, progress, observations,
                                                     config, args.duration, args.suite, source_progress,
                                                     0 if case.get("warmup") else args.min_durable_rps,
                                                     input_info=input_info,
                                                     max_source_lag_s=getattr(args, "max_source_lag_ms", 1000) / 1000,
                                                     require_source_schedule=not case.get("warmup"))
        result["performance"]["offered_unique_records_per_s"] = (config["rate"] * input_info.get("unique_records", unique_count(config)) / config["records"]
                                                                  if config["rate"] else None)
        result["performance"]["committed_attempts_per_s_including_drain"] = counter(receiver_metrics, "committed_attempts") / receiver_metrics["elapsed_s"]
        result["performance"]["physical_rows"] = result["oracle"].get("physical_rows")
        result["status"] = "passed"
    except (Exception, KeyboardInterrupt) as exc:
        result.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        result["correctness"]["passed"] = False
        if isinstance(exc, (InterruptedError, KeyboardInterrupt)):
            result["interrupted"] = True
    finally:
        try:
            if not (case_dir / "profiling.json").exists():
                profiler.finish(owner)
        except Exception as exc:
            profiler.info["finalization_error"] = str(exc)
        try:
            owner.close()
        except Exception as exc:
            result.update(status="failed", cleanup_error=f"{type(exc).__name__}: {exc}")
            result["correctness"]["passed"] = False
        result["profiling"] = profiler.info
        if trace_enabled and (case_dir / "receiver.strace").is_file():
            try:
                db_paths = [case_dir / name for name in ("full-record.sqlite3", "compact-record.sqlite3", "segment-index.sqlite3",
                                                          "full-record-rocksdb-v1", "segment-rocksdb-v1/index.rocksdb")]
                analysis = analyze_trace(case_dir / "receiver.strace", db_paths=db_paths,
                                         segment_paths=[case_dir / "records.segments", case_dir / "segment-rocksdb-v1/records.segments"],
                                         root_pid=owner.target_pids.get("receiver"))
                save_json(case_dir / "syscalls.json", analysis)
                result["syscalls"].update(status=analysis["status"], report=str(case_dir / "syscalls.json"), summary=analysis.get("summary"))
                if analysis["status"] != "ok":
                    result.update(status="failed", diagnostic_error="syscall trace is incomplete/invalid; no complete cost claim")
            except Exception as exc:
                result["syscalls"].update(status="failed", error=str(exc))
                result.update(status="failed", diagnostic_error=f"cannot analyze syscall trace: {exc}")
        result["forced_cleanup"] = list(owner.forced)
        result["ended_unix_s"] = time.time()
        try:
            result["binary_sha256_after"] = sha256_file(binary)
            if result["binary_sha256_after"] != result["binary_sha256"]:
                result.update(status="failed", error="benchmark executable changed during the case")
                result["correctness"]["passed"] = False
        except OSError as exc:
            result.update(status="failed", error=f"cannot verify executable after case: {exc}")
            result["correctness"]["passed"] = False
        save_json(case_dir / "result.json", result)
    return result


def stage_summary(source, receiver):
    result = {"measurement": "wall-clock stage instrumentation, NOT CPU hotspot attribution",
              "caveat": "nested/overlapping timers are not additive; percentile bounds are per timed invocation"}
    for role, metrics in (("source", source), ("receiver", receiver)):
        records = counter(metrics, "source_sent" if role == "source" else "received")
        result[role] = {}
        for name, stage in metrics.get("metrics", {}).get("stages", {}).items():
            is_age = name.endswith(("_age", "_lag"))
            kind = "observed_source_lag" if name.endswith("_lag") else "observed_queue_age" if is_age else "timed_work"
            result[role][name] = {**stage, "measurement_kind": kind,
                                  "mean_ns_per_call": stage["sum_ns"] / stage["count"] if stage["count"] else None,
                                  "amortized_ns_per_input_record": stage["sum_ns"] / records if records and not is_age else None,
                                  "wall_fraction_of_role_elapsed": None if is_age else stage["sum_ns"] / (metrics["elapsed_s"] * 1e9)}
    return result


def save_results_csv(path, results):
    columns = ("case", "repeat", "warmup", "status", "directory", "records", "source_generated", "source_sent",
               "received", "parsed", "dedup_dropped", "forwarded", "committed_attempts", "sink_consumed",
               "expected_unique", "actual_unique", "physical_rows", "physical_duplicates", "ack_durable",
               "dedup_evictions", "dedup_entries", "dedup_payload_bytes", "dedup_cache_bytes",
               "source_send_elapsed_s", "source_ack_elapsed_s", "receiver_elapsed_s", "offered_records_per_s",
               "offered_unique_records_per_s", "actual_source_records_per_s", "durable_unique_records_per_s_including_drain",
               "end_to_end_durable_unique_records_per_s", "minimum_durable_records_per_s",
               "minimum_durable_rate_passed", "full_chain_eligible", "input_kind", "production_validation", "syscalls_status",
               "source_send_complete_lag_max_s", "source_planned_minus_sent_peak", "source_plan_gap_slope_records_per_s",
               "gap_slope_records_per_s", "source_commit_gap_slope_records_per_s", "verdict", "error", "diagnostic_error")
    with Path(path).open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        for result in results:
            correctness, oracle = result.get("correctness", {}), result.get("oracle", {})
            performance = result.get("performance", {})
            row = {key: result.get(key) for key in columns if key in result}
            row.update({key: performance.get(key) for key in columns if key in performance})
            row.update({key: correctness.get(key) for key in ("expected_unique", "ack_durable")})
            row.update({key: value for key, value in correctness.get("counters", {}).items() if key in columns})
            row.update({key: performance.get("dedup_cache", {}).get(key) for key in
                        ("dedup_evictions", "dedup_entries", "dedup_payload_bytes", "dedup_cache_bytes")})
            row["full_chain_eligible"] = performance.get("full_chain_contract", {}).get("eligible")
            origin = performance.get("input_source", result.get("input_source", {}))
            row.update(input_kind=origin.get("kind"), production_validation=origin.get("production_validation"),
                       syscalls_status=result.get("syscalls", {}).get("status"))
            schedule = performance.get("source_schedule", {})
            row.update(source_send_complete_lag_max_s=schedule.get("send_complete_lag_max_s"),
                       source_planned_minus_sent_peak=schedule.get("planned_minus_sent_peak"),
                       source_plan_gap_slope_records_per_s=schedule.get("planned_minus_sent_slope_records_per_s"))
            row.update({key: oracle.get(key) for key in ("actual_unique", "physical_rows", "physical_duplicates")})
            row.update({key: performance.get("backlog", {}).get(key) for key in
                        ("gap_slope_records_per_s", "source_commit_gap_slope_records_per_s")})
            row["records"] = result["config"]["records"]
            row["receiver_elapsed_s"] = result.get("receiver", {}).get("elapsed_s")
            writer.writerow(row)


def aggregate(results):
    groups = {}
    for result in results:
        if result.get("warmup"):
            continue
        key = result["case"]
        group = groups.setdefault(key, {"runs": 0, "correctness_failures": 0, "execution_failures": 0, "source_rates": [],
                                        "durable_unique_rates": [], "end_to_end_durable_unique_rates": [],
                                        "full_chain_eligible_runs": 0, "target_verdicts": []})
        group["runs"] += 1
        if result["status"] != "passed":
            group["execution_failures"] += 1
            group["correctness_failures"] += int(result.get("correctness", {}).get("passed") is not True)
            continue
        performance = result["performance"]
        group["source_rates"].append(performance["actual_source_records_per_s"])
        if performance["durable_unique_records_per_s_including_drain"] is not None:
            group["durable_unique_rates"].append(performance["durable_unique_records_per_s_including_drain"])
        if performance.get("end_to_end_durable_unique_records_per_s") is not None:
            group["end_to_end_durable_unique_rates"].append(performance["end_to_end_durable_unique_records_per_s"])
        group["full_chain_eligible_runs"] += int(performance.get("full_chain_contract", {}).get("eligible", False))
        group["target_verdicts"].append(performance["verdict"])
    for group in groups.values():
        for key in ("source_rates", "durable_unique_rates", "end_to_end_durable_unique_rates"):
            values = group[key]
            group[key + "_summary"] = ({"median": statistics.median(values), "min": min(values), "max": max(values)}
                                        if values else None)
    return groups


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--build", type=Path, default=ROOT / "build", help="build directory or pipeline_bench executable")
    result.add_argument("--suite", choices=("smoke", "diagnostic", "target"), default="smoke")
    result.add_argument("--output", type=Path, help="NEW artifact directory; required except --check-syscalls")
    result.add_argument("--config", help="flat config overrides JSON text or filename; runner owns paths/ports")
    result.add_argument("--capture", type=Path, help="explicit authorized JSON(L) payload file; preserve original bytes, never infer production authenticity")
    result.add_argument("--capture-source-kind", choices=("synthetic", "production_shape", "production_capture"))
    result.add_argument("--capture-provenance", default="", help="source/schema/transformation declaration; not independent proof of origin")
    result.add_argument("--capture-authorized", action="store_true", help="declare permission to use the supplied production capture/shape")
    result.add_argument("--syscalls", choices=("off", "trace"), default="off", help="Linux owned-process strace, separate observer run; no privilege escalation")
    result.add_argument("--check-syscalls", action="store_true", help="print tracing capability JSON and exit, without a benchmark")
    result.add_argument("--max-source-lag-ms", type=float, default=1000, help="target bound for planned send-completion lateness; default 1000ms")
    result.add_argument("--matrix", type=Path, help="JSON axes product or named cases, replacing built-in cells without code edits")
    result.add_argument("--records", type=int)
    result.add_argument("--record-limit", type=int, default=2000000, help="explicit bounded allowance for long soaks; at most 20000000, default 2000000")
    result.add_argument("--repeat", type=int)
    result.add_argument("--rate", type=int)
    result.add_argument("--min-durable-rps", type=float, default=0,
                        help="hard end-to-end completion floor plus full JSON/five-level/SHA/Redis/FULL contract (target only)")
    result.add_argument("--duration", type=float, default=10, help="target offered interval; records=ceil(rate*duration) unless explicit")
    result.add_argument("--seed", type=int, default=42)
    result.add_argument("--order-seed", type=int, default=20261113)
    result.add_argument("--profile", choices=("off", "always", "trigger", "after"), default="off")
    result.add_argument("--profile-trigger-s", type=float, default=2, help="minimum observation time for anomaly trigger; elapsed threshold for after mode")
    result.add_argument("--profile-min-rps", type=float, default=0, help="trigger completion rate threshold; 0 uses 90%% of target unique/attempt rate")
    result.add_argument("--profile-backlog-records", type=int, default=0, help="trigger source-aligned backlog threshold; 0 selects a conservative automatic bound")
    result.add_argument("--profile-commit-ms", type=float, default=100, help="trigger on a newly observed commit/file-sync max above this; 0 disables")
    result.add_argument("--profile-windows", type=int, default=2, help="distinct progress windows required for low-rate/backlog conditions")
    result.add_argument("--compare-profiles", action="store_true", help="add a full_profiled cell; full baseline stays off")
    result.add_argument("--include-normal", action="store_true", help="add explicitly weaker SQLite NORMAL durability cell")
    result.add_argument("--work-dir", type=Path, help="existing parent disk directory for UUID case data; kept after run")
    result.add_argument("--timeout", type=float, default=120, help="bounded measurement/export timeout per case")
    result.add_argument("--max-disk-mib", type=int, default=2048)
    result.add_argument("--reserve-free-mib", type=int, default=256)
    result.add_argument("--no-warmup", action="store_true", help="explicitly disclose no warmup rather than hide one")
    return result


def prepare_case_input(binary, case, config, fixtures, cache, *, args, stop, output, work):
    dataset = None
    if args.capture:
        from pipeline_bench_capture import prepare_capture
        if config["input"] != "json" or config["parse"] != "real" or case.get("source_mode") == "generated_cpp":
            raise RunError("capture requires raw JSON file replay and real parsing; generated/replay-bypass cells are not capture tests")
        if not hasattr(args, "dedup_policy"):
            checked = command_capture([binary, "--dedup-policy"])
            if checked.get("returncode") != 0:
                raise RunError("capture requires a rebuilt executable with --dedup-policy")
            args.dedup_policy = json.loads(checked["stdout"])
            if args.dedup_policy.get("schema_version") != 1:
                raise RunError("unsupported compiled dedup policy schema")
        policy = {**args.dedup_policy, "dedup": config["hash"] == "dedup", "max_entries": config["dedup_max_entries"],
                  "hash_algorithm": config["hash_algorithm"], "hash_mode": config["hash_mode"]}
        signature = ("capture", str(args.capture.resolve()), config["hash"], config["dedup_max_entries"], config["hash_algorithm"], config["hash_mode"])
        if signature not in cache:
            def budget():
                return enforce_budget([output, work], args.max_disk_mib * 1024 * 1024, args.reserve_free_mib * 1024 * 1024)
            budget()
            dataset = prepare_capture(args.capture, fixtures / ("capture-" + uuid.uuid4().hex),
                                      source_config={"kind": args.capture_source_kind, "provenance": args.capture_provenance,
                                                     "authorized": args.capture_authorized, "schema_profile": "experimental-ticks-v1"},
                                      policy=policy, check=stop.check, deadline=time.monotonic() + args.timeout, budget=budget,
                                      max_records=args.record_limit)
            cache[signature] = dataset
        dataset = cache[signature]
        info = {**dataset.input_info, "source_mode": "capture_original_json"}
        if (args.capture_expected_records is not None or "records" in case["overrides"]) and config["records"] != info["records"]:
            raise RunError("capture record count differs from --records/matrix; split the explicit input instead of silently truncating it")
        if not 1 <= info["records"] <= args.record_limit:
            raise RunError("capture exceeds the explicit record limit")
        manifest = read_json(Path(info["input_manifest"]))
        config["records"] = info["records"]
        config["batch_records"] = max(config["batch_records"], max(frame["records"] for frame in manifest["frames"]))
        config["input_file"] = info["path"]
        config["input_manifest"] = info["input_manifest"]
    else:
        signature = tuple(config[key] for key in ("input", "records", "symbols", "depth", "seed", "duplicate_every", "optional_fields", "batch_records"))
        if signature not in cache:
            fixture_path = fixtures / ("input-" + uuid.uuid4().hex + ".iqb")
            estimate = config["records"] * (2400 + config["depth"] * 240)
            enforce_budget([output, work], args.max_disk_mib * 1024 * 1024, args.reserve_free_mib * 1024 * 1024 + estimate)
            if tree_bytes([output, work]) + estimate > args.max_disk_mib * 1024 * 1024:
                raise RunError("estimated next case exceeds artifact budget; reduce records/repeats or explicitly raise --max-disk-mib")
            info = write_input(fixture_path, config, stop=stop)
            save_json(fixture_path.with_suffix(".json"), info)
            cache[signature] = info
        reference_info = cache[signature]
        config["input_manifest"] = ""
        if case.get("source_mode") == "generated_cpp":
            config["input_file"] = ""
            info = {key: value for key, value in reference_info.items() if key not in ("path", "sha256", "bytes")}
            info.update(source_mode="generated_cpp", canonical_reference_fixture=reference_info,
                        wire_identity="not-assumed-equal-to-Python-file; canonical records independently equal")
        else:
            config["input_file"] = reference_info["path"]
            info = {**reference_info, "source_mode": "file_replay"}
    config["output_file"] = ""
    return config, info, dataset


def main(argv=None):
    cli = parser()
    args = cli.parse_args(argv)
    if args.check_syscalls:
        capability = syscall_preflight()
        cleanup_supported = hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal")
        print(json.dumps({**asdict(capability), "runner_pidfd_cleanup_supported": cleanup_supported}, sort_keys=True))
        return 0 if capability.supported and cleanup_supported else 1
    try:
        if args.output is None:
            raise ValueError("--output is required for benchmark runs")
        if not math.isfinite(args.max_source_lag_ms) or args.max_source_lag_ms <= 0:
            raise ValueError("max-source-lag-ms must be finite positive")
        args.syscall_capability = syscall_preflight() if args.syscalls == "trace" else None
        if args.syscalls == "trace":
            if not args.syscall_capability.supported:
                raise ValueError(f"syscall tracing unavailable: {args.syscall_capability.reason}")
            if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
                raise ValueError("syscall tracing requires safe Linux pidfd process cleanup support")
        if args.capture:
            if not args.capture.is_file():
                raise ValueError("capture must be an explicitly supplied regular file")
            if not args.capture_source_kind:
                raise ValueError("--capture-source-kind is required; a file is not proof of production origin")
            if args.capture_source_kind != "synthetic" and not (args.capture_authorized and args.capture_provenance):
                raise ValueError("production capture/shape requires authorization and provenance declarations")
        elif args.capture_source_kind or args.capture_provenance or args.capture_authorized:
            raise ValueError("capture provenance flags require --capture")
        binary = binary_path(args.build)
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise ValueError(f"benchmark executable not found/executable: {binary}")
        if args.duration <= 0 or not math.isfinite(args.duration) or not 1 <= args.timeout <= 86400:
            raise ValueError("duration must be finite positive; timeout must be 1..86400")
        if (args.max_disk_mib < 16 or args.reserve_free_mib < 0 or
                any(not math.isfinite(v) or v < 0 for v in (args.profile_trigger_s, args.profile_min_rps, args.profile_commit_ms)) or
                args.profile_backlog_records < 0 or not 1 <= args.profile_windows <= 100):
            raise ValueError("invalid disk budget/reserve or profiling trigger")
        overrides = config_overrides(args.config)
        args.capture_expected_records = args.records if args.records is not None else overrides.get("records")
        custom_specs = load_matrix(args.matrix) if args.matrix else None
        if custom_specs and args.compare_profiles and ("full", {}) not in custom_specs:
            raise ValueError("custom --compare-profiles requires a full case with empty overrides")
        base = {**DEFAULTS, **overrides}
        if args.capture and (base["input"] != "json" or base["parse"] != "real"):
            raise ValueError("capture only supports original JSON with real parsing")
        if args.records is not None:
            base["records"] = args.records
        elif "records" not in overrides:
            base["records"] = 2160 if args.suite == "smoke" else 54000
        base["seed"] = args.seed if "seed" not in overrides or args.seed != 42 else overrides["seed"]
        if args.rate is not None:
            base["rate"] = args.rate
        elif args.suite == "target" and "rate" not in overrides:
            base["rate"] = 30000
        if args.suite == "target" and args.records is None and "records" not in overrides:
            base["records"] = math.ceil(base["rate"] * args.duration)
        if args.suite == "target" and base["rate"] <= 0:
            raise ValueError("target suite requires a positive offered rate")
        if not math.isfinite(args.min_durable_rps) or args.min_durable_rps < 0:
            raise ValueError("min-durable-rps must be finite and nonnegative")
        if args.min_durable_rps and (args.suite != "target" or base["rate"] < args.min_durable_rps):
            raise ValueError("min-durable-rps requires target suite with offered rate >= capacity floor")
        if not 1 <= args.record_limit <= 20000000 or (not args.capture and not 1 <= base["records"] <= args.record_limit):
            raise ValueError("records exceed --record-limit (default 2000000); explicitly raise up to 20000000 with an appropriate disk budget for long soaks")
        if not 1 <= base["symbols"] <= 50000 or not 0 <= base["depth"] <= 10 or not 1 <= base["batch_records"] <= 8192:
            raise ValueError("invalid symbol/depth/batch bounds")
        if base["duplicate_every"] < 0 or base["duplicate_every"] == 1:
            raise ValueError("duplicate_every must be zero or >=2")
        if base["dedup_max_entries"] <= 0:
            raise ValueError("dedup_max_entries must be positive")
        base["timeout_s"] = math.ceil(args.timeout)
        repeats = args.repeat if args.repeat is not None else (3 if args.suite == "diagnostic" else 1)
        if not 1 <= repeats <= 20:
            raise ValueError("repeat must be 1..20")
        if custom_specs and (len(custom_specs) + int(args.compare_profiles) + int(args.include_normal)) * repeats > 256:
            raise ValueError("custom matrix exceeds 256 case/repeat combinations")
        output = args.output.expanduser().absolute()
        if output.exists() or output.is_symlink():
            raise ValueError(f"refusing existing output: {output}")
        if not output.parent.is_dir():
            raise ValueError(f"output parent must exist: {output.parent}")
        work_parent = args.work_dir.expanduser().resolve() if args.work_dir else output.parent.resolve()
        if not work_parent.is_dir():
            raise ValueError(f"work parent must already be a directory: {work_parent}")
        output.mkdir(mode=0o700)
    except (OSError, ValueError, TypeError) as exc:
        cli.error(str(exc))

    results = []
    work = output / "cases" if not args.work_dir else work_parent / ("pipeline-bench-" + uuid.uuid4().hex)
    summary = {"schema_version": 2, "suite": args.suite, "binary": str(binary),
               "output": str(output.resolve()), "work_directory": str(work.resolve()),
               "repeat": repeats, "order_seed": args.order_seed, "record_limit": args.record_limit,
               "minimum_durable_records_per_s": args.min_durable_rps,
               "warmup_policy": ("capture-no-warmup; original input is not truncated or looped" if args.capture else
                                 "explicit-no-warmup" if args.no_warmup else "separate-unscored-full-case-up-to-540-records"),
               "input_source_kind": args.capture_source_kind if args.capture else "synthetic", "production_qualified": False,
               "production_validation": ("user-attested capture replay only; not live production certification"
                                         if args.capture_source_kind == "production_capture" else "not-performed; no production capture supplied"),
               "syscalls_mode": args.syscalls, "max_source_lag_ms": args.max_source_lag_ms,
               "disk_budget_bytes": args.max_disk_mib * 1024 * 1024,
               "reserve_free_bytes": args.reserve_free_mib * 1024 * 1024,
               "status": "running", "results": results,
               "limitations": ["Experimental IQB1 transport, not an official iQuant wire format.",
                               "No physical power-loss proof; ACK durable is backend sync completion only.",
                               "Sampling requires inspecting saved artifacts before claiming CPU hotspots.",
                               "Source offered schedule is not actual receiver capacity; finite target observations only."]}
    try:
        summary["startup_directory_sync"] = sync_created_directory(output)
        work.mkdir(mode=0o700)
        summary["startup_directory_sync"].extend(sync_created_directory(work))
        save_json(output / "environment.json", environment(binary, output, work))
        save_json(output / "summary.json", summary)
        specs = list(custom_specs if custom_specs is not None else
                     DIAGNOSTIC[:1] if args.capture else
                     SMOKE if args.suite == "smoke" else DIAGNOSTIC if args.suite == "diagnostic" else DIAGNOSTIC[:1])
        summary["matrix"] = {"path": str(args.matrix.resolve()), "sha256": sha256_file(args.matrix)} if args.matrix else "built-in"
        if args.include_normal:
            specs.append(("sqlite_normal_weaker_durability", {"synchronous": "NORMAL"}))
        if args.compare_profiles:
            specs.append(("full_profiled", {}))
        if len({name for name, _ in specs}) != len(specs):
            raise RunError("matrix case names collide with an added normal/profile comparison")
        if args.capture and any(name == "generated_json_source" or changes.get("input", base["input"]) != "json" or
                                changes.get("parse", base["parse"]) != "real" for name, changes in specs):
            raise RunError("capture matrix cannot regenerate, use binary input, or bypass original JSON parsing")
        cases = [{"name": name, "repeat": repeat + 1, "overrides": changes,
                  "source_mode": "generated_cpp" if name == "generated_json_source" else "file_replay",
                  "profile": "always" if name == "full_profiled" else "off" if args.compare_profiles and name == "full" else args.profile}
                 for repeat in range(repeats) for name, changes in specs]
        random.Random(args.order_seed).shuffle(cases)
        if not args.no_warmup and not args.capture:
            cases.insert(0, {"name": "warmup_full", "repeat": 0, "warmup": True, "profile": "off",
                             "overrides": {"records": min(540, base["records"]), "rate": 0, "parse": "real"}})
        save_json(output / "schedule.json", cases)
        print(f"input_source_kind={summary['input_source_kind']}; production_qualified=false; origin declarations are not production certification", flush=True)
        fixtures = output / "inputs"
        fixtures.mkdir(mode=0o700)
        input_cache = {}
        with StopFlag() as stop:
            for case in cases:
                stop.check()
                config = {**base, **case["overrides"]}
                if not args.capture and not 1 <= config["records"] <= args.record_limit:
                    raise RunError("matrix records exceed the explicit record limit")
                if args.suite == "target" and not case.get("warmup") and config["rate"] <= 0:
                    raise RunError("every target matrix cell requires a positive offered rate")
                if args.min_durable_rps and not case.get("warmup") and config["rate"] < args.min_durable_rps:
                    raise RunError("target matrix offered rate is below the requested capacity floor")
                # Validate each combination before generating any potentially large fixture.
                preflight_path = output / ("preflight-" + uuid.uuid4().hex + ".json")
                save_json(preflight_path, {**config, "directory": str(work / "validation-only")})
                checked = command_capture([binary, "--config", preflight_path, "--check-config"])
                if checked.get("returncode") != 0:
                    raise RunError("matrix cell config rejected before fixture generation: " + checked.get("stderr", checked.get("error", "unknown")))
                config, info, dataset = prepare_case_input(binary, case, config, fixtures, input_cache,
                                                          args=args, stop=stop, output=output, work=work)
                case_dir = work / (case["name"] + f"-r{case['repeat']}-" + uuid.uuid4().hex)
                result = run_case(binary, case, config, case_dir, info, stop=stop, args=args, output=output, work=work, dataset=dataset)
                results.append(result)
                summary["groups"] = aggregate(results)
                save_json(output / "summary.json", summary)
                save_results_csv(output / "results.csv", results)
                print(f"{case['name']} repeat={case['repeat']} {result['status']} {case_dir}", flush=True)
                if result.get("interrupted"):
                    raise InterruptedError("stop requested")
                enforce_budget([output, work], args.max_disk_mib * 1024 * 1024, args.reserve_free_mib * 1024 * 1024)
        summary["correctness_passed"] = bool(results) and all(result.get("correctness", {}).get("passed") is True for result in results)
        summary["execution_passed"] = bool(results) and all(result["status"] == "passed" for result in results)
        summary["diagnostics_passed"] = all(result.get("syscalls", {}).get("status") in ("ok", "not-requested") for result in results)
        target_results = [result for result in results if not result.get("warmup")]
        summary["target_passed"] = (all(result.get("performance", {}).get("verdict") in ("pass-bounded-target-observation", "pass-full-chain-capacity")
                                        for result in target_results) if args.suite == "target" else None)
        summary["status"] = ("passed" if summary["correctness_passed"] and summary["execution_passed"] and
                             summary["diagnostics_passed"] and summary["target_passed"] is not False else "failed")
    except (Exception, KeyboardInterrupt) as exc:
        summary.update(status="failed", error=f"{type(exc).__name__}: {exc}")
    finally:
        summary["groups"] = aggregate(results)
        summary["ended_unix_s"] = time.time()
        save_json(output / "summary.json", summary)
        save_results_csv(output / "results.csv", results)
    if summary["status"] != "passed":
        print(f"benchmark failed; artifacts retained at {output / 'summary.json'}", file=sys.stderr)
        return 1
    print(f"correctness passed; input_source_kind={summary['input_source_kind']}; production_qualified=false; performance verdicts: {output / 'summary.json'}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
