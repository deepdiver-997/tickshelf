"""Logical ablation cells and evidence-based projections of real pipeline runs.

P/H/D/R/S describe ingress work, not everything the storage engine does. The
same descriptor feeds JSON/CSV/Markdown; no separate execution or oracle lives
here. A bypass row is never a full-chain capacity certification.
"""
from __future__ import annotations

import csv
import itertools
import json
import math
import statistics
from pathlib import Path

CONTRACT_ID = "tickshelf-delivery-v1-20261117"
PROFILE = "JSON/IQB1/RCD1"
BITS = ("P", "H", "D", "R", "S")
REAL_BACKENDS = ("sqlite", "sqlite_compact", "rocksdb", "segment", "segment_rocksdb")


def truth_cells(base):
    """All 32 logical rows; only the 24 legal configurations are executable."""
    backend = base.get("backend", "sqlite")
    if backend not in REAL_BACKENDS:
        raise ValueError("truth-table requires one real backend as the fixed storage profile")
    if base.get("input", "json") != "json" or base.get("hash_algorithm", "sha256") != "sha256":
        raise ValueError("truth-table profile requires JSON and SHA-256")
    if base.get("synchronous", "FULL") != "FULL" or base.get("store_lookup", "exact") != "exact":
        raise ValueError("truth-table profile requires FULL and exact backend lookup; compare diagnostic lookup separately")
    cells = []
    for values in itertools.product((0, 1), repeat=5):
        p, h, d, r, s = values
        bits = dict(zip(BITS, values))
        valid = not (d and not h)
        changes = {
            "input": "json", "parse": "real" if p else "replay",
            "hash": "dedup" if d else "only" if h else "off",
            "redis": "roundtrip" if r else "off", "backend": backend if s else "null",
            "store_lookup": "exact",
        }
        if not s:
            changes["sqlite_compact_lookup"] = "subquery"
        cells.append({
            "case": "".join(f"{name.lower()}{bit}" for name, bit in bits.items()),
            **bits, "valid": valid,
            "invalid_reason": "FIFO lookup requires ingress digest computation (D=1,H=0 is not implemented)" if not valid else None,
            "profile": PROFILE, "storage_profile": backend,
            "overrides": changes if valid else None,
            "measurement_class": "full-path-observation" if all(values) else "ablation",
            "capacity_certified": False,
        })
    return cells


def truth_specs(base):
    return [(cell["case"], cell["overrides"]) for cell in truth_cells(base) if cell["valid"]]


def effective_work(config, receiver):
    """Keep configured work, observed counts and disagreements separate.

    Missing evidence is unknown, not zero. In particular hash=off may still
    cause SHA fallback at compact SQLite / full-record RocksDB.
    """
    metrics = receiver.get("metrics", {})
    counters = metrics.get("counters", {})
    stages = metrics.get("stages", {})
    backend = config.get("backend")
    schema = counters.get("ablation_work_schema")
    received, forwarded = counters.get("received"), counters.get("forwarded")
    network = config.get("redis") in ("publish", "roundtrip")
    lookup = config.get("store_lookup", "exact")
    configured = {
        "parse": config.get("parse"), "canonical_encode": True,
        "ingress_hash": config.get("hash") != "off",
        "ingress_fifo_lookup": config.get("hash") == "dedup",
        "redis_build": config.get("redis") != "off", "redis_network": network,
        "redis_readback": config.get("redis") == "roundtrip",
        "storage_commit": backend != "null", "backend": backend,
        "backend_lookup_policy": lookup,
        "backend_digest_required": backend in ("sqlite_compact", "rocksdb"),
        "database_internal_uniqueness_checks": backend == "sqlite",
        "lease_arbitration": "not-part-of-this-experimental-pipeline",
    }
    names = (
        "record_encoded_records", "hash_invocations", "hash_bypassed_records",
        "dedup_lookup_records", "dedup_candidate_comparisons", "dedup_dropped",
        "redis_build_records", "redis_publish_records", "redis_readback_records",
        "redis_network_execs", "redis_commands", "redis_cleanup_batches",
        "backend_digest_recomputed", "backend_digest_cached", "backend_lookup_calls",
        "backend_candidates", "backend_inserted", "backend_ignored",
        "backend_lookup_exact", "backend_lookup_assume_unique",
        "sqlite_step_calls", "sqlite_lookup_step_calls", "rocksdb_seek_calls",
        "rocksdb_write_calls", "pipeline_transactions", "committed_attempts",
        "sink_consumed", "null_consumed_rows", "null_consumed_bytes", "canonical_bytes",
        "transaction_rows_min", "transaction_rows_max", "transaction_rows_sum",
        "transaction_bytes_min", "transaction_bytes_max", "transaction_bytes_sum",
    )
    observed = {name: counters.get(name) for name in names}
    observed["backend_candidates_observable"] = counters.get("backend_candidates_observable")
    if observed["backend_candidates_observable"] == 0:
        observed["backend_candidates"] = None
    # Legacy metrics omit zero-valued hash calls. Under work schema 1 a missing
    # legacy counter is known zero; newly introduced evidence must be explicit.
    if schema == 1:
        for name in ("hash_invocations", "redis_commands"):
            observed[name] = counters.get(name, 0)
    observed["parse_calls"] = stages.get("json_parse_normalize", {}).get("count")
    observed["replay_copy_calls"] = stages.get("replay_copy", {}).get("count")
    observed["store"] = receiver.get("store", {})
    mismatches = []
    if schema == 1:
        for name, value in (("received", received), ("forwarded", forwarded),
                            ("dedup_dropped", counters.get("dedup_dropped"))):
            if type(value) is not int or value < 0:
                mismatches.append({"counter": name, "expected": "nonnegative native integer", "actual": value})
        if received != config.get("records"):
            mismatches.append({"counter": "received", "expected": config.get("records"), "actual": received})
    if schema == 1 and type(received) is int and type(forwarded) is int:
        dropped = counters.get("dedup_dropped")
        if type(dropped) is int and (forwarded + dropped != received or
                                    (not configured["ingress_fifo_lookup"] and dropped != 0)):
            mismatches.append({"counter": "forwarded-plus-dropped", "expected": received, "actual": forwarded + dropped})
        expected = {
            "record_encoded_records": received,
            "hash_invocations": received if configured["ingress_hash"] else 0,
            "hash_bypassed_records": 0 if configured["ingress_hash"] else received,
            "dedup_lookup_records": received if configured["ingress_fifo_lookup"] else 0,
            "redis_build_records": forwarded if configured["redis_build"] else 0,
            "redis_publish_records": forwarded if network else 0,
            "redis_readback_records": forwarded if configured["redis_readback"] else 0,
            "committed_attempts": forwarded if backend != "null" else 0,
            "sink_consumed": forwarded if backend == "null" else 0,
        }
        for name, count in expected.items():
            if observed.get(name) != count:
                mismatches.append({"counter": name, "expected": count, "actual": observed.get(name)})
        if not network and observed["redis_network_execs"] != 0:
            mismatches.append({"counter": "redis_network_execs", "expected": 0, "actual": observed["redis_network_execs"]})
        if configured["backend_digest_required"]:
            hashes = (observed["backend_digest_recomputed"], observed["backend_digest_cached"])
            if any(type(value) is not int for value in hashes) or sum(hashes) != forwarded:
                mismatches.append({"counter": "backend_digest_total", "expected": forwarded, "actual": hashes})
            expected_flags = {"backend_lookup_exact": int(lookup == "exact"),
                              "backend_lookup_assume_unique": int(lookup == "assume_unique")}
            for name, value in expected_flags.items():
                if observed.get(name) != value:
                    mismatches.append({"counter": name, "expected": value, "actual": observed.get(name)})
            calls = observed["backend_lookup_calls"]
            if lookup == "assume_unique":
                if calls != 0:
                    mismatches.append({"counter": "backend_lookup_calls", "expected": 0, "actual": calls})
            elif backend == "sqlite_compact":
                if calls != forwarded:
                    mismatches.append({"counter": "backend_lookup_calls", "expected": forwarded, "actual": calls})
            elif type(calls) is not int or not (1 <= calls <= forwarded if forwarded else calls == 0):
                mismatches.append({"counter": "backend_lookup_calls", "expected": "one persisted lookup per distinct digest per nonempty batch", "actual": calls})
        if config.get("input") == "json":
            active = "parse_calls" if config.get("parse") == "real" else "replay_copy_calls"
            inactive = "replay_copy_calls" if active == "parse_calls" else "parse_calls"
            if not observed.get(active) or observed.get(inactive, 0) not in (None, 0):
                mismatches.append({"counter": "parse_path", "expected": active, "actual": [observed.get(active), observed.get(inactive)]})
    status = "failed" if mismatches else "verified" if schema == 1 else "unverified-missing-native-work-counters"
    return {
        "schema_version": 1, "status": status, "configured": configured,
        "observed": observed, "mismatches": mismatches,
        "scope": "workload counters only; SQLite VM internal B-tree probes, kernel syscalls and hidden engine work are not enumerated by these API counts",
        "residual_work": [
            "parse replay still validates payload equality, copies decoded records and encodes canonical bytes",
            "hash off affects ingress only; storage may compute SHA and perform exact lookup",
            "Redis off retains stage queues, batching and handoff",
            "null sink validates and checksums bytes; it does not persist data",
        ],
    }


def describe_run(config, receiver):
    dimensions = {
        "P": int(config.get("parse") == "real" and config.get("input") == "json"),
        "H": int(config.get("hash") != "off"), "D": int(config.get("hash") == "dedup"),
        "R": int(config.get("redis") in ("publish", "roundtrip")),
        "S": int(config.get("backend") != "null"),
    }
    intact = (all(dimensions.values()) and config.get("redis") == "roundtrip"
              and config.get("store_lookup", "exact") == "exact")
    return {
        "profile": PROFILE, **dimensions,
        "measurement_class": "full-path-observation" if intact else "ablation",
        "delivery_contract": CONTRACT_ID, "dcrb_compatibility": "not-adapted",
        "production_qualified": False, "capacity_certified": False,
        "effective_work": effective_work(config, receiver),
    }


def _summary(values):
    valid = [value for value in values if type(value) in (int, float) and math.isfinite(value)]
    return {"median": statistics.median(valid), "min": min(valid), "max": max(valid)} if valid else None


def build_truth_report(base, results, expected_repeats):
    grouped = {}
    measured = [result for result in results if not result.get("warmup")]
    for result in measured:
        grouped.setdefault(result["case"], []).append(result)
    hashes = {result.get("input", {}).get("sha256") for result in measured}
    input_errors = []
    if measured and (None in hashes or len(hashes) != 1):
        input_errors.append("all measured cells/repeats must reference the same controlled input SHA-256")
    owned_keys = {"directory", "host", "port", "redis_host", "redis_port", "input_file", "input_manifest", "output_file"}
    rows = []
    for cell in truth_cells(base):
        row = {key: value for key, value in cell.items() if key != "overrides"}
        runs = grouped.get(cell["case"], [])
        good, evidence_errors = [], []
        expected = {**base, **(cell["overrides"] or {})}
        for run in runs:
            errors = list(input_errors)
            config = run.get("config", {})
            for key, value in expected.items():
                if key not in owned_keys and config.get(key) != value:
                    errors.append(f"config disagrees with logical cell: {key}")
            if run.get("input", {}).get("records") != config.get("records"):
                errors.append("input record count disagrees with effective config")
            if run.get("input_source", {}).get("kind") != "synthetic":
                errors.append("truth-table requires the declared synthetic profile")
            if effective_work(config, run.get("receiver", {}))["status"] != "verified":
                errors.append("raw native work evidence is not verified")
            if run.get("ablation", {}).get("effective_work", {}).get("status") != "verified":
                errors.append("recorded work evidence is not verified")
            if errors:
                evidence_errors.append({"repeat": run.get("repeat"), "reasons": errors})
            if not errors and run.get("status") == "passed" and run.get("correctness", {}).get("passed") is True:
                good.append(run)
        repeat_ids = [run.get("repeat") for run in runs]
        repeats_complete = (all(type(value) is int for value in repeat_ids) and
                            sorted(repeat_ids) == list(range(1, expected_repeats + 1)))
        row.update(
            status="invalid" if not cell["valid"] else "not-run" if not runs else
                   "passed" if len(good) == expected_repeats and repeats_complete else "failed-or-partial",
            evidence_errors=evidence_errors,
            runs=len(runs), passed_runs=len(good), expected_runs=expected_repeats if cell["valid"] else 0,
            artifact_directories=[run.get("directory") for run in runs],
            errors=[run["error"] for run in runs if run.get("error")],
            source_kinds=sorted({run.get("input_source", {}).get("kind", "unknown") for run in runs}),
            input_sha256=sorted({run.get("input", {}).get("sha256") for run in runs if run.get("input", {}).get("sha256")}),
            effective_work=[run.get("ablation", {}).get("effective_work") for run in runs],
        )
        metrics = {
            "completed_input_records_per_s": "end_to_end_input_records_per_s",
            "committed_unique_records_per_s": "end_to_end_durable_unique_records_per_s",
            "source_send_s": "source_send_elapsed_s", "source_ack_s": "source_ack_elapsed_s",
            "ack_tail_s": "ack_tail_after_send_s", "raw_payload_bytes_per_s": "raw_payload_bytes_per_s",
            "wire_bytes_per_s": "wire_bytes_per_s", "canonical_bytes_per_s": "canonical_bytes_per_s",
        }
        for output, key in metrics.items():
            row[output] = _summary([run.get("performance", {}).get(key) for run in good])
        for name in ("record_encoded_records", "hash_invocations", "dedup_lookup_records",
                     "redis_publish_records", "redis_readback_records", "backend_digest_recomputed",
                     "backend_digest_cached", "backend_lookup_calls", "backend_candidates",
                     "backend_inserted", "backend_ignored", "sink_consumed", "pipeline_transactions"):
            row[name] = _summary([run.get("ablation", {}).get("effective_work", {}).get("observed", {}).get(name)
                                  for run in good])
        row["full_path_eligible"] = bool(good) and all(run.get("performance", {}).get("full_chain_contract", {}).get("eligible") for run in good)
        row["capacity_certified"] = False  # finite ablation is not a target capacity run
        row["workload_details"] = [{
            "repeat": run.get("repeat"), "receiver_elapsed_s": run.get("receiver", {}).get("elapsed_s"),
            "input_records": run.get("config", {}).get("records"),
            "physical_rows": run.get("oracle", {}).get("physical_rows"),
            "actual_unique": run.get("oracle", {}).get("actual_unique"),
            "backlog": run.get("performance", {}).get("backlog"),
            "source_schedule": run.get("performance", {}).get("source_schedule"),
            "stages": run.get("stage_summary"),
        } for run in runs]
        rows.append(row)
    by_bits = {tuple(row[bit] for bit in BITS): row for row in rows}
    pairs = []
    for bits, enabled in by_bits.items():
        if enabled["status"] != "passed":
            continue
        for index, axis in enumerate(BITS):
            if not bits[index]:
                continue
            disabled_bits = (*bits[:index], 0, *bits[index + 1:])
            disabled = by_bits[disabled_bits]
            if disabled["status"] != "passed":
                continue
            on = enabled["completed_input_records_per_s"]["median"]
            off = disabled["completed_input_records_per_s"]["median"]
            pairs.append({"axis": axis, "enabled_case": enabled["case"], "disabled_case": disabled["case"],
                          "observed_completion_ratio_on_over_off": on / off if off else None,
                          "interpretation": "single ingress switch difference; dependent backend work may change; ratios are not additive component costs or confidence intervals"})
    return {
        "schema": "tickshelf.rcd1-truth-table.v1", "profile": PROFILE,
        "delivery_contract": CONTRACT_ID, "storage_profile": base.get("backend"),
        "input_source_kind": "synthetic", "production_qualified": False,
        "dcrb_compatibility": "not-adapted", "logical_rows": 32, "legal_rows": 24,
        "expected_repeats": expected_repeats, "single_switch_comparisons": pairs,
        "input_coherence_errors": input_errors,
        "all_legal_rows_passed": all(row["status"] == "passed" for row in rows if row["valid"]),
        "interpretation": "Measured completion rates at this input size and hardware, not theoretical limits or proof of saturation. Do not add ablation speedups or compare a consume-only row as durable throughput.",
        "rows": rows,
    }


def write_truth_report(directory, base, results, expected_repeats):
    directory = Path(directory)
    report = build_truth_report(base, results, expected_repeats)
    (directory / "truth-table.json").write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n")
    scalar_columns = ("case", *BITS, "valid", "status", "storage_profile", "measurement_class", "runs", "passed_runs",
                      "full_path_eligible", "capacity_certified", "invalid_reason")
    numeric_columns = ("completed_input_records_per_s", "committed_unique_records_per_s", "canonical_bytes_per_s",
                       "raw_payload_bytes_per_s", "wire_bytes_per_s", "source_send_s", "source_ack_s", "ack_tail_s",
                       "hash_invocations", "dedup_lookup_records", "redis_publish_records", "redis_readback_records",
                       "backend_digest_recomputed", "backend_digest_cached", "backend_lookup_calls", "backend_candidates",
                       "backend_inserted", "backend_ignored", "sink_consumed", "pipeline_transactions")
    columns = (*scalar_columns, *(f"{key}_{stat}" for key in numeric_columns for stat in ("min", "median", "max")))
    with (directory / "truth-table.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        for row in report["rows"]:
            values = {key: row.get(key) for key in scalar_columns}
            for key in numeric_columns:
                for stat in ("min", "median", "max"):
                    values[f"{key}_{stat}"] = row[key][stat] if row.get(key) is not None else None
            writer.writerow(values)
    def number(row, key):
        value = row.get(key)
        return f"{value['median']:,.1f}" if value is not None else "—"
    def backend_hash(row):
        evidence = row.get("effective_work") or []
        if not evidence:
            return "—"
        values = [work.get("observed", {}).get("backend_digest_recomputed") for work in evidence if work]
        return str(values[0]) if values and len(set(values)) == 1 and values[0] is not None else "见 JSON"
    lines = [
        f"# 原生消融组合表：{PROFILE} / {report['storage_profile']}", "",
        "**实测观察，不是理论极限；合成输入，production_qualified=false，DCRB 尚未适配。**", "",
        "P=真实JSON解析，H=入口摘要，D=入口FIFO查重，R=Redis真实往返，S=真实存储。",
        "0 只表示指定工作被短路；复制、canonical编码、排队、后端摘要和索引等剩余成本仍存在。",
        "D=1/H=0 的8格非法，不执行；其余24格使用同一原生实现与独立oracle。", "",
        "| P | H | D | R | S | 状态 | 完成输入条/s 中位数 | 唯一提交条/s 中位数 | 后端重算SHA/轮 | 完整路径资格 |",
        "|---:|---:|---:|---:|---:|---|---:|---:|---:|---|",
    ]
    for row in report["rows"]:
        lines.append("| " + " | ".join([
            *(str(row[bit]) for bit in BITS), row["status"],
            number(row, "completed_input_records_per_s"), number(row, "committed_unique_records_per_s"),
            backend_hash(row), "是（非容量认证）" if row["full_path_eligible"] else "否",
        ]) + " |")
    lines += ["", "JSON/CSV 保留 min/median/max、每轮实际工作计数、事务/阶段/源积压、输入SHA和现场路径。",
              "Null 行只代表消费完成；无法获得持久化吞吐。即使全1格通过，此短跑也不证明长期容量或生产代表性。", ""]
    (directory / "truth-table.md").write_text("\n".join(lines))
    return report
