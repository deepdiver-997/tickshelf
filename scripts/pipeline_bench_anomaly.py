"""Pure, timestamped gates for capturing a slow run without reproducing it.

A trigger captures the *remaining* live run. It cannot reconstruct a stack from
an already completed transient. Rare pre-event stacks require a separate
continuous/ring-buffer profiler, not this tool's bounded two-second samples.
"""
from __future__ import annotations


class AnomalyTrigger:
    def __init__(self, *, minimum_elapsed_s=2, minimum_records_per_s=0,
                 maximum_backlog_records=4000, maximum_commit_ms=100,
                 sustained_windows=2, backend="sqlite"):
        self.minimum_elapsed_s = minimum_elapsed_s
        self.minimum_records_per_s = minimum_records_per_s
        self.maximum_backlog_records = maximum_backlog_records
        self.maximum_commit_ns = maximum_commit_ms * 1_000_000
        self.sustained_windows = sustained_windows
        self.backend = backend
        self.previous = None
        self.previous_commit_max = 0
        self.low_rate_windows = self.backlog_windows = 0
        self.last_evidence = None

    def observe(self, elapsed_s, receiver, source=None):
        if not receiver:
            return None
        timestamp = receiver.get("monotonic_ns")
        counters = receiver.get("metrics", {}).get("counters", {})
        terminal = "sink_consumed" if self.backend == "null" else "committed_attempts"
        committed = counters.get(terminal)
        if type(timestamp) is not int or type(committed) is not int:
            return None
        if self.previous is not None and timestamp <= self.previous[0]:
            return None  # Re-reading one progress point is not a sustained window.
        interval_rate = None
        if self.previous is not None:
            dt = (timestamp - self.previous[0]) / 1e9
            interval_rate = (committed - self.previous[1]) / dt
        self.previous = (timestamp, committed)
        stages = receiver.get("metrics", {}).get("stages", {})
        commit_max = max((stages.get(name, {}).get("max_ns", 0)
                          for name in ("sqlite_commit", "file_sync")), default=0)
        new_commit_peak = commit_max > self.previous_commit_max
        previous_peak = self.previous_commit_max
        self.previous_commit_max = max(commit_max, previous_peak)
        source_gap = None
        if source and type(source.get("monotonic_ns")) is int and source["monotonic_ns"] <= timestamp:
            sent = source.get("metrics", {}).get("counters", {}).get("source_sent")
            if type(sent) is int:
                source_gap = max(0, sent - committed - counters.get("dedup_dropped", 0))
        if elapsed_s < self.minimum_elapsed_s:
            return None
        self.low_rate_windows = (self.low_rate_windows + 1 if self.minimum_records_per_s > 0
                                 and interval_rate is not None and interval_rate < self.minimum_records_per_s else 0)
        self.backlog_windows = (self.backlog_windows + 1 if self.maximum_backlog_records > 0
                               and source_gap is not None and source_gap > self.maximum_backlog_records else 0)
        reasons = []
        if self.low_rate_windows >= self.sustained_windows:
            reasons.append("sustained-low-completion-rate")
        if self.backlog_windows >= self.sustained_windows:
            reasons.append("sustained-source-to-commit-backlog")
        if self.maximum_commit_ns > 0 and new_commit_peak and commit_max > self.maximum_commit_ns:
            reasons.append("new-commit-or-file-sync-latency-peak")
        self.last_evidence = {"receiver_monotonic_ns": timestamp, "measurement_elapsed_s": elapsed_s,
                              "completion_counter": terminal, "completion_records_per_s": interval_rate,
                              "source_aligned_backlog_records_lower_bound": source_gap,
                              "low_rate_windows": self.low_rate_windows, "backlog_windows": self.backlog_windows,
                              "new_commit_or_file_sync_max_ns": commit_max if new_commit_peak else None,
                              "previous_commit_or_file_sync_max_ns": previous_peak,
                              "thresholds": {"minimum_elapsed_s": self.minimum_elapsed_s,
                                             "minimum_records_per_s": self.minimum_records_per_s,
                                             "maximum_backlog_records": self.maximum_backlog_records,
                                             "maximum_commit_ms": self.maximum_commit_ns / 1e6,
                                             "sustained_windows": self.sustained_windows},
                              "reasons": reasons}
        return self.last_evidence if reasons else None
