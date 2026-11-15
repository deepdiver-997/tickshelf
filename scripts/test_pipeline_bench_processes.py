#!/usr/bin/env python3
"""Owned-process cleanup checks; no attachment to existing processes."""
import io
import signal
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from pipeline_bench_processes import BenchmarkProcesses
from stack_runtime import wait_until


class ProcessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="tickshelf-owned-process-test-")
        self.addCleanup(self.temp.cleanup)
        self.owner = BenchmarkProcesses(Path(self.temp.name) / "processes")

    def test_untraced_readiness_uses_its_owned_child_pid(self):
        self.addCleanup(self.owner.close)
        child = self.owner.start("receiver", [sys.executable, "-I", "-c",
            "import os,time; print(f'BENCH_READY pid={os.getpid()}',flush=True); time.sleep(30)"])
        wait_until(lambda: self.owner.receiver_ready(Path(sys.executable)), timeout=5,
                   processes=self.owner, names=("receiver",))
        self.assertEqual(self.owner.process_pid("receiver"), child.pid)

    def test_application_pid_is_separate_from_supervisor_pid(self):
        self.owner.children["receiver"] = SimpleNamespace(pid=101)
        self.owner.target_pids["receiver"] = 202
        self.assertEqual(self.owner.process_pid("receiver"), 202)
        self.assertEqual(self.owner.children["receiver"].pid, 101)

    def test_traced_cleanup_signals_owned_group_while_leader_is_unreaped(self):
        child = Mock(pid=123456)
        child.poll.return_value = None
        child.wait.return_value = 0
        child.returncode = 0
        self.owner.children["receiver"] = child
        self.owner.logs["receiver"] = io.BytesIO()
        self.owner.traced.add("receiver")
        with patch("pipeline_bench_processes.os.killpg") as kill:
            self.assertEqual(self.owner.stop("receiver"), 0)
        kill.assert_called_once_with(child.pid, signal.SIGINT)
        self.assertTrue(self.owner.logs["receiver"].closed)

    def test_group_timeout_escalates_only_the_original_unreaped_leader(self):
        child = Mock(pid=123456)
        child.poll.return_value = None
        child.wait.side_effect = [subprocess.TimeoutExpired("synthetic mock", 1), 0]
        child.returncode = -9
        self.owner.children["receiver"] = child
        self.owner.logs["receiver"] = io.BytesIO()
        self.owner.traced.add("receiver")
        with patch("pipeline_bench_processes.os.killpg") as kill:
            self.owner.stop("receiver", timeout=1)
        self.assertEqual([call.args for call in kill.call_args_list],
                         [(child.pid, signal.SIGINT), (child.pid, signal.SIGKILL)])
        self.assertEqual(self.owner.forced, ["receiver"])

    def test_reaped_tracer_pid_is_never_used_for_group_termination(self):
        child = Mock(pid=123456)
        child.poll.return_value = 0
        child.returncode = 0
        self.owner.children["receiver"] = child
        self.owner.logs["receiver"] = io.BytesIO()
        self.owner.traced.add("receiver")
        with patch("pipeline_bench_processes.os.killpg") as kill:
            self.owner.stop("receiver")
        kill.assert_not_called()


if __name__ == "__main__":
    unittest.main()
