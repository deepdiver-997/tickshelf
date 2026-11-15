"""Owned benchmark processes, including a traced receiver's distinct PID."""
from __future__ import annotations

import os
import re
import select
import signal
import subprocess
from pathlib import Path

from stack_runtime import OwnedProcesses, RunError


class BenchmarkProcesses(OwnedProcesses):
    def __init__(self, directory):
        super().__init__(directory)
        self.traced = set()
        self.target_pids = {}
        self.target_pidfds = {}

    def process_pid(self, name):
        return self.target_pids.get(name, self.children[name].pid)

    def receiver_ready(self, binary, *, traced=False):
        child = self.children["receiver"]
        text = self.text("receiver")
        if not traced:
            return f"BENCH_READY pid={child.pid}" in text
        self.traced.add("receiver")
        matches = re.findall(r"^BENCH_READY pid=([0-9]+)\s*$", text, re.MULTILINE)
        if not matches:
            return False
        pid = int(matches[-1])
        if "receiver" in self.target_pids:
            if self.target_pids["receiver"] != pid:
                raise RunError("traced receiver readiness PID changed")
            return True
        # Popen created a new session: a log marker alone is not ownership proof.
        status = Path(f"/proc/{pid}/stat").read_text()
        fields = status[status.rfind(")") + 2:].split()
        if int(fields[2]) != child.pid or int(fields[3]) != child.pid:
            raise RunError("tracee does not belong to the owned process group/session")
        if not Path(f"/proc/{pid}/exe").samefile(binary):
            raise RunError("traced receiver executable differs from the owned benchmark")
        if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
            raise RunError("safe traced-process cleanup requires Linux pidfd support")
        try:
            pidfd = os.pidfd_open(pid)
        except OSError as exc:
            raise RunError(f"cannot retain owned tracee identity: {exc}") from exc
        try:
            current = Path(f"/proc/{pid}/stat").read_text()
            if current[current.rfind(")") + 2:].split()[19] != fields[19]:
                raise RunError("tracee identity changed while opening pidfd")
        except BaseException:
            os.close(pidfd)
            raise
        self.target_pids["receiver"] = pid
        self.target_pidfds["receiver"] = pidfd
        return True

    def stop(self, name, timeout=10.0):
        if name not in self.traced:
            return super().stop(name, timeout)
        child = self.children[name]
        # While this unreaped Popen child is alive its session ID cannot be
        # reused. Signal the owned group, not only strace (which can detach).
        if child.poll() is None:
            try:
                os.killpg(child.pid, signal.SIGINT)
            except ProcessLookupError:
                pass
            try:
                child.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.forced.append(name)
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                child.wait(timeout=5)
        pidfd = self.target_pidfds.pop(name, None)
        if pidfd is not None:
            try:
                poller = select.poll()
                poller.register(pidfd, select.POLLIN)
                if not poller.poll(0):
                    try:
                        signal.pidfd_send_signal(pidfd, signal.SIGINT)
                    except ProcessLookupError:
                        pass
                    if not poller.poll(1000):
                        self.forced.append(name + "-tracee")
                        signal.pidfd_send_signal(pidfd, signal.SIGKILL)
                        if not poller.poll(5000):
                            raise RunError("owned tracee did not exit after cleanup")
            finally:
                os.close(pidfd)
        self.logs[name].close()
        return child.returncode
