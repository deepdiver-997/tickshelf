#!/usr/bin/env python3
"""Opt-in owned Linux strace launch and explicit-path, payload-free summaries.

No attach, process discovery, privilege changes, or benchmark runs occur on import.
See docs/pipeline-syscalls.md for the supported trace grammar and interpretation.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
import json
import math
import os
from pathlib import Path
import platform
import posixpath
import re
import shutil
import subprocess
import sys
from typing import Sequence

# Deliberately not "all": omit ioctl, application strings, environment, and dumps.
TRACE_SYSCALLS = (
    "read", "write", "pread64", "pwrite64", "readv", "writev", "preadv", "pwritev",
    "preadv2", "pwritev2", "recvfrom", "sendto", "recvmsg", "sendmsg",
    "fdatasync", "fsync", "syncfs", "fcntl", "flock", "futex", "mmap", "munmap",
    "msync", "mremap", "open", "openat", "close", "close_range", "dup", "dup2",
    "dup3", "socket", "socketpair", "connect", "accept", "accept4", "shutdown",
    "clone", "clone3", "fork", "vfork", "execve", "exit", "exit_group", "unshare",
    "fstat", "newfstatat", "stat", "statx", "lseek", "madvise", "brk", "mprotect",
    "poll", "ppoll", "select", "pselect6", "epoll_wait", "epoll_pwait",
    "clock_gettime", "gettimeofday",
)
# Keep vector/message structures as pointers: even unusual decoders cannot dump
# iov_base or ancillary data. Their requested byte counts are explicitly unknown.
RAW_SYSCALLS = ("readv", "writev", "preadv", "pwritev", "preadv2", "pwritev2",
                "recvmsg", "sendmsg")


@dataclass(frozen=True)
class Capability:
    supported: bool
    status: str
    reason: str
    strace_path: str | None = None
    probe_returncode: int | None = None


class UnsupportedError(RuntimeError):
    def __init__(self, capability: Capability):
        self.capability = capability
        super().__init__(f"syscall tracing unsupported: {capability.reason}")


@dataclass(frozen=True)
class LaunchPlan:
    command: tuple[str, ...]
    trace_path: Path | None
    capability: Capability
    wrapper_pid_is_target: bool = False
    target_pid_source: str = "validated BENCH_READY pid, not tracer Popen.pid"


def _strace_options() -> list[str]:
    return ["-f", "-ttt", "-T", "-yy", "-s", "0", "-e",
            "trace=" + ",".join(TRACE_SYSCALLS), "-e", "raw=" + ",".join(RAW_SYSCALLS)]


def preflight(*, timeout: float = 5.0) -> Capability:
    """Probe only a newly owned, harmless interpreter; never attach or elevate.

    The probe uses exactly the flags the wrapper needs, writes only /dev/null,
    and catches unsupported strace versions as well as ptrace/permission denial.
    macOS exits before locating or executing any sampling command.
    """
    if platform.system() != "Linux":
        return Capability(False, "unsupported", "Linux strace launch is required; on macOS use an existing sample or explicit offline trace")
    executable = shutil.which("strace")
    if not executable:
        return Capability(False, "unsupported", "strace is not installed or not on PATH")
    try:
        done = subprocess.run(
            [executable, *_strace_options(), "-o", os.devnull, "--", sys.executable,
             "-I", "-c", "pass"], stdin=subprocess.DEVNULL, capture_output=True,
            text=True, errors="replace", timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return Capability(False, "unsupported", f"strace capability probe failed ({type(exc).__name__})", executable)
    if done.returncode != 0:
        # Do not copy stderr: it may contain paths/command arguments. The user
        # can run the documented safe probe to see their local policy message.
        denied = re.search(r"not permitted|permission denied|ptrace.*denied", done.stderr, re.I)
        reason = "strace/ptrace permission denied" if denied else "strace probe failed (unsupported flags, policy, or executable)"
        return Capability(False, "unsupported", reason, executable, done.returncode)
    return Capability(True, "supported", "owned launch-under-strace probe succeeded; runtime policy may still fail", executable, 0)


def prepare_launch(command: Sequence[str], *, trace_path: Path, owned_binary: Path,
                   enabled: bool = False, capability: Capability | None = None) -> LaunchPlan:
    """Reserve a private trace and return argv for the runner's existing start.

    Explicit owned_binary must name argv[0] itself, not a shell or an attach PID.
    This is an ownership assertion by the caller, not a sandbox for untrusted
    programs. The benchmark must not launch unrelated external processes.
    No command is launched here. A reserved trace must not be reused.
    """
    argv = tuple(str(arg) for arg in command)
    if not argv or any("\0" in arg for arg in argv):
        raise ValueError("command must be a nonempty argv without NUL")
    if not enabled:
        return LaunchPlan(argv, None, Capability(False, "disabled", "tracing explicitly off"), True, "direct owned process PID")
    cap = capability if capability is not None else preflight()
    if not cap.supported or not cap.strace_path:
        raise UnsupportedError(cap)
    if platform.system() != "Linux":
        raise UnsupportedError(Capability(False, "unsupported", "Linux required even with a supplied capability"))
    binary = Path(owned_binary).expanduser().resolve(strict=True)
    executable = Path(argv[0]).expanduser()
    if not executable.is_absolute() or not executable.samefile(binary):
        raise ValueError("argv[0] must be the absolute, explicitly owned benchmark binary")
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ValueError("owned binary must be an executable regular file")
    target = Path(trace_path).expanduser().absolute()
    parent = target.parent.stat()
    if parent.st_uid != os.getuid() or parent.st_mode & 0o022:
        raise ValueError("trace parent must be owned by this user and not group/world writable")
    # O_EXCL rejects existing paths and symlinks; preserve owner-only permissions.
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(descriptor)
    return LaunchPlan((cap.strace_path, *_strace_options(), "-o", str(target), "--", str(binary), *argv[1:]), target, cap)


_SIMPLE_BYTES = {"read": 2, "write": 2, "pread64": 2, "pwrite64": 2,
                 "recvfrom": 2, "sendto": 2, "recv": 2, "send": 2}
_VECTOR_BYTES = set(RAW_SYSCALLS)
_FD_FIRST = set(_SIMPLE_BYTES) | _VECTOR_BYTES | {
    "close", "fcntl", "flock", "fdatasync", "fsync", "syncfs", "dup", "dup2", "dup3",
    "connect", "accept", "accept4", "shutdown", "fstat", "fstat64", "lseek", "ftruncate",
    "epoll_wait", "epoll_pwait",
}
_PATH_CALLS = {"stat", "newfstatat", "statx", "lstat", "access", "faccessat"}
_AT_PATH_CALLS = {"openat", "openat2", "newfstatat", "statx", "faccessat"}
_MULTI_FD_WAIT = {"poll", "ppoll", "select", "pselect6"}
_NO_FD = {"futex", "futex_waitv", "munmap", "msync", "mremap", "clone", "clone3",
          "fork", "vfork", "execve", "exit", "exit_group", "unshare", "close_range",
          "madvise", "brk", "mprotect", "clock_gettime", "gettimeofday"}
_TIMESTAMP = re.compile(r"^(?:\d+\.\d+|\d{2}:\d{2}:\d{2}(?:\.\d+)?)\s+")
_DURATION = re.compile(r"\s+<(\d+(?:\.\d+)?)>\s*$")
_NUMBER = re.compile(r"^(-?(?:0x[0-9a-fA-F]+|\d+))(?![\w])")
_FD = re.compile(r"^(-?(?:0x[0-9a-fA-F]+|\d+))(?:<(.*)>)?$")
_MAX_LINE = 1024 * 1024


def _number(value: str) -> int | None:
    match = _NUMBER.match(value.strip())
    if not match:
        return None
    token = match.group(1)
    return int(token, 16 if "x" in token.lower() else 10)


def _arguments(value: str) -> list[str]:
    """Split top-level commas, preserving quoted strings and -yy <fd paths>."""
    result, start, stack = [], 0, []
    quoted = escaped = False
    for index, char in enumerate(value):
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted = False
            continue
        if stack and stack[-1] == "<":
            # -yy paths are not quoted; braces, commas and spaces are literal.
            # Socket arrows (->address) are not an annotation terminator.
            if char == ">" and (index + 1 == len(value) or value[index + 1] in ", )]}"):
                stack.pop()
            continue
        if char == '"':
            quoted = True
        elif char in "([{<":
            stack.append(char)
        elif stack and ((char == ")" and stack[-1] == "(") or
                        (char == "]" and stack[-1] == "[") or
                        (char == "}" and stack[-1] == "{") or
                        (char == ">" and stack[-1] == "<")):
            stack.pop()
        elif char == "," and not stack:
            result.append(value[start:index].strip())
            start = index + 1
    if quoted or stack:
        raise ValueError("unbalanced trace argument delimiters")
    result.append(value[start:].strip())
    return result if value.strip() else []


def _quoted_path(value: str) -> str | None:
    # JSON and strace C strings agree for common paths. Decode the additional C
    # octal/hex escapes without eval; unfamiliar escapes remain unknown.
    if not re.fullmatch(r'"(?:[^"\\]|\\.)*"', value.strip()):
        return None
    text = value.strip()[1:-1]
    def decode(match):
        item = match.group(1)
        if item.startswith("x"):
            return chr(int(item[1:], 16))
        if item[0].isdigit():
            return chr(int(item, 8))
        return {"n": "\n", "r": "\r", "t": "\t", "\\": "\\", '"': '"'}.get(item, "\ufffd")
    text = re.sub(r"\\(x[0-9a-fA-F]{2}|[0-7]{1,3}|.)", decode, text)
    return None if "\ufffd" in text else text


@dataclass
class _Event:
    tid: int | None
    name: str
    arguments: list[str]
    result: str
    elapsed: float | None
    start: int
    end: int
    fd: int | None = None
    filekind: str = "unknown"


@dataclass(frozen=True)
class _Descriptor:
    target: str | None
    cloexec: bool = False
    kind: str | None = None


@dataclass
class _Stats:
    counts: int = 0
    failures: int = 0
    unknown_results: int = 0
    errnos: Counter = field(default_factory=Counter)
    byte_calls: int = 0
    bytes_requested: int = 0
    bytes_returned: int = 0
    requested_samples: int = 0
    returned_samples: int = 0
    unknown_requested_byte_calls: int = 0
    unknown_returned_byte_calls: int = 0
    short_io_calls: int = 0
    elapsed: list[float] = field(default_factory=list)

    def add(self, event: _Event):
        self.counts += 1
        result = _number(event.result)
        errno = re.match(r"-(?:1|0x1)\s+(E[A-Z0-9_]+)\b", event.result)
        if errno or (result is not None and result < 0 and
                     (event.name in _SIMPLE_BYTES or event.name in _VECTOR_BYTES)):
            # fcntl(F_GETOWN), unlike I/O, can successfully return a negative
            # process-group ID. A numeric sign alone is not an errno decoder.
            self.failures += 1
            self.errnos[errno.group(1) if errno else "unknown_errno"] += 1
        elif result is None:
            self.unknown_results += 1
        if event.elapsed is not None:
            self.elapsed.append(event.elapsed)
        if event.name not in _SIMPLE_BYTES and event.name not in _VECTOR_BYTES:
            return
        self.byte_calls += 1
        requested = None
        if event.name in _SIMPLE_BYTES:
            index = _SIMPLE_BYTES[event.name]
            if len(event.arguments) > index:
                requested = _number(event.arguments[index])
        elif len(event.arguments) > 1:
            vector = event.arguments[1]
            lengths = re.findall(r"\biov_len=(\d+)\b", vector)
            if lengths and not re.search(r",\s*\.\.\.", vector):
                requested = sum(int(item) for item in lengths)
        if requested is not None and requested >= 0:
            self.bytes_requested += requested
            self.requested_samples += 1
        else:
            self.unknown_requested_byte_calls += 1
        if result is not None:
            # A failed read/write returns no bytes, NOT minus one byte.
            returned = max(0, result)
            self.bytes_returned += returned
            self.returned_samples += 1
            if result >= 0 and requested is not None and returned < requested:
                self.short_io_calls += 1
        else:
            self.unknown_returned_byte_calls += 1

    def report(self) -> dict:
        ordered = sorted(self.elapsed)
        count = len(ordered)
        quantiles = {name: ordered[math.ceil(fraction * count) - 1] if count >= minimum else None
                     for name, fraction, minimum in (("p50", .5, 2), ("p95", .95, 20), ("p99", .99, 100))}
        return {"counts": self.counts, "failures": self.failures,
                "unknown_results": self.unknown_results, "errno_counts": dict(sorted(self.errnos.items())),
                "byte_calls": self.byte_calls, "bytes_requested": self.bytes_requested,
                "bytes_returned": self.bytes_returned, "requested_samples": self.requested_samples,
                "returned_samples": self.returned_samples,
                "unknown_requested_byte_calls": self.unknown_requested_byte_calls,
                "unknown_returned_byte_calls": self.unknown_returned_byte_calls,
                "short_io_calls": self.short_io_calls, "elapsed_samples": count,
                "elapsed_missing": self.counts - count,
                "elapsed_total_s": math.fsum(ordered) if ordered else None,
                "elapsed_max_s": ordered[-1] if ordered else None,
                "elapsed_quantiles_s": quantiles}


class _Analyzer:
    def __init__(self, db_paths, segment_paths, root_pid):
        self.db_paths = {posixpath.normpath(str(path)) for path in db_paths}
        self.segment_paths = {posixpath.normpath(str(path)) for path in segment_paths}
        self.root_pid = root_pid
        self.events: list[_Event] = []
        self.coverage = Counter()
        self.diagnostics: list[dict] = []
        self.tables: dict[int | None, dict[int, _Descriptor]] = {}
        self.processes = {root_pid: root_pid} if root_pid is not None else {}

    def diagnostic(self, line, code):
        self.coverage[code] += 1
        if len(self.diagnostics) < 100:
            # Never quote a trace line or arguments: external traces may contain payloads.
            self.diagnostics.append({"line": line, "code": code})
        else:
            self.coverage["diagnostics_omitted"] += 1

    def parse(self, source):
        pending = defaultdict(list)
        line_number = 0
        while True:
            raw = source.readline(_MAX_LINE + 1)
            if not raw:
                break
            line_number += 1
            self.coverage["physical_lines"] += 1
            if len(raw) > _MAX_LINE:
                while raw and not raw.endswith(b"\n"):
                    raw = source.readline(_MAX_LINE + 1)
                self.coverage["unparsed_lines"] += 1
                self.diagnostic(line_number, "oversized_line")
                continue
            text = raw.decode("utf-8", errors="replace").strip()
            if "\ufffd" in text:
                self.diagnostic(line_number, "invalid_utf8")
            tid = self.root_pid
            match = re.match(r"^\[pid\s+(\d+)\]\s+(.*)$", text)
            if not match:
                match = re.match(r"^(\d+)\s+(.*)$", text)
            if match:
                tid, text = int(match.group(1)), match.group(2)
            text = _TIMESTAMP.sub("", text, count=1)
            if not text or text.startswith(("+++ ", "--- ")):
                self.coverage["ignored_lines"] += 1
                continue
            if re.fullmatch(r"strace: Process \d+ (?:attached|detached)", text):
                self.coverage["ignored_lines"] += 1
                continue
            if text.endswith("<unfinished ...>"):
                fragment = text[:-len("<unfinished ...>")].rstrip()
                match = re.match(r"^(\w+)\(", fragment)
                if match:
                    pending[tid].append((match.group(1), fragment, line_number))
                    self.coverage["unfinished_entries"] += 1
                    continue
            resumed = re.match(r"^<\.\.\. (\w+) resumed>(.*)$", text)
            start = line_number
            if resumed:
                candidates = pending[tid]
                index = next((i for i in range(len(candidates) - 1, -1, -1)
                              if candidates[i][0] == resumed.group(1)), None)
                if index is None:
                    self.coverage["unparsed_lines"] += 1
                    self.diagnostic(line_number, "unmatched_resumed")
                    continue
                _, fragment, start = candidates.pop(index)
                text = fragment + resumed.group(2)
                self.coverage["resumed_pairs"] += 1
            duration = _DURATION.search(text)
            elapsed = float(duration.group(1)) if duration else None
            if elapsed is not None and not math.isfinite(elapsed):
                elapsed = None
                self.diagnostic(line_number, "invalid_duration")
            if duration:
                text = text[:duration.start()]
            call = re.fullmatch(r"(\w+)\((.*)\)\s+=\s+(.+)", text)
            if not call:
                self.coverage["unparsed_lines"] += 1
                self.diagnostic(line_number, "unrecognized_line")
                continue
            name, arguments, result = call.groups()
            try:
                split_arguments = _arguments(arguments)
            except ValueError:
                self.coverage["unparsed_lines"] += 1
                self.diagnostic(line_number, "unbalanced_arguments")
                continue
            if elapsed is None and name not in {"exit", "exit_group"}:
                self.diagnostic(line_number, "missing_duration")
            self.events.append(_Event(tid, name, split_arguments, result, elapsed, start, line_number))
        for entries in pending.values():
            for _, _, start in entries:
                self.coverage["incomplete_syscalls"] += 1
                self.diagnostic(start, "unfinished_without_resume")

    def table(self, tid):
        return self.tables.setdefault(tid, {})

    def classify(self, descriptor):
        if descriptor is None:
            return "unknown"
        if descriptor.kind:
            return descriptor.kind
        path = descriptor.target
        if not path:
            return "unknown"
        if re.match(r"(?:socket:|(?:TCP|UDP|UNIX|NETLINK|PACKET|SCTP)[\w-]*:)", path):
            return "socket"
        path = path.removesuffix(" (deleted)")
        if not path.startswith("/"):
            return "other" if path.startswith(("pipe:", "anon_inode:")) else "unknown"
        path = posixpath.normpath(path)
        if path in self.db_paths:
            return "db"
        if any(path == base + "-wal" for base in self.db_paths):
            return "wal"
        if any(path == base + "-shm" for base in self.db_paths):
            return "shm"
        if path in self.segment_paths:
            return "segment"
        # db_paths is an explicit declaration of database files OR directory
        # roots. Match path components, never a mere prefix/"rocks" substring;
        # do not inspect the analyst's filesystem for external server paths.
        if any(path.startswith(root.rstrip("/") + "/") for root in self.db_paths):
            # RocksDB numeric log files are WALs, including archive/NNNNNN.log.
            # LOG, LOG.old.*, MANIFEST, OPTIONS and arbitrary *.log are metadata.
            return "wal" if re.fullmatch(r"[0-9]+\.log", posixpath.basename(path)) else "db"
        return "other"

    def descriptor(self, value, table):
        match = _FD.fullmatch(value.strip())
        if not match:
            return None, None
        fd, annotation = _number(match.group(1)), match.group(2)
        if annotation is not None and fd is not None and fd >= 0:
            previous = table.get(fd)
            table[fd] = _Descriptor(annotation, previous.cloexec if previous else False)
        return fd, table.get(fd)

    def path_target(self, event, table):
        index = 1 if event.name in _AT_PATH_CALLS else 0
        if len(event.arguments) <= index:
            return None
        path = _quoted_path(event.arguments[index])
        if path is None or path.startswith("/"):
            return path
        if path == "":
            return None
        if index and event.arguments[0] != "AT_FDCWD":
            _, directory = self.descriptor(event.arguments[0], table)
            if directory and directory.target and directory.target.startswith("/"):
                return posixpath.join(directory.target, path)
        # No cwd inference from the analyst's working directory.
        return None

    def start(self, event):
        table = self.table(event.tid)
        if event.name in _NO_FD:
            event.filekind = "not_fd"
        child = _number(event.result)
        if event.name in {"clone", "clone3", "fork", "vfork"} and child is not None and child > 0:
            flags = " ".join(event.arguments)
            decoded = re.search(r"\bflags=([^,}]+)", flags)
            known_flags = (decoded is not None and
                           re.fullmatch(r"(?:CLONE_[A-Z0-9_]+|SIG[A-Z0-9]+|0)(?:\|(?:CLONE_[A-Z0-9_]+|SIG[A-Z0-9]+|0))*",
                                        decoded.group(1).strip()) is not None)
            if event.name in {"clone", "clone3"} and not known_flags:
                # Numeric/raw clone flags are not portable across architectures.
                self.diagnostic(event.start, "unknown_clone_flags")
                self.tables.setdefault(child, {})
            else:
                self.tables[child] = table if "CLONE_FILES" in flags else table.copy()
                self.processes[child] = self.processes.get(event.tid) if "CLONE_THREAD" in flags else child
        if event.name in {"open", "openat", "openat2"}:
            fd, record = self.descriptor(event.result, {})
            # A return value can include trailing descriptive text; normal open
            # returns are simply number<path> or number.
            event.fd = fd if fd is not None and fd >= 0 else None
            event.filekind = self.classify(record or _Descriptor(self.path_target(event, table)))
        elif event.name in _PATH_CALLS:
            if (event.name in _AT_PATH_CALLS and len(event.arguments) > 1 and
                    _quoted_path(event.arguments[1]) == "" and
                    "AT_EMPTY_PATH" in " ".join(event.arguments)):
                event.fd, record = self.descriptor(event.arguments[0], table)
                event.filekind = self.classify(record)
            else:
                event.filekind = self.classify(_Descriptor(self.path_target(event, table)))
        elif event.name in _MULTI_FD_WAIT:
            # A wait over a set of FDs is not attributable to one DB/socket.
            # Keep its real duration without pretending nfds is an actual FD.
            self.coverage["multi_fd_wait_calls"] += 1
        elif event.name in _FD_FIRST or event.name in {"mmap", "mmap2"}:
            index = 4 if event.name in {"mmap", "mmap2"} else 0
            if len(event.arguments) <= index:
                event.filekind = "unknown"
                self.diagnostic(event.start, "missing_fd_argument")
                return
            event.fd, record = self.descriptor(event.arguments[index], table)
            if event.name in {"mmap", "mmap2"} and (event.fd == -1 or
                    (len(event.arguments) > 3 and re.search(r"\bMAP_ANON(?:YMOUS)?\b", event.arguments[3]))):
                event.filekind = "not_fd"
            else:
                event.filekind = self.classify(record)
        elif event.name in {"socket", "socketpair"}:
            event.filekind = "socket"
            if event.name == "socket" and child is not None and child >= 0:
                event.fd = child

    def finish(self, event):
        result = _number(event.result)
        table = self.table(event.tid)
        if event.name == "close" and event.fd is not None and result is not None and result < 0:
            # Linux close may report EINTR/EIO after releasing the descriptor;
            # EBADF says it was already invalid. Neither justifies a known path.
            table.pop(event.fd, None)
            self.diagnostic(event.end, "close_error_fd_invalidated")
            return
        if result is None or result < 0:
            return
        args = event.arguments
        if event.name in {"open", "openat", "openat2"}:
            _, record = self.descriptor(event.result, {})
            table[result] = _Descriptor(record.target if record else self.path_target(event, table),
                                        "O_CLOEXEC" in " ".join(args))
        elif event.name == "close" and event.fd is not None:
            table.pop(event.fd, None)
        elif event.name in {"dup", "dup2", "dup3"} or (event.name == "fcntl" and len(args) > 1 and args[1].startswith("F_DUPFD")):
            original = table.get(event.fd)
            table[result] = _Descriptor(original.target if original else None,
                "CLOEXEC" in " ".join(args), original.kind if original else None)
        elif event.name == "fcntl" and len(args) > 2 and args[1] == "F_SETFD" and event.fd in table:
            old = table[event.fd]
            table[event.fd] = _Descriptor(old.target, "FD_CLOEXEC" in args[2], old.kind)
        elif event.name in {"socket", "accept", "accept4"}:
            _, record = self.descriptor(event.result, {})
            table[result] = _Descriptor(record.target if record else None, "SOCK_CLOEXEC" in " ".join(args), "socket")
        elif event.name == "socketpair" and args:
            for value in _arguments(args[-1].strip("[]")):
                fd, record = self.descriptor(value, {})
                if fd is not None and fd >= 0:
                    table[fd] = _Descriptor(record.target if record else None, "SOCK_CLOEXEC" in " ".join(args), "socket")
        elif event.name == "unshare" and "CLONE_FILES" in " ".join(args):
            self.tables[event.tid] = table.copy()
        elif event.name == "close_range" and len(args) >= 2:
            if "CLOSE_RANGE_UNSHARE" in " ".join(args):
                table = table.copy()
                self.tables[event.tid] = table
            low, high = _number(args[0]), _number(args[1])
            if args[1] == "~0U":
                high = 2**32 - 1
            if low is None or high is None:
                table.clear()
                self.diagnostic(event.end, "unknown_close_range")
            else:
                for fd in list(table):
                    if low <= fd <= high:
                        if "CLOSE_RANGE_CLOEXEC" in " ".join(args):
                            old = table[fd]
                            table[fd] = _Descriptor(old.target, True, old.kind)
                        else:
                            del table[fd]
        elif event.name == "execve":
            # exec unshares CLONE_FILES and applies close-on-exec.
            self.tables[event.tid] = {fd: item for fd, item in table.items() if not item.cloexec}

    def report(self):
        # Bind an unfinished call's FD at entry, apply open/close at return.
        # clone child tables exist from clone entry, including children scheduled
        # before the parent's resumed return. No timestamps are summed as CPU.
        timeline = [(event.start, 0, index) for index, event in enumerate(self.events)]
        timeline += [(event.end, 1, index) for index, event in enumerate(self.events)]
        for _, phase, index in sorted(timeline):
            (self.start if phase == 0 else self.finish)(self.events[index])
        total = _Stats()
        by_name, by_tid, by_process, by_fd = (defaultdict(_Stats) for _ in range(4))
        for event in self.events:
            process = self.processes.get(event.tid)
            for stats in (total, by_name[(event.name, event.filekind)], by_tid[event.tid],
                          by_process[process], by_fd[(event.tid, event.fd, event.filekind)]):
                stats.add(event)
        self.coverage["completed_syscalls"] = len(self.events)
        self.coverage["unknown_filekind_calls"] = sum(event.filekind == "unknown" for event in self.events)
        self.coverage["unknown_process_calls"] = sum(self.processes.get(event.tid) is None for event in self.events)
        self.coverage["unknown_tid_calls"] = sum(event.tid is None for event in self.events)
        for name in ("physical_lines", "ignored_lines", "unparsed_lines", "incomplete_syscalls",
                     "unfinished_entries", "resumed_pairs", "missing_duration"):
            self.coverage.setdefault(name, 0)
        partial = any(self.coverage[name] for name in ("unparsed_lines", "incomplete_syscalls", "missing_duration",
                                                       "invalid_utf8", "invalid_duration", "unknown_clone_flags", "unknown_close_range", "missing_fd_argument"))
        status = ("partial" if partial else "ok") if self.events else ("invalid" if self.coverage["unparsed_lines"] else "empty")
        sortkey = lambda item: str(item[0])
        return {"schema_version": 1, "status": status,
                "timing_semantics": "strace -T elapsed wall syscall duration; not kernel CPU; overlapping thread/nested durations are not additive CPU percentages",
                "kernel_cpu_s": None, "root_pid": self.root_pid,
                "quantile_method": "exact nearest rank; p50 needs 2, p95 20, p99 100 timed observations; null below threshold",
                "coverage": dict(sorted(self.coverage.items())), "diagnostics": self.diagnostics,
                "totals": total.report(),
                "by_name_filekind": [{"name": key[0], "filekind": key[1], **value.report()}
                                     for key, value in sorted(by_name.items())],
                "by_tid": [{"tid": key, "process_id": self.processes.get(key), **value.report()}
                           for key, value in sorted(by_tid.items(), key=sortkey)],
                "by_process": [{"process_id": key, **value.report()}
                               for key, value in sorted(by_process.items(), key=sortkey)],
                "by_tid_fd_filekind": [{"tid": key[0], "fd": key[1], "filekind": key[2], **value.report()}
                                       for key, value in sorted(by_fd.items(), key=sortkey)]}


def analyze_trace(trace_path: Path, *, db_paths=(), segment_paths=(), root_pid: int | None = None) -> dict:
    """Read exactly one explicit strace file; do not glob/search or consult /proc.

    Numeric multiplexed -f output, [pid N], -ttt/-tt timestamps, and unprefixed
    single-process traces are supported. Unprefixed traces need root_pid for a
    known TID/process. No ancestry is guessed from adjacent TIDs. db_paths
    explicitly declares exact DB files (with SQLite -wal/-shm companions) or
    directory roots, whose descendants are DB files except numeric *.log WALs.
    No directory discovery or host-filesystem existence check is used for roots.
    """
    if root_pid is not None and (type(root_pid) is not int or root_pid <= 0):
        raise ValueError("root_pid must be a positive integer")
    path = Path(trace_path).expanduser()
    if not path.is_file():
        raise ValueError("trace_path must name an explicit regular trace file")
    analyzer = _Analyzer(db_paths, segment_paths, root_pid)
    with path.open("rb") as source:
        analyzer.parse(source)
    result = analyzer.report()
    result["source"] = str(path.absolute())
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    commands.add_parser("preflight", help="safe capability check only; no benchmark")
    analyze = commands.add_parser("analyze", help="offline analysis of ONE explicitly provided raw trace")
    analyze.add_argument("trace", type=Path)
    analyze.add_argument("--db", type=Path, action="append", default=[], help="explicit DB file or directory root; SQLite -wal/-shm and root-local numeric .log WALs")
    analyze.add_argument("--segment", type=Path, action="append", default=[], help="exact known segment file path (repeatable)")
    analyze.add_argument("--root-pid", type=int, help="actual receiver PID, NOT strace PID")
    analyze.add_argument("--output", type=Path, help="new owner-only JSON file; defaults to stdout")
    args = parser.parse_args(argv)
    if args.action == "preflight":
        cap = preflight()
        print(json.dumps(asdict(cap), indent=2))
        return 0 if cap.supported else 2
    try:
        result = analyze_trace(args.trace, db_paths=args.db, segment_paths=args.segment, root_pid=args.root_pid)
        text = json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n"
        if args.output:
            fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w") as output:
                output.write(text)
        else:
            print(text, end="")
        return 0 if result["status"] == "ok" else 2
    except (OSError, ValueError) as exc:
        # Avoid echoing untrusted line contents in exception messages.
        print(f"offline trace analysis failed ({type(exc).__name__})", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
