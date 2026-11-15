#!/usr/bin/env python3
"""Artificial trace fixtures only: these are NOT measured benchmark results."""
from dataclasses import asdict
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from pipeline_bench_syscalls import (Capability, TRACE_SYSCALLS, UnsupportedError,
                                     analyze_trace, main, preflight, prepare_launch)


# Entirely synthetic, with deliberately chosen small/large durations and PIDs.
DB = "/tmp/owned work/main, data.sqlite"
SEGMENT = "/tmp/owned work/records.segment"
ARTIFICIAL_TRACE = f'''101 1700000000.000001 openat(AT_FDCWD, "{DB}", O_RDWR|O_CREAT|O_CLOEXEC, 0600) = 3<{DB}> <0.000020>
101 1700000000.000002 openat(AT_FDCWD, "{DB}-wal", O_RDWR|O_CREAT, 0600) = 4<{DB}-wal> <0.000030>
101 1700000000.000003 open("{DB}-shm", O_RDWR, 0600) = 5<{DB}-shm> <0.000040>
101 1700000000.000004 openat(AT_FDCWD, "{SEGMENT}", O_RDWR|O_CREAT, 0600) = 6<{SEGMENT}> <0.000050>
101 1700000000.000005 read(3<{DB}>, ""..., 100) = 40 <0.001000>
101 1700000000.000006 pwrite64(4<{DB}-wal>, ""..., 120, 4096) = 60 <0.002000>
101 1700000000.000007 pwrite64(4<{DB}-wal>, ""..., 10, 0) = -1 ENOSPC (No space left on device) <0.003000>
101 1700000000.000008 fdatasync(4<{DB}-wal>) = 0 <0.004000>
101 1700000000.000009 fcntl(3<{DB}>, F_SETLK, {{l_type=F_WRLCK, l_whence=SEEK_SET, l_start=0, l_len=1}}) = 0 <0.000100>
101 1700000000.000010 futex(0x1234, FUTEX_WAIT_PRIVATE, 2, NULL) = -1 EAGAIN (Resource temporarily unavailable) <0.010000>
101 1700000000.000011 mmap(NULL, 32768, PROT_READ|PROT_WRITE, MAP_SHARED, 5<{DB}-shm>, 0) = 0x7f1234000 <0.000060>
101 1700000000.000012 clone(child_stack=NULL, flags=CLONE_VM|CLONE_FILES|CLONE_THREAD|CLONE_SIGHAND) = 102 <0.000200>
[pid 102] 1700000000.000013 pwrite64(4, ""..., 50, 8192) = 50 <0.005000>
[pid 102] 1700000000.000014 openat(AT_FDCWD, "/tmp/owned work/other file", O_RDONLY) = 7</tmp/owned work/other file> <0.000020>
101 1700000000.000015 read(7, ""..., 5) = 5 <0.000010>
101 1700000000.000016 close(4<{DB}-wal>) = 0 <0.000010>
102 1700000000.000017 pwrite64(4, ""..., 20, 0) = -1 EBADF (Bad file descriptor) <0.000010>
101 1700000000.000018 socket(AF_INET, SOCK_STREAM|SOCK_CLOEXEC, IPPROTO_TCP) = 8<TCP:[12345]> <0.000020>
102 1700000000.000019 read(8<TCP:[127.0.0.1:1234->127.0.0.1:5678]>, ""..., 8) = 8 <0.000020>
102 1700000000.000020 read(9, ""..., 8) = 0 <0.000010>
102 1700000000.000021 read(3, <unfinished ...>
101 1700000000.000022 close(3<{DB}>) = 0 <0.000010>
102 1700000000.000023 <... read resumed>""..., 10) = 10 <0.020000>
101 1700000000.000024 fork() = 201 <0.000100>
101 1700000000.000025 close(6<{SEGMENT}>) = 0 <0.000010>
201 1700000000.000026 write(6, ""..., 12) = 12 <0.000040>
201 1700000000.000027 +++ exited with 0 +++
'''


class TraceTests(unittest.TestCase):
    def analyze(self, text, **kwargs):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "explicit.trace"
            path.write_text(text)
            return analyze_trace(path, **kwargs)

    def entry(self, result, name, kind):
        return next(item for item in result["by_name_filekind"]
                    if item["name"] == name and item["filekind"] == kind)

    def test_explicit_rocks_roots_numeric_wal_metadata_and_component_boundaries(self):
        # Entirely synthetic roots, intentionally absent from this Mac. The
        # first has no "rocks" name: classification is by explicit scope only.
        root = "/external/owned run/storage"
        second = "/external/owned run/index.rocksdb"
        cases = [
            (root + "/000001.sst", "db"),
            (root + "/000002.log", "wal"),
            (root + "/archive/000003.log", "wal"),
            (root + "/CURRENT", "db"),
            (root + "/MANIFEST-000004", "db"),
            (root + "/OPTIONS-000005", "db"),
            (root + "/LOG", "db"),
            (root + "/LOG.old.1700000000", "db"),
            (root + "/info.log", "db"),
            (root + "/archive/diagnostics.log", "db"),
            (root + "/000006.log.bak", "db"),
            (root + "/000007.LOG", "db"),
            (second + "/000008.sst", "db"),
            (second + "/archive/000009.log", "wal"),
            (root + "-neighbor/000010.log", "other"),
            (root + "backup/000011.sst", "other"),
            (second + "-backup/000012.log", "other"),
            (root + "/../unlisted/000013.log", "other"),
            ("/external/unlisted-rocks/000014.log", "other"),
            ("/external/owned run/progress.json", "other"),
        ]
        text = "".join(f'70 pwrite64({fd}<{path}>, ""..., 16, 0) = 16 <0.000001>\n'
                       for fd, (path, _) in enumerate(cases, 3))
        result = self.analyze(text, db_paths=[Path(root + "/"), Path(second)], root_pid=70)
        self.assertEqual(result["status"], "ok")
        buckets = {row["fd"]: row for row in result["by_tid_fd_filekind"]}
        for fd, (path, expected) in enumerate(cases, 3):
            with self.subTest(path=path):
                self.assertEqual(buckets[fd]["filekind"], expected)
                self.assertEqual(buckets[fd]["bytes_returned"], 16)
        self.assertEqual(self.entry(result, "pwrite64", "wal")["counts"], 3)
        self.assertEqual(self.entry(result, "pwrite64", "other")["counts"], 6)
        # Merely including "rocks" in a path never declares its DB ownership.
        undeclared = self.analyze(text, root_pid=70)
        self.assertEqual(self.entry(undeclared, "pwrite64", "other")["counts"], len(cases))

    def test_sqlite_exact_and_companion_file_classification_unchanged(self):
        cases = [(DB, "db"), (DB + "-wal", "wal"), (DB + "-shm", "shm"),
                 (DB + ".backup", "other"), (DB + "-wal.old", "other"),
                 (DB + "-shm.old", "other"), (DB + "-neighbor/000001.log", "other")]
        text = "".join(f'71 read({fd}<{path}>, ""..., 1) = 1 <0.000001>\n'
                       for fd, (path, _) in enumerate(cases, 3))
        result = self.analyze(text, db_paths=[DB], root_pid=71)
        buckets = {row["fd"]: row for row in result["by_tid_fd_filekind"]}
        for fd, (path, expected) in enumerate(cases, 3):
            with self.subTest(path=path):
                self.assertEqual(buckets[fd]["filekind"], expected)
        self.assertEqual(result["coverage"]["completed_syscalls"], len(cases))

    def test_artificial_db_wal_shm_segment_socket_and_shared_thread_fds(self):
        result = self.analyze(ARTIFICIAL_TRACE, db_paths=[DB], segment_paths=[SEGMENT], root_pid=101)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["coverage"]["completed_syscalls"], 25)
        self.assertEqual(result["coverage"]["resumed_pairs"], 1)
        self.assertEqual(result["coverage"]["unparsed_lines"], 0)
        wal = self.entry(result, "pwrite64", "wal")
        self.assertEqual(wal["counts"], 3)
        self.assertEqual(wal["bytes_requested"], 180)
        self.assertEqual(wal["bytes_returned"], 110)
        self.assertEqual(wal["short_io_calls"], 1)
        self.assertEqual(wal["failures"], 1)
        self.assertEqual(wal["errno_counts"], {"ENOSPC": 1})
        self.assertAlmostEqual(wal["elapsed_total_s"], .010)
        self.assertEqual(wal["elapsed_max_s"], .005)
        self.assertIsNone(wal["elapsed_quantiles_s"]["p95"])
        self.assertEqual(self.entry(result, "read", "db")["bytes_returned"], 50)
        self.assertEqual(self.entry(result, "fdatasync", "wal")["counts"], 1)
        self.assertEqual(self.entry(result, "fcntl", "db")["counts"], 1)
        self.assertEqual(self.entry(result, "futex", "not_fd")["failures"], 1)
        self.assertEqual(self.entry(result, "mmap", "shm")["byte_calls"], 0)
        self.assertEqual(self.entry(result, "write", "segment")["bytes_returned"], 12)
        self.assertEqual(self.entry(result, "read", "socket")["bytes_returned"], 8)
        self.assertEqual(self.entry(result, "read", "other")["counts"], 1)
        self.assertEqual(self.entry(result, "pwrite64", "unknown")["errno_counts"], {"EBADF": 1})
        tids = {row["tid"]: row for row in result["by_tid"]}
        self.assertEqual(tids[102]["process_id"], 101)
        self.assertEqual(tids[201]["process_id"], 201)
        processes = {row["process_id"]: row for row in result["by_process"]}
        self.assertEqual(processes[101]["counts"], tids[101]["counts"] + tids[102]["counts"])
        self.assertEqual(processes[201]["counts"], 1)
        self.assertTrue(any(row["tid"] == 102 and row["fd"] == 4 and row["filekind"] == "wal"
                            for row in result["by_tid_fd_filekind"]))
        self.assertIsNone(result["kernel_cpu_s"])
        self.assertIn("not kernel CPU", result["timing_semantics"])

    def test_percentiles_need_enough_actual_timed_calls(self):
        text = "".join(f"1 futex(0x1, FUTEX_WAKE_PRIVATE, 1) = 0 <{index / 1000:.6f}>\n"
                       for index in range(1, 101))
        result = self.analyze(text, root_pid=1)
        stats = self.entry(result, "futex", "not_fd")
        self.assertAlmostEqual(stats["elapsed_total_s"], 5.05)
        self.assertEqual(stats["elapsed_max_s"], .1)
        self.assertEqual(stats["elapsed_quantiles_s"], {"p50": .05, "p95": .095, "p99": .099})
        result = self.analyze('read(3, ""..., 4096) = 3 <0.000020>\n', root_pid=7)
        self.assertEqual(result["by_tid"][0]["tid"], 7)
        self.assertTrue(all(value is None for value in result["totals"]["elapsed_quantiles_s"].values()))

    def test_raw_hex_io_and_vector_requested_bytes_are_explicitly_unknown(self):
        result = self.analyze('''7 open("/tmp/data", O_RDONLY) = 3</tmp/data> <0.000001>
7 writev(0x3, 0x12345678, 0x2) = 0x18 <0.000002>
7 readv(3, [{iov_base=""..., iov_len=10}, {iov_base=""..., iov_len=20}], 2) = 15 <0.000003>
''', root_pid=7)
        raw = self.entry(result, "writev", "other")
        self.assertEqual(raw["bytes_returned"], 24)
        self.assertEqual(raw["requested_samples"], 0)
        self.assertEqual(raw["unknown_requested_byte_calls"], 1)
        decoded = self.entry(result, "readv", "other")
        self.assertEqual(decoded["bytes_requested"], 30)
        self.assertEqual(decoded["bytes_returned"], 15)

    def test_unparsed_incomplete_missing_duration_and_payload_not_echoed(self):
        secret = "CONFIDENTIAL_BUFFER_MUST_NOT_APPEAR"
        result = self.analyze(f'''12 read(3, <unfinished ...>
12 <... pwrite64 resumed>"{secret}", 8, 0) = 8 <0.01>
12 this is an unsupported trace line containing {secret}
12 fdatasync(4) = 0
''')
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["coverage"]["unparsed_lines"], 2)
        self.assertEqual(result["coverage"]["incomplete_syscalls"], 1)
        self.assertEqual(result["coverage"]["missing_duration"], 1)
        self.assertIsNone(result["totals"]["elapsed_total_s"])
        self.assertNotIn(secret, json.dumps(result))
        self.assertEqual(result["by_process"][0]["process_id"], None)
        self.assertEqual(self.analyze("")["status"], "empty")
        self.assertEqual(self.analyze("not strace\n")["status"], "invalid")

    def test_child_before_parent_clone_resumes_shares_fds(self):
        text = f'''31 open("{DB}", O_RDWR) = 3<{DB}> <0.000001>
31 clone(child_stack=NULL, flags=CLONE_FILES|CLONE_THREAD|CLONE_VM <unfinished ...>
32 pwrite64(3, ""..., 10, 0) = 10 <0.000002>
31 <... clone resumed>) = 32 <0.000003>
'''
        result = self.analyze(text, db_paths=[DB], root_pid=31)
        self.assertEqual(self.entry(result, "pwrite64", "db")["counts"], 1)
        self.assertEqual(next(row for row in result["by_tid"] if row["tid"] == 32)["process_id"], 31)

    def test_unknown_process_and_raw_clone_are_not_assumed_threads(self):
        result = self.analyze('''11 read(3, ""..., 5) = 5 <0.01>
12 read(3, ""..., 5) = 5 <0.02>
11 clone(0x400, 0, 0, 0, 0) = 13 <0.001>
13 read(3, ""..., 5) = 5 <0.02>
''')
        self.assertTrue(all(row["process_id"] is None for row in result["by_tid"]))
        self.assertEqual(result["coverage"]["unknown_clone_flags"], 1)
        self.assertEqual(result["status"], "partial")

    def test_fcntl_dup_close_on_exec_unshare_and_relative_paths(self):
        text = f'''5 open("{DB}", O_RDONLY|O_CLOEXEC) = 3<{DB}> <0.000001>
5 fcntl(3, F_DUPFD, 8) = 8<{DB}> <0.000001>
5 fcntl(8, F_SETFD, FD_CLOEXEC) = 0 <0.000001>
5 dup3(3, 9, O_CLOEXEC) = 9<{DB}> <0.000001>
5 dup(3) = 10<{DB}> <0.000001>
5 clone(child_stack=NULL, flags=CLONE_FILES|CLONE_THREAD) = 6 <0.000001>
6 unshare(CLONE_FILES) = 0 <0.000001>
6 close(10) = 0 <0.000001>
5 read(10, ""..., 1) = 1 <0.000001>
5 execve("/owned/benchmark", [""...], 0x1234) = 0 <0.000001>
5 read(8, ""..., 1) = -1 EBADF (Bad file descriptor) <0.000001>
5 read(10, ""..., 1) = 1 <0.000001>
5 openat(AT_FDCWD, "relative.sqlite", O_RDONLY) = 11 <0.000001>
5 read(11, ""..., 1) = 1 <0.000001>
5 open("/tmp/owned work", O_RDONLY|O_DIRECTORY) = 12</tmp/owned work> <0.000001>
5 openat(12, "main, data.sqlite", O_RDONLY) = 13 <0.000001>
5 read(13, ""..., 1) = 1 <0.000001>
'''
        result = self.analyze(text, db_paths=[DB], root_pid=5)
        self.assertEqual(self.entry(result, "read", "db")["counts"], 3)
        self.assertEqual(self.entry(result, "read", "unknown")["counts"], 2)
        self.assertEqual(self.entry(result, "openat", "db")["counts"], 1)

    def test_close_range_invalidates_shared_table_and_failed_io_bytes_zero(self):
        result = self.analyze(f'''4 open("{DB}", O_RDWR) = 3<{DB}> <0.01>
4 clone(child_stack=NULL, flags=CLONE_FILES|CLONE_THREAD) = 5 <0.01>
5 close_range(3, ~0U, 0) = 0 <0.01>
4 read(3, 0x1234, 100) = -1 EBADF (Bad file descriptor) <0.01>
''', db_paths=[DB], root_pid=4)
        read = self.entry(result, "read", "unknown")
        self.assertEqual(read["bytes_requested"], 100)
        self.assertEqual(read["bytes_returned"], 0)
        self.assertEqual(read["short_io_calls"], 0)

    def test_timestamps_without_pid_and_deleted_annotation(self):
        result = self.analyze(f'12:30:10.123456 read(3<{DB} (deleted)>, ""..., 2) = 2 <0.000001>\n', db_paths=[DB])
        self.assertEqual(self.entry(result, "read", "db")["counts"], 1)
        self.assertIsNone(result["by_tid"][0]["tid"])

    def test_close_errors_do_not_leave_confident_fd_paths(self):
        result = self.analyze(f'''4 open("{DB}", O_RDWR) = 3<{DB}> <0.01>
4 close(3) = -1 EINTR (Interrupted system call) <0.01>
4 read(3, ""..., 1) = -1 EBADF (Bad file descriptor) <0.01>
''', db_paths=[DB], root_pid=4)
        self.assertEqual(self.entry(result, "read", "unknown")["failures"], 1)
        self.assertEqual(result["coverage"]["close_error_fd_invalidated"], 1)

    def test_socketpair_and_unknown_syscall_semantics(self):
        result = self.analyze('''9 socketpair(AF_UNIX, SOCK_STREAM, 0, [3<UNIX-STREAM:[1]>, 4<UNIX-STREAM:[2]>]) = 0 <0.01>
9 sendto(4, ""..., 10, 0, NULL, 0) = 10 <0.02>
9 custom_kernel_call(3, 8) = 0 <0.03>
''', root_pid=9)
        self.assertEqual(self.entry(result, "sendto", "socket")["bytes_returned"], 10)
        self.assertEqual(self.entry(result, "custom_kernel_call", "unknown")["counts"], 1)

    def test_clone3_flags_and_numeric_flags_are_not_guessed(self):
        result = self.analyze(f'''20 open("{DB}", O_RDONLY) = 3<{DB}> <0.01>
20 clone3({{flags=CLONE_VM|CLONE_FILES|CLONE_THREAD, exit_signal=0}}, 88) = 21 <0.01>
21 read(3, ""..., 1) = 1 <0.01>
20 clone3({{flags=0x400, exit_signal=0}}, 88) = 22 <0.01>
22 read(3, ""..., 1) = 1 <0.01>
''', db_paths=[DB], root_pid=20)
        self.assertEqual(self.entry(result, "read", "db")["counts"], 1)
        self.assertEqual(self.entry(result, "read", "unknown")["counts"], 1)
        self.assertEqual(result["coverage"]["unknown_clone_flags"], 1)
        child = next(row for row in result["by_tid"] if row["tid"] == 22)
        self.assertIsNone(child["process_id"])

    def test_diagnostics_bounded_and_trace_with_payload_not_reproduced(self):
        result = self.analyze("unsupported line SECRET\n" * 105)
        self.assertEqual(result["coverage"]["unparsed_lines"], 105)
        self.assertEqual(len(result["diagnostics"]), 100)
        self.assertEqual(result["coverage"]["diagnostics_omitted"], 5)
        self.assertNotIn("SECRET", json.dumps(result))
        result = self.analyze('1 write(1, "PRIVATE PAYLOAD", 15) = 15 <0.01>\n', root_pid=1)
        self.assertNotIn("PRIVATE PAYLOAD", json.dumps(result))

    def test_negative_fcntl_owner_is_not_errno_and_anonymous_mmap_has_no_fd(self):
        result = self.analyze(f'''8 open("{DB}", O_RDONLY) = 3<{DB}> <0.01>
8 fcntl(3, F_GETOWN) = -42 <0.02>
8 mmap(NULL, 4096, PROT_READ, MAP_PRIVATE|MAP_ANONYMOUS, 3, 0) = 0x1234 <0.03>
''', db_paths=[DB], root_pid=8)
        self.assertEqual(self.entry(result, "fcntl", "db")["failures"], 0)
        self.assertEqual(self.entry(result, "mmap", "not_fd")["counts"], 1)

    def test_malformed_quoted_arguments_are_diagnosed(self):
        result = self.analyze('1 read(3, "UNTERMINATED, 8) = 8 <0.01>\n')
        self.assertEqual(result["status"], "invalid")
        self.assertEqual(result["coverage"]["unparsed_lines"], 1)
        self.assertEqual(result["diagnostics"][0]["code"], "unbalanced_arguments")

    def test_metadata_memory_wait_and_clock_allowlist_is_counted_not_silent(self):
        names = {"fstat", "newfstatat", "stat", "statx", "lseek", "madvise", "brk", "mprotect",
                 "poll", "ppoll", "select", "pselect6", "epoll_wait", "epoll_pwait",
                 "clock_gettime", "gettimeofday"}
        self.assertTrue(names.issubset(TRACE_SYSCALLS))
        # All synthetic. The 16 requested metadata/wait/clock names each have
        # actual fixture entries, not just an allowlist membership assertion.
        result = self.analyze(f'''40 open("{DB}", O_RDONLY) = 3<{DB}> <0.000001>
40 fstat(3, {{st_mode=S_IFREG|0600, st_size=4096}}) = 0 <0.000002>
40 newfstatat(AT_FDCWD, "relative.sqlite", {{st_mode=S_IFREG|0600}}, 0) = 0 <0.000003>
40 newfstatat(3, "", {{st_mode=S_IFREG|0600}}, AT_EMPTY_PATH) = 0 <0.000004>
40 stat("{DB}", {{st_mode=S_IFREG|0600}}) = 0 <0.000005>
40 statx(AT_FDCWD, "{DB}", AT_STATX_SYNC_AS_STAT, STATX_BASIC_STATS, {{stx_size=4096}}) = 0 <0.000006>
40 lseek(3, 0, SEEK_END) = 4096 <0.000007>
40 madvise(0x1234, 4096, MADV_DONTNEED) = 0 <0.000008>
40 brk(NULL) = 0x1234 <0.000009>
40 mprotect(0x1234, 4096, PROT_READ) = 0 <0.000010>
40 poll([{{fd=3<{DB}>, events=POLLIN}}, {{fd=5<TCP:[123]>, events=POLLOUT}}], 2, 0) = 1 <0.000011>
40 ppoll([{{fd=3, events=POLLIN}}], 1, {{tv_sec=0, tv_nsec=1}}, NULL, 8) = 0 <0.000012>
40 select(4, [3], NULL, NULL, {{tv_sec=0, tv_usec=0}}) = 1 <0.000013>
40 pselect6(4, [3], NULL, NULL, NULL, {{sigmask=NULL, sigsetsize=8}}) = 1 <0.000014>
40 epoll_wait(4<anon_inode:[eventpoll]>, [{{events=EPOLLIN, data={{u32=3, u64=3}}}}], 10, 0) = 1 <0.000015>
40 epoll_pwait(4, [], 10, 0, NULL, 8) = 0 <0.000016>
40 clock_gettime(CLOCK_MONOTONIC, {{tv_sec=100, tv_nsec=1}}) = 0 <0.000017>
40 gettimeofday({{tv_sec=1700000000, tv_usec=1}}, NULL) = 0 <0.000018>
''', db_paths=[DB], root_pid=40)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["coverage"]["completed_syscalls"], 18)
        self.assertTrue(names.issubset({row["name"] for row in result["by_name_filekind"]}))
        self.assertEqual(self.entry(result, "fstat", "db")["counts"], 1)
        self.assertEqual(self.entry(result, "lseek", "db")["counts"], 1)
        self.assertEqual(self.entry(result, "stat", "db")["counts"], 1)
        self.assertEqual(self.entry(result, "statx", "db")["counts"], 1)
        self.assertEqual(self.entry(result, "newfstatat", "unknown")["counts"], 1)
        self.assertEqual(self.entry(result, "newfstatat", "db")["counts"], 1)
        self.assertEqual(result["coverage"]["multi_fd_wait_calls"], 4)
        for name in ("poll", "ppoll", "select", "pselect6"):
            self.assertEqual(self.entry(result, name, "unknown")["counts"], 1)
        for name in ("epoll_wait", "epoll_pwait"):
            self.assertEqual(self.entry(result, name, "other")["counts"], 1)
        for name in ("madvise", "brk", "mprotect", "clock_gettime", "gettimeofday"):
            self.assertEqual(self.entry(result, name, "not_fd")["counts"], 1)
        self.assertEqual(result["totals"]["byte_calls"], 0)

    def test_cli_explicit_path_and_exclusive_output(self):
        with tempfile.TemporaryDirectory() as directory:
            trace, output = Path(directory) / "one.trace", Path(directory) / "summary.json"
            trace.write_text('1 fdatasync(3) = 0 <0.001>\n')
            self.assertEqual(main(["analyze", str(trace), "--output", str(output)]), 0)
            self.assertEqual(output.stat().st_mode & 0o777, 0o600)
            before = output.read_bytes()
            with patch("sys.stderr", new_callable=io.StringIO):
                self.assertEqual(main(["analyze", str(trace), "--output", str(output)]), 2)
            self.assertEqual(output.read_bytes(), before)
            with self.assertRaises(ValueError):
                analyze_trace(Path(directory) / "*.trace")
            with self.assertRaises(ValueError):
                analyze_trace(trace, root_pid=0)


class CapabilityAndLaunchTests(unittest.TestCase):
    def test_macos_never_executes_sampling_commands(self):
        with patch("pipeline_bench_syscalls.platform.system", return_value="Darwin"), \
             patch("pipeline_bench_syscalls.shutil.which") as which, \
             patch("pipeline_bench_syscalls.subprocess.run") as run:
            cap = preflight()
        self.assertFalse(cap.supported)
        self.assertEqual(cap.status, "unsupported")
        self.assertIn("macOS", cap.reason)
        which.assert_not_called()
        run.assert_not_called()

    def test_missing_strace_denial_invalid_flags_and_success(self):
        with patch("pipeline_bench_syscalls.platform.system", return_value="Linux"), \
             patch("pipeline_bench_syscalls.shutil.which", return_value=None):
            self.assertFalse(preflight().supported)
        for returncode, stderr, expected in ((1, "ptrace: Operation not permitted", False),
                                             (1, "invalid system call clone3", False), (0, "", True)):
            with self.subTest(stderr=stderr), \
                 patch("pipeline_bench_syscalls.platform.system", return_value="Linux"), \
                 patch("pipeline_bench_syscalls.shutil.which", return_value="/usr/bin/strace"), \
                 patch("pipeline_bench_syscalls.subprocess.run", return_value=subprocess.CompletedProcess([], returncode, "", stderr)) as run:
                cap = preflight()
                self.assertEqual(cap.supported, expected)
                self.assertIn("-s", run.call_args.args[0])
                self.assertIn("0", run.call_args.args[0])
                self.assertNotIn("-p", run.call_args.args[0])
                self.assertIn(os.devnull, run.call_args.args[0])
                self.assertNotIn("stderr", asdict(cap))

    def test_probe_exceptions_are_unsupported(self):
        for error in (OSError("unavailable"), subprocess.TimeoutExpired("strace", 5)):
            with patch("pipeline_bench_syscalls.platform.system", return_value="Linux"), \
                 patch("pipeline_bench_syscalls.shutil.which", return_value="/usr/bin/strace"), \
                 patch("pipeline_bench_syscalls.subprocess.run", side_effect=error):
                self.assertFalse(preflight().supported)

    def test_disabled_is_inert_and_unsupported_is_not_zero_success(self):
        with patch("pipeline_bench_syscalls.preflight") as probe:
            plan = prepare_launch(["/owned/benchmark", "run"], trace_path=Path("/unused/trace"), owned_binary=Path("/unused/binary"))
        self.assertEqual(plan.command, ("/owned/benchmark", "run"))
        self.assertIsNone(plan.trace_path)
        self.assertTrue(plan.wrapper_pid_is_target)
        probe.assert_not_called()
        unsupported = Capability(False, "unsupported", "permission denied")
        with self.assertRaises(UnsupportedError):
            prepare_launch(["/owned/benchmark"], trace_path=Path("/unused/trace"), owned_binary=Path("/unused/binary"), enabled=True, capability=unsupported)

    def test_opt_in_owned_binary_private_trace_and_no_attach_or_dump(self):
        cap = Capability(True, "supported", "mock probe only", "/usr/bin/strace", 0)
        with tempfile.TemporaryDirectory() as directory, \
             patch("pipeline_bench_syscalls.platform.system", return_value="Linux"):
            trace = Path(directory) / "owned.trace"
            # Construct argv only; this test does not run strace or a benchmark.
            binary = Path(sys.executable).resolve()
            plan = prepare_launch([str(binary), "--help"], trace_path=trace, owned_binary=binary, enabled=True, capability=cap)
            self.assertFalse(plan.wrapper_pid_is_target)
            self.assertEqual(plan.command[0], "/usr/bin/strace")
            self.assertIn("-f", plan.command)
            self.assertIn("raw=readv", " ".join(plan.command))
            self.assertNotIn("-p", plan.command)
            self.assertNotIn("-xx", plan.command)
            self.assertNotIn("read=", " ".join(plan.command))
            self.assertEqual(trace.stat().st_mode & 0o777, 0o600)
            self.assertEqual(trace.read_bytes(), b"")
            with self.assertRaises(FileExistsError):
                prepare_launch([str(binary)], trace_path=trace, owned_binary=binary, enabled=True, capability=cap)
            with self.assertRaises(ValueError):
                prepare_launch(["python"], trace_path=Path(directory) / "other.trace", owned_binary=binary, enabled=True, capability=cap)
            os.chmod(directory, 0o777)
            with self.assertRaises(ValueError):
                prepare_launch([str(binary)], trace_path=Path(directory) / "public.trace", owned_binary=binary, enabled=True, capability=cap)
            os.chmod(directory, 0o700)

    def test_supplied_capability_cannot_bypass_nonlinux(self):
        with patch("pipeline_bench_syscalls.platform.system", return_value="Darwin"), self.assertRaises(UnsupportedError):
            prepare_launch([sys.executable], trace_path=Path("/unused"), owned_binary=Path(sys.executable), enabled=True,
                           capability=Capability(True, "supported", "fake", "/strace", 0))

    def test_preflight_cli_unsupported_exit_status(self):
        with patch("pipeline_bench_syscalls.preflight", return_value=Capability(False, "unsupported", "test")), \
             patch("sys.stdout", new_callable=io.StringIO) as output:
            self.assertEqual(main(["preflight"]), 2)
            self.assertEqual(json.loads(output.getvalue())["status"], "unsupported")


if __name__ == "__main__":
    unittest.main()
