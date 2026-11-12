"""Local process ownership and readiness helpers for startup and isolated drills.

No service discovery, shared-Redis cleanup, or port-based process termination.
"""
from __future__ import annotations

import json
import signal
import socket
import subprocess
import time
from pathlib import Path


class RunError(RuntimeError):
    pass


class StopFlag:
    def __init__(self):
        self.requested = False
        self.previous = {}

    def __enter__(self):
        for sig in (signal.SIGINT, signal.SIGTERM):
            self.previous[sig] = signal.signal(sig, self.handle)
        return self

    def handle(self, _sig, _frame):
        self.requested = True

    def check(self):
        if self.requested:
            raise InterruptedError("stop requested")

    def __exit__(self, *_):
        for sig, handler in self.previous.items():
            signal.signal(sig, handler)


class OwnedProcesses:
    def __init__(self, directory: Path):
        self.directory = directory
        directory.mkdir(parents=True, exist_ok=True)
        self.children = {}
        self.logs = {}
        self.forced = []

    def start(self, name, argv, *, env=None):
        if name in self.children:
            raise RunError(f"duplicate child name: {name}")
        log = (self.directory / f"{name}.log").open("xb")
        try:
            child = subprocess.Popen([str(a) for a in argv], stdout=log,
                                     stderr=subprocess.STDOUT, env=env,
                                     start_new_session=True)
        except BaseException:
            log.close()
            raise
        self.children[name] = child
        self.logs[name] = log
        return child

    def text(self, name):
        return (self.directory / f"{name}.log").read_text(errors="replace")

    def require_alive(self, *names):
        for name in names:
            rc = self.children[name].poll()
            if rc is not None:
                raise RunError(f"{name} exited {rc}; see {self.directory / (name + '.log')}")

    def stop(self, name, timeout=10.0):
        child = self.children[name]
        if child.poll() is None:
            child.send_signal(signal.SIGINT)
        try:
            rc = child.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.forced.append(name)
            child.kill()
            rc = child.wait(timeout=5)
        self.logs[name].close()
        return rc

    def close(self):
        # Stop ingestion first, then storage; never signal a PID not created here.
        order = [n for n in ("bridge", "tickshelfd", "evictor", "queryd", "persister")
                 if n in self.children]
        order += [n for n in reversed(self.children) if n not in order]
        for name in order:
            self.stop(name)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def wait_until(predicate, *, timeout=10.0, description="ready", processes=None,
               names=(), stop=None):
    end = time.monotonic() + timeout
    while True:
        if stop:
            stop.check()
        if processes:
            processes.require_alive(*names)
        if predicate():
            return
        if time.monotonic() >= end:
            raise RunError(f"timeout waiting for {description}")
        time.sleep(0.025)


class RedisConnection:
    """Small RESP reader for this harness's PING/INFO/GET observations only."""
    def __init__(self, host, port, timeout=1.0):
        self.socket = socket.create_connection((host, port), timeout=timeout)
        self.file = self.socket.makefile("rb")

    def read(self):
        line = self.file.readline()
        if not line.endswith(b"\r\n"):
            raise RunError("incomplete Redis response")
        prefix, body = line[:1], line[1:-2]
        if prefix == b"+":
            return body
        if prefix == b"-":
            raise RunError(f"Redis: {body.decode(errors='replace')}")
        if prefix == b":":
            return int(body)
        if prefix == b"$":
            size = int(body)
            if size == -1:
                return None
            if not 0 <= size <= 16 * 1024 * 1024:
                raise RunError("invalid Redis bulk size")
            data = self.file.read(size + 2)
            if len(data) != size + 2 or data[-2:] != b"\r\n":
                raise RunError("incomplete Redis bulk response")
            return data[:-2]
        if prefix == b"*":
            count = int(body)
            if count == -1:
                return None
            if not 0 <= count <= 100000:
                raise RunError("invalid Redis array size")
            return [self.read() for _ in range(count)]
        raise RunError("unknown Redis response")

    def command(self, *args):
        parts = [str(a).encode() for a in args]
        data = b"*%d\r\n" % len(parts)
        for part in parts:
            data += b"$%d\r\n" % len(part) + part + b"\r\n"
        self.socket.sendall(data)
        return self.read()

    def close(self):
        self.file.close()
        self.socket.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def info_fields(data):
    return dict(line.split(":", 1) for line in data.decode().splitlines()
                if line and not line.startswith("#") and ":" in line)


def candidate_ports(count):
    # Bind all candidates together. The executable must still prove successful
    # bind after release; choosing a free port is not an ownership guarantee.
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def start_private_redis(processes, binary, port, stop=None):
    redis_dir = processes.directory / "redis"
    redis_dir.mkdir()
    child = processes.start("redis", [binary, "--bind", "127.0.0.1", "--port", port,
                            "--protected-mode", "yes", "--save", "", "--appendonly", "no",
                            "--daemonize", "no", "--dir", redis_dir,
                            "--maxmemory", "512mb", "--maxmemory-policy", "noeviction"])

    def ready():
        try:
            with RedisConnection("127.0.0.1", port, timeout=0.2) as rc:
                server = info_fields(rc.command("INFO", "server"))
                if int(server["process_id"]) != child.pid:
                    raise RunError(f"port {port} belongs to another Redis; refusing reuse")
                return rc.command("PING") == b"PONG"
        except (OSError, ConnectionError):
            return False

    wait_until(ready, description="owned Redis", processes=processes,
               names=("redis",), stop=stop)


def resolved_configs(build: Path, config: Path):
    result = {}
    for name in ("persister", "queryd", "tickshelfd", "evictor"):
        command = [str(build / name), "--config", str(config), "--print-config"]
        done = subprocess.run(command, capture_output=True, text=True, timeout=5)
        if done.returncode:
            raise RunError(f"{name} config rejected: {done.stderr.strip()}")
        try:
            result[name] = json.loads(done.stdout)
        except (ValueError, TypeError) as exc:
            raise RunError(f"{name} did not return --print-config JSON") from exc
    if result["persister"]["once"]:
        raise RunError("run_stack requires persister once=false")
    redis = {(v["redis_host"], v["redis_port"]) for v in result.values()}
    storage = {(v["db"], v["shards"]) for v in result.values() if "db" in v}
    if len(redis) != 1 or len(storage) != 1:
        raise RunError("services disagree on Redis or storage layout")
    if result["tickshelfd"]["port"] == result["queryd"]["port"]:
        raise RunError("ingest and query ports must differ")
    return result


def start_stack(processes, build, config, *, stop=None, timeout=10.0):
    options = resolved_configs(build, config)
    redis = options["persister"]
    with RedisConnection(redis["redis_host"], redis["redis_port"]) as rc:
        if rc.command("PING") != b"PONG":
            raise RunError("Redis did not respond to PING")
    for name in ("persister", "queryd", "tickshelfd", "evictor"):
        child = processes.start(name, [build / name, "--config", config])
        if name == "persister":
            worker = f"worker={options[name]['worker']}-{child.pid};".encode()

            def ready():
                with RedisConnection(redis["redis_host"], redis["redis_port"]) as rc:
                    value = rc.command("GET", "stats:persister")
                    return value is not None and worker in value
        else:
            marker = "soft=" if name == "evictor" else "listening "

            def ready(name=name, marker=marker):
                return marker in processes.text(name)
        wait_until(ready, timeout=timeout, description=name, processes=processes,
                   names=tuple(processes.children), stop=stop)
    return options
