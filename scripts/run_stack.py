#!/usr/bin/env python3
"""Start four configured services in the foreground; Ctrl-C stops owned children.

Redis must already be running. Each service parses the same INI itself; this
launcher uses --print-config for validation instead of a second INI dialect.
"""
import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from stack_runtime import OwnedProcesses, RunError, StopFlag, start_stack


def main():
    root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--build", type=Path, default=root / "build-release")
    parser.add_argument("--log-dir", type=Path)
    parser.add_argument("--ready-timeout", type=float, default=10.0)
    args = parser.parse_args()
    if not 0 < args.ready_timeout <= 120:
        parser.error("--ready-timeout must be in (0,120]")
    logs = (args.log_dir or Path(tempfile.mkdtemp(prefix="tickshelf-stack-"))).resolve()
    processes = OwnedProcesses(logs)
    result = 0
    # Keep handlers installed during wait/kill too: repeated Ctrl-C must not
    # abandon children that deliberately run in their own sessions.
    with StopFlag() as stop:
        try:
            start_stack(processes, args.build.resolve(), args.config.resolve(),
                        stop=stop, timeout=args.ready_timeout)
            print("STACK_READY " + json.dumps({"logs": str(logs),
                  "pids": {name: child.pid for name, child in processes.children.items()}},
                  separators=(",", ":")), flush=True)
            while not stop.requested:
                processes.require_alive(*processes.children)
                time.sleep(0.1)
        except InterruptedError:
            pass
        except (RunError, OSError, ValueError, subprocess.SubprocessError) as exc:
            print(f"run_stack: {exc}", file=sys.stderr)
            result = 1
        finally:
            processes.close()
            if processes.forced or any(child.returncode != 0 for child in processes.children.values()):
                print(f"run_stack: abnormal child exit; forced={processes.forced}", file=sys.stderr)
                result = 1
            print(f"run_stack: stopped; logs retained at {logs}", file=sys.stderr)
    return result


if __name__ == "__main__":
    sys.exit(main())
