import os
import sys
import re
import json
import time
import math
import shutil
import signal
import socket
import hashlib
import platform
import subprocess
from datetime import datetime
from typing import Dict, Any, List, Optional

class ResourceExhaustedException(Exception):
    pass

class MemoryExceededException(Exception):
    """Raised when estimated memory exceeds the configured limit."""
    pass



def _kill_port(port):
    """Kill any process listening on the given port.

    Uses lsof on macOS, ss+fuser on Linux, and handles missing tools gracefully."""
    system = platform.system()
    if system == "Linux":
        # ss is universally available on modern Linux
        ss_res = subprocess.run(
            f"ss -tlnp 'sport = :{port}' 2>/dev/null",
            shell=True, capture_output=True, text=True, timeout=5
        )
        # Extract PIDs from ss output (format: users:(("foo",pid,fd),...))
        if ss_res.stdout:
            for match in re.finditer(r'pid=(\d+)', ss_res.stdout):
                pid = match.group(1)
                subprocess.run(f"kill -9 {pid} 2>/dev/null", shell=True, timeout=5)
        # fuser -k as backup
        subprocess.run(f"fuser -k {port}/tcp 2>/dev/null", shell=True, timeout=5)
    else:
        subprocess.run(
            f"lsof -ti:{port} 2>/dev/null | xargs -r kill -9 2>/dev/null || true",
            shell=True, timeout=5
        )


def run_command(cmd, env=None, capture_output=True, timeout=None, shell=False):
    import shlex
    import time
    try:
        if shell:
            args = cmd
        else:
            args = shlex.split(cmd)
            
        kwargs = {
            "env": env,
            "text": True,
            "shell": shell,
            "preexec_fn": os.setsid,  # Start in new session so child processes are tracked
        }
        if capture_output:
            kwargs["stdout"] = subprocess.PIPE
            kwargs["stderr"] = subprocess.PIPE
            
        process = subprocess.Popen(args, **kwargs)
        try:
            stdout, stderr = process.communicate(timeout=timeout)
            return subprocess.CompletedProcess(process.args, process.returncode, stdout, stderr)
        except subprocess.TimeoutExpired:
            print(f"  Command timed out after {timeout}s. Terminating gracefully...")
            # Send SIGTERM to the entire process group
            try:
                pgid = os.getpgid(process.pid)
                os.killpg(pgid, signal.SIGTERM)
            except (ProcessLookupError, OSError):
                process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                print("  Graceful termination failed. Killing process group...")
                try:
                    pgid = os.getpgid(process.pid)
                    os.killpg(pgid, signal.SIGKILL)
                except (ProcessLookupError, OSError):
                    process.kill()
                process.wait()
            return None
    except Exception as e:
        print(f"  Error running command: {e}")
        return None


def parse_bench_json(json_file):
    """Parse benchmark-tool JSON output to extract metrics."""
    try:
        with open(json_file) as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

    metrics = {}
    truncated_modes = []
    mode_orders = []
    if isinstance(data, list):
        for entry in data:
            name = entry.get("name", "")
            if entry.get("mode_order"):
                mode_orders = entry["mode_order"]
            if name == "DoPut":
                metrics["ingest_vec_per_sec"] = entry.get("throughput", 0)
                metrics["ingest_duration_seconds"] = entry.get("duration_seconds", 0)
                if entry.get("indexing_duration_seconds"):
                    metrics["indexing_duration_seconds"] = entry.get("indexing_duration_seconds", 0)
            elif name == "Indexing":
                metrics["indexing_duration_seconds"] = entry.get("duration_seconds", 0)
                metrics["indexing_vec_per_sec"] = entry.get("throughput", 0)
            elif name == "DoGet":
                metrics["get_vec_per_sec"] = entry.get("throughput", 0)
            elif name.startswith("Search_"):
                prefix = name.replace("Search_", "").lower()

                # R10: a mode whose own context budget expired was truncated.
                # Its throughput is a floor, not a measurement, and recording it
                # as a QPS number is how a truncated run becomes an apparent
                # regression. Skip it and report it separately.
                if entry.get("context_deadline_exceeded"):
                    truncated_modes.append({
                        "mode": name.replace("Search_", ""),
                        "requested": entry.get("queries_requested", 0),
                        "completed": entry.get("rows", 0),
                        "truncated": entry.get("queries_truncated", 0),
                    })
                    continue

                metrics[f"{prefix}_qps"] = entry.get("throughput", 0)
                metrics[f"{prefix}_mean_ms"] = entry.get("mean_latency_ms", 0)
                metrics[f"{prefix}_p50_ms"] = entry.get("p50_latency_ms", 0)
                metrics[f"{prefix}_p95_ms"] = entry.get("p95_latency_ms", 0)
                metrics[f"{prefix}_p99_ms"] = entry.get("p99_latency_ms", 0)
    elif isinstance(data, dict):
        metrics = data

    # R12: record the mode order that produced these numbers. A baseline taken
    # from 9 modes cannot be compared against a 13-mode run, because the modes
    # before the one under test decide its cache and GC state.
    if mode_orders:
        metrics["_mode_order"] = mode_orders
    if truncated_modes:
        metrics["_truncated_modes"] = truncated_modes

    return metrics


# Memory ceiling resolution (roadmap R14, H2/H3/H4, R14a).
#
# Three things were wrong with the ceiling before this. --memory was documented as
# "10GB" but typed as int bytes, so it could only be passed as 10737418240.
# LONGBOW_MAX_MEMORY is documented everywhere else as a size string, and
# int("18GB") raised ValueError, so --estimate-memory crashed whenever the
# variable was set. And start_server ignored both, reading a hardcoded 18 GiB, so
# the documented knob did nothing at all: on a 22 GiB host the default sat above
# the safe ceiling and the client was OOM-killed mid-indexing with no diagnostic.
#
# One parser, one resolution order, used everywhere.

_SIZE_UNITS = {
    "": 1,
    "B": 1,
    "K": 1024, "KB": 1024, "KIB": 1024,
    "M": 1024 ** 2, "MB": 1024 ** 2, "MIB": 1024 ** 2,
    "G": 1024 ** 3, "GB": 1024 ** 3, "GIB": 1024 ** 3,
    "T": 1024 ** 4, "TB": 1024 ** 4, "TIB": 1024 ** 4,
}

# Fraction of physical RAM used when neither --memory nor LONGBOW_MAX_MEMORY is
# given. The previous 18 GiB literal was an absolute constant that happened to be
# too high on a 22 GiB host and absurdly too low on a 512 GiB one.
DEFAULT_MEMORY_FRACTION = 0.60


