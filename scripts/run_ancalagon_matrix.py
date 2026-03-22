#!/usr/bin/env python3
"""Ancalagon (Linux) Performance Matrix Benchmark.
Fresh server per test, WAL clean before each start."""

import subprocess
import json
import time
import os
import shutil
import signal
import sys
from datetime import datetime
from pathlib import Path

DIMS = [128, 384]
DTYPES = [
    "float32",
    "float64",
    "int8",
    "int16",
    "int32",
    "uint32",
    "complex64",
    "complex128",
]
COUNTS = [1000, 5000, 10000, 25000]
QUERIES = 200
TIMEOUT = 900

SCRIPT_DIR = Path(__file__).parent
REPO_DIR = SCRIPT_DIR.parent
BIN_DIR = REPO_DIR / "bin"
DATA_DIR = REPO_DIR / "data" / "bench"
LOG_DIR = (
    REPO_DIR
    / "data"
    / "perf_logs"
    / f"ancalagon_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
)
WAL_FILE = REPO_DIR / "data" / "wal.log"
SNAPSHOTS_DIR = REPO_DIR / "data" / "snapshots"

LONGBOW_BIN = BIN_DIR / "longbow"
BENCH_TOOL = BIN_DIR / "benchmark-tool"
URI = "127.0.0.1:3000"
MEMORY = 12884901888  # 12GB

results = []
server_proc = None


def cleanup():
    global server_proc
    if server_proc:
        try:
            os.kill(server_proc.pid, signal.SIGKILL)
            server_proc.wait(timeout=5)
        except Exception:
            pass
        server_proc = None
    # Clean up orphaned longbow processes
    try:
        subprocess.run(["killall", "-9", "longbow"], capture_output=True, timeout=5)
    except Exception:
        pass


def start_server():
    global server_proc
    cleanup()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Clean WAL and snapshots - critical to avoid replay issues
    if WAL_FILE.exists():
        WAL_FILE.unlink()
    if SNAPSHOTS_DIR.exists():
        shutil.rmtree(SNAPSHOTS_DIR, ignore_errors=True)
        SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    for d in DATA_DIR.glob("node*"):
        if d.is_dir():
            shutil.rmtree(d, ignore_errors=True)
    for f in DATA_DIR.glob("*.log"):
        f.unlink()

    env = os.environ.copy()
    env["LONGBOW_MAX_MEMORY"] = str(MEMORY)
    env["ARROW_DISABLE_LOCKING"] = "1"
    env["PYTHONUNBUFFERED"] = "1"

    # Start server with nohup-style detachment
    proc = subprocess.Popen(
        [
            str(LONGBOW_BIN),
            "--listen-addr",
            URI,
            "--data-path",
            str(DATA_DIR),
            "--node-id",
            "bench1",
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    server_proc = proc

    # Wait for server ready
    for _ in range(30):
        time.sleep(1)
        try:
            r = subprocess.run(["lsof", "-i", ":3000"], capture_output=True, timeout=5)
            if b"LISTEN" in r.stdout or proc.poll() is None:
                time.sleep(1)
                return True
        except Exception:
            pass
    print(f"  WARNING: Server may not be ready (pid={proc.pid})")
    return True


def run_benchmark(dim, dtype, count):
    dataset = f"bench_{dtype}_{dim}_{count}"
    json_file = LOG_DIR / f"result_{dtype}_{dim}_{count}.json"
    log_file = f"/tmp/bench_{dtype}_{dim}_{count}.log"

    cmd = [
        str(BENCH_TOOL),
        "--uri",
        URI,
        "--dim",
        str(dim),
        "--dtype",
        dtype,
        "--scale",
        str(count),
        "--queries",
        str(QUERIES),
        "--dataset",
        dataset,
        "--json",
        str(json_file),
    ]

    start = time.time()
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=TIMEOUT)
    except subprocess.TimeoutExpired:
        print(f"    TIMEOUT after {TIMEOUT}s")
        return None

    elapsed = time.time() - start

    if result.returncode == 0 and json_file.exists():
        with open(json_file) as f:
            data = json.load(f)
        doput = next((r["throughput"] for r in data if r["name"] == "DoPut"), 0)
        dense = next((r["throughput"] for r in data if r["name"] == "Search_Dense"), 0)
        hybrid = next(
            (r["throughput"] for r in data if r["name"] == "Search_Hybrid"), 0
        )
        filtered = next(
            (r["throughput"] for r in data if r["name"] == "Search_Filtered"), 0
        )
        print(
            f"    DoPut: {doput:,.0f} | Dense: {dense:,.0f} | Hybrid: {hybrid:,.0f} | Filtered: {filtered:,.0f} | {elapsed:.0f}s"
        )
        return {"dtype": dtype, "dim": dim, "count": count, "results": data}
    else:
        print(f"    FAILED: {result.stderr[:200] if result.stderr else 'unknown'}")
        return None


def main():
    print(f"Ancalagon Performance Matrix — {datetime.now()}")
    print(
        f"Memory: {MEMORY / 1024**3:.0f}GB | Total: {len(DIMS) * len(DTYPES) * len(COUNTS)} configs"
    )

    import atexit

    atexit.register(cleanup)

    total = len(DIMS) * len(DTYPES) * len(COUNTS)
    run_num = 0

    for dim in DIMS:
        for dtype in DTYPES:
            for count in COUNTS:
                run_num += 1
                print(f"\n[{run_num}/{total}] {dtype} dim={dim} count={count:,}")
                start_server()
                entry = run_benchmark(dim, dtype, count)
                if entry:
                    results.append(entry)

    cleanup()

    # Save results
    out = LOG_DIR / "summary.json"
    with open(out, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nDONE. {len(results)}/{total} results -> {out}")


if __name__ == "__main__":
    main()
