#!/usr/bin/env python3
"""Full Performance Matrix — each test gets a fresh server to avoid OOM."""

import subprocess
import json
import time
import os
import signal
import sys
from datetime import datetime

DIMS = [128, 384]
DTYPES = ["float32", "float64"]
COUNTS = [1000, 5000, 10000, 25000]
URI = "127.0.0.1:3000"
BENCHMARK_TOOL = "./bin/benchmark-tool"
DATA_DIR = "/Users/rsd/REPOS/longbow/data"
LONGOW_BIN = "./bin/longbow"
QUERIES = 200

results = []
server_pid = None


def start_server():
    global server_pid
    subprocess.run(["rm", "-rf", f"{DATA_DIR}/node1/*"], shell=True)
    env = os.environ.copy()
    env["LONGBOW_MAX_MEMORY"] = "21474836480"
    env["ARROW_DISABLE_LOCKING"] = "1"
    proc = subprocess.Popen(
        [LONGOW_BIN, "--dir", DATA_DIR],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    server_pid = proc.pid
    time.sleep(3)
    print(f"    [server pid={server_pid}]")


def stop_server():
    global server_pid
    if server_pid:
        try:
            os.kill(server_pid, signal.SIGTERM)
            time.sleep(2)
        except ProcessLookupError:
            pass
        server_pid = None


def run_benchmark(dim, dtype, count, queries=QUERIES):
    dataset = f"bench_{dtype}_{dim}_{count}"
    cmd = [
        BENCHMARK_TOOL,
        "--uri",
        URI,
        "--dim",
        str(dim),
        "--dtype",
        dtype,
        "--scale",
        str(count),
        "--queries",
        str(queries),
        "--dataset",
        dataset,
    ]
    start = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    elapsed = time.time() - start

    if result.returncode != 0:
        print(f"    FAILED: {result.stderr[:200]}")
        return None

    output = result.stdout
    entry = {
        "dtype": dtype,
        "dim": dim,
        "count": count,
        "elapsed_s": round(elapsed, 1),
    }

    for line in output.split("\n"):
        line = line.strip()
        if "|" not in line:
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 4:
            continue
        name = parts[0].strip()
        if name in ("Name", ""):
            continue
        try:
            tput = float(parts[1].replace(",", ""))
        except ValueError:
            continue
        entry[name.lower().replace(" ", "_").replace("search_", "search_")] = tput

    return entry


def main():
    print("=" * 70)
    print("FULL PERFORMANCE MATRIX (Go Benchmark Tool, 20GB single node)")
    print(f"Started: {datetime.now()}")
    print("=" * 70)

    total = len(DIMS) * len(DTYPES) * len(COUNTS)
    run_num = 0

    for dim in DIMS:
        for dtype in DTYPES:
            for count in COUNTS:
                run_num += 1
                label = f"{dtype} dim={dim} count={count:,}"
                print(f"\n[{run_num}/{total}] {label}")

                stop_server()
                time.sleep(1)
                start_server()

                entry = run_benchmark(dim, dtype, count)
                if entry:
                    results.append(entry)
                    doget = entry.get("doget", 0)
                    dense_qps = entry.get("search_dense", 0)
                    idx_s = entry.get("indexing_s", 0)
                    print(
                        f"    DoGet: {doget:,.0f} vec/s | Dense QPS: {dense_qps:,.0f} | Index: {idx_s:.1f}s"
                    )
                else:
                    print(f"    FAILED — skipping")

    stop_server()

    out = f"benchmark_results_matrix_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(out, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nSaved to {out}")
    print(f"Completed: {datetime.now()}")

    print("\n" + "=" * 70)
    print(
        f"{'DType':<10} {'Dim':<5} {'Count':<8} {'DoGet vec/s':<14} {'Dense QPS':<12} {'Index s':<10} {'Elapsed s'}"
    )
    print("-" * 70)
    for r in results:
        print(
            f"{r['dtype']:<10} {r['dim']:<5} {r['count']:<8} "
            f"{r.get('doget', 0):<14,.0f} "
            f"{r.get('search_dense', 0):<12,.0f} "
            f"{r.get('indexing_s', 0):<10.1f} "
            f"{r.get('elapsed_s', 0):.1f}"
        )


if __name__ == "__main__":
    try:
        main()
    finally:
        stop_server()
