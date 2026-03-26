#!/usr/bin/env python3
"""Full Performance Matrix — Go Benchmark Tool.
Each test gets a fresh server to avoid OOM/GC interference.
Outputs JSON + Markdown summary.

Usage:
    python3 scripts/run_perf_matrix.py [--memory 12884901888] [--dims 128,384] [--counts 1000,5000,10000,25000]
"""

import subprocess
import json
import time
import os
import shutil
import signal
import sys
import platform
from datetime import datetime
from pathlib import Path

# Defaults
DIMS = [128, 384]
DTYPES = [
    "float32",
    "int8",
    "uint32",
    "complex64",
    "turboquant",
]
COUNTS = [1000, 3000, 5000, 10000, 15000, 25000]
QUERIES = 500
TIMEOUT = 600  # seconds per test

SCRIPT_DIR = Path(__file__).parent
REPO_DIR = SCRIPT_DIR.parent
BIN_DIR = REPO_DIR / "bin"
DATA_DIR = REPO_DIR / "data" / "bench"
LOG_DIR = REPO_DIR / "data" / "perf_logs"

LONGBOW_BIN = BIN_DIR / "longbow"
BENCH_TOOL = BIN_DIR / "benchmark-tool"
URI = "127.0.0.1:3000"
PORT = 3000

# Platform detection
IS_LINUX = platform.system() == "Linux"
IS_MACOS = platform.system() == "Darwin"
PLATFORM = "linux" if IS_LINUX else "macos"


# Parse CLI args
def parse_args():
    memory = 21474836480  # 20GB
    dims = DIMS
    counts = COUNTS
    binary = str(LONGBOW_BIN)
    for arg in sys.argv[1:]:
        if arg.startswith("--memory="):
            memory = int(arg.split("=")[1])
        elif arg.startswith("--dims="):
            dims = [int(x) for x in arg.split("=")[1].split(",")]
        elif arg.startswith("--counts="):
            counts = [int(x) for x in arg.split("=")[1].split(",")]
        elif arg.startswith("--binary="):
            binary = arg.split("=")[1]
    return memory, dims, counts, binary


results = []
server_pid = None


def cleanup():
    global server_pid
    if server_pid:
        try:
            os.kill(server_pid, signal.SIGKILL)
            time.sleep(1)
        except (ProcessLookupError, OSError):
            pass
        server_pid = None
    
    # AGGRESSIVE: kill any orphaned longbow processes
    subprocess.run(["pkill", "-9", "longbow"], capture_output=True)
    time.sleep(1)


def start_server(memory_bytes, binary_path):
    global server_pid
    # WIPE DATA for each clean run
    if DATA_DIR.exists():
        shutil.rmtree(DATA_DIR)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Clean data
    for d in DATA_DIR.glob("node*"):
        if d.is_dir():
            shutil.rmtree(d, ignore_errors=True)

    env = os.environ.copy()
    env["LONGBOW_MAX_MEMORY"] = str(memory_bytes)
    env["LONGBOW_LISTEN_ADDR"] = URI
    env["LONGBOW_DATA_PATH"] = str(DATA_DIR)
    env["LONGBOW_NODE_ID"] = "bench1"
    env["ARROW_DISABLE_LOCKING"] = "1"
    
    # VERIFY port is clean
    r = subprocess.run(["lsof", "-i", f":{PORT}"], capture_output=True)
    if b"LISTEN" in r.stdout:
        print(f"  WARNING: Port {PORT} still in use after cleanup! Force killing again.")
        subprocess.run(["pkill", "-9", "longbow"], capture_output=True)
        time.sleep(2)

    cmd = [binary_path]

    server_log = open(LOG_DIR / "server.log", "a")
    proc = subprocess.Popen(
        cmd,
        env=env,
        stdout=server_log,
        stderr=server_log,
    )
    server_pid = proc.pid

    # Wait for server ready
    for i in range(30):
        time.sleep(1)
        try:
            r = subprocess.run(["lsof", "-i", f":{PORT}"], capture_output=True, timeout=5)
            if b"LISTEN" in r.stdout:
                if i > 0:
                    time.sleep(1) # Settle
                return True
        except Exception:
            pass
    
    if proc.poll() is not None:
         print(f"  ERROR: Server crashed on startup (pid={server_pid})")
         return False
         
    print(f"  WARNING: Server may not be ready (pid={server_pid})")
    return True


def stop_server():
    cleanup()


def capture_pprof(dtype, dim, count, tag):
    """Capture CPU and Heap profiles."""
    try:
        profile_name = f"{tag}_{dtype}_{dim}_{count}"
        cpu_file = LOG_DIR / f"cpu_{profile_name}.pprof"
        heap_file = LOG_DIR / f"heap_{profile_name}.pprof"
        
        # Capture 5s CPU profile during search or just after
        subprocess.run(["curl", "-s", "-o", str(cpu_file), f"http://localhost:3000/debug/pprof/profile?seconds=5"], timeout=10)
        # Capture heap
        subprocess.run(["curl", "-s", "-o", str(heap_file), f"http://localhost:3000/debug/pprof/heap"], timeout=5)
    except Exception as e:
        print(f"    PPROF Capture Failed: {e}")


def run_benchmark(dim, dtype, count, queries=QUERIES):
    dataset = f"bench_{dtype}_{dim}_{count}"
    json_file = str(LOG_DIR / f"result_{dtype}_{dim}_{count}.json")

    cmd = [
        str(BENCH_TOOL),
        "--uri",
        str(URI),
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
        "--json",
        json_file,
    ]

    start_time = time.time()
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=TIMEOUT)
    except subprocess.TimeoutExpired:
        print(f"    TIMEOUT after {TIMEOUT}s")
        return None

    elapsed = time.time() - start_time

    if result.returncode != 0:
        err_msg = result.stderr[:200] if result.stderr else "unknown"
        print(f"    FAILED: {err_msg}")
        return None

    # Parse JSON output
    if os.path.exists(json_file):
        try:
            with open(json_file) as f:
                data = json.load(f)
            return {
                "results": data,
                "elapsed_s": elapsed,
                "dtype": dtype,
                "dim": dim,
                "count": count,
            }
        except Exception:
            pass

    # Fallback: parse stdout
    output = result.stdout
    results_list = []

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
        except (ValueError, IndexError):
            continue
        
        entry = {"name": name, "throughput": tput}
        try:
            entry["throughput_mbs"] = float(parts[2].replace(",", ""))
        except (ValueError, IndexError):
            pass
        results_list.append(entry)

    return {
        "results": results_list,
        "elapsed_s": elapsed,
        "dtype": dtype,
        "dim": dim,
        "count": count,
    }


def extract_column(json_results, name):
    """Extract a specific metric from JSON BenchmarkResult list."""
    if not json_results:
        return 0
    # Search name or lowercase key (for fallback)
    target_key = name.lower().replace(" ", "_")
    for r in json_results:
        if isinstance(r, dict):
            if r.get("name") == name or r.get("Name") == name:
                return r.get("throughput", r.get("Throughput", 0))
            if target_key in r:
                return r[target_key]
    return 0


def generate_markdown(results, memory_bytes, platform_str):
    """Generate Markdown table from results."""
    lines = []
    lines.append(f"# Performance Results — {platform_str}")
    lines.append("")
    lines.append(f"**Date**: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"**Memory**: {memory_bytes / (1024**3):.0f}GB allocated")
    lines.append(f"**Platform**: {platform_str} ({platform.machine()})")
    lines.append("")

    # Group by dtype
    for dtype in DTYPES:
        dtype_results = [r for r in results if r.get("dtype") == dtype]
        if not dtype_results:
            continue

        lines.append(f"## {dtype}")
        lines.append("")
        lines.append(
            "| Dim | Count | DoPut (vec/s) | DoPut (MB/s) | DoGet (vec/s) | DoGet (MB/s) | Index (s) | Dense QPS | Dense P50 | Hybrid QPS | Filtered QPS | SearchByID QPS |"
        )
        lines.append(
            "|-----|-------|---------------|--------------|---------------|--------------|-----------|-----------|-----------|------------|--------------|----------------|"
        )

        for r in sorted(dtype_results, key=lambda x: (x["dim"], x["count"])):
            dim = r["dim"]
            count = r["count"]
            res = r.get("results", [])

            doput_vs = extract_column(res, "DoPut")
            doput_mbs = next(
                (
                    x.get("ThroughputMBs", 0)
                    for x in res
                    if isinstance(x, dict) and x.get("Name") == "DoPut"
                ),
                0,
            )
            doget_vs = extract_column(res, "DoGet")
            doget_mbs = next(
                (
                    x.get("ThroughputMBs", 0)
                    for x in res
                    if isinstance(x, dict) and x.get("Name") == "DoGet"
                ),
                0,
            )
            dense_qps = extract_column(res, "Search_Dense")
            dense_p50 = next(
                (
                    x.get("P50LatencyMs", 0)
                    for x in res
                    if isinstance(x, dict) and x.get("Name") == "Search_Dense"
                ),
                0,
            )
            hybrid_qps = extract_column(res, "Search_Hybrid")
            filtered_qps = extract_column(res, "Search_Filtered")
            search_by_id_qps = extract_column(res, "Search_ByID")
            idx_s = r.get("elapsed_s", 0) - next(
                (
                    x.get("DurationSeconds", 0)
                    for x in res
                    if isinstance(x, dict) and x.get("Name") in ("DoPut", "DoGet")
                ),
                0,
            )

            lines.append(
                f"| {dim} | {count:,} | {doput_vs:,.0f} | {doput_mbs:,.0f} | {doget_vs:,.0f} | {doget_mbs:,.0f} | {idx_s:.1f} | {dense_qps:,.0f} | {dense_p50:.2f}ms | {hybrid_qps:,.0f} | {filtered_qps:,.0f} | {search_by_id_qps:,.0f} |"
            )

        lines.append("")

    return "\n".join(lines)


def main():
    memory, dims, counts, binary = parse_args()

    print("=" * 70)
    print(
        f"FULL PERFORMANCE MATRIX ({PLATFORM}, {platform.machine()}, {memory / 1024**3:.0f}GB)"
    )
    print(f"Started: {datetime.now()}")
    print(f"DIMS={dims} COUNTS={counts} DTYPES={DTYPES}")
    print(f"Total configs: {len(dims) * len(counts) * len(DTYPES)}")
    print("=" * 70)

    total = len(dims) * len(counts) * len(DTYPES)
    run_num = 0

    import atexit
    atexit.register(cleanup)

    # Clean legacy logs and data
    if LOG_DIR.exists():
        shutil.rmtree(LOG_DIR)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    if DATA_DIR.exists():
        shutil.rmtree(DATA_DIR)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    for dim in dims:
        for dtype in DTYPES:
            for count in counts:
                run_num += 1
                label = f"{dtype} dim={dim} count={count:,}"
                print(f"\n[{run_num}/{total}] {label}")

                stop_server()
                time.sleep(1)
                start_server(memory, binary)
                time.sleep(2)

                entry = run_benchmark(dim, dtype, count)
                if entry:
                    results.append(entry)
                    if count >= 15000:
                        print(f"    Capturing pprof for scale {count}...")
                        tag = "cpu" if "metal" not in binary.lower() else "metal"
                        capture_pprof(dtype, dim, count, tag)
                    
                    doput = extract_column(entry.get("results", []), "DoPut")
                    dense = extract_column(entry.get("results", []), "Search_Dense")
                    elapsed = entry.get("elapsed_s", 0)
                    print(
                        f"    DoPut: {doput:,.0f} vec/s | Dense: {dense:,.0f} QPS | {elapsed:.0f}s"
                    )
                else:
                    print(f"    FAILED")

    stop_server()

    # Save JSON
    out_json = str(
        LOG_DIR
        / f"perf_matrix_{PLATFORM}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )
    with open(out_json, "w") as f:
        json.dump(results, f, indent=2, default=str)

    # Save Markdown
    md = generate_markdown(results, memory, PLATFORM)
    out_md = str(
        LOG_DIR
        / f"perf_matrix_{PLATFORM}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    )
    with open(out_md, "w") as f:
        f.write(md)

    print(f"\nJSON saved to: {out_json}")
    print(f"Markdown saved to: {out_md}")
    print(f"Completed: {datetime.now()}")


if __name__ == "__main__":
    try:
        main()
    finally:
        cleanup()
