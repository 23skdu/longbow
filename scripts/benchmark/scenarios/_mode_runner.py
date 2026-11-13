"""Shared benchmark helper for specialized search mode scenarios."""

from datetime import datetime
import json
import os
import time

try:
    import numpy as np

    HAS_ANALYSIS_LIBS = True
except ImportError:
    HAS_ANALYSIS_LIBS = False

try:
    from longbow import LongbowClient

    HAS_LONGBOW_SDK = True
except ImportError:
    HAS_LONGBOW_SDK = False

from ..process import parse_bench_json, run_command


def connect_sdk_client(server_addr):
    """Create and connect a LongbowClient for the running server."""
    host_port = server_addr.split(":")
    meta_port = int(host_port[-1]) + 1
    client = LongbowClient(
        uri=f"grpc://{server_addr}",
        meta_uri=f"grpc://127.0.0.1:{meta_port}",
    )
    client.connect()
    return client


def summarize_latencies(latencies_ms, total_elapsed_s):
    """Compute QPS and percentile summary (in ms) from query latencies."""
    if not latencies_ms or total_elapsed_s <= 0:
        return {"qps": 0.0, "mean_ms": 0.0, "p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0}
    arr = sorted(latencies_ms)
    n = len(arr)
    return {
        "qps": n / total_elapsed_s,
        "mean_ms": sum(arr) / n,
        "p50_ms": arr[n // 2],
        "p95_ms": arr[min(int(n * 0.95), n - 1)],
        "p99_ms": arr[min(int(n * 0.99), n - 1)],
    }


def run_bench_tool_mode_fallback(orchestrator, label, search_mode, dim, count):
    """Fallback runner invoking bin/bench-tool for a single search mode."""
    bench_tool = orchestrator.get_bench_tool_path()
    if not os.path.exists(bench_tool):
        print(f"  bench-tool not found at {bench_tool}; skipping {search_mode} fallback.")
        return None
    json_file = os.path.join(orchestrator.log_dir, f"{label}_{search_mode}.json")
    uri = orchestrator.server_addr
    if not uri.startswith("grpc://"):
        uri = f"grpc://{uri}"
    queries = getattr(orchestrator.args, "queries", 100)
    workers = getattr(orchestrator.args, "workers", 4)
    cmd = (
        f"{bench_tool} -mode vec -uri {uri} -dim {dim} -dtype float32 "
        f"-scale {count} -queries {queries} -workers {workers} "
        f"-dataset {label} -json {json_file} -search-modes {search_mode}"
    )
    run_command(cmd, timeout=getattr(orchestrator.args, "timeout", 600))
    return parse_bench_json(json_file)


def run_scenario_matrix(orchestrator, scenario_name, title, bench_mode, query_runner):
    """Execute a search-mode scenario across dims/counts with SDK + bench-tool fallback."""
    dims = [int(d) for d in orchestrator.args.dims.split(",")]
    counts = [int(c) for c in orchestrator.args.counts.split(",")]

    print("=" * 80)
    print(f"{title}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Dims: {dims}, Counts: {counts}")
    print("=" * 80)

    for count in counts:
        for dim in dims:
            label = f"{scenario_name}_float32_{dim}_{count}"
            if not orchestrator.start_server(label):
                print(f"  Failed to start server for {label}!")
                continue
            try:
                if HAS_LONGBOW_SDK and HAS_ANALYSIS_LIBS:
                    client = connect_sdk_client(orchestrator.server_addr)
                    try:
                        row = query_runner(client, orchestrator, dim, count, label)
                    finally:
                        client.close()
                else:
                    metrics = run_bench_tool_mode_fallback(
                        orchestrator, label, bench_mode, dim, count
                    )
                    row = {
                        "scenario": scenario_name,
                        "dim": dim,
                        "count": count,
                        "dtype": "float32",
                        "runner": "bench-tool",
                        "metrics": metrics or {},
                    }
                if row:
                    orchestrator.results.append(row)
                    print(f"  {label}: {json.dumps(row, default=str)}")
            finally:
                orchestrator.stop_server()
