#!/usr/bin/env python3
"""Collation script for agentsoak Go tool runs.

Runs cmd/agentsoak multiple times, aggregates metrics, and emits a summary
JSON + CSV report. The Go tool enforces RBAC by default, so set
LONGBOW_RBAC_BOOTSTRAP_KEY (or pass -admin-api-key to the tool) before running.

The summary keeps the historical top-level columns and adds per-operation
QPS/error/p99/stall aggregates from each run's ``by_op_stats`` and ``stalls``.

Usage:
    python3 scripts/agent_soak_bench.py --runs 3 --agents 8 --duration 60
"""
import argparse
import json
import math
import os
import statistics
import subprocess
import time

REQUIRED_FIELDS = ("qps", "error_rate", "p99_ms", "ops", "errors")


def run_once(binary, args, out_path=None, timeout=None):
    if out_path is None:
        out_path = f"/tmp/opencode/agentsoak_{int(time.time() * 1000)}_{os.getpid()}.json"
    cmd = [binary] + list(args) + ["-json", out_path]
    t0 = time.time()
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    elapsed = time.time() - t0
    try:
        with open(out_path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        data = {"error": f"cannot read JSON report {out_path}: {e}", "stderr": proc.stderr[-500:]}
    data["_elapsed"] = elapsed
    data["_returncode"] = proc.returncode
    return data


def _number(value, label, default=0.0):
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be numeric, got {value!r}")
    value = float(value)
    if not math.isfinite(value):
        raise ValueError(f"{label} must be finite, got {value!r}")
    return value


def _stall_counts(run):
    """Extract per-op stall counts from Go's list or a Python integer."""
    counts = {}
    stalls = run.get("stalls", 0)
    if isinstance(stalls, dict):
        for op, value in stalls.items():
            counts[op] = int(_number(value, f"stalls[{op!r}]"))
    elif isinstance(stalls, list):
        for item in stalls:
            if not isinstance(item, dict) or "op" not in item:
                raise ValueError("stalls entries must be objects with an 'op' field")
            op = item["op"]
            counts[op] = counts.get(op, 0) + 1
    else:
        total = int(_number(stalls, "stalls"))
        if total:
            counts["total"] = total
    return counts


def validate_run(run, index):
    """Normalize one report, failing clearly on empty or malformed output."""
    label = f"run {index}"
    if not isinstance(run, dict):
        raise ValueError(f"{label} output must be a JSON object")
    if "error" in run:
        raise ValueError(f"{label} output is an error report: {run['error']}")
    for field in REQUIRED_FIELDS:
        if field not in run:
            raise ValueError(f"{label} output is missing required field {field!r}")
    normalized = {field: _number(run[field], f"{label}.{field}") for field in REQUIRED_FIELDS}
    by_op = run.get("by_op_stats", run.get("by_op", {}))
    if by_op is None:
        by_op = {}
    if not isinstance(by_op, dict):
        raise ValueError(f"{label}.by_op_stats must be an object")
    stats = {}
    for op, values in by_op.items():
        if isinstance(values, (int, float)):  # by_op is a plain op -> count map
            values = {"ops": values}
        if not isinstance(values, dict):
            raise ValueError(f"{label}.by_op_stats[{op!r}] must be an object")
        op_label = f"{label}.by_op_stats[{op!r}]"
        ops = _number(values.get("ops", 0), f"{op_label}.ops")
        errors = _number(values.get("errors", 0), f"{op_label}.errors")
        rate = values.get("error_rate", errors / ops if ops else 0.0)
        stats[op] = {
            "ops": ops,
            "errors": errors,
            "error_rate": _number(rate, f"{op_label}.error_rate"),
            "qps": _number(values.get("qps", 0), f"{op_label}.qps"),
            "p99_ms": _number(values.get("p99_ms", 0), f"{op_label}.p99_ms"),
            "stalls": _number(values.get("stalls", 0), f"{op_label}.stalls"),
        }
    normalized["by_op_stats"] = stats
    normalized["stall_counts"] = _stall_counts(run)
    normalized["cross_verified"] = _number(run.get("cross_verified", 0), f"{label}.cross_verified")
    normalized["raw"] = run
    return normalized


def summarize_runs(runs, agents=0, duration_s=0):
    """Aggregate validated runs; raises ValueError on empty/malformed input."""
    if not runs:
        raise ValueError("no runs to summarize (empty or unreadable reports)")
    validated = [validate_run(run, i + 1) for i, run in enumerate(runs)]
    qps = [r["qps"] for r in validated]
    err_rates = [r["error_rate"] for r in validated]
    p99s = [r["p99_ms"] for r in validated]
    stall_totals = [sum(r["stall_counts"].values()) for r in validated]
    summary = {
        "runs": len(validated),
        "agents": agents,
        "duration_s": duration_s,
        "qps_mean": statistics.mean(qps),
        "qps_min": min(qps),
        "qps_max": max(qps),
        "error_rate_mean": statistics.mean(err_rates),
        "error_rate_max": max(err_rates),
        "p99_ms_mean": statistics.mean(p99s),
        "p99_ms_max": max(p99s),
        "stalls_mean": statistics.mean(stall_totals),
        "stalls_max": max(stall_totals),
        "all_runs": [r["raw"] for r in validated],
    }
    op_names = sorted({op for r in validated for op in r["by_op_stats"]} |
                      {op for r in validated for op in r["stall_counts"] if op != "total"})
    by_op = {}
    for op in op_names:
        stats = [r["by_op_stats"].get(op, {}) for r in validated]
        stall_counts = [
            max(stats[i].get("stalls", 0), r["stall_counts"].get(op, 0))
            for i, r in enumerate(validated)
        ]
        by_op[op] = {
            "runs": len(validated),
            "ops_mean": statistics.mean([s.get("ops", 0) for s in stats]),
            "errors_mean": statistics.mean([s.get("errors", 0) for s in stats]),
            "qps_mean": statistics.mean([s.get("qps", 0) for s in stats]),
            "qps_min": min([s.get("qps", 0) for s in stats]),
            "qps_max": max([s.get("qps", 0) for s in stats]),
            "error_rate_mean": statistics.mean([s.get("error_rate", 0) for s in stats]),
            "error_rate_min": min([s.get("error_rate", 0) for s in stats]),
            "error_rate_max": max([s.get("error_rate", 0) for s in stats]),
            "p99_ms_mean": statistics.mean([s.get("p99_ms", 0) for s in stats]),
            "p99_ms_min": min([s.get("p99_ms", 0) for s in stats]),
            "p99_ms_max": max([s.get("p99_ms", 0) for s in stats]),
            "stalls_mean": statistics.mean(stall_counts),
            "stalls_min": min(stall_counts),
            "stalls_max": max(stall_counts),
            "stalls_total": sum(stall_counts),
        }
    summary["by_op"] = by_op
    return summary


def csv_columns(op_names):
    columns = ["run", "qps", "error_rate", "p99_ms", "ops", "errors", "cross_verified", "stalls"]
    for op in op_names:
        columns.extend([
            f"qps_{op}", f"error_rate_{op}", f"p99_ms_{op}", f"stalls_{op}",
        ])
    return columns


def write_csv(path, runs, op_names):
    validated = [validate_run(run, i + 1) for i, run in enumerate(runs)]
    with open(path, "w", encoding="utf-8") as f:
        f.write(",".join(csv_columns(op_names)) + "\n")
        for i, run in enumerate(validated):
            row = [
                i + 1, f"{run['qps']:.2f}", f"{run['error_rate']:.4f}",
                f"{run['p99_ms']:.2f}", int(run["ops"]), int(run["errors"]),
                int(run["cross_verified"]), sum(run["stall_counts"].values()),
            ]
            for op in op_names:
                stats = run["by_op_stats"].get(op, {})
                row.extend([
                    f"{stats.get('qps', 0):.3f}",
                    f"{stats.get('error_rate', 0):.4f}",
                    f"{stats.get('p99_ms', 0):.2f}",
                    int(max(stats.get("stalls", 0), run["stall_counts"].get(op, 0))),
                ])
            f.write(",".join(str(v) for v in row) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", default="./bin/agentsoak")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--agents", type=int, default=8)
    ap.add_argument("--duration", type=int, default=60, help="seconds")
    ap.add_argument("--dim", type=int, default=128)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out-dir", default="data/soak_bench")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    go_args = ["-agents", str(args.agents), "-duration", f"{args.duration}s",
               "-dim", str(args.dim), "-seed", str(args.seed), "-uri", "127.0.0.1:3000"]

    runs = []
    for i in range(args.runs):
        print(f"Run {i + 1}/{args.runs}...")
        r = run_once(args.binary, go_args)
        runs.append(r)
        try:
            validate_run(r, i + 1)
        except ValueError as e:
            ap.error(str(e))
        print(json.dumps({"qps": r.get("qps"), "error_rate": r.get("error_rate"),
                          "p99_ms": r.get("p99_ms")}))

    try:
        summary = summarize_runs(runs, args.agents, args.duration)
    except ValueError as e:
        ap.error(str(e))

    stamp = int(time.time())
    out_json = os.path.join(args.out_dir, f"summary_{stamp}.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    out_csv = os.path.join(args.out_dir, f"summary_{stamp}.csv")
    op_names = sorted(summary["by_op"])
    write_csv(out_csv, runs, op_names)
    print(f"\nSummary written to {out_json} and {out_csv}")
    print(json.dumps({k: v for k, v in summary.items() if k != "all_runs"}, indent=2))


if __name__ == "__main__":
    main()
