#!/usr/bin/env python3
"""Collation script for agentsoak Go tool runs.

Runs cmd/agentsoak multiple times, aggregates metrics, and emits a summary
JSON + CSV report. The Go tool enforces RBAC by default, so set
LONGBOW_RBAC_BOOTSTRAP_KEY (or pass -admin-api-key to the tool) before running.

Usage:
    python3 scripts/agent_soak_bench.py --runs 3 --agents 8 --duration 60
"""
import argparse, json, os, subprocess, sys, time, statistics

def run_once(binary, args):
    out_path = f"/tmp/opencode/agentsoak_{int(time.time()*1000)}.json"
    cmd = [binary] + args + ["-json", out_path]
    t0 = time.time()
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=args and None)
    elapsed = time.time() - t0
    try:
        with open(out_path) as f:
            data = json.load(f)
    except Exception as e:
        data = {"error": str(e), "stderr": proc.stderr[-500:]}
    data["_elapsed"] = elapsed
    data["_returncode"] = proc.returncode
    return data

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
        print(f"Run {i+1}/{args.runs}...")
        r = run_once(args.binary, go_args)
        runs.append(r)
        print(json.dumps(r, indent=2)[:500])

    # Aggregate
    qps = [r.get("qps", 0) for r in runs if "qps" in r]
    err_rates = [r.get("error_rate", 0) for r in runs if "error_rate" in r]
    p99s = [r.get("p99_ms", 0) for r in runs if "p99_ms" in r]
    summary = {
        "runs": len(runs),
        "agents": args.agents,
        "duration_s": args.duration,
        "qps_mean": statistics.mean(qps) if qps else 0,
        "qps_min": min(qps) if qps else 0,
        "qps_max": max(qps) if qps else 0,
        "error_rate_mean": statistics.mean(err_rates) if err_rates else 0,
        "error_rate_max": max(err_rates) if err_rates else 0,
        "p99_ms_mean": statistics.mean(p99s) if p99s else 0,
        "p99_ms_max": max(p99s) if p99s else 0,
        "all_runs": runs,
    }
    out_json = os.path.join(args.out_dir, f"summary_{int(time.time())}.json")
    with open(out_json, "w") as f:
        json.dump(summary, f, indent=2)
    # CSV
    out_csv = os.path.join(args.out_dir, f"summary_{int(time.time())}.csv")
    with open(out_csv, "w") as f:
        f.write("run,qps,error_rate,p99_ms,ops,errors,cross_verified\n")
        for i, r in enumerate(runs):
            f.write(f"{i+1},{r.get('qps',0):.2f},{r.get('error_rate',0):.4f},{r.get('p99_ms',0):.2f},{r.get('ops',0)},{r.get('errors',0)},{r.get('cross_verified',0)}\n")
    print(f"\nSummary written to {out_json} and {out_csv}")
    print(json.dumps({k: summary[k] for k in summary if k != "all_runs"}, indent=2))

if __name__ == "__main__":
    main()