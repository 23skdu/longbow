#!/usr/bin/env python3
"""Index-time regression gate companion to ab_benchmark.py (roadmap Item 32).

Reads two perf_matrix (or baseline_emlgo) JSON files and reports percentage
deltas of ``ingest.indexing_duration_seconds`` and representative search QPS
across every (engine, scale, dtype) configuration common to both.

Exit codes:
  0  no delta exceeds the threshold
  1  at least one delta exceeds the threshold
  2  setup error (bad files, no shared configs)
"""

import argparse, json, sys


def index_by_config(path):
    """Map (engine, scale, dtype) -> {index: seconds, search_qps: {mode: float}}."""
    d = json.load(open(path))
    runs = d.get("runs", [d])
    out = {}
    for run in runs:
        for r in (run.get("results") or []):
            key = (run.get("mode", "?"), r.get("count"), r.get("dtype"))
            out[key] = {
                "index_time": r["ingest"]["indexing_duration_seconds"],
                "peak_rss": r["peak_memory_mb"],
                "dense_qps": (r.get("search") or {}).get("dense", {}).get("qps"),
            }
    return out


def pct(after, before):
    if before is None or before == 0:
        return float("nan")
    return ((after - before) / before) * 100.0


def main():
    ap = argparse.ArgumentParser(description="Per-dtype index-time/QPS regression gate, roadmap Item 32")
    ap.add_argument("--before", required=True)
    ap.add_argument("--after", required=True)
    ap.add_argument("--threshold", type=float, default=5.0, help="Allowed percentage drift (default 5)")
    ap.add_argument("--mode", choices=["index", "qps"], default="index",
                    help="Which metric gates — index time or dense QPS")
    args = ap.parse_args()

    try:
        a = index_by_config(args.before)
        b = index_by_config(args.after)
    except Exception as e:
        print("setup error:", e)
        sys.exit(2)

    common = sorted(set(a) & set(b))
    if not common:
        print("setup error: no shared (engine, scale, dtype) configurations")
        sys.exit(2)

    offending = []
    print("%-7s %10s %-14s %10s %10s %10s" % ("engine", "scale", "dtype", "before", "after", "delta%"))
    for key in common:
        ba, af = a[key], b[key]
        if args.mode == "index":
            before, after = ba["index_time"], af["index_time"]
        else:
            before, after = ba["dense_qps"], af["dense_qps"]
        delta = pct(after, before)
        flag = "REGRESS" if (args.mode == "index" and delta >= args.threshold) or \
                           (args.mode == "qps" and delta <= -args.threshold) else "ok"
        if flag == "REGRESS":
            offending.append((key, before, after, delta))
        print("%-7s %10s %-14s %10.2f %10.2f %+9.1f%%  %s" % (*key, before, after, delta, flag))

    print()
    print("shared configurations: %d  offending: %d" % (len(common), len(offending)))
    if offending:
        print("REGRESSION DETECTED")
        sys.exit(1)
    print("NO REGRESSION")
    sys.exit(0)


if __name__ == "__main__":
    main()