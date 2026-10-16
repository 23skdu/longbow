#!/usr/bin/env python3
"""Index-time regression gate companion to ab_benchmark.py (roadmap Item 32).

Reads two perf_matrix (or baseline_emlgo) JSON files, or an ab_benchmark verdict JSON,
and reports percentage deltas of ``ingest.indexing_duration_seconds`` and representative search QPS
across every (engine, scale, dtype) configuration.

Exit codes:
  0  no delta exceeds the threshold (clean / pass)
  1  at least one delta exceeds the threshold (regression detected)
  2  setup error (bad files, missing arguments, no shared configs)
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
            ingest = r.get("ingest") or {}
            idx_sec = ingest.get("indexing_duration_seconds")
            if idx_sec is None:
                idx_sec = ingest.get("duration_seconds", 0.0)
            out[key] = {
                "index_time": idx_sec,
                "peak_rss": r.get("peak_memory_mb", 0.0),
                "dense_qps": (r.get("search") or {}).get("dense", {}).get("qps"),
            }
    return out


def pct(after, before):
    if before is None or before == 0:
        return float("nan")
    return ((after - before) / before) * 100.0


def check_verdict_file(path, threshold, target_mode="index"):
    """Evaluate an ab_benchmark verdict JSON for index-time or QPS regressions."""
    d = json.load(open(path))
    rows = d.get("rows", [])
    if not rows:
        print("setup error: no rows found in verdict file")
        sys.exit(2)

    offending = []
    print("%-14s %10s %-14s %10s %10s %10s" % ("dtype", "scale", "mode", "median%", "spread", "status"))
    checked_count = 0
    for r in rows:
        mode = r.get("mode")
        if target_mode == "index" and mode != "index_time":
            continue
        if target_mode == "qps" and mode == "index_time":
            continue
        checked_count += 1
        dtype = r.get("dtype")
        count = r.get("count")
        median = r.get("median_pct", 0.0)
        spread = f"{r.get('min_pct', 0.0):+.0f}..{r.get('max_pct', 0.0):+.0f}%" if r.get("pairs", 0) > 1 else "n/a"
        verdict = r.get("verdict", "ok")
        is_regress = (verdict == "regression") or \
                     (mode == "index_time" and median >= threshold) or \
                     (mode != "index_time" and median <= -threshold)
        flag = "REGRESS" if is_regress else "ok"
        if is_regress:
            offending.append((dtype, count, mode, median, verdict))
        print("%-14s %10s %-14s %+9.1f%% %10s  %s" % (str(dtype), str(count), str(mode), median, spread, flag))

    print()
    print("evaluated configs: %d  offending: %d" % (checked_count, len(offending)))
    if checked_count == 0:
        print(f"setup error: no {target_mode} rows found to evaluate in verdict file")
        sys.exit(2)
    if offending:
        print("REGRESSION DETECTED")
        sys.exit(1)
    print("NO REGRESSION")
    sys.exit(0)


def main():
    ap = argparse.ArgumentParser(description="Per-dtype index-time/QPS regression gate, roadmap Item 32")
    ap.add_argument("--before", help="Baseline / before benchmark JSON")
    ap.add_argument("--after", help="Candidate / after benchmark JSON")
    ap.add_argument("--verdict", help="Interleaved A/B verdict JSON (ab_verdict.json)")
    ap.add_argument("--threshold", type=float, default=5.0, help="Allowed percentage drift (default 5.0)")
    ap.add_argument("--mode", choices=["index", "qps"], default="index",
                    help="Which metric gates — index time or dense QPS")
    args = ap.parse_args()

    threshold = abs(args.threshold)

    if args.verdict:
        try:
            check_verdict_file(args.verdict, threshold, args.mode)
        except SystemExit:
            raise
        except Exception as e:
            print("setup error:", e)
            sys.exit(2)
        return

    if not args.before or not args.after:
        print("setup error: specify either (--before AND --after) or --verdict")
        sys.exit(2)

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
        if before is None or after is None:
            print("%-7s %10s %-14s %10s %10s %10s  %s" % (*key, str(before), str(after), "n/a", "missing"))
            continue
        delta = pct(after, before)
        flag = "REGRESS" if (args.mode == "index" and delta >= threshold) or \
                           (args.mode == "qps" and delta <= -threshold) else "ok"
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