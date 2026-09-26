#!/usr/bin/env python3
"""check_regression.py - Compare benchmark results against a baseline.

Fails if any configuration regresses beyond a configurable threshold.

Usage:
    python3 scripts/check_regression.py \\
        --baseline benchmarks/baseline_cpu.json \\
        --results data/perf_logs/perf_matrix_latest.json \\
        --threshold 10

Consolidated multi-variant baselines (benchmarks/baseline_matrix.json) can be
compared per build variant:

    python3 scripts/check_regression.py \\
        --baseline benchmarks/baseline_matrix.json \\
        --results data/perf_logs/perf_matrix_cpu_cpu_std_nodisk_*.json \\
        --variant cpu_std_nodisk

Exit codes:
    0 = no regressions beyond threshold
    1 = regressions detected
    2 = missing files or parse errors
"""

import argparse
import json
import sys
from pathlib import Path


def load_json(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def match_config(baseline_cfg: dict, result_cfg: dict) -> bool:
    # Consolidated baselines (benchmarks/baseline_matrix.json) tag every entry
    # with the build/disk variant it came from; only compare like with like.
    b_variant = baseline_cfg.get("variant")
    r_variant = result_cfg.get("variant")
    if b_variant is not None and r_variant is not None and b_variant != r_variant:
        return False
    return (
        baseline_cfg.get("dim") == result_cfg.get("dim")
        and baseline_cfg.get("dtype") == result_cfg.get("dtype")
        and baseline_cfg.get("count") == result_cfg.get("count")
    )


def check_regressions(baseline: dict, results: dict, threshold: float, variant: str | None = None) -> list:
    regressions = []
    baseline_configs = baseline.get("configs", baseline.get("results", []))
    if variant:
        baseline_configs = [
            cfg for cfg in baseline_configs if cfg.get("variant", variant) == variant
        ]
    result_configs = results.get("configs", results.get("results", []))

    for b_cfg in baseline_configs:
        for r_cfg in result_configs:
            if not match_config(b_cfg, r_cfg):
                continue

            b_search = b_cfg.get("search", {})
            r_search = r_cfg.get("search", {})

            for mode, b_data in b_search.items():
                r_data = r_search.get(mode)
                if not r_data:
                    continue

                b_qps = b_data.get("qps", 0)
                r_qps = r_data.get("qps", 0)

                if b_qps <= 0:
                    continue

                change_pct = ((r_qps - b_qps) / b_qps) * 100

                if change_pct < -threshold:
                    regressions.append({
                        "dim": b_cfg.get("dim"),
                        "dtype": b_cfg.get("dtype"),
                        "count": b_cfg.get("count"),
                        "mode": mode,
                        "baseline_qps": b_qps,
                        "result_qps": r_qps,
                        "change_pct": round(change_pct, 1),
                    })

            # Check ingest
            b_ingest = b_cfg.get("ingest", {}).get("vec_per_sec", 0)
            r_ingest = r_cfg.get("ingest", {}).get("vec_per_sec", 0)
            if b_ingest > 0:
                ingest_change = ((r_ingest - b_ingest) / b_ingest) * 100
                if ingest_change < -threshold:
                    regressions.append({
                        "dim": b_cfg.get("dim"),
                        "dtype": b_cfg.get("dtype"),
                        "count": b_cfg.get("count"),
                        "mode": "ingest",
                        "baseline_qps": b_ingest,
                        "result_qps": r_ingest,
                        "change_pct": round(ingest_change, 1),
                    })

    return regressions


def main():
    parser = argparse.ArgumentParser(description="Check benchmark regressions")
    parser.add_argument("--baseline", required=True, help="Path to baseline JSON")
    parser.add_argument("--results", required=True, help="Path to results JSON")
    parser.add_argument("--threshold", type=float, default=10.0,
                        help="Regression threshold percentage (default: 10)")
    parser.add_argument("--variant", default=None,
                        help="Compare only this build variant of a consolidated baseline "
                             "(e.g. cpu_std_nodisk, gpu_emlgo_disk)")
    args = parser.parse_args()

    if not Path(args.baseline).exists():
        print(f"ERROR: baseline file not found: {args.baseline}")
        sys.exit(2)

    if not Path(args.results).exists():
        print(f"ERROR: results file not found: {args.results}")
        sys.exit(2)

    try:
        baseline = load_json(args.baseline)
        results = load_json(args.results)
    except json.JSONDecodeError as e:
        print(f"ERROR: failed to parse JSON: {e}")
        sys.exit(2)

    regressions = check_regressions(baseline, results, args.threshold, args.variant)

    if regressions:
        print(f"FAIL: {len(regressions)} regression(s) detected (threshold: {args.threshold}%)\n")
        print(f"{'Dim':<6} {'Dtype':<12} {'Count':<8} {'Mode':<12} {'Baseline':<12} {'Result':<12} {'Change':<10}")
        print("-" * 80)
        for r in regressions:
            print(
                f"{r['dim']:<6} {r['dtype']:<12} {r['count']:<8} {r['mode']:<12} "
                f"{r['baseline_qps']:<12.1f} {r['result_qps']:<12.1f} {r['change_pct']:<+10.1f}%"
            )
        sys.exit(1)
    else:
        print(f"PASS: no regressions beyond {args.threshold}% threshold")
        sys.exit(0)


if __name__ == "__main__":
    main()
