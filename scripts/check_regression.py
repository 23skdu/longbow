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

Both inputs are first checked against qps * mean_latency_ms ~= workers. A report that
violates it cannot gate a regression, because its QPS and latency are not true of
its own recorded worker count. Use --skip-self-validation to override.

Exit codes:
    0 = no regressions beyond threshold
    1 = regressions detected, or a report failed self-validation
    2 = missing files or parse errors
"""

import argparse
import json
import sys
from pathlib import Path


def load_json(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def _worker_count(doc: dict) -> int | None:
    """Recover the worker count a report was produced with (roadmap R2).

    Newer reports carry it in provenance; older ones only have it in the
    validation block the writer now emits. Returns None when the report predates
    provenance, which is itself worth reporting.
    """
    prov = doc.get("provenance") or {}
    if prov.get("workers"):
        return int(prov["workers"])
    validation = doc.get("validation") or {}
    if validation.get("workers"):
        return int(validation["workers"])
    return None


def _self_validation_violations(doc: dict, label: str) -> tuple:
    """Re-run the QPS x mean_latency ~= workers identity over a report.

    Returns (violations, unverifiable). A violation is a row that is impossible
    given the worker count the report itself records, and it is a hard failure. A
    report with no worker count at all is unverifiable rather than wrong: there is
    nothing to be inconsistent with, and the fix is R2 (record provenance), not a
    rejection of the run.

    check_regression compares two reports and reports the difference. It cannot
    tell you that either one is impossible on its own terms - a baseline whose QPS
    and latency were produced under a different worker count than it records makes
    every percentage it produces meaningless - so the invariant is re-checked here
    as well, at the point where someone is about to act on the comparison.

    Throughput is completed/duration and mean latency is sum/completed, so
    QPS x mean = sum(latencies)/wall_duration, which equals the worker count only
    if the workers were busy for the whole window. They are never quite 100% busy -
    start-up and the final in-flight query both cost a little - so this is a band:
    implied cannot exceed the worker count, and must be at least 75% of it. Across
    all 13 search modes at 20k on 4 workers the implied count ran 3.66-3.96 against
    a recorded 4. This checks the mean, not P50: a percentile carries no such bound,
    and using P50 made this fire on any right-skewed latency distribution and reject
    valid reports. Kept in step with validate_concurrency_invariant in
    unified_benchmark.py, which owns the canonical version; it is duplicated rather
    than imported because that module pulls in optional dependencies this tool must
    not require.

    Rows with no recorded mean fall back to P50 and are reported as unverifiable
    rather than judged on a bound they cannot support.
    """
    violations = []
    unverifiable = []
    for key in ("results", "configs"):
        rows = doc.get(key)
        if not isinstance(rows, list) or not rows:
            continue
        workers = _worker_count(doc)
        if workers is None:
            unverifiable.append(
                f"{label}: no worker count recorded, so qps * mean_latency ~= workers "
                f"cannot be checked. This report predates provenance recording."
            )
            continue
        busy_floor = 0.75
        for row in rows:
            if not isinstance(row, dict):
                continue
            where = f"dim={row.get('dim')} count={row.get('count')} dtype={row.get('dtype')}"
            search = row.get("search")
            if isinstance(search, dict):
                for mode, m in search.items():
                    if not isinstance(m, dict):
                        continue
                    qps, mean, p50 = m.get("qps"), m.get("mean"), m.get("p50")
                    if not qps or qps <= 0:
                        continue
                    latency, source = (mean, "mean") if mean and mean > 0 else (None, None)
                    if latency is None:
                        unverifiable.append(
                            f"{label} {where} mode={mode}: no mean latency recorded; "
                            f"cannot check the worker identity (predates mean)"
                        )
                        continue
                    implied = qps * latency / 1000.0
                    if implied > float(workers) * 1.02:
                        violations.append(
                            f"{label} {where} mode={mode}: QPS {qps:.1f} x {source} "
                            f"{latency:.3f}ms = {implied:.2f} workers, which exceeds the "
                            f"recorded {workers}"
                        )
                    elif implied < float(workers) * busy_floor:
                        violations.append(
                            f"{label} {where} mode={mode}: QPS {qps:.1f} x {source} "
                            f"{latency:.3f}ms = {implied:.2f} workers, only "
                            f"{implied / float(workers) * 100:.0f}% of the recorded {workers}"
                        )
                continue
            qps, mean = row.get("qps"), row.get("mean")
            if not qps or qps <= 0:
                continue
            if not mean or mean <= 0:
                unverifiable.append(
                    f"{label} {where} mode={row.get('operation', '?')}: no mean latency "
                    f"recorded; cannot check the worker identity"
                )
                continue
            implied = qps * mean / 1000.0
            if implied > float(workers) * 1.02:
                violations.append(
                    f"{label} {where} mode={row.get('operation', '?')}: QPS {qps:.1f} x mean "
                    f"{mean:.3f}ms = {implied:.2f} workers, which exceeds the recorded {workers}"
                )
            elif implied < float(workers) * busy_floor:
                violations.append(
                    f"{label} {where} mode={row.get('operation', '?')}: QPS {qps:.1f} x mean "
                    f"{mean:.3f}ms = {implied:.2f} workers, only "
                    f"{implied / float(workers) * 100:.0f}% of the recorded {workers}"
                )
    return violations, unverifiable


def match_config(baseline_cfg: dict, result_cfg: dict) -> bool:
    # Consolidated baselines (benchmarks/baseline_matrix.json) tag every entry
    # with the build/disk variant it came from; only compare like with like.
    b_variant = baseline_cfg.get("variant")
    r_variant = result_cfg.get("variant")
    if b_variant is not None and r_variant is not None and b_variant != r_variant:
        return False
    # R15 (H5): the bit depth is part of the identity of a TurboQuant config.
    # turboquant4 and turboquant8 differ only in this field once the dtype
    # string is normalised, so a comparison that ignores it can pair a 4-bit
    # baseline against an 8-bit result and call the difference a regression.
    return (
        baseline_cfg.get("dim") == result_cfg.get("dim")
        and baseline_cfg.get("dtype") == result_cfg.get("dtype")
        and baseline_cfg.get("count") == result_cfg.get("count")
        and _tq_bits(baseline_cfg) == _tq_bits(result_cfg)
    )


def _tq_bits(cfg):
    """Bit depth of a config, defaulting to 0 for non-TurboQuant dtypes.

    Read from tq_bits when present, otherwise recovered from the dtype string, so
    reports written before R15 are still distinguishable from each other.
    """
    explicit = cfg.get("tq_bits")
    if explicit is not None:
        return int(explicit)
    dtype = str(cfg.get("dtype") or "")
    if dtype.endswith("4"):
        return 4
    if dtype.endswith("8"):
        return 8
    return 0


def merge_runs_minimum(baseline, runs):
    """Combine several run files by taking the minimum QPS per (config, mode).

    Also applies to ingest throughput, and carries provenance and validation from
    the first run that has them. Anything not comparable across runs - the
    completed/failed/exhausted config lists, which are per-invocation - is taken
    from the first run rather than merged, since a union would misreport which run
    actually completed what.

    IMPORTANT: this is a convenience, not a noise remedy. Roadmap R36 measured it
    and it makes the gate worse, not better - min, median and max over more runs
    all raise the false-positive rate, because variation between separate harness
    invocations is systematic rather than zero-mean. Interleaving the two binaries
    within one invocation is the only approach that worked.
    """
    # baseline is optional here: merge_runs_minimum is used both for combining
    # candidate runs and for reducing a multi-run baseline, and only the latter
    # needs the first run preserved as the metadata donor.
    def key_of(cfg):
        return (cfg.get("dim"), cfg.get("dtype"), cfg.get("count"), _tq_bits(cfg))

    best = {}
    order = []
    for run in runs:
        for cfg in run.get("configs", run.get("results", [])):
            k = key_of(cfg)
            if k not in best:
                order.append(k)
                best[k] = json.loads(json.dumps(cfg))  # deep copy
                continue
            cur = best[k]
            b_ing = (cur.get("ingest") or {}).get("vec_per_sec") or 0
            r_ing = (cfg.get("ingest") or {}).get("vec_per_sec") or 0
            if r_ing > b_ing:
                cur.setdefault("ingest", {})["vec_per_sec"] = r_ing
            for mode, m in (cfg.get("search") or {}).items():
                rm = m.get("qps") or 0
                if rm <= 0:
                    continue
                cm = cur.setdefault("search", {}).setdefault(mode, {})
                if (cm.get("qps") or 0) <= 0 or rm < cm["qps"]:
                    # Strictly less: ties keep the earlier run's value, which keeps
                    # the result independent of file ordering.
                    cur["search"][mode] = dict(m)
    if not best:
        return {}
    out = dict(runs[0]) if runs[0] is not None else {}
    out["results"] = [best[k] for k in order]
    out["configs"] = [best[k] for k in order]
    for field in ("provenance", "validation", "mode"):
        for run in runs:
            if field in run:
                out[field] = run[field]
                break
    out["merged_from"] = len(runs)
    return out


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
                        "tq_bits": _tq_bits(b_cfg),
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
    parser.add_argument(
        "--baseline",
        required=True,
        nargs="+",
        help=(
            "One or more baseline JSON files, combined by minimum QPS per "
            "configuration, symmetrically with --results. See the note on "
            "--results: combining runs is convenient but does not make a "
            "percentage threshold on un-interleaved runs usable (R36)."
        ),
    )
    parser.add_argument(
        "--results",
        required=True,
        nargs="+",
        help=(
            "One or more results JSON files. With several, the MINIMUM QPS per "
            "configuration is combined across them. "
            "NOTE: this does NOT make the gate reliable. Measured over the runs "
            "already in data/perf_logs (docs/roadmap.md R35/R36), a single-run "
            "gate at a 10%% threshold fires falsely 38-55%% of the time against "
            "another run of the same configuration, and combining runs makes it "
            "WORSE, monotonically: min-of-3 reaches 63-92%% and min-of-5 78-98%%. "
            "Min, median and max all behave the same way, because the variation "
            "between separate harness invocations is systematic rather than "
            "zero-mean noise. The only remedy that works is interleaving the "
            "baseline binary and the candidate binary inside one invocation."
        ),
    )
    parser.add_argument("--threshold", type=float, default=10.0,
                        help="Regression threshold percentage (default: 10)")
    parser.add_argument("--variant", default=None,
                        help="Compare only this build variant of a consolidated baseline "
                             "(e.g. cpu_std_nodisk, gpu_emlgo_disk)")
    parser.add_argument("--skip-self-validation", action="store_true",
                        help="Compare even though a report violates qps * mean ~= workers")
    args = parser.parse_args()

    for path in args.baseline:
        if not Path(path).exists():
            print(f"ERROR: baseline file not found: {path}")
            sys.exit(2)

    for path in args.results:
        if not Path(path).exists():
            print(f"ERROR: results file not found: {path}")
            sys.exit(2)

    try:
        if len(args.baseline) > 1:
            baseline = merge_runs_minimum(
                None, [load_json(p) for p in args.baseline]
            )
            print(f"Merging {len(args.baseline)} baseline runs by minimum QPS.")
        else:
            baseline = load_json(args.baseline[0])
        runs = []
        for path in args.results:
            if not Path(path).exists():
                print(f"ERROR: results file not found: {path}")
                sys.exit(2)
            runs.append(load_json(path))
    except json.JSONDecodeError as e:
        print(f"ERROR: failed to parse JSON: {e}")
        sys.exit(2)

    if len(runs) > 1:
        results = merge_runs_minimum(baseline, runs)
        print(f"Merging {len(runs)} runs by minimum QPS per configuration.")
    else:
        results = runs[0]

    # Roadmap R3: refuse to gate on a report that is impossible on its own terms.
    b_viol, b_unver = _self_validation_violations(baseline, "baseline")
    r_viol, r_unver = _self_validation_violations(results, "results")
    self_validation = b_viol + r_viol
    unverifiable = b_unver + r_unver

    regressions = check_regressions(baseline, results, args.threshold, args.variant)

    if unverifiable:
        print(f"WARNING: {len(unverifiable)} report(s) carry no worker count, so the")
        print("qps * mean ~= workers invariant was not checked for them:")
        for v in unverifiable:
            print(f"  - {v}")
        print("Regenerate with --save-baseline to record provenance (roadmap R2).")
        print()

    if self_validation:
        print(f"FAIL: self-validation failed ({len(self_validation)} violation(s)).")
        print("A report whose QPS and latency exceed its own worker count cannot gate a")
        print("regression: every percentage derived from it is meaningless.\n")
        for v in self_validation[:25]:
            print(f"  - {v}")
        if len(self_validation) > 25:
            print(f"  ... and {len(self_validation) - 25} more")
        print()
        if not args.skip_self_validation:
            print("Pass --skip-self-validation to compare anyway. Not recommended.")
            sys.exit(1)

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
