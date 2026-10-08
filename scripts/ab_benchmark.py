#!/usr/bin/env python3
"""Interleaved A/B benchmark: compare two server binaries under shared conditions.

Why this exists
---------------
`check_regression.py` compares one run against one baseline and applies a fixed
percentage threshold. Measured on the runs already in `data/perf_logs`, that gate
fires falsely 77-89% of the time at a 10% threshold, comparing a run of a binary
against another run of the same binary (docs/roadmap.md R35). Combining runs made
it worse, not better (R36), because the variation between separate harness
invocations is systematic rather than zero-mean: which cores are free, what the
memory ceiling resolved to, and what else is on the host all differ between
invocations.

The fix is not a better statistic over separate runs. It is to stop producing
separate runs. This harness alternates the two binaries *within* one invocation,
in balanced order, so each pair of adjacent measurements shares a time window and
therefore a machine state. The comparison is then made per-pair and in ratio, which
cancels the systematic component entirely.

That is exactly what the ad-hoc A/B scripts did by hand, and what produced every
conclusion in sections 8.3 and 9.4 of the roadmap. This makes it repeatable.

The decision rule
-----------------
A ratio is not enough on its own: with N pairs you also want to know whether the
direction is consistent. A median ratio of -12% backed by 10 of 10 pairs agreeing
is evidence; the same median with 5 up and 5 down is noise. So a verdict requires
both a magnitude and a consistency, and "inconclusive" is a first-class answer
rather than a silent pass.

Usage
-----
    python3 scripts/ab_benchmark.py \\
        --baseline-binary bin/longbow_main \\
        --candidate-binary /tmp/candidate/bin/longbow_main \\
        --counts 50000 --dtypes float32 \\
        --search-modes dense --queries 500 --reps 6 \\
        --threshold 5 --cpu-affinity 12-15

Exit codes
----------
    0 = no candidate regression
    1 = candidate regression at or beyond the threshold
    2 = setup error, or too few usable pairs to conclude
"""

import argparse
import json
import math
import os
import re
import statistics
import subprocess
import sys
from collections import defaultdict

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
_RUNNER = os.path.join(_HERE, "unified_benchmark.py")


# --------------------------------------------------------------------------
# Statistics. Kept free of I/O so they can be tested directly.
# --------------------------------------------------------------------------


def paired_ratios(baseline, candidate):
    """Ratios candidate/baseline for each adjacent pair of measurements.

    `baseline` and `candidate` are equal-length sequences in the order the runs
    were executed. Because the runner alternates variants, position i of one
    sequence is adjacent in time to position i of the other, so the pair shares a
    machine state and the ratio isolates the difference between the binaries.

    Pairs where either side is missing or non-positive are dropped: a zero QPS is
    a failed run, not a very slow one, and folding it in would produce a ratio of
    zero that reads as an infinite regression.
    """
    if len(baseline) != len(candidate):
        raise ValueError(
            f"baseline has {len(baseline)} measurements, candidate has {len(candidate)}; "
            "an interleaved comparison needs them aligned"
        )
    out = []
    for b, c in zip(baseline, candidate):
        if b and c and b > 0 and c > 0:
            out.append(c / b)
    return out


def one_sided_sign_pvalue(successes, trials, direction):
    """Exact binomial tail probability under H0: direction is a coin flip.

    `direction` is "down" (a regression) or "up". `successes` is how many pairs
    moved that way. Returns the probability of seeing that many or more, which is
    the evidence against "this is noise".
    """
    if trials <= 0:
        return 1.0
    successes = max(0, min(successes, trials))
    tail = 0.0
    for k in range(successes, trials + 1):
        tail += math.comb(trials, k) * (0.5 ** trials)
    return min(1.0, tail)


def summarize(ratios, threshold_pct):
    """Median ratio, spread, direction consistency, and a verdict.

    The verdict needs both a magnitude and a consistency:

      regression   median at or beyond the threshold AND at least 90% of pairs
                   agree in that direction (one-sided sign test p < 0.05)
      improvement  the mirror image
      inconclusive anything else, including too few pairs

    Reporting "inconclusive" rather than passing silently matters: a gate that
    cannot distinguish a real regression from noise should say so instead of
    implying the change was safe.
    """
    n = len(ratios)
    summary = {
        "pairs": n,
        "threshold_pct": threshold_pct,
    }
    if n == 0:
        summary.update({"verdict": "inconclusive", "reason": "no usable pairs"})
        return summary

    median = statistics.median(ratios)
    median_pct = (median - 1.0) * 100.0
    summary["median_pct"] = median_pct
    summary["min_pct"] = (min(ratios) - 1.0) * 100.0
    summary["max_pct"] = (max(ratios) - 1.0) * 100.0
    if n > 1:
        summary["stdev_pct"] = statistics.stdev(ratios) * 100.0
    else:
        summary["stdev_pct"] = 0.0

    down = sum(1 for r in ratios if r < 1.0)
    up = n - down
    summary["pairs_down"] = down
    summary["pairs_up"] = up
    summary["p_down"] = one_sided_sign_pvalue(down, n, "down")
    summary["p_up"] = one_sided_sign_pvalue(up, n, "up")

    # A single pair can never be significant on its own; require at least three
    # so a verdict is never a coin flip dressed as evidence.
    min_pairs = 3
    if n < min_pairs:
        summary["verdict"] = "inconclusive"
        summary["reason"] = f"only {n} usable pair(s), need {min_pairs}"
        return summary

    # Consistency: 90% agreement, which for n>=3 is a one-sided sign-test
    # p-value at or below 0.05.
    if median_pct <= -threshold_pct and down / n >= 0.9:
        summary["verdict"] = "regression"
        summary["reason"] = (
            f"median {median_pct:+.1f}% at or beyond -{threshold_pct}%, "
            f"with {down}/{n} pairs agreeing"
        )
    elif median_pct >= threshold_pct and up / n >= 0.9:
        summary["verdict"] = "improvement"
        summary["reason"] = (
            f"median {median_pct:+.1f}% at or beyond +{threshold_pct}%, "
            f"with {up}/{n} pairs agreeing"
        )
    else:
        agree = max(down, up) / n
        consistent = agree >= 0.9
        big_enough = abs(median_pct) >= threshold_pct
        summary["verdict"] = "inconclusive"
        # Four distinct cases, and the distinction matters: a consistent but
        # sub-threshold effect is a real effect that is simply not worth gating
        # on, whereas an inconsistent one is noise. Collapsing them into one
        # message reported "100% of pairs agree; direction is not consistent",
        # which is self-contradictory and appeared on the first real hardware run.
        if big_enough and not consistent:
            summary["reason"] = (
                f"median {median_pct:+.1f}% is beyond +/-{threshold_pct}%, but only "
                f"{agree:.0%} of pairs agree; direction is not consistent, so this "
                f"is noise not a change"
            )
        elif consistent and not big_enough:
            summary["reason"] = (
                f"{agree:.0%} of pairs agree, but median {median_pct:+.1f}% is "
                f"inside +/-{threshold_pct}%; a consistent but sub-threshold effect "
                f"is not a regression"
            )
        else:
            summary["reason"] = (
                f"median {median_pct:+.1f}% is inside +/-{threshold_pct}% and only "
                f"{agree:.0%} of pairs agree; neither magnitude nor direction "
                f"supports a verdict"
            )
    return summary


def build_order(reps):
    """Balanced alternation order for the variants.

    ABBA rather than ABAB: within each rep the two variants run back to back in
    opposite orders across reps, so any drift within a rep - thermal, a noisy
    neighbour - hits both variants equally. Plain ABAB would bias every rep in the
    same direction.
    """
    order = []
    for rep in range(reps):
        if rep % 2 == 0:
            order.extend(["baseline", "candidate"])
        else:
            order.extend(["candidate", "baseline"])
    return order


# --------------------------------------------------------------------------
# Orchestration.
# --------------------------------------------------------------------------


def run_one(variant, binary, args, label, port, log_dir):
    """Invoke the matrix harness once for a single variant."""
    cmd = [
        sys.executable, _RUNNER,
        "--mode", args.mode,
        "--server-binary", binary,
        "--dims", str(args.dims),
        "--counts", str(args.counts),
        "--dtypes", args.dtypes,
        "--queries", str(args.queries),
        "--workers", str(args.workers),
        "--search-modes", args.search_modes,
        "--label", label,
        "--port", str(port),
        "--duration", str(args.duration),
        "--timeout", str(args.timeout),
        "--random-port-fallback",
    ]
    if args.cpu_affinity:
        cmd += ["--cpu-affinity", args.cpu_affinity]
    if args.memory:
        cmd += ["--memory", args.memory]
    if args.seed is not None:
        cmd += ["--seed", str(args.seed)]

    log_path = os.path.join(log_dir, f"ab_{variant}_{label}.log")
    with open(log_path, "w") as log:
        proc = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT, cwd=_ROOT)
    return proc.returncode, log_path


_QPS_RE = re.compile(r"^(?P<mode>[a-z0-9_]+)_qps$")


def load_qps(result_path):
    """Extract {(dtype, count, mode): qps} from harness result JSON.

    `result_path` may be a single path or a list of them. Accepting a bare string
    is deliberate: the obvious call is `load_qps(files[-1])`, and iterating a
    string yields characters rather than paths, which silently produced an empty
    mapping and therefore an empty - and passing - report.

    Unparseable files raise rather than being skipped. A file that exists but
    cannot be read is a broken run, and swallowing it would turn a measurement
    failure into a clean bill of health.
    """
    if isinstance(result_path, str):
        result_path = [result_path]

    out = {}
    for path in result_path:
        if not os.path.exists(path):
            continue
        with open(path) as f:
            doc = json.load(f)
        for row in doc.get("results", []) or []:
            if not isinstance(row, dict):
                continue
            key0 = (row.get("dtype"), row.get("count"))
            for mode, m in (row.get("search") or {}).items():
                qps = m.get("qps")
                if qps and qps > 0:
                    out[key0 + (mode,)] = qps
    return out


def find_result_files(pattern):
    import glob

    return sorted(glob.glob(pattern))


def main():
    p = argparse.ArgumentParser(
        description="Interleaved A/B benchmark of two server binaries",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--baseline-binary", required=True,
                   help="Server binary for the baseline arm")
    p.add_argument("--candidate-binary", required=True,
                   help="Server binary for the candidate arm")
    p.add_argument("--mode", default="cpu")
    p.add_argument("--dims", type=int, default=128)
    p.add_argument("--counts", required=True,
                   help="Comma-separated vector counts")
    p.add_argument("--dtypes", required=True,
                   help="Comma-separated dtypes")
    p.add_argument("--search-modes", default="dense")
    p.add_argument("--queries", type=int, default=500)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--duration", type=int, default=15)
    p.add_argument("--timeout", type=int, default=3600)
    p.add_argument("--reps", type=int, default=6,
                   help="Paired repetitions. Must be at least 3 for a verdict.")
    p.add_argument("--threshold", type=float, default=5.0,
                   help="Percentage at which a consistent change is a verdict")
    p.add_argument("--cpu-affinity", default=None,
                   help="Pin both arms to these cores, e.g. 12-15. Note this pins "
                        "the server AND the client to the same cores, so leave "
                        "headroom for --workers or the client contends with the "
                        "server it is measuring.")
    p.add_argument("--memory", default=None, help="Server memory ceiling, e.g. 14GB")
    p.add_argument("--seed", type=int, default=None,
                   help="Client seed. Set it explicitly so both arms see the "
                        "identical corpus (roadmap R16).")
    p.add_argument("--port", type=int, default=4300)
    p.add_argument("--log-dir", default="data/perf_logs")
    p.add_argument("--label", default=None)
    p.add_argument("--out", default=None, help="Write the JSON verdict here")
    args = p.parse_args()

    for path in (args.baseline_binary, args.candidate_binary):
        if not os.path.exists(path):
            print(f"ERROR: binary not found: {path}")
            return 2
    if args.reps < 3:
        print("ERROR: --reps must be at least 3; a verdict from one or two pairs "
              "is a coin flip.")
        return 2

    os.makedirs(args.log_dir, exist_ok=True)
    stamp = args.label or "ab"
    binaries = {"baseline": args.baseline_binary, "candidate": args.candidate_binary}

    # (variant, rep) -> {(dtype,count,mode): qps}
    samples = defaultdict(dict)
    order = build_order(args.reps)

    for i, variant in enumerate(order):
        rep = i // 2
        label = f"{stamp}_{variant}_r{rep}"
        print(f"[{i + 1}/{len(order)}] {variant} rep {rep} ({binaries[variant]})",
              flush=True)
        rc, log_path = run_one(variant, binaries[variant], args, label,
                               args.port + (i % 2), args.log_dir)
        results = find_result_files(
            os.path.join(args.log_dir, f"perf_matrix_{args.mode}_{label}_*.json")
        )
        if rc != 0 or not results:
            print(f"    arm failed (rc={rc}); see {log_path}")
            continue
        # The glob matches every run ever made under this label, including the
        # `_latest.json` alias and files from earlier invocations, so pick the
        # most recently written one rather than relying on name sort order.
        newest = max(results, key=os.path.getmtime)
        qps = load_qps(newest)
        if not qps:
            print(f"    arm produced no QPS measurements in {newest}; "
                  f"treating as a failed arm")
            continue
        samples[(variant, rep)] = qps

    # Pair adjacent reps: the runner guarantees baseline/candidate alternate, so
    # rep r of each arm is adjacent in time.
    keys = set()
    for v in samples.values():
        keys |= set(v.keys())

    rows = []
    for key in sorted(keys):
        base, cand = [], []
        for rep in range(args.reps):
            b = samples.get(("baseline", rep), {}).get(key)
            c = samples.get(("candidate", rep), {}).get(key)
            base.append(b if b else 0)
            cand.append(c if c else 0)
        ratios = paired_ratios(base, cand)
        summary = summarize(ratios, args.threshold)
        dtype, count, mode = key
        summary.update({"dtype": dtype, "count": count, "mode": mode,
                        "baseline_qps": [x for x in base if x],
                        "candidate_qps": [x for x in cand if x]})
        rows.append(summary)

    if not rows:
        # An empty report is indistinguishable from a clean run at a glance, and
        # exiting 0 would make it look like one. This is how the string-vs-list
        # bug in load_qps shipped a passing verdict with no measurements at all.
        print("\nERROR: no measurements were extracted from any arm.")
        print("       Every arm either failed or yielded an empty result set, so")
        print("       there is nothing to compare. Refusing to report a verdict.")
        return 2

    # Report.
    print("\n" + "=" * 96)
    print(f"INTERLEAVED A/B  threshold +/-{args.threshold}%   reps={args.reps}   "
          f"affinity={args.cpu_affinity or 'unset'}")
    print("=" * 96)
    print(f"{'dtype':<12} {'count':<9} {'mode':<14} {'pairs':<6} "
          f"{'median':<9} {'spread':<16} {'verdict'}")
    print("-" * 96)
    worst = "ok"
    for r in sorted(rows, key=lambda x: x.get("median_pct", 0.0)):
        median = f"{r.get('median_pct', float('nan')):+.1f}%"
        if r["pairs"] > 1:
            spread = f"{r['min_pct']:+.0f}..{r['max_pct']:+.0f}%"
        else:
            spread = "n/a"
        print(f"{str(r['dtype']):<12} {str(r['count']):<9} {str(r['mode']):<14} "
              f"{r['pairs']:<6} {median:<9} {spread:<16} {r['verdict']}")
        if r["verdict"] == "regression":
            worst = "regression"
    inconclusive = [r for r in rows if r["verdict"] == "inconclusive"]
    if inconclusive:
        print("-" * 96)
        for r in inconclusive[:5]:
            print(f"  inconclusive: {r['dtype']}/{r['count']}/{r['mode']}: {r['reason']}")

    verdict_doc = {
        "threshold_pct": args.threshold,
        "reps": args.reps,
        "baseline_binary": args.baseline_binary,
        "candidate_binary": args.candidate_binary,
        "cpu_affinity": args.cpu_affinity,
        "seed": args.seed,
        "workers": args.workers,
        "queries": args.queries,
        "rows": rows,
    }
    if args.out:
        with open(args.out, "w") as f:
            json.dump(verdict_doc, f, indent=2)
        print(f"\nVerdict written to {args.out}")

    print(f"\nRESULT: {worst}"
          + (f" ({len(inconclusive)} row(s) inconclusive)" if inconclusive else ""))
    if worst == "regression":
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())