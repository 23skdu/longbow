#!/usr/bin/env python3
"""
Comprehensive benchmark aggregator for Longbow.

Discovers the newest unified_benchmark matrix run for each of the 8
build/disk variants, generates docs/performance.md (ingest table, A/B tables
per device / search mode / disk mode), compares against the archived previous
baseline, and emits a consolidated baseline under benchmarks/.

Usage:
    python3 scripts/analyze_benchmark_matrix.py \
        [--out docs/performance.md] \
        [--baseline-dir data/perf_logs/archive_20260924] \
        [--baseline-out benchmarks/baseline_matrix.json]
"""
import argparse
import glob
import json
import os
from datetime import datetime

# variant key -> (benchmark mode, human label)
VARIANTS = [
    ("cpu_std_nodisk", "cpu", "CPU Standard (NoDisk)"),
    ("cpu_std_disk", "cpu", "CPU Standard (Disk)"),
    ("cpu_emlgo_nodisk", "cpu", "CPU EMLGo (NoDisk)"),
    ("cpu_emlgo_disk", "cpu", "CPU EMLGo (Disk)"),
    ("gpu_std_nodisk", "cuda", "GPU Standard (NoDisk)"),
    ("gpu_std_disk", "cuda", "GPU Standard (Disk)"),
    ("gpu_emlgo_nodisk", "cuda", "GPU EMLGo (NoDisk)"),
    ("gpu_emlgo_disk", "cuda", "GPU EMLGo (Disk)"),
]

# Display order from docs/testplan.md §3.4
DTYPES = [
    "int8", "uint8", "int16", "uint16", "int32", "uint32", "int64", "uint64",
    "float16", "float32", "float64", "complex64", "complex128",
    "turboquant2", "turboquant4", "turboquant8",
]

# Display order from docs/testplan.md §3.5 (bench-tool key -> testplan name)
SEARCH_MODES = [
    ("dense", "dense"),
    ("hybrid", "hybrid"),
    ("sparse", "sparse"),
    ("filtered", "filtered"),
    ("byid", "by_id"),
    ("graphrag", "graphrag"),
    ("geo", "geo"),
    ("temporal", "temporal"),
    ("learnedindex", "learned_index"),
]

COUNTS = [100000, 250000]


def load_json(path):
    with open(path) as f:
        return json.load(f)


def latest(pattern):
    files = glob.glob(pattern)
    if not files:
        return None
    return max(files, key=os.path.getmtime)


def dtype_label(entry):
    """Result entries collapse turboquantN -> dtype 'turboquant' + tq_bits."""
    bits = entry.get("tq_bits", 0)
    if entry.get("dtype") == "turboquant" and bits:
        return f"turboquant{bits}"
    return entry.get("dtype")


def index_results(raw):
    """Index results as (count, dtype_label) -> entry."""
    idx = {}
    if not raw:
        return idx
    for entry in raw.get("results", []):
        idx[(entry["count"], dtype_label(entry))] = entry
    return idx


def find_files(log_dir, suffix=""):
    """Discover the newest result file for every variant."""
    out = {}
    for key, mode, _ in VARIANTS:
        pattern = os.path.join(log_dir, f"perf_matrix_{mode}_{key}{suffix}_*.json")
        path = latest(pattern)
        if path:
            out[key] = path
    return out


def qps(entry, mode_key):
    if not entry:
        return 0.0
    return float(entry.get("search", {}).get(mode_key, {}).get("qps", 0) or 0)


def ab_table(indexed, std_key, emlgo_key, mode_key):
    std_idx = indexed[std_key]
    eml_idx = indexed[emlgo_key]
    lines = [
        "| dtype | 100k std | 100k emlgo | 100k delta | 250k std | 250k emlgo | 250k delta |",
        "|---|---|---|---|---|---|---|",
    ]
    for dt in DTYPES:
        row = [dt]
        for c in COUNTS:
            s = qps(std_idx.get((c, dt)), mode_key)
            e = qps(eml_idx.get((c, dt)), mode_key)
            delta = ((e - s) / s * 100) if s > 0 else 0.0
            row.extend([f"{int(round(s))}", f"{int(round(e))}", f"{delta:+.1f}%"])
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def ingest_table(indexed):
    lines = [
        "| Configuration | 100k (vec/s) | 250k (vec/s) | Peak RSS (MB) | Peak Disk (MB) |",
        "|---|---|---|---|---|",
    ]
    for key, _, display in VARIANTS:
        idx = indexed[key]
        speeds, rss, disk = [], [], []
        for c in COUNTS:
            vals = [
                idx[(c, dt)].get("ingest", {}).get("vec_per_sec", 0)
                for dt in DTYPES
                if (c, dt) in idx
            ]
            avg = sum(vals) / len(vals) if vals else 0
            speeds.append(f"{int(round(avg)):,}")
        for dt in DTYPES:
            e = idx.get((250000, dt))
            if e:
                rss.append(e.get("peak_memory_mb", 0) or 0)
                disk.append(e.get("disk_usage_mb", 0) or 0)
        peak_rss = max(rss) if rss else 0
        peak_disk = max(disk) if disk else 0
        lines.append(
            f"| {display} | {speeds[0]} | {speeds[1]} | {peak_rss:.1f} MB | {peak_disk:.1f} MB |"
        )
    return "\n".join(lines)


def compare(indexed, base_indexed):
    """Compare current results against a previous baseline run.

    Returns (comparable_count, regressions, gains) where regressions are
    drops beyond -10% and gains are improvements beyond +20%.
    """
    comparable, regressions, gains = 0, [], []
    for key, mode, _ in VARIANTS:
        if key not in indexed or key not in base_indexed:
            continue
        cur, base = indexed[key], base_indexed[key]
        for count in COUNTS:
            for dt in DTYPES:
                r_cur, r_base = cur.get((count, dt)), base.get((count, dt))
                if not r_cur or not r_base:
                    continue
                for mode_key, _name in SEARCH_MODES:
                    q_cur = qps(r_cur, mode_key)
                    q_base = qps(r_base, mode_key)
                    if q_base <= 0:
                        continue
                    comparable += 1
                    delta = (q_cur - q_base) / q_base * 100
                    if delta < -10.0:
                        regressions.append((key, count, dt, _name, q_base, q_cur, delta))
                    elif delta > 20.0:
                        gains.append((key, count, dt, _name, q_base, q_cur, delta))
    regressions.sort(key=lambda x: x[6])
    gains.sort(key=lambda x: x[6], reverse=True)
    return comparable, regressions, gains


def emlgo_summary(indexed):
    """Best/worst emlgo-vs-standard deltas across every mode and count."""
    rows = []
    for device in ("cpu", "gpu"):
        for disk in ("nodisk", "disk"):
            s, e = f"{device}_std_{disk}", f"{device}_emlgo_{disk}"
            if s not in indexed or e not in indexed:
                continue
            for count in COUNTS:
                for dt in DTYPES:
                    for mode_key, name in SEARCH_MODES:
                        qs = qps(indexed[s].get((count, dt)), mode_key)
                        qe = qps(indexed[e].get((count, dt)), mode_key)
                        if qs <= 0:
                            continue
                        rows.append(
                            (device, disk, count, dt, name, qs, qe, (qe - qs) / qs * 100)
                        )
    rows.sort(key=lambda x: x[7])
    return rows


def disk_summary(indexed):
    """Average QPS change of disk mode vs nodisk for the standard builds."""
    deltas = []
    for device in ("cpu", "gpu"):
        n, d = f"{device}_std_nodisk", f"{device}_std_disk"
        if n not in indexed or d not in indexed:
            continue
        for count in COUNTS:
            for dt in DTYPES:
                for mode_key, _name in SEARCH_MODES:
                    qn = qps(indexed[n].get((count, dt)), mode_key)
                    qd = qps(indexed[d].get((count, dt)), mode_key)
                    if qn <= 0:
                        continue
                    deltas.append((qd - qn) / qn * 100)
    if not deltas:
        return 0.0, 0
    return sum(deltas) / len(deltas), len(deltas)


def peak_qps(indexed, device_filter=None):
    best = (0, None)
    for key, mode, _ in VARIANTS:
        if device_filter and not key.startswith(device_filter):
            continue
        for (count, dt), entry in indexed.get(key, {}).items():
            for mode_key, name in SEARCH_MODES:
                q = qps(entry, mode_key)
                if q > best[0]:
                    best = (q, f"{key} / {dt} / {count:,} / {name}")
    return best


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log-dir", default="data/perf_logs")
    ap.add_argument("--baseline-dir", default="data/perf_logs/archive_20260924")
    ap.add_argument("--out", default="docs/performance.md")
    ap.add_argument("--baseline-out", default="benchmarks/baseline_matrix.json")
    ap.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"))
    args = ap.parse_args()

    files = find_files(args.log_dir)
    missing = [k for k, _, _ in VARIANTS if k not in files]
    if missing:
        raise SystemExit(f"Missing result files for variants: {missing}")
    base_files = find_files(args.baseline_dir)

    data = {k: load_json(p) for k, p in files.items()}
    indexed = {k: index_results(v) for k, v in data.items()}
    base_indexed = {k: index_results(load_json(p)) for k, p in base_files.items()} if base_files else {}

    comparable, regressions, gains = (0, [], [])
    if base_indexed:
        comparable, regressions, gains = compare(indexed, base_indexed)

    eml_rows = emlgo_summary(indexed)
    disk_avg, disk_n = disk_summary(indexed)

    def pretty(mode_key, mode_name):
        return mode_name.replace("_", " ").capitalize()
    cpu_peak, cpu_peak_where = peak_qps(indexed, "cpu")
    gpu_peak, gpu_peak_where = peak_qps(indexed, "gpu")

    n_dt = len(DTYPES)
    n_modes = len(SEARCH_MODES)
    n_points = len(VARIANTS) * len(COUNTS) * n_dt * n_modes

    out = []
    w = out.append

    w("# Longbow Performance Benchmarks\n")
    w(f"**Date:** {args.date}  ")
    w("**Baseline Release Candidate:** `v0.2.4-rc1` / `v0.2.1-rc3`  ")
    w("")
    w("## System Specifications\n")
    w("| Component | Detail |")
    w("|---|---|")
    w("| CPU | Intel Core i7-12650H (16 vCPUs, AVX2, x86_64) |")
    w("| RAM | 23 GB |")
    w("| GPU | NVIDIA GeForce RTX 4060 Laptop (8 GB VRAM, sm_89, CUDA 12.4) |")
    w("| Go Runtime | Go 1.24+ / 1.27 (CGO enabled) |")
    w("")
    w("| Binary | Description |")
    w("|---|---|")
    w("| `bin/longbow_main` | CPU standard build |")
    w("| `bin/longbow_emlgo` | CPU emlgo SIMD build (`-tags emlgo`) |")
    w("| `bin/longbow-cuda_main` | GPU standard build (`-tags gpu`) |")
    w("| `bin/longbow-cuda_emlgo` | GPU emlgo build (`-tags \"gpu,emlgo\"`) |")
    w("")
    w("**Configuration:** 8 concurrency workers, 500 queries, 128 dimensions, 16GB memory ceiling.")
    w(f"**Scaling Tiers:** 100,000 (100k) and 250,000 (250k) vectors.")
    w(f"**Data Types:** all {n_dt} test-plan dtypes (`int8` → `turboquant8`).")
    w(f"**Search Modes:** all {n_modes} engine modalities (see `docs/testplan.md` §3.5).")
    w("**Disk Modes:** `use_disk=no` (pure memory) and `use_disk=yes` (auto-spill with 60% memory threshold).")
    w("")
    w("---\n")

    # ------------------------------------------------------------------ summary
    w("## 1. Executive Summary\n")
    w(
        f"This baseline covers **{n_points} metric points** "
        f"({len(VARIANTS)} build variants × {len(COUNTS)} scale tiers × "
        f"{n_dt} dtypes × {n_modes} search modes) on freshly rebuilt binaries."
    )
    w("")
    w("### Key Observations")
    w(
        f"1. **Peak Throughput**: CPU reaches **{cpu_peak:,.0f} QPS** ({cpu_peak_where}); "
        f"GPU reaches **{gpu_peak:,.0f} QPS** ({gpu_peak_where})."
    )
    w(
        f"2. **EMLGo SIMD A/B**: across all {len(eml_rows)} comparable points, "
        f"{sum(1 for r in eml_rows if r[7] > 0)} favour emlgo and "
        f"{sum(1 for r in eml_rows if r[7] <= 0)} favour standard. "
        + (
            f"Largest gain **{eml_rows[-1][7]:+.1f}%** "
            f"({eml_rows[-1][0]} {eml_rows[-1][1]} {eml_rows[-1][3]} {eml_rows[-1][4]} "
            f"at {eml_rows[-1][2]:,}); largest loss **{eml_rows[0][7]:+.1f}%** "
            f"({eml_rows[0][0]} {eml_rows[0][1]} {eml_rows[0][3]} {eml_rows[0][4]} "
            f"at {eml_rows[0][2]:,})."
            if eml_rows
            else ""
        )
    )
    w(
        f"3. **Auto-Spill (`use_disk=yes`)**: standard-build disk mode averages "
        f"**{disk_avg:+.1f}%** QPS versus pure memory across {disk_n} measurements, "
        "while bounding RSS to the 60% memory ceiling."
    )
    if base_indexed:
        w(
            f"4. **vs. previous baseline**: {len(regressions)} of {comparable} comparable "
            f"points regressed beyond -10%, while {len(gains)} improved by more than +20%."
        )
    w("")
    w("---\n")

    # ------------------------------------------------------------------- ingest
    w("## 2. Ingestion Throughput & Memory Scaling\n")
    w(ingest_table(indexed))
    w("")
    w("---\n")

    # -------------------------------------------------------------- A/B tables
    section = 3
    for device, title in (("cpu", "CPU"), ("gpu", "GPU")):
        for mode_key, mode_name in SEARCH_MODES:
            for disk, disk_name in (("nodisk", "NoDisk"), ("disk", "Disk")):
                w(f"## {section}. {title} A/B — {pretty(mode_key, mode_name)} Search ({disk_name})\n")
                w(ab_table(indexed, f"{device}_std_{disk}", f"{device}_emlgo_{disk}", mode_key))
                w("")
                w("---\n")
                section += 1

    # ------------------------------------------------------------- regressions
    if base_indexed:
        baseline_label = os.path.basename(os.path.normpath(args.baseline_dir))
        w(f"## {section}. Regression Investigation vs Previous Baseline (`{baseline_label}`)\n")
        w(
            f"Out of **{comparable} comparable metric points**, "
            f"**{len(regressions)} points** regressed beyond the -10% threshold, while "
            f"**{len(gains)} points** gained more than +20%.\n"
        )
        if regressions:
            w("### Top Observed Regressions\n")
            w("| Configuration | Count | Dtype | Search Mode | Baseline QPS | Current QPS | Delta |")
            w("|---|---|---|---|---|---|---|")
            for r in regressions[:15]:
                w(f"| {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]:.1f} | {r[5]:.1f} | {r[6]:+.1f}% |")
            w("")
        else:
            w("No metric point regressed beyond -10%.\n")
        w("### Top Observed Gains\n")
        w("| Configuration | Count | Dtype | Search Mode | Baseline QPS | Current QPS | Delta |")
        w("|---|---|---|---|---|---|---|")
        for r in gains[:15]:
            w(f"| {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]:.1f} | {r[5]:.1f} | {r[6]:+.1f}% |")
        w("")
        w("---\n")
        section += 1

    # ------------------------------------------------------------- observations
    w(f"## {section}. Performance & Stability Observations for Roadmap\n")
    w("See `docs/roadmap.md` for the tracked action items derived from this run.")
    w("")

    with open(args.out, "w") as f:
        f.write("\n".join(out))

    # ------------------------------------------------------- consolidated baseline
    baseline = {
        "format": "longbow-baseline-matrix/1",
        "generated": datetime.now().isoformat(timespec="seconds"),
        "date": args.date,
        "platform": data["cpu_std_nodisk"].get("platform"),
        "config": {
            "dims": [128],
            "counts": COUNTS,
            "dtypes": DTYPES,
            "search_modes": [n for _, n in SEARCH_MODES],
            "queries": 500,
            "workers": 8,
            "memory": 17179869184,
        },
        "source_files": files,
        "variants": {},
        "results": [],
    }
    for key, _mode, _display in VARIANTS:
        entries = []
        for entry in data[key].get("results", []):
            entry = dict(entry)
            entry["variant"] = key
            entries.append(entry)
        baseline["variants"][key] = entries
        baseline["results"].extend(entries)
    with open(args.baseline_out, "w") as f:
        json.dump(baseline, f, indent=2)

    print(f"Generated {args.out} ({section} sections).")
    print(f"Wrote {args.baseline_out} ({len(baseline['results'])} entries).")
    print(f"Comparable vs previous: {comparable}, regressions: {len(regressions)}, gains: {len(gains)}")


if __name__ == "__main__":
    main()
