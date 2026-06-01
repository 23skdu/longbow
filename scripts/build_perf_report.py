#!/usr/bin/env python3
"""Build performance.md from perf_matrix JSON results."""
import json, os, glob
from datetime import datetime

RESULTS_DIR = "data/perf_logs"
PERF_MD = "docs/performance.md"

files = sorted(glob.glob(os.path.join(RESULTS_DIR, "perf_matrix_*.json")))
by_platform = {}
for f in files:
    with open(f) as fh:
        data = json.load(fh)
    mode = data.get('mode', '?')
    label = data.get('config', {}).get('label', '') or data.get('label', '')
    basename = os.path.basename(f)
    host = 'localhost'
    if 'ancalagon' in basename or 'ancalagon' in label:
        host = 'ancalagon'
    platform_key = (host, mode)
    if platform_key not in by_platform:
        by_platform[platform_key] = []
    for r in data.get('results', []):
        by_platform[platform_key].append(r)

commit = os.popen("git rev-parse --short HEAD").read().strip()
now = datetime.now()

platform_info = {
    ("localhost", "cpu"): ("Localhost (M3 Pro)", "CPU"),
    ("localhost", "metal"): ("Localhost (M3 Pro)", "Metal"),
    ("ancalagon", "cpu"): ("Ancalagon (i7-12650H)", "CPU"),
    ("ancalagon", "cuda"): ("Ancalagon (i7-12650H)", "CUDA"),
}

for key in by_platform:
    seen = {}
    for r in by_platform[key]:
        dk = (r.get('dim', 0), r.get('dtype', ''), r.get('count', 0))
        seen[dk] = r
    by_platform[key] = list(seen.values())

table_sections = []
for platform_key in sorted(k for k in by_platform.keys() if k in platform_info):
    results = by_platform[platform_key]
    host, accel = platform_info[platform_key]
    section = [f"### {host} — {accel}", "",
               "| Dim | Type | Count | Ingest (vec/s) | Dense QPS | Dense P50 | Dense P95 | Sparse QPS | Sparse P50 | Sparse P95 | Disk (MB) |",
               "|-----|------|-------|---------------|-----------|-----------|-----------|------------|------------|------------|-----------|"]
    for r in sorted(results, key=lambda x: (x.get('dtype', ''), x.get('dim', 0), x.get('count', 0))):
        dtype = r.get('dtype', '?')
        dim = r.get('dim', 0)
        count = r.get('count', 0)
        ingest = r.get('ingest', {}).get('vec_per_sec', 0)
        search = r.get('search', {})
        d = search.get('dense', {})
        s = search.get('sparse', {})
        d_qps = d.get('qps', 0) or 0
        d_p50 = d.get('p50', 0) or 0
        d_p95 = d.get('p95', 0) or 0
        s_qps = s.get('qps', 0) or 0
        s_p50 = s.get('p50', 0) or 0
        s_p95 = s.get('p95', 0) or 0
        disk = r.get('disk_usage_mb', 0) or 0
        section.append(f"| {dim:>3} | {dtype:>12} | {count:>7} | {ingest:>12.0f} | {d_qps:>9.1f} | {d_p50:>8.3f}ms | {d_p95:>8.3f}ms | {s_qps:>9.1f} | {s_p50:>8.3f}ms | {s_p95:>8.3f}ms | {disk:>8.1f} |")
    table_sections.append("\n".join(section))

# Observations from current run
observations = [
    "1. **float32 dense search is fastest** across all counts and dims — float32 kernels remain the most optimized code path.",
    "2. **turboquant8 matches float32 dense QPS at 15k** (~7980 vs 7350 at dim=128), but may degrade at higher counts.",
    "3. **int8/uint8 dense search is 15-30% slower than float32** at 15k, but drops to ~6x slower at 200k (940 vs 5850 QPS dim=128). The proportional gap widens with index size.",
    "4. **float16 dense search is 7-15x slower than float32** (549 vs 5850 QPS at 200k dim=128). No SIMD kernel for float16.",
    "5. **complex128 dense search is ~3x slower than float32** — generic distance computation path, no SIMD.",
    "6. **Sparse search is consistent across all types** at ~11,500 QPS regardless of dtype, dim, or count — bypasses vector distance computation.",
    "7. **Ingest speed drops 20-40x from 15k to 200k** — HNSW edge construction is O(N\u00b7log N).",
]

doc_parts = [
    f"# Longbow Performance Benchmark Matrix (LATEST)",
    "",
    f"Generated on: {now.strftime('%Y-%m-%d')}",
    f"Commit: `{commit}`",
    "",
    "## v0.2.1-rc5 — Localhost CPU Run (2026-05-31)",
    "",
    "> [!IMPORTANT]",
    f"> Localhost (M3 Pro, 18GB) with `LONGBOW_USE_DISK=1`. Dimensions 128 and 384, counts 15k\u2013800k, all dtypes. DiskVectorStore enabled. Ancalagon host unreachable (SSH timeout). Metal run pending.",
    "",
]

doc_parts.extend(table_sections)

# Only add comparison table if we have 15k and 200k data for float32 at dim=128
f32_15k_128 = None
f32_200k_128 = None
for r in by_platform.get(("localhost", "cpu"), []):
    if r.get('dtype') == 'float32' and r.get('dim') == 128:
        if r.get('count') == 15000:
            f32_15k_128 = r
        elif r.get('count') == 200000:
            f32_200k_128 = r

if f32_15k_128 and f32_200k_128:
    i15 = f32_15k_128['ingest']['vec_per_sec']
    i200 = f32_200k_128['ingest']['vec_per_sec']
    d15 = f32_15k_128['search']['dense']
    d200 = f32_200k_128['search']['dense']
    doc_parts.extend([
        "",
        "### Scale Comparison (float32, dim=128)",
        "",
        "| Metric | 15k | 200k | Ratio (200k/15k) |",
        "|--------|-----|------|-------------------|",
        f"| Ingest (vec/s) | {i15:,.0f} | {i200:,.0f} | {i200/i15:.2%} |",
        f"| Dense QPS | {d15['qps']:,.1f} | {d200['qps']:,.1f} | {d200['qps']/d15['qps']:.2%} |",
        f"| Dense P50 (ms) | {d15['p50']:.3f} | {d200['p50']:.3f} | {d200['p50']/d15['p50']:.1f}x |",
    ])

doc_parts.extend([
    "",
    "### Key Observations",
    "",
] + [f"{o}" for o in observations])

doc_parts.extend([
    "",
    "### Hardware",
    "",
    "- **Local**: Apple Silicon M3 Pro, 18GB memory (18GB allocated)",
    "",
    "### Coverage (CPU run)",
    "",
    "- **Platforms:** CPU (Metal pending, ancalagon unreachable)",
    "- **Data Types:** float16, float32, int8, uint8, complex128, turboquant8",
    "- **Dimensions:** 128, 384",
    "- **Counts:** 15k, 200k (500k, 800k in progress)",
    "- **Search Modes:** dense, sparse",
    "- **DiskVectorStore:** enabled (LONGBOW_USE_DISK=1). Disk (MB) column shows on-disk size of the vector store files after ingest.",
    "",
    "### Known Issues",
    "",
    "1. **float16 dense search is 7-15x slower than float32** — lacks SIMD-optimized distance kernel. Add NEON/AVX float16 path.",
    "2. **int8/uint8 dense search degrades sharply at scale** — 1.5x at 15k but ~6x at 200k vs float32. int8 SIMD kernel may not scale well with index size.",
    "3. **complex128 ingest/search slow** — generic distance path, no SIMD. 3x slower than float32 at all counts.",
    "4. **800k OOM expected at 384d** — HNSW graph overhead exceeds 18GB for large dims at high counts.",
    "5. **Ingest speed drops 20-40x from 15k to 200k** — O(N\u00b7log N) edge construction is the bottleneck. Consider adaptive M during bulk insert.",
    "6. **Metal benchmarks not yet run** — will follow CPU completion.",
    "7. **Ancalagon unreachable** — CPU + CUDA benchmarks on i7-12650H + RTX 4060 pending.",
])

with open(PERF_MD, "w") as f:
    f.write("\n".join(doc_parts) + "\n")
print(f"Written {PERF_MD}")
