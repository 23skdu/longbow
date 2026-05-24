import sys

with open('updated_tables.md', 'r') as f:
    tables_content = f.read()

perf_md_content = f"""# Longbow Performance Benchmarks (0.2.1-rc4)

This document contains the latest performance benchmarking results collated from the local macOS machine (M3) and the high-end multi-socket AMD64 server `ancalagon`.

The tests cover CPU, Metal, and CUDA backends across various datatypes and vector counts for 128 and 384 dimensions. Note that due to resource exhaustion limits, maximum vector count achieved was 100,000 before hitting the safety thresholds.

## Regression Analysis vs v0.2.0 Baseline

A comprehensive audit of the latest 0.2.1-rc4 benchmark logs against the v0.2.0 baseline reveals significant regressions, primarily stemming from scaling issues:

1. **Massive Throughput Regression at Scale**:
   - **Dense Search**: Dropped from ~30,576 QPS (baseline, count=5,000) to **1,259 QPS** (local M3 float32, dim=128, count=50,000) and **951 QPS** at count=100,000. This is a 96% regression.
   - **Temporal Search**: Dropped from ~29,389 QPS (baseline, count=5,000) to **2,082 QPS** (local M3 float32, dim=128, count=100,000).

2. **Ingestion Bottlenecks**:
   - Ingestion rate for float32/128d on Apple Silicon M3 plummeted from **~459,000 vec/s** (baseline) to **838 vec/s** at the 100,000 vector scale. This indicates severe memory fragmentation or an `O(N^2)` indexing complexity bug introduced in recent commits.

3. **High-Dimensional Degradation**:
   - At 384 dimensions, `float32` ingestion drops to just **288 vec/s** locally and **355 vec/s** on the remote server, while Dense QPS barely breaks 600.

4. **Resource Exhaustion Thresholds**:
   - The benchmarks successfully ran up to 100k vectors. However, attempts to scale to 500k vectors hit hard memory limits. Specifically, memory pressure caused the system to halt via `ResourceExhausted` backpressure at ~375k vectors for the 18GB local cap and ~275k vectors for the 14GB remote cap.

---

{tables_content}

## Stability Findings

The system safely handles memory exhaustion by throwing `rpc error: code = ResourceExhausted desc = critical memory pressure` instead of hard crashing (OOM kill). However, the active benchmark server does not correctly terminate zombie processes, requiring manual intervention.

## Previous Baselines
For historical comparison, see [v0.2.0 Baseline Docs](performance_0.2.0_baseline.md).
"""

with open('docs/performance.md', 'w') as f:
    f.write(perf_md_content)

nextsteps_content = """# Actionable Stability and Performance Recommendations

Based on the audit of the v0.2.1-rc4 benchmark matrix (see `docs/performance.md`), the following immediate actions are recommended:

## 1. Address Ingestion Memory Scaling Limits (< 20GB Configurations)
**Finding**: The memory-based ingestion limit restricts capacity to 300k-400k float32 vectors on nodes with less than 20GB of memory. It hits a hard `ResourceExhausted` ceiling at ~375k vectors for 18GB limits and ~275k for 14GB limits.
**Action**: 
- Investigate indexing memory overhead. The raw vector size for 375k `float32` 128d vectors is only ~192MB. An 18GB footprint indicates a ~90x overhead per vector in the current indexing structure (likely the GraphRAG or HNSW edges).
- Implement chunked disk spilling or on-disk indices for datasets exceeding 100k vectors to respect the 18GB/14GB boundaries.

## 2. Resolve `O(N)` or `O(N^2)` Ingestion Degradation
**Finding**: Ingestion rates dropped from 459k vec/s down to <1k vec/s as the dataset grew from 5k to 100k vectors.
**Action**:
- Profile the `DoPut` hot path to identify locking contention or index re-balancing that scales poorly with dataset size.
- Ensure the `LockFreeSlice` integration from Phase 7 is actually bypassing `epMu` spinlocks during bulk ingestion.

## 3. Fix High-Dimensional Search Contention
**Finding**: Dense search QPS at 384 dimensions fell below 500 QPS on the remote Ancalagon server.
**Action**:
- Re-verify AVX2/AVX-512 distance kernels on Ancalagon. The 288 QPS implies fallback to naive scalar loops.
- Check SIMD register saturation or cache line misses for 384d `float32` arrays.

## 4. Benchmark Server Process Management
**Finding**: `unified_benchmark.py` correctly reports `ResourceExhausted` failures but leaves the server binaries running as zombie processes.
**Action**:
- Add explicit cleanup logic (`killall longbow` or equivalent `os.kill`) in the benchmark orchestrator's exception handlers to prevent stale processes from interfering with subsequent runs.
"""

with open('docs/nextsteps.md', 'w') as f:
    f.write(nextsteps_content)

print("Successfully wrote docs/performance.md and docs/nextsteps.md")
