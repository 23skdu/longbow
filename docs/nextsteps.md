# Actionable Stability and Performance Recommendations

Based on the audit of the v0.2.1-rc4 benchmark matrix (see `docs/performance.md`), the following immediate actions are recommended:

## P0 BLOCKER: Test Suite & Codebase Context Window Optimization

**Problem Context:** 
1. The monolithic `internal/store` package contains over 280 test files. When run with `-race`, it exceeds standard 15-minute execution limits due to sequential execution and heavy stress tests.
2. Key files like `navigation.go` (>2600 lines) and `arrow_hnsw.go` (>1500 lines) exceed standard LLM context windows, degrading the efficiency of agentic code modifications.
3. The test suite contains overlapping, frivolous tests that increase runtime without significantly improving coverage or catching bugs.

**Action Plan:**
- **Phase 1 (Test Consolidation & Pruning):** Audit `internal/store` to combine repetitive single-case tests into shared table-driven tests. Delete frivolous or redundant tests.
- **Phase 2 (Race Optimization):** Wrap heavy dataset generation and extreme stress tests in `if testing.Short() { t.Skip("skipping heavy ingestion in short mode") }` so `go test -short -race` can run rapidly.
- **Phase 3 (Mega-Package Mitigation):** Break the monolithic `internal/store` package into sub-packages (e.g., `internal/store/index`, `internal/store/wal`, `internal/store/cluster`) to parallelize test execution at the package level.
- **Phase 4 (Context-Window Optimization):** Refactor massive files like `navigation.go` into <=800 line chunks based on behavior (e.g., extract polymorphic dispatch into `distance_dispatch.go`).

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
