# 10-Part Plan: Error Handling & Logging Standardization (STABILIZATION)

**Goal**: Move Longbow from "research" grade to "production" grade by standardizing how errors are reported and how the system logs its state.

## Current Priority: Stabilization Plan

- [ ] **1. Allocator Hardening**: Replace `panic()` in `internal/memory/allocator.go` (`AssertSize`) with `testing.T.Errorf` or error returns where applicable.
- [ ] **2. SIMD Length Safeguards**: Refactor SIMD functions in `internal/simd/simd.go` to return errors on `length mismatch` instead of panicking.
- [ ] **3. Storage Engine Silent Error Cleanup**: Fix `_ = err` calls in `internal/storage/engine.go` (e.g., lines 375-376) to properly propagate or log errors.
- [ ] **4. Fuzz Test Robustness**: Replace `_ = store.Close()` and other ignored errors in `internal/store/fuzz_test.go` with proper checks to catch silent failures during fuzzing.
- [ ] **5. Config Validation**: Call `ValidateConfig()` in `cmd/longbow/main.go` immediately after loading the configuration to prevent invalid states from propagating.
- [ ] **6. Production Logging Implementation**: Replace `fmt.Printf` and `fmt.Fprintf(os.Stderr, ...)` in `internal/simd/simd.go`, `internal/storage/engine.go`, and `cmd/longbow/main.go` with `zerolog`.
- [ ] **7. Storage Engine Debug Print Removal**: Remove or convert `fmt.Printf` calls in `internal/storage/engine.go` (lines 265, 423, 451, 454) to `logger.Debug()`.
- [ ] **8. Query Parser Logging Refactor**: Replace `DEBUG PARSER` prints in `internal/query/zero_alloc_vector_search.go` with a proper `zerolog.Logger` instance.
- [ ] **9. Codebase-wide fmt.Printf Audit**: Perform a final grep for `fmt.Printf` in the `internal/` directory to ensure no leakages of unformatted logs to stdout.
- [ ] **10. Verification & Linting**: Run `golangci-lint` to ensure all new error returns are handled and that the `errcheck` pass is clean.

---

# 10-Part Plan: Optimizing 384d Ingestion Throughput (v0.1.5)

**Goal**: Achieve > 800 MB/s ingestion throughput for all data types (especially Float32) at 384 dimensions.
**Current State**: Float32 @ 384d is ~608 MB/s. Int8 @ 384d is ~551 MB/s.

## Current Progress Status

- [ ] **1. Deep Profiling & Instability Investigation**: **ACTIVE**. Logs show significant memory pressure and GC churn.
- [ ] **2. Client-Side Batch Size Optimization**: **PENDING**. Need to verify if 1k batch still outperforms 10k in current stabilization.
- [x] **3. Server-Side Batch Aggregation Tuning**: **IMPLEMENTED**. `DoPut` aggregates batches (via `concatenateBatches`) up to 50k rows or 32MB.
- [x] **4. Ingestion Worker Scaling**: **IMPLEMENTED**. `VectorStore` uses `ingestionQueue` and configurable workers (default 12 in logs).
- [x] **5. Zero-Copy Pathway Audit**: **REVIEWED**. `GraphData.SetVector` uses `copy` for `float32`. Native zero-copy from Arrow buffers is blocked by the HNSW chunked-arena storage requirement.
- [x] **6. SIMD Distance Optimization for 384d**: **IMPLEMENTED**. Optimized `euclidean384AVX512` and `dot384AVX512` kernels exist.
- [x] **7. WAL Async Write Tuning**: **IMPLEMENTED**. `persistenceQueue` and async persistence worker handle WAL writes.
- [x] **8. Memory Allocator & GC Tuning**: **IMPLEMENTED**. `SlabArena` and `TypedArena` are used to reduce object churn.
- [x] **9. Index Construction Parameter Tuning**: **IMPLEMENTED**. `AddBatchBulk` uses parallelized linkage and robust pruning.
- [x] **10. Pre-allocation Strategy**: **IMPLEMENTED**. `NewGraphData` and `Grow` handle capacity reservation.

---

## Identified Regressions & Critical Issues (v0.1.4-rc1)

### A. Memory Pressure & GC Thrashing

**Observation**: `node1.log` shows constant GC cycles (e.g., `gc 20`, `gc 21`, ...) once the heap reaches ~6GB.
**Impact**: Throughput drops drastically as the CPU spends >20% of time in GC.
**Root Cause**: Ingestion of large datasets (soak test) exceeding the configured `max_memory` limit too quickly, or `TypedArena` not reclaiming memory fast enough for the benchmark pace.
**Action**: increase `max_memory` for benchmark environments or tune `GOGC` dynamically based on available RAM.

### B. Metrics Server Port Conflict

**Observation**: `server.log` reports `listen tcp 0.0.0.0:9090: bind: address already in use`.
**Impact**: Losing visibility into Prometheus metrics during soak tests.
**Action**: Add retry logic with port increment or allow configurable metrics port per node.

### C. Validation Script OOM/Termination

**Observation**: `validation_output.txt` shows `python3 scripts/verify_global_search.py` being killed (Signal 9).
**Impact**: Search validation fails during soak tests.
**Root Cause**: Local machine memory exhaustion or resource limits.

---

## Revised Action Plan

1. **Investigate Heap Growth**: Profile `TypedArena` during the 5th-10th minute of soak to see why memory isn't stabilizing.
2. **Batch Size Re-verification**: Re-run 1k vs 10k test now that server-side aggregation is active.
3. **Metrics Port Fix**: Ensure each node in the cluster uses a unique metrics port.
