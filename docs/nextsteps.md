# Longbow Performance Optimization Status

## Current Benchmark Results (2026-03-16)

### SIMD Microbenchmarks (Apple M3 Pro)
| Operation | ns/op | MB/s |
|-----------|-------|-------|
| Euclidean128 | 31.33 | 16,133 |
| Euclidean384 | 98.91 | 15,433 |
| Euclidean768 | 155.5 | 19,055 |
| Euclidean1536 | 368.4 | 16,548 |

### Integration Benchmarks (float32, dim=384, InitialCapacity=50k)

| Vectors | DoPut (MB/s) | DoGet (MB/s) | Search (QPS) |
|---------|--------------|--------------|--------------|
| 1,000 | 414 | 443 | 1,526 |
| 5,000 | 716 | 1,240 | 622 |
| 10,000 | 1,270 | 1,779 | 944 |
| 15,000 | 1,297 | 1,874 | 897 |
| 25,000 | 1,416 | 2,099 | 812 |

### Validation Tests (25k vectors, dim=128)

| Metric | Result | Target | Status |
|--------|--------|--------|--------|
| Ingest | 1,235 MB/s | 800 MB/s | ✅ PASS |
| DoGet | 2,223 MB/s | 1,700 MB/s | ✅ PASS |

---

## Float32 Fragmentation Fix ✅

### Problem
When vectors exceeded InitialCapacity (10k), Grow() created multiple small arena allocations causing:
- DoGet: 85% throughput drop
- Search: 92% QPS drop
- Incorrect search results (0 found)

### Solution
Increased default InitialCapacity from 10,000 to **50,000** in `internal/store/arrow_hnsw.go:102`

### Results After Fix
- 15k vectors: DoGet 271→1,874 MB/s (**6.9x**), Search 75→897 QPS (**12x**)
- 25k vectors: DoGet 271→2,099 MB/s, Search 812 QPS (correct results)

### 1. Optimize HNSW Dimension Index Parameters (Float32 Collapse Fix)
**Files**: `internal/store/arrow_hnsw.go`, `internal/store/insertion_core.go`

**Status**: Under Investigation
**Problem**: The integration benchmarks revealed a massive throughput dropoff for Float32 dense searches under specific configurations (e.g., Dimension 384, Scale 15,000+), falling below 100 QPS.
**Analysis**:
- Complex64 and Float32 both execute mathematically identical scalar/pointer arithmetic under unrolled Go loops.
- Both use zero-copy direct array fetch mechanisms (`GetVector`).
- This isolates the QPS dropoff to graph-traversal iterations sizing (number of node steps taken). The `Float32` pathways are taking significantly longer paths, likely caused by suboptimal connectivity layout links relative to dimension bounds.
**Action**:
- Profile and adjust internal adaptive level sizes to satisfy continuous dimensional struct bounds.
- Analyze scalar loop unroll impacts on CPU cache lines versus vector-sizes.

### 2. Review Grow() Trigger Alignment Thresholds
**Files**: `internal/store/insertion_core.go`, `internal/store/arrow_hnsw.go`

**Status**: Pending
**Problem**: Sub-optimal alignment constraints inside graph slice chunks might force cascading reallocations or skew HNSW level multipliers.
**Action**:
- Verify adaptive layer growth weight thresholds (`AdaptiveMEnabled`).
- Ensure node sizing link bounds satisfy scaling limits cleanly without forced degradation loops.

---

## MEDIUM PRIORITY Issues

### 3. SIMD Filter Operations - NOT ENABLED
**File**: `internal/query/filter_evaluator_test.go:372-1291`

**Issue**: Many SIMD filter tests are skipped.

**Action**: Enable SIMD filter operations for better query performance.

---

### 9. Generic Quantizer - Limited Types
**File**: `internal/store/generic_quantizer_test.go:264,271`

**Issue**: Float16 and Int8 types not supported.

**Action**: Extend quantizer type support.

---

### 10. Arrow Utils - Type Casting
**File**: `internal/store/arrow_utils.go:269`

**Issue**: `ExtractVectorFromArrow` returns error for certain type conversions.

**Action**: Implement full type conversion support.

---

### 4. Test Fixes Needed

| Test File | Issue | Action |
|-----------|-------|--------|
| `dataset_map_rcu_test.go:183` | Pending implementation | Implement or remove |
| `vector_search_action_test.go:8` | Undefined mocks | Fix mocks |
| `rate_limit_integration_test.go:11` | Refactor needed | Fix or remove |
| `structured_errors_test.go:9` | Refactor needed | Fix or remove |
| `arrow_neighbors_test.go:38` | Memory issues | Fix memory management |

---

## LOW PRIORITY / BY DESIGN

### Platform-Specific Stubs (No Action Needed)

| Feature | File | Platform |
|---------|------|----------|
| NUMA | `internal/store/numa_*_stub.go` | Linux only |
| io_uring | `internal/storage/wal_backend_stub.go` | Linux only |
| ONNX Metal | `internal/onnx/metal/stub.go` | macOS ARM64 only |
| GPU | `internal/gpu/memory.go` | Build tag gated |

### GPU Memory Operations (Requires `-tags=gpu`)
**File**: `internal/gpu/memory.go:170,175,185,190,195,200`

These return "not implemented yet" when built without GPU support. Build with `-tags=gpu` for actual implementations.

---

## Scripts Updated for 12GB Memory

Updated benchmark scripts to use 12GB memory limit for performance testing:

| Script | Previous | Updated |
|--------|----------|---------|
| `scripts/benchmark_runner.py` | env var | 12GB |
| `scripts/run_3node_performance.py` | 8GB | 12GB |
| `scripts/benchmark_suite.sh` | 6GB | 12GB |
| `scripts/start_bench_node.sh` | 8GB | 12GB |
| `scripts/run_iouring_comparison.sh` | 4GB | 12GB |

---

Last Updated: 2026-03-16
