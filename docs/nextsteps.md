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

---

## HIGH PRIORITY Incomplete Features

### 1. PQ Encoder Training - NOT IMPLEMENTED
**Files**: `internal/pq/encoder.go`, `internal/pq/encoder_test.go:42-131`

**Issue**: Product Quantization training returns "not implemented" and tests are skipped.

**Action**: Implement PQ training algorithm (k-means clustering for codebook generation).

---

### 2. Stream Aggregator - INCOMPLETE
**File**: `internal/sharding/stream_aggregator.go:113,126,185,290`

**Issue**: Multiple methods return `nil, nil` - not fully implemented.

**Action**: Complete stream aggregation implementation for distributed queries.

---

### 3. Forwarder - NOT IMPLEMENTED
**File**: `internal/sharding/forwarder.go:252`

**Issue**: Returns `"forwarding for method %s not yet implemented"`

**Action**: Implement gRPC method forwarding for cross-node operations.

---

### 4. Graph Store Arrow Serialization - NOT IMPLEMENTED
**File**: `internal/store/graph_store_test.go:135`

**Issue**: Test skipped - Arrow serialization not implemented.

**Action**: Implement Arrow serialization for GraphStore.

---

### 5. Vector Extraction - MISSING HELPERS
**File**: `internal/store/vector_extraction_test.go:8`

**Issue**: Test skipped - missing internal helpers.

**Action**: Implement vector extraction helpers or fix test.

---

### 6. CleanupTombstones - STUB
**File**: `internal/store/arrow_hnsw.go:1176-1178`

```go
func (h *ArrowHNSW) CleanupTombstones(threshold int) (int, error) {
    return 0, nil // Stub
}
```

**Action**: Implement tombstone cleanup for deleted vectors.

---

### 7. SetIndexedColumns - STUB
**File**: `internal/store/arrow_hnsw.go:1180-1182`

```go
func (h *ArrowHNSW) SetIndexedColumns(columns []string) {
    // No-op for now
}
```

**Action**: Implement indexed column tracking for selective indexing.

---

## MEDIUM PRIORITY Issues

### 8. SIMD Filter Operations - NOT ENABLED
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

### 11. Test Fixes Needed
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
