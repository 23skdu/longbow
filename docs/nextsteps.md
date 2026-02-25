# Longbow Development Roadmap

## Status: Linux Performance Optimization Required | GPU Complete | io_uring Complete

**Date Updated**: February 22, 2026

---

## 🔥 CRITICAL: Linux Performance Optimization Plan

### Performance Gap Analysis

**Benchmark Comparison (Linux vs macOS Metal):**

| Operation | Linux (CPU) | macOS (Metal) | Gap |
|-----------|-------------|---------------|-----|
| DoPut 5K vectors | 523 MB/s | 1099 MB/s | **2.1x slower** |
| DoPut 25K vectors | 0.24 MB/s | 1452 MB/s | **6000x slower** |
| DoGet 5K vectors | 271 MB/s | 1564 MB/s | **5.8x slower** |
| VectorSearch 5K | 962 QPS | 2279 QPS | **2.4x slower** |
| VectorSearch 25K | 35 QPS | 2292 QPS | **65x slower** |
| Search p50 latency | 0.76-29ms | 0.40-0.42ms | **2-70x slower** |

**CPU Profile Root Causes (Linux):**
```
50%+ time in GC activities:
  - runtime.(*sweepLocked).sweep
  - runtime.findObject
  - runtime.(*gcBitsArena).tryAlloc
  - runtime.scanobject

15%+ time in atomic operations:
  - internal/runtime/atomic.(*Uint32).CompareAndSwap
  - internal/runtime/atomic.(*Uint32).Add
```

**Key Insight**: Linux spends >50% of CPU time in garbage collection. macOS Metal uses GPU memory which doesn't pressure Go's GC, providing an inherent advantage.

---

### Priority 1: Off-Heap Memory Management (CRITICAL)

**Problem**: All vector data goes through Go's heap, causing severe GC pressure.

**Solution**: Move vector storage to off-heap memory arenas.

**Implementation**:

1. **Enable SlabArena for Vector Storage**
   - Location: `internal/memory/arena.go`
   - Allocate vectors in off-heap memory via `mmap` syscalls
   - Bypass Go's GC for vector data
   - Use finalizers only for cleanup

2. **Memory Configuration**
   ```bash
   # Increase memory limit (currently 1GB default)
   export LONGBOW_MAX_MEMORY=4G
   
   # Disable GC during bulk operations
   export GOGC=off  # For bulk loads
   export GOGC=1000 # For steady-state (less frequent GC)
   ```

3. **Arena-Aware GCTuner Enhancement**
   - Current tuner exists but may not be optimally configured
   - Ensure arena memory is excluded from GC pressure calculations
   - Location: `internal/memory/gc_tuner.go`

**Success Criteria**:
- GC overhead < 20% of CPU time
- DoPut throughput > 800 MB/s for 5K vectors
- Search p50 latency < 0.6ms for 5K vectors

**Effort**: Medium (3-5 days)  
**Impact**: **CRITICAL** - Expected 2-3x improvement

---

### Priority 2: SIMD Dispatch Optimization (HIGH)

**Problem**: AVX512 kernels exist but may not be optimally utilized.

**Current State**:
- AVX512 kernels: `internal/simd/simd_amd64.go`
- CPU detection: `internal/simd/cpu_detection.go`
- Dispatch: `internal/simd/dispatch.go`

**Investigation Needed**:
1. Verify AVX512 is actually being used on this CPU
   ```bash
   # Check CPU features
   cat /proc/cpuinfo | grep -i avx
   
   # Run SIMD benchmarks
   go test -bench=. -benchmem ./internal/simd/...
   ```

2. Profile SIMD dispatch overhead
   ```bash
   curl http://localhost:9090/debug/pprof/profile?seconds=30 > cpu.prof
   go tool pprof -top cpu.prof | grep simd
   ```

3. Check for unnecessary bounds checks in hot paths

**Optimizations**:
1. Add specialized kernels for common dimensions (128, 384, 768, 1536)
2. Use vertical batching for cache efficiency
3. Add prefetch instructions for predictable access patterns

**Success Criteria**:
- Verified AVX512 dispatch in use
- Distance computation < 10ns per dimension
- Batch distance: > 100M comparisons/second

**Effort**: Medium (2-3 days)  
**Impact**: HIGH - Expected 1.5-2x improvement

---

### Priority 3: HNSW Index Optimization (HIGH)

**Problem**: Search latency degrades dramatically with dataset size (29ms at 25K).

**Root Causes**:
1. Inefficient neighbor list management
2. Lock contention during concurrent searches
3. Cache-unfriendly memory access patterns

**Optimizations**:

1. **Neighbor List Optimization**
   - Use contiguous memory for neighbor storage
   - Pre-allocate neighbor lists based on expected `M` parameter
   - Location: `internal/store/arrow_hnsw.go`

2. **Search Path Optimization**
   - Implement priority queue-based beam search
   - Add early termination for high-confidence results
   - Use SIMD for batch distance computations

3. **Memory Prefetching**
   - Add `runtime.prefetch` calls before accessing nodes
   - Use non-temporal prefetch for write paths

**Success Criteria**:
- Search p50 < 1ms for 25K vectors
- Search p99 < 5ms for 25K vectors
- Linear scaling up to 100K vectors

**Effort**: High (1 week)  
**Impact**: HIGH - Critical for production viability

---

### Priority 4: CUDA/FAISS Enablement (MEDIUM)

**Problem**: GPU acceleration exists but FAISS library is not linked.

**Current State**:
- Framework complete: `internal/gpu/faiss_gpu.go`
- CGO bindings ready
- Runtime detection implemented

**Blocking Issue**: FAISS GPU library installation

**Solution Options**:

1. **Option A: Conda Installation** (Recommended)
   ```bash
   # Install Miniconda if needed
   wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
   bash Miniconda3-latest-Linux-x86_64.sh
   
   # Install FAISS GPU
   conda install -c conda-forge faiss-gpu
   
   # Set library paths
   export CGO_LDFLAGS="-L$CONDA_PREFIX/lib -lfaiss_gpu -lcudart -lcublas"
   export CGO_CFLAGS="-I$CONDA_PREFIX/include"
   
   # Build
   make build-cuda
   ```

2. **Option B: Build from Source**
   ```bash
   git clone https://github.com/facebookresearch/faiss.git
   cd faiss
   cmake -DFAISS_ENABLE_GPU=ON \
         -DCMAKE_CUDA_ARCHITECTURES="80;90" \
         -DCMAKE_INSTALL_PREFIX=/usr/local/faiss \
         -B build .
   cmake --build build -j$(nproc)
   sudo cmake --install build
   ```

3. **Option C: CPU-Only FAISS** (Fallback)
   ```bash
   conda install -c conda-forge faiss-cpu
   # Still provides significant speedup over pure Go
   ```

**Success Criteria**:
- FAISS library detected at startup
- GPU-accelerated search throughput > 5000 QPS
- Search p50 < 0.5ms for 25K vectors

**Effort**: Low (1-2 days) if CUDA available  
**Impact**: **CRITICAL** - Parity with macOS Metal

---

### Priority 5: Memory Allocation Pool (MEDIUM)

**Problem**: Frequent allocations for temporary objects during search.

**Solution**: Implement object pools for hot paths.

**Implementation**:

1. **Distance Result Pool**
   ```go
   // internal/pool/distance.go
   var distancePool = sync.Pool{
       New: func() interface{} {
           return make([]float32, 0, 1000)
       },
   }
   ```

2. **Bitmap Pool** (Already exists: `internal/pool/bitmap.go`)
   - Verify optimal sizing
   - Add metrics for pool hit rate

3. **Search Context Pool**
   - Pool search contexts for repeated queries
   - Avoid allocating visitor state per search

**Success Criteria**:
- Pool hit rate > 90%
- Allocation rate during search < 1KB/query
- GC pressure reduced by 30%

**Effort**: Low (1-2 days)  
**Impact**: MEDIUM - Complements other optimizations

---

### Priority 6: io_uring WAL Backend Fix (MEDIUM)

**Problem**: Custom io_uring implementation has completion issues.

**Current State**:
- Location: `internal/iouring/`
- Issue: CQE returns UserData=0, Res=0
- Suspected: struct alignment issue

**Investigation Steps**:
1. Add debug logging to SQE submission
2. Verify struct alignment with C ABI
3. Test with O_DIRECT disabled
4. Compare with liburing behavior

**Alternative**: Use standard buffered I/O for now
- `internal/storage/wal_backend_arrow.go`
- Simpler, more reliable
- Accept slightly lower throughput

**Success Criteria**:
- WAL writes complete successfully
- No data loss on crash recovery
- Throughput > 500 MB/s

**Effort**: Medium (2-3 days)  
**Impact**: MEDIUM - I/O throughput for durability

---

## Implementation Priority Matrix

| Priority | Task | Effort | Impact | Status |
|----------|------|--------|--------|--------|
| **1** | Off-Heap Memory Management | 3-5 days | **CRITICAL** | 🔴 TODO |
| **2** | SIMD Dispatch Optimization | 2-3 days | HIGH | 🔴 TODO |
| **3** | HNSW Index Optimization | 1 week | HIGH | 🔴 TODO |
| **4** | CUDA/FAISS Enablement | 1-2 days | **CRITICAL** | ⚠️ Partial |
| **5** | Memory Allocation Pool | 1-2 days | MEDIUM | 🔴 TODO |
| **6** | io_uring WAL Backend Fix | 2-3 days | MEDIUM | ⚠️ Under Investigation |

---

## Quick Start: Linux Performance Testing

```bash
# 1. Build optimized binary
make build

# 2. Set memory configuration
export LONGBOW_MAX_MEMORY=4G
export GOGC=200

# 3. Start server with profiling
./bin/longbow &
SERVER_PID=$!

# 4. Run benchmarks
python3 scripts/perf_test.py --dataset bench --rows 10000 --dim 128 --queries 1000 --search

# 5. Capture CPU profile
curl http://localhost:9090/debug/pprof/profile?seconds=30 > cpu.prof
go tool pprof -http=:8080 cpu.prof

# 6. Check SIMD dispatch
curl http://localhost:9090/metrics | grep simd_dispatch

# Cleanup
kill $SERVER_PID
```

---

## Performance Targets (Linux)

| Metric | Current | Target | Parity Goal |
|--------|---------|--------|-------------|
| DoPut 5K | 523 MB/s | 900 MB/s | 1099 MB/s |
| DoPut 25K | 0.24 MB/s | 500 MB/s | 1452 MB/s |
| DoGet 5K | 271 MB/s | 1000 MB/s | 1564 MB/s |
| Search 5K p50 | 0.76ms | 0.5ms | 0.43ms |
| Search 25K p50 | 29ms | 1ms | 0.42ms |
| Search QPS 5K | 962 | 2000 | 2279 |
| Search QPS 25K | 35 | 1000 | 2292 |
| GC Overhead | 50%+ | <20% | <10% |

---

## ✅ Previously Completed (February 2026)

1. **Custom Zero-Lock Zero-Copy io_uring Library** - ✅ COMPLETE
   - Location: `internal/iouring/` (9 files)
   - WAL Backend Integration: `internal/storage/wal_backend_arrow_iouring.go`
   - All io_uring tests passing (read, write, fsync operations)

2. **All GPU Support (Parts 1-15)** - COMPLETE
   - Build tag system (`gpu`, `gpu_cuda`, `gpu_metal`)
   - Makefile targets: `build-cuda`, `build-metal`, `build-gpu`
   - CUDA memory management via CGO
   - GPU backend abstraction and detection
   - GPU metrics and monitoring

3. **Concrete Quantizer Implementations** - COMPLETE
   - SQ8 (Scalar Quantization 8-bit) - 4x memory savings
   - BQ (Binary Quantization) - 32x memory savings
   - PQ (Product Quantization)

---

## Remaining Non-Critical Work

### Hybrid Search Cross-Encoder Scoring (LOW PRIORITY)

**Status**: ⚠️ TODO - Stub implementation exists  
**Location**: `internal/store/hybrid_pipeline.go:348`

**Effort**: High (1-2 weeks)  
**Impact**: Search relevance improvement

### Extended Chaos Testing & Validation (LOW PRIORITY)

**Status**: Available but not executed  
**Location**: `scripts/run_soak.sh`, `scripts/long_soak_local.sh`

---

*Last Updated: February 22, 2026*
