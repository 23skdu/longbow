# Longbow Linux/CUDA Performance Optimization Plan

**Status**: ALL OPTIMIZATIONS COMPLETE
**Target**: Match/Exceed macOS Metal Performance on Linux/CUDA  
**Date Updated**: March 6, 2026

---

## Performance Gap Analysis

| Operation | Current Linux | macOS Metal | Gap |
|-----------|---------------|-------------|-----|
| DoPut 5K vectors | 435 MB/s | 1099 MB/s | **2.5x slower** |
| DoPut 25K vectors | ~0.24 MB/s | 1452 MB/s | **6000x slower** |
| DoGet 5K vectors | 429 MB/s | 1564 MB/s | **3.6x slower** |
| VectorSearch 5K | 1130 QPS | 2279 Q/s | **2x slower** |
| VectorSearch 25K | 35 QPS | 2292 Q/s | **65x slower** |
| Search p50 latency | 0.82-0.84ms | 0.40-0.42ms | **2x slower** |

**Root Cause Analysis**:
- GC overhead: ~50%+ CPU time in garbage collection
- No GPU acceleration for vector search (FAISS stub only)
- Missing specialized SIMD kernels for common dimensions
- No CUDA memory pooling/management
- Suboptimal HNSW search path

---

## Implementation Priority Matrix

| Priority | Task | Effort | Impact | Status |
|----------|------|--------|--------|--------|
| **1** | CUDA/FAISS GPU Acceleration | 5-7 days | **CRITICAL** | ✅ COMPLETE |
| **2** | SIMD Dimension-Specific Kernels | 3-4 days | HIGH | ✅ COMPLETE |
| **3** | HNSW Search Path Optimization | 5-7 days | HIGH | ✅ COMPLETE |
| **4** | GPU-CPU Memory Transfer | 2-3 days | HIGH | ✅ COMPLETE |
| **5** | Advanced GC Tuning | 2-3 days | MEDIUM | ✅ COMPLETE |
| **6** | NUMA-Aware Allocation | 2-3 days | MEDIUM | ✅ COMPLETE |
| **7** | WAL io_uring Fix | 3-4 days | MEDIUM | ✅ COMPLETE |
| **8** | Batch Search Optimization | 3-4 days | MEDIUM | ✅ COMPLETE |
| **9** | Unified Memory Model | 5-7 days | MEDIUM | ✅ COMPLETE |
| **10** | Profiling and Auto-Tuning | 3-4 days | MEDIUM | ✅ COMPLETE |

---

## Performance Targets

| Metric | Current Linux | Target | macOS Metal Parity |
|--------|---------------|--------|-------------------|
| DoPut 5K | 435 MB/s | 900 MB/s | 1099 MB/s |
| DoPut 25K | 0.24 MB/s | 800 MB/s | 1452 MB/s |
| DoGet 5K | 429 MB/s | 1000 MB/s | 1564 MB/s |
| Search 5K p50 | 0.84ms | 0.5ms | 0.43ms |
| Search 25K p50 | 29ms | 1ms | 0.42ms |
| Search QPS 5K | 1130 | 2500 | 2279 |
| Search QPS 25K | 35 | 1500 | 2292 |
| GPU Search QPS 25K | N/A | 5000+ | N/A |
| GC Overhead | 50%+ | <15% | <10% |

---

## Quick Start: Implementation

```bash
# Step 1: Install FAISS GPU
conda install -c conda-forge faiss-gpu
export CGO_LDFLAGS="-L$CONDA_PREFIX/lib -lfaiss_gpu -lcudart -lcublas"
export CGO_CFLAGS="-I$CONDA_PREFIX/include"

# Step 2: Build with CUDA support
make build-cuda

# Step 3: Run benchmarks
python3 scripts/perf_test.py --dataset bench --rows 25000 --dim 128 --search

# Step 4: Profile GPU utilization
nvidia-smi dmon -s u

# Step 5: Capture detailed profile
curl http://localhost:9090/debug/pprof/profile?seconds=30 > cpu.prof
go tool pprof -http=:8080 cpu.prof
```

---

## Previously Completed Work

1. **Off-Heap Memory (SlabArena)** - ✅ COMPLETE
2. **Memory Allocation Pool** - ✅ COMPLETE
3. **io_uring Library** - ✅ COMPLETE (needs fix)
4. **GPU Framework** - ✅ COMPLETE (needs acceleration)
5. **SIMD Dispatch (AVX512/AVX2/NEON)** - ✅ COMPLETE (needs dimension-specific kernels)
6. **PQ Quantization (SQ8, BQ, PQ)** - ✅ COMPLETE

### March 4, 2026 Additions:

7. **CUDA/FAISS GPU Acceleration** - ✅ COMPLETE
   - Location: `internal/gpu/faiss_gpu.go`
   - Implemented actual FAISS GPU bindings (Flat, IVF, IVF-PQ index types)
   - GPU memory pool: `internal/gpu/memory_pool.go`
   - CGO bindings with proper CUDA/FAISS headers

8. **SIMD Dimension-Specific Kernels (768, 1536)** - ✅ COMPLETE
   - Location: `internal/simd/simd_amd64.go`, `internal/simd/simd_amd64.s`
   - Added AVX512 kernels for 768 and 1536 dimensions
   - Registered in dispatch table: `internal/simd/dispatch.go`
   - Fixed CPU detection to properly detect NEON on ARM64 only

9. **HNSW Search Path Optimization** - ✅ COMPLETE
   - Location: `internal/store/hnsw_optimized_search.go`
   - Implemented beam search with early termination
   - Added parallel search path support
   - Search metrics tracking

10. **GPU-CPU Memory Transfer** - ✅ COMPLETE
    - Location: `internal/gpu/memory_pool.go`
    - Implemented pinned memory pool for transfers
    - Transfer buffer management

11. **Advanced GC Tuning for GPU Workloads** - ✅ COMPLETE
    - Location: `internal/memory/gc_tuner.go`, `internal/memory/bulk_operations.go`
    - Added GPU utilization tracking via nvidia-smi integration
    - Dynamic GOGC adjustment based on GPU utilization
    - When GPU is highly utilized, reduces GOGC to lower CPU overhead
    - Added bulk operation GC bypass for bulk vector ingestion
    - Metrics: GCTunerGPUUtilization added to track GPU utilization

12. **NUMA-Aware Memory Allocation** - ✅ COMPLETE
    - Location: `internal/store/store.go`, `internal/metrics/metrics.go`
    - Integrated NUMA topology detection into VectorStore initialization
    - Added NUMATopology field to VectorStore for tracking NUMA nodes
    - Added GetNUMATopology() and IsNUMAEnabled() methods
    - Metrics: NUMANodeCount and NUMAEnabled metrics added
    - Logs NUMA topology info at startup

13. **WAL io_uring Fix** - ✅ COMPLETE
    - Location: `internal/iouring/cq.go`, `internal/storage/wal_backend_arrow_iouring.go`
    - Fixed CQE validation to skip spurious completions (UserData=0, Res=0)
    - Added double-buffering for write coalescing
    - Added proper error handling and recovery in WAL backend
    - Added flush threshold for automatic buffer flushing
    - Added buffered ops tracking in Stats

14. **Batch Search Optimization** - ✅ COMPLETE
    - Location: `internal/store/batch_search.go`, `internal/scheduler/work_queue.go`
    - Implemented BatchSearchProcessor for concurrent query processing
    - Added BatchSearchBatcher for query batching
    - Added ComputeBatchDistancesSIMD for SIMD batch distance computation
    - Created scheduler package with WorkQueue and PriorityWorkQueue
    - WorkQueue supports non-blocking submission and backpressure
    - PriorityWorkQueue supports priority levels (Low, Normal, High)
    - Added batch search metrics for monitoring

15. **Metal-Style Unified Memory Model** - ✅ COMPLETE
    - Location: `internal/storage/tiered_storage.go`, `internal/storage/mmap_vector.go`, `internal/gpu/zero_copy.go`
    - Implemented TierManager for hot/warm/cold tier detection
    - Auto-migrates vectors between tiers based on access patterns
    - Added GPU tier for hot data when GPU memory available
    - Implemented MmapVectorStorage for memory-mapped vector storage
    - Added zero-copy buffer management with ZeroCopyBuffer
    - Added pinned memory support for fast GPU transfers
    - Added ZeroCopyManager for async transfer coordination

16. **Performance Profiling and Auto-Tuning** - ✅ COMPLETE
    - Location: `internal/profiling/cpu.go`, `internal/tuning/auto_tuner.go`
    - Implemented Profiler with CPU, heap, goroutine profiling
    - Added automatic profile collection at configurable intervals
    - Added GPURecorder for tracking GPU kernel execution
    - Implemented AutoTuner for dynamic parameter tuning
    - Added WorkloadAnalyzer for classifying workloads
    - Auto-tuner adjusts parameters based on performance metrics

---

*Last Updated: March 5, 2026*
