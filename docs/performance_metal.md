# Longbow Mac Metal Performance Benchmarks

**Date**: March 22, 2026
**Platform**: macOS (Darwin arm64)
**CPU**: Apple M3 Pro (6P + 6E cores)
**Memory**: 36 GB RAM
**GPU**: Apple Metal GPU (integrated)

> **Note**: Metal GPU performance benchmarks require running with adequate memory allocation
> to avoid GC pressure. Use `LONGBOW_MAX_MEMORY=20g` for Apple Silicon M3 Pro.

---

## Build Status

### Metal GPU Build

```bash
# Build with Metal GPU support
make build-metal

# Or manually
CGO_ENABLED=1 go build -tags gpu -o bin/longbow-metal ./cmd/longbow
```

**Build Output**:
- Binary: `bin/longbow-metal`
- Build tags: `gpu && darwin && arm64`
- Metal framework detection: ✓

### Run Performance Tests

```bash
# Run Metal performance benchmark suite
./scripts/run_metal_perf.sh
```

The script:
1. Detects Metal framework availability
2. Builds `longbow-metal` binary
3. Starts server with Metal GPU support (20GB memory)
4. Runs ingestion, dense search, and hybrid search benchmarks
5. Outputs JSON results to `results_metal_*.json`

---

## Benchmark Configuration

- **Test Tool**: `bin/bench-tool` (Go-based)
- **Test Types**: Ingestion, Dense Search
- **Dimensions**: 128
- **Batch Size**: 5000 (ingestion), 1 (search)
- **Data Type**: float32
- **Metric**: Euclidean (L2)
- **Search k**: 10
- **Concurrent Workers**: 4
- **Test Duration**: 10 seconds
- **Memory Allocation**: 20 GB

---

## Implementation Details

### GPU Memory Management

Metal GPU implementation uses a dedicated memory pool (`GPUMemPool`) with:

- **Allocation Strategy**: On-demand allocation from Metal heap
- **Memory Pooling**: Reuses deallocated buffers to reduce GPU memory fragmentation
- **Thread Safety**: Synchronized access via mutex

### Files

| File | Purpose |
|------|---------|
| `internal/gpu/memory_base.go` | Base `GPUMemPool` struct (all platforms) |
| `internal/gpu/memory_types.go` | Darwin-specific types + `NewGPUMemPool` |
| `internal/gpu/memory_metal.go` | Metal-specific memory implementations |
| `internal/gpu/metal_gpu.go` | Base Metal GPU implementation |
| `internal/gpu/metal_gpu_hybrid.go` | Hybrid search GPU kernels |
| `internal/gpu/metal_gpu_optimized.go` | Optimized GPU kernels (SIMD) |
| `internal/gpu/gpu_enabled_darwin.go` | Darwin build configuration |

### Build Tags

```go
//go:build gpu && darwin && arm64
```

Required for all Metal-specific files.

---

## Known Issues

### Memory Pressure with Pre-existing Data

The default memory allocation (1GB) may cause high heap utilization warnings when
pre-existing benchmark data is loaded. For performance testing:

```bash
LONGBOW_MAX_MEMORY=20g ./bin/longbow-metal server
```

### MetalIndexOptimized Tests

`TestMetalIndexOptimized_Basic` is skipped due to Metal shader compilation issues
with heap-based top-k selection. The shader has a variable scoping bug that needs
to be addressed.

---

## Observed Results

### Test Run: M3 Pro, 20GB Memory, 128-dim, float32

**Ingestion** (4 concurrent workers, batch-size 5000):
- Throughput: 66.85 ops/sec
- Avg Latency: 59.77ms
- Errors: 1

**Search** (4 concurrent workers, batch-size 1):
- Throughput: 39,352 ops/sec (~39K QPS)
- Avg Latency: 101.5µs
- Errors: 0

### Performance Notes

- Search performance is excellent - 39K QPS with sub-millisecond latency
- Ingestion limited by HNSW indexing overhead, not network/server
- Memory utilization stable with 20GB allocation
- Go bench-tool properly batches records, avoiding queue buildup issues

---

## Performance Expectations

Based on M3 Pro Metal GPU architecture:

| Operation | Expected Performance |
|-----------|---------------------|
| Vector Ingestion | 500K+ vec/s (ideal conditions) |
| Dense Search (128-dim) | 10K+ QPS |
| Dense Search (384-dim) | 5K+ QPS |
| Hybrid Search | 1K+ QPS |

> **Note**: Actual performance depends on workload characteristics and memory allocation.
> Current benchmark script needs optimization for batched ingestion mode.

---

## Troubleshooting

### Metal Framework Not Found

```bash
# Verify Metal is available
ls -la /System/Library/Frameworks/Metal.framework
```

### Build Fails with Undefined Symbols

Ensure `CGO_ENABLED=1` and macOS ARM64 architecture:
```bash
CGO_ENABLED=1 go build -tags gpu -o bin/longbow-metal ./cmd/longbow
```

### High Memory Utilization

Increase memory allocation:
```bash
LONGBOW_MAX_MEMORY=20g ./bin/longbow-metal server
```

### Benchmark Script Timeout

If the benchmark script times out, reduce the duration in `scripts/run_metal_perf.sh`:
- Reduce `--duration 10s` to `--duration 5s`
- Reduce `--concurrency 4` to `--concurrency 2`

---

*Last Updated: March 22, 2026*
