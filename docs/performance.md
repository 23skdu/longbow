# Performance Optimization Guides

This document provides performance optimization guidance for Longbow.

## I/O Performance Optimization

### WAL Backend Selection

#### Standard Backend

- Good for: Small workloads, compatibility
- Overhead: System call per write/fsync

#### io_uring Backend

- Good for: High-throughput workloads, bulk writes
- Overhead: Batched async operations
- Requirements: Linux kernel 5.1+

### Configuration Recommendations

```bash
# Enable io_uring for high-throughput scenarios
export LONGBOW_STORAGE_USE_IOURING=true
export LONGBOW_STORAGE_ASYNC_FSYNC=true
export LONGBOW_WAL_COMPRESSION=true
```

### Performance Tuning

#### Batch Size Optimization

- Default: 100 entries per batch
- High throughput: 1000 entries per batch
- Low latency: 10-50 entries per batch

#### Compression Trade-offs

- Snappy compression: Reduces I/O by 60-80%
- CPU overhead: 10-15% additional CPU usage

### Benchmarking Results

Typical performance characteristics:

| Backend      | Throughput | Latency | CPU Usage | Memory |
|--------------|-----------|----------|------------|--------|
| Standard      | 10K ops/s  | 1ms     | 20%   | Baseline |
| io_uring     | 50K ops/s  | 0.5ms   | 15%   | +10%   |

## Scale Validation (50k Vectors)

Verified performance on 50k vectors (M1 Pro equivalent):

| Type | Dims | Ingestion (vec/s) | Search p99 (ms) | Notes |
|---|---|---|---|---|
| **Float32** | 128 | ~1413 | < 0.1 | Zero-copy path |
| **Float64** | 128 | ~1367 | < 0.1 | |
| **Int8** | 128 | ~975 | < 0.1 | Conversion overhead |
| **Complex64** | 128 | ~1172 | < 0.1 | |

Scaling behavior: Ingestion throughput inversely proportional to dimension. Search latency remains sub-millisecond for up to 50k vectors due to HNSW logarithmic scaling.

## High-Throughput Validation (>800 Mb/s)

Target: >800 Mb/s Ingestion, >2000 Mb/s Retrieval.
Verified on 3-node cluster (sharded):

| Dim | Type | Ingest (Mb/s) | Retrieve (Mb/s) | Status |
|---|---|---|---|---|
| **128** | Float32 | 243 | 24,875 | **Miss** (Overhead limited) |
| | Float64 | 735 | 29,651 | **Near** |
| | Int8 | 143 | 14,409 | **Miss** |
| | Complex64 | **1,064** | 30,250 | **PASS** |
| | Complex128 | **1,120** | 33,104 | **PASS** |
| **768** | Float32 | **2,316** | 33,367 | **PASS** |
| | Float64 | **2,635** | 34,278 | **PASS** |
| | Int8 | 565 | 25,521 | **Miss** |
| | Complex64 | **2,074** | 32,824 | **PASS** |
| | Complex128 | **1,646** | 32,731 | **PASS** |
| **1536** | Float32 | **2,085** | 29,466 | **PASS** |
| | Float64 | **2,255** | 32,056 | **PASS** |
| | Int8 | 785 | 17,742 | **Near** |
| | Complex64 | **2,549** | 32,552 | **PASS** |
| | Complex128 | **1,943** | 31,803 | **PASS** |

**Analysis**:

- **Retrieval**: Consistently exceeds 2,000 Mb/s target by an order of magnitude (>20 Gb/s), leveraging Flight's zero-copy read paths.
- **Ingestion**:
  - **Passes** for medium/large vectors and complex types where payload size amortizes RPC overhead.
  - **Misses** for small vectors (128d Float/Int8) where per-vector processing overhead dominates bandwidth. Throughput (vec/s) is high, but total bandwidth is low due to small payload.
  - **Int8**: Lower bandwidth utilization due to 4x compression ratio vs Float32, making it harder to saturate link bandwidth with small payloads.

## Memory Performance

### Allocation Strategies

#### Arena Allocation

- Use memory arenas for hot paths
- Reduces GC pressure significantly
- Recommended for vector operations

#### Size-Classes

- Tiny: < 64 bytes
- Small: 64-256 bytes
- Medium: 256-1KB
- Large: 1KB-64KB
- Huge: > 64KB

### Garbage Collection Tuning

```go
// Aggressive GC tuning for memory-sensitive workloads
GOGC=20

// Conservative for latency-sensitive workloads
GOGC=100
```

## SIMD Optimizations

### Vector Distance Calculations

#### CPU Dispatch

- Automatic CPU feature detection
- Runtime dispatch to optimal implementation

#### SIMD Instruction Sets

- AVX512: 512-bit vectors (best performance)
- AVX2: 256-bit vectors
- SSE4.2: 128-bit vectors (fallback)

### Performance Metrics

Monitor these key metrics:

- `longbow_storage_write_ops_total` - I/O operations per second
- `longbow_storage_write_latency_seconds` - Average write latency
- `longbow_memory_alloc_bytes_total` - Memory allocation rate
- `longbow_gc_pause_seconds_total` - GC pause time

## Scaling Considerations

### Horizontal Scaling

#### Read Scaling

- Add more replicas for read-heavy workloads
- Use consistent hashing for data distribution

#### Write Scaling

- Partition writes by namespace
- Use sharding for high write throughput

### Vertical Scaling

#### Memory

- Recommended: 64GB+ for production
- Formula: 2x dataset size for comfortable operation

#### CPU

- Vector operations benefit from higher clock speeds
- SIMD instructions critical for performance

## Troubleshooting

### Common Performance Issues

1. **High GC Pressure**
   - Symptoms: Frequent GC pauses, high CPU usage
   - Solution: Increase GOGC, use arena allocators

2. **I/O Bottlenecks**
   - Symptoms: High write latency, low throughput
   - Solution: Enable io_uring, increase batch sizes

3. **Memory Leaks**
   - Symptoms: Memory growth over time
   - Solution: Profile with pprof, check for unreleased resources

4. **Lock Contention**
   - Symptoms: High CPU in goroutines, low throughput
   - Solution: Use lock-free data structures, reduce critical sections

### Performance Analysis Tools

```bash
# CPU profiling
curl http://localhost:9090/debug/pprof/profile > cpu.pprof
go tool pprof cpu.pprof

# Memory profiling
curl http://localhost:9090/debug/pprof/heap > heap.pprof
go tool pprof heap.pprof

# Goroutine analysis
curl http://localhost:9090/debug/pprof/goroutine > goroutine.pprof
go tool pprof goroutine.pprof
```

## Best Practices

### Code Level

1. **Profile before optimizing**
2. **Focus on hot paths first**
3. **Measure real workloads**
4. **Consider trade-offs**

### System Level

1. **Use appropriate Linux kernel versions**
2. **Configure for NUMA architectures**
3. **Monitor system resources**

### Deployment Level

1. **Set appropriate resource limits**
2. **Monitor key performance metrics**
3. **Use performance alerts**
