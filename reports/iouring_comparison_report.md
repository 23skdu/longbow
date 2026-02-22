# io_uring Performance Comparison Report

**Generated:** 2026-02-19  
**Platform:** Linux ancalagon 6.17.0-14-generic #14-Ubuntu SMP PREEMPT_DYNAMIC x86_64 GNU/Linux  
**Go Version:** go1.24.9 linux/amd64  
**CPU:** 12th Gen Intel(R) Core(TM) i7-12650H

---

## Executive Summary

The io_uring implementation **exists but is broken** in the current codebase. While the standard WAL backend works perfectly and shows excellent performance, the io_uring backend fails to compile due to missing metrics and incorrect type usage.

---

## Test Results

### Standard WAL Backend (WITHOUT io_uring)

**Status:** ✅ PASS

**Results:**
```
BenchmarkWALStandard-16    67326    180414 ns/op    23883.21 MB/s    207588 B/op    2804 allocs/op
```

**Key Metrics:**
- **Operations:** 67,326 iterations
- **Latency:** 180,414 ns/op (~0.18 ms per operation)
- **Throughput:** 23,883.21 MB/s
- **Memory:** 207,588 B/op
- **Allocations:** 2,804 allocs/op

### io_uring WAL Backend (WITH io_uring)

**Status:** ❌ FAIL - Compilation Errors

**Error Details:**
```
# github.com/23skdu/longbow/internal/storage
internal/storage/storage_backend_linux.go:53:10: undefined: metrics.IOReadLatencySeconds
internal/storage/storage_backend_linux.go:81:10: undefined: metrics.IOWriteLatencySeconds
internal/storage/storage_backend_linux.go:121:13: cannot use req (variable of type iouring.Request) 
    as *iouring.Result value in assignment
internal/storage/storage_backend_linux.go:148:13: cannot use req (variable of type iouring.Request) 
    as *iouring.Result value in assignment
```

**Root Causes:**
1. **Missing Metrics:** The code references `metrics.IOReadLatencySeconds` and `metrics.IOWriteLatencySeconds` which don't exist in the codebase
2. **Type Mismatch:** The code treats `iouring.Request` as `*iouring.Result`, which is incorrect based on the `iouring-go` library API

---

## Code Analysis

### Build Tags

The io_uring implementation uses conditional compilation:

- **io_uring enabled:** `//go:build linux && iouring`
- **io_uring disabled (stub):** `//go:build !linux || (linux && !iouring)`

To enable io_uring, you must build with the `iouring` tag:
```bash
go build -tags=iouring ./...
```

### Configuration

io_uring can be enabled via environment variable:
```bash
export LONGBOW_STORAGE_USE_IOURING=true  # Default: false
```

Or in code via the storage engine config:
```go
storage.Config{
    UseIOUring: true,  // Enable io_uring backend
}
```

---

## Required Fixes

To make io_uring functional, the following fixes are needed:

### 1. Add Missing Metrics (internal/metrics/storage_metrics.go)

```go
// Add these metrics to support io_uring
var (
    IOReadLatencySeconds = promauto.NewHistogramVec(
        prometheus.HistogramOpts{
            Name:    "longbow_io_read_latency_seconds",
            Help:    "Latency of I/O read operations",
            Buckets: prometheus.DefBuckets,
        },
        []string{"backend"},
    )

    IOWriteLatencySeconds = promauto.NewHistogramVec(
        prometheus.HistogramOpts{
            Name:    "longbow_io_write_latency_seconds",
            Help:    "Latency of I/O write operations",
            Buckets: prometheus.DefBuckets,
        },
        []string{"backend"},
    )
)
```

### 2. Fix Type Issues (internal/storage/storage_backend_linux.go)

The `iouring-go` library API has changed. The code needs to be updated to use the correct types:

```go
// Current (broken):
req, err := b.ring.SubmitRequest(iouring.Pread(...), nil)
reqs[i] = req  // *iouring.Result - WRONG TYPE

// Should be:
req, err := b.ring.SubmitRequest(iouring.Pread(...), nil)
// req is iouring.Request, not *iouring.Result
```

### 3. Fix vectored I/O implementation

Lines 114-136 and 139-163 need to be updated to handle the library's actual API.

---

## Performance Comparison (Expected)

Based on the standard WAL benchmark results, here's what we would expect if io_uring were functional:

| Metric | Standard WAL | io_uring WAL | Expected Improvement |
|--------|-------------|--------------|---------------------|
| Throughput | 23,883 MB/s | ~30,000-35,000 MB/s* | +25-47% |
| Latency | 0.18 ms/op | ~0.12-0.15 ms/op* | -15-33% |
| CPU Usage | Baseline | Lower | -10-20% |
| Syscalls | Many | Fewer | Significant reduction |

*Estimated based on typical io_uring performance gains for I/O-bound workloads

---

## Recommendations

### Immediate Actions

1. **Fix the compilation errors** - The io_uring implementation needs:
   - Addition of missing metrics
   - Correction of type mismatches with the iouring-go library

2. **Update dependencies** - Check if a newer version of `github.com/iceber/iouring-go` has a different API

3. **Add CI tests** - Run `go build -tags=iouring ./...` in CI to catch compilation errors

### Long-term

1. **Implement proper benchmarking** - Once compilation is fixed, run comprehensive benchmarks comparing:
   - Sequential write throughput
   - Random write performance
   - Fsync latency
   - CPU utilization under load

2. **Add feature detection** - Check kernel version at runtime (requires Linux 5.1+)

3. **Consider AIO as fallback** - For older kernels, implement Linux AIO as an intermediate option

---

## Files Involved

| File | Purpose | Status |
|------|---------|--------|
| `internal/storage/wal_backend_linux.go` | io_uring WAL implementation | ✅ Compiles |
| `internal/storage/storage_backend_linux.go` | io_uring storage backend | ❌ Broken |
| `internal/storage/wal_backend_stub.go` | Non-Linux fallback | ✅ Works |
| `internal/storage/benchmark/io_benchmark_test.go` | Benchmark tests | ✅ Works (Linux only) |
| `internal/metrics/wal_metrics.go` | WAL-specific metrics | ✅ Complete |
| `internal/metrics/storage_metrics.go` | General storage metrics | ❌ Missing io_uring metrics |

---

## Test Commands

### Standard WAL (Works)
```bash
go test -v -bench=BenchmarkWALStandard -benchtime=10s ./internal/storage/benchmark/
```

### io_uring WAL (Broken - needs fixes)
```bash
go test -tags=iouring -v -bench=BenchmarkWALIOUring -benchtime=10s ./internal/storage/benchmark/
```

### Full Application Benchmark
```bash
# Without io_uring
LONGBOW_STORAGE_USE_IOURING=false ./scripts/benchmark_suite.sh

# With io_uring (after fixes)
LONGBOW_STORAGE_USE_IOURING=true ./scripts/benchmark_suite.sh
```

---

## Conclusion

The io_uring implementation in Longbow is **architecturally present but not functional**. The standard WAL backend shows excellent performance (23.8 GB/s throughput), but the io_uring backend cannot be tested due to compilation errors.

**Estimated effort to fix:** 2-4 hours
1. Add missing metrics (30 min)
2. Fix type issues in storage_backend_linux.go (1-2 hours)
3. Test and validate (1 hour)

Once fixed, io_uring should provide significant performance improvements for I/O-bound workloads, particularly for:
- High-throughput ingestion
- Write-heavy workloads
- Scenarios with many concurrent writers
