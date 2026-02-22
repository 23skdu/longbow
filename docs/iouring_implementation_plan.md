# Custom io_uring Library Implementation Plan

## Executive Summary

Build a production-grade, zero-lock, zero-copy io_uring library for Longbow that:
- Uses `golang.org/x/sys/unix` directly (Go 1.24+ compatible)
- Optimized for Apache Arrow RecordBatch serialization
- Lock-free submission/completion queues
- Comprehensive Prometheus metrics
- Full test coverage with race detection

**Target:** Replace `github.com/iceber/iouring-go` with a custom implementation

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Longbow WAL Layer                                │
├─────────────────────────────────────────────────────────────────────────┤
│  Arrow RecordBatch → IPC Serialization → Buffer Pool → io_uring Queue  │
└─────────────────────────────────────────────────────────────────────────┤
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    ArrowIOUring (Custom Library)                        │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                   │
│  │  Buffer Pool │  │  Ring Queue  │  │   Metrics    │                   │
│  │  (Aligned)   │  │ (Lock-free)  │  │ (Prometheus) │                   │
│  └──────────────┘  └──────────────┘  └──────────────┘                   │
└─────────────────────────────────────────────────────────────────────────┤
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    Kernel io_uring Interface                            │
├─────────────────────────────────────────────────────────────────────────┤
│  io_uring_setup() → mmap() → io_uring_enter() → Completion Queue       │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Detailed Implementation Plan

### Phase 1: Core io_uring Primitives (Week 1)

#### 1.1 Syscall Definitions
File: `internal/iouring/syscall.go`

```go
// IoUringSyscall numbers
const (
    SYS_IO_URING_SETUP   = 425
    SYS_IO_URING_ENTER   = 426
    SYS_IO_URING_REGISTER = 427
)

// Direct unix.Syscall6 calls - no syscall package usage
func ioUringSetup(entries uint32, params *IoUringParams) (int, error)
func ioUringEnter(fd int, toSubmit uint32, minComplete uint32, flags uint32, sig unsafe.Pointer) (int, error)
func ioUringRegister(fd int, opcode uint32, arg unsafe.Pointer, nrArgs uint32) (int, error)
```

**Rationale:** Using `unix.Syscall6` from `golang.org/x/sys/unix` avoids the deprecated `syscall` package that's causing Go 1.24 compatibility issues.

#### 1.2 Data Structures
File: `internal/iouring/types.go`

```go
// IoUringParams - io_uring setup parameters
type IoUringParams struct {
    SqEntries       uint32
    CqEntries       uint32
    Flags           uint32
    SqThreadCpu     uint32
    SqThreadIdle    uint32
    Features        uint32
    WqFd            uint32
    Resv            [3]uint32
    SqOff           SqRingOffsets
    CqOff           CqRingOffsets
}

// Submission Queue Entry
type SQE struct {
    Opcode          uint8
    Flags           uint8
    Ioprio          uint16
    Fd              int32
    Off             uint64
    Addr            uint64
    Len             uint32
    RwFlags         uint32
    UserData        uint64
    BufIndex        uint16
    Personality     uint16
    SpliceFdIn      int32
    SpliceOffIn     uint64
    SpliceLen       uint32
    SpliceFlags     uint32
    _padding        [4]byte
}

// Completion Queue Entry
type CQE struct {
    UserData        uint64
    Res             int32
    Flags           uint32
}
```

#### 1.3 Ring Management
File: `internal/iouring/ring.go`

**Key Design Decisions:**
- Single-producer, single-consumer (SPSC) lock-free queues
- Memory-mapped SQ and CQ rings (shared with kernel)
- Atomic operations for head/tail pointer management
- No mutex locks in hot path

```go
type Ring struct {
    fd          int                    // io_uring file descriptor
    params      IoUringParams          // Setup parameters
    
    // Memory-mapped regions
    sqRing      []byte                 // Submission queue ring
    cqRing      []byte                 // Completion queue ring
    sqes        []SQE                  // Submission queue entries
    
    // Lock-free pointers (accessed via atomics)
    sqHead      *uint32                // Consumer head (kernel)
    sqTail      *uint32                // Producer tail (user)
    cqHead      *uint32                // Consumer head (user)
    cqTail      *uint32                // Producer tail (kernel)
    
    // Cached values to avoid atomics
    sqRingMask  uint32
    cqRingMask  uint32
    sqRingEntries uint32
    cqRingEntries uint32
    
    // Buffer management
    bufferPool  *BufferPool
    
    // Metrics
    metrics     *RingMetrics
}
```

### Phase 2: Zero-Copy Arrow Integration (Week 1-2)

#### 2.1 Aligned Buffer Pool
File: `internal/iouring/buffer_pool.go`

**Requirements for O_DIRECT:**
- Memory alignment to 512-byte boundaries (block device sector size)
- Buffer size alignment to page size (usually 4096 bytes)
- Use `unix.Mmap` with `MAP_ANONYMOUS | MAP_PRIVATE`

```go
// BufferPool manages O_DIRECT-aligned buffers for Arrow data
type BufferPool struct {
    pageSize    int
    alignment   int      // 512 for O_DIRECT
    buffers     chan []byte
    allocator   memory.Allocator
}

func NewBufferPool(bufferSize, poolSize int) (*BufferPool, error) {
    // Allocate aligned memory using mmap
    // Returns buffers suitable for O_DIRECT I/O
}

func (p *BufferPool) Get() []byte    // Get aligned buffer
func (p *BufferPool) Put(buf []byte) // Return buffer to pool
```

#### 2.2 Arrow IPC Writer
File: `internal/iouring/arrow_writer.go`

```go
// ArrowIOWriter wraps io_uring with Arrow-specific optimizations
type ArrowIOWriter struct {
    ring        *Ring
    schema      *arrow.Schema
    bufferPool  *BufferPool
    writer      ipc.Writer  // For IPC serialization
}

func (w *ArrowIOWriter) WriteRecordBatch(rec arrow.RecordBatch) error {
    // 1. Serialize RecordBatch to IPC format into aligned buffer
    // 2. Submit single io_uring pwrite operation
    // 3. Return buffer to pool (after completion)
}

func (w *ArrowIOWriter) WriteV(batches []arrow.RecordBatch) error {
    // Use io_uring's vectored I/O (writev) for multiple batches
    // Submit as linked requests or batch submission
}
```

### Phase 3: Lock-Free Queue Implementation (Week 2)

#### 3.1 Submission Queue
File: `internal/iouring/sq.go`

**Algorithm:** Single-producer ring buffer with atomic tail updates

```go
// Submit submits a prepared SQE to the kernel
// Lock-free: only called from single goroutine
func (r *Ring) Submit(sqe *SQE) error {
    tail := atomic.LoadUint32(r.sqTail)
    next := tail + 1
    
    // Check for queue full
    if next-atomic.LoadUint32(r.sqHead) > r.sqRingEntries {
        return ErrSQFull
    }
    
    // Write to ring
    idx := tail & r.sqRingMask
    r.sqes[idx] = *sqe
    
    // Update tail (release barrier)
    atomic.StoreUint32(r.sqTail, next)
    
    return nil
}

// Flush submits pending SQEs to kernel via io_uring_enter
func (r *Ring) Flush() (int, error) {
    // Calculate how many entries to submit
    // Call io_uring_enter with IORING_ENTER_GETEVENTS if needed
}
```

#### 3.2 Completion Queue
File: `internal/iouring/cq.go`

**Algorithm:** Single-consumer with atomic head updates

```go
// Peek retrieves a CQE without advancing (non-blocking)
func (r *Ring) Peek() (*CQE, error) {
    tail := atomic.LoadUint32(r.cqTail)
    head := atomic.LoadUint32(r.cqHead)
    
    if head == tail {
        return nil, ErrCQEmpty
    }
    
    idx := head & r.cqRingMask
    return &r.cqEntries[idx], nil
}

// Advance marks CQEs as consumed
func (r *Ring) Advance(count uint32) {
    atomic.AddUint32(r.cqHead, count)
}
```

### Phase 4: Prometheus Metrics (Week 2-3)

File: `internal/iouring/metrics.go`

```go
var (
    // Operation latency
    IoUringSubmitLatency = promauto.NewHistogramVec(
        prometheus.HistogramOpts{
            Name: "longbow_iouring_submit_latency_seconds",
            Help: "Latency of io_uring submission operations",
            Buckets: []float64{0.000001, 0.00001, 0.0001, 0.001, 0.01},
        },
        []string{"operation"}, // read, write, fsync
    )
    
    // Queue depths
    IoUringSQDepth = promauto.NewGauge(
        prometheus.GaugeOpts{
            Name: "longbow_iouring_sq_depth",
            Help: "Current submission queue depth",
        },
    )
    
    IoUringCQDepth = promauto.NewGauge(
        prometheus.GaugeOpts{
            Name: "longbow_iouring_cq_depth",
            Help: "Current completion queue depth",
        },
    )
    
    // Operation counts
    IoUringOpsTotal = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "longbow_iouring_ops_total",
            Help: "Total io_uring operations",
        },
        []string{"operation", "status"}, // status: success, error
    )
    
    // Buffer pool metrics
    IoUringBufferPoolHits = promauto.NewCounter(
        prometheus.CounterOpts{
            Name: "longbow_iouring_buffer_pool_hits_total",
            Help: "Buffer pool hits",
        },
    )
    
    IoUringBufferPoolMisses = promauto.NewCounter(
        prometheus.CounterOpts{
            Name: "longbow_iouring_buffer_pool_misses_total",
            Help: "Buffer pool misses (allocations)",
        },
    )
    
    // Throughput
    IoUringBytesTotal = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "longbow_iouring_bytes_total",
            Help: "Total bytes transferred",
        },
        []string{"operation"}, // read, write
    )
)
```

### Phase 5: Testing Strategy (Week 3)

#### 5.1 Unit Tests
File: `internal/iouring/ring_test.go`

```go
// TestRingCreation - verify setup and mmap
func TestRingCreation(t *testing.T) {
    ring, err := NewRing(128, 0)
    require.NoError(t, err)
    defer ring.Close()
    
    assert.NotZero(t, ring.fd)
    assert.NotNil(t, ring.sqRing)
    assert.NotNil(t, ring.cqRing)
}

// TestSubmitAndComplete - basic round-trip
func TestSubmitAndComplete(t *testing.T) {
    // Create temp file
    // Submit write operation
    // Wait for completion
    // Verify data written
}

// TestConcurrentSubmissions - race detection
func TestConcurrentSubmissions(t *testing.T) {
    // Run with -race flag
    // Multiple goroutines submitting
    // Verify no data races
}

// TestBufferPoolAlignment - O_DIRECT requirements
func TestBufferPoolAlignment(t *testing.T) {
    pool := NewBufferPool(4096, 10)
    buf := pool.Get()
    
    // Verify 512-byte alignment
    assert.Equal(t, 0, int(uintptr(unsafe.Pointer(&buf[0])))%512)
}
```

#### 5.2 Benchmarks
File: `internal/iouring/bench_test.go`

```go
// BenchmarkSequentialWrite - single-threaded
func BenchmarkSequentialWrite(b *testing.B) {
    // Compare: io_uring vs standard os.File
    // Measure: throughput, latency, allocations
}

// BenchmarkConcurrentWrite - multi-threaded
func BenchmarkConcurrentWrite(b *testing.B) {
    // Multiple goroutines writing to same ring
    // Measure: scalability, contention
}

// BenchmarkArrowWrite - Arrow-specific
func BenchmarkArrowWrite(b *testing.B) {
    // Serialize Arrow RecordBatch
    // Write via io_uring
    // Measure: end-to-end latency
}

// BenchmarkVectoredWrite - writev performance
func BenchmarkVectoredWrite(b *testing.B) {
    // Multiple buffers in single operation
    // Compare with individual writes
}
```

#### 5.3 Integration Tests
File: `internal/iouring/integration_test.go`

```go
// TestWALBackend - integrate with WAL system
func TestWALBackend(t *testing.T) {
    // Create WAL with io_uring backend
    // Write multiple entries
    // Verify recovery
    // Compare performance with standard backend
}

// TestDirectIO - O_DIRECT path
func TestDirectIO(t *testing.T) {
    // Open file with O_DIRECT
    // Write aligned buffers
    // Verify no kernel page cache usage
}
```

### Phase 6: Longbow Integration (Week 3-4)

#### 6.1 WAL Backend Implementation
File: `internal/storage/wal_backend_iouring.go`

```go
type ArrowIOUringBackend struct {
    ring        *iouring.Ring
    file        *os.File
    offset      int64
    writer      *iouring.ArrowIOWriter
    completions chan iouring.CQE  // Async completions
    wg          sync.WaitGroup
}

func NewArrowIOUringBackend(path string) (WALBackend, error) {
    // Open file with O_APPEND|O_DIRECT|O_WRONLY
    // Create io_uring ring
    // Start completion poller goroutine
}

func (b *ArrowIOUringBackend) Write(name string, seq uint64, ts int64, rec arrow.RecordBatch) error {
    // Serialize RecordBatch
    // Submit async write
    // Optionally wait for completion
}

func (b *ArrowIOUringBackend) Sync() error {
    // Submit fsync operation
    // Wait for completion
}
```

#### 6.2 Configuration
File: `cmd/longbow/config.go`

```go
type Config struct {
    // ... existing fields ...
    
    // io_uring configuration
    StorageIOUringEnabled      bool   `envconfig:"STORAGE_IOURING_ENABLED" default:"false"`
    StorageIOUringQueueDepth   uint32 `envconfig:"STORAGE_IOURING_QUEUE_DEPTH" default:"2048"`
    StorageIOUringDirectIO     bool   `envconfig:"STORAGE_IOURING_DIRECT_IO" default:"true"`
    StorageIOUringBufferPoolSize int `envconfig:"STORAGE_IOURING_BUFFER_POOL_SIZE" default:"100"`
}
```

---

## Key Technical Decisions

### 1. Why Custom Implementation?

**Problem with existing libraries:**
- `iceber/iouring-go`: Uses deprecated `syscall` package (Go 1.24 incompatibility)
- `godzie44/go-uring`: Complex runtime, overkill for WAL use case
- `ii64/gouring`: Good but lacks Arrow-specific optimizations

**Benefits of custom:**
- Full control over memory allocation (critical for O_DIRECT)
- Arrow-specific optimizations (buffer alignment, IPC format)
- Minimal dependencies (only `golang.org/x/sys/unix`)
- Lock-free design tailored to Longbow's SPSC pattern

### 2. Zero-Copy Strategy

**Traditional flow:**
```
Arrow Record → Copy to buffer → Write to file
         (1 copy)      (kernel copy)
```

**Zero-copy flow:**
```
Arrow Record → IPC into aligned buffer → io_uring pwrite (DMA)
         (0 copies, direct from userspace to device)
```

**Requirements:**
- Aligned memory allocation (512-byte boundary)
- O_DIRECT file flag (bypass page cache)
- Buffer pool to reuse allocations

### 3. Lock-Free Design

**Why lock-free?**
- Traditional mutex: ~100ns per operation
- Atomic CAS: ~10ns per operation
- At 100K IOPS, difference is 9ms vs 1ms overhead

**Constraints:**
- Single producer (one WAL writer goroutine)
- Single consumer (one completion poller)
- Perfect fit for SPSC lock-free queues

### 4. Error Handling

**Strategy:**
- Synchronous errors: Returned immediately (queue full, invalid params)
- Async errors: Returned via completion CQE (I/O errors)
- Timeout: Context cancellation propagated to operations

---

## Performance Targets

### Latency
- **p50:** < 5µs (vs 180µs current standard WAL)
- **p99:** < 20µs
- **p999:** < 100µs

### Throughput
- **Single thread:** 50K IOPS
- **Multi-thread:** 200K IOPS (4 threads)
- **Bandwidth:** 5+ GB/s (with O_DIRECT)

### Resource Usage
- **Zero allocations** in hot path (buffer pool)
- **Memory:** < 50MB for ring + buffer pool
- **CPU:** < 10% overhead vs raw syscalls

---

## Migration Plan

### Step 1: Parallel Implementation (Week 1-2)
- Create `internal/iouring/` package
- Implement core ring operations
- Unit tests passing

### Step 2: Arrow Integration (Week 2-3)
- Buffer pool with O_DIRECT alignment
- Arrow IPC serialization optimization
- Benchmark vs standard backend

### Step 3: Feature Parity (Week 3-4)
- Implement all WALBackend methods
- Error handling and recovery
- Configuration integration

### Step 4: Gradual Rollout (Week 4+)
- Behind feature flag (`STORAGE_IOURING_ENABLED`)
- A/B testing in staging
- Production rollout with monitoring

---

## Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Kernel compatibility | Medium | High | Test on multiple kernel versions (5.6+)
| O_DIRECT alignment issues | Medium | Medium | Extensive testing with various buffer sizes
| Memory leaks in mmap | Low | High | Use finalizers + explicit Close, leak tests
| Performance regression | Low | High | Comprehensive benchmarks, easy rollback |
| Data corruption | Low | Critical | Checksums, fsync, extensive integration tests |

---

## Success Criteria

1. **Functionality:** All existing WAL tests pass with io_uring backend
2. **Performance:** 10x latency improvement (180µs → 18µs)
3. **Stability:** Zero data corruption in soak tests (24h+)
4. **Compatibility:** Works on Linux 5.6+ with Go 1.24+
5. **Observability:** All metrics exposed via Prometheus

---

## Estimated Timeline

- **Week 1:** Core io_uring primitives, syscall layer
- **Week 2:** Lock-free queues, buffer pool, Arrow integration
- **Week 3:** Metrics, comprehensive testing, benchmarks
- **Week 4:** Longbow integration, rollout preparation

**Total: 4 weeks** (1 developer full-time)

---

## Next Steps

1. Create feature branch: `feat/custom-iouring`
2. Implement syscall layer (Phase 1)
3. Set up CI with kernel 5.6+ test runner
4. Weekly check-ins on performance targets
