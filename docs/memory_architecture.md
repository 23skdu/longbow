# Memory Architecture

This document describes Longbow's memory management architecture, including arena allocation, slab pooling, and compaction strategies.

## Overview

Longbow uses a custom arena-based memory management system to reduce GC pressure and enable efficient memory reuse. The system is designed for high-throughput vector similarity workloads where:
- Large amounts of memory are allocated (e.g., 1MB+ slabs for vector data)
- Data has varying lifetimes (short-lived queries vs long-lived vector data)
- Fragmentation can accumulate due to deletions and updates

## Core Components

### SlabArena

`SlabArena` manages large contiguous blocks of memory called "slabs". Each slab is a fixed-size allocation (default 1MB, configurable).

**Key Properties:**
- **Fixed slab size**: Slabs are allocated in fixed chunks (1MB default, configurable via `SlabArena.slabCap`)
- **Append-only allocation**: Memory is allocated from slabs sequentially. Once allocated, the same memory cannot be reused for different allocations within the same slab.
- **No double-freeing**: Slabs are tracked and only released when fully unused

**API:**
```go
type SlabArena struct {
    // Allocate allocates bytes from the current slab
    Alloc(bytes int) (uint64, error)
    
    // AllocDirty allocates bytes without zeroing the memory
    AllocDirty(bytes int) (uint64, error)
    
    // Get retrieves a byte slice by offset
    Get(offset uint64, length uint64) []byte
    
    // Release returns memory to the OS via madvise(MADV_DONTNEED) or VirtualFree
    ReleaseSlab(b []byte) error
}
```

**Use Cases:**
- Vector storage: Raw embedding vectors stored in contiguous memory
- Graph data: HNSW neighbor lists and adjacency structures
- Batch records: Arrow RecordBatches for vector data

### TypedArena

`TypedArena` provides type-safe access to arena-allocated memory.

```go
type TypedArena[T any] struct {
    arena *SlabArena
    mu    sync.RWMutex
}

func (ta *TypedArena[T]) AllocSlice(count int) (SliceRef, error)
func (ta *TypedArena[T]) AllocSliceDirty(count int) (SliceRef, error)
func (ta *TypedArena[T]) Get(ref SliceRef) []T
func (ta *TypedArena[T]) Compact(liveRefs []SliceRef) (*CompactionStats, error)
func (ta *TypedArena[T]) TotalAllocated() int64
```

**Thread Safety:**
- Reads use `sync.RWMutex.RLock()` for concurrent access
- Writes (Alloc, Compact) use `sync.RWMutex.Lock()` for exclusive access

### SlabPool

`SlabPool` recycles byte slices to avoid repeated allocations and OS zeroing costs.

**Key Features:**
- **Per-size pools**: Separate pools for 4MB, 8MB, 16MB, 32MB slabs
- **Metrics tracking**: Tracks `SlabPoolAllocationsTotal` by size and hit/miss status
- **Max pooled limit**: Keeps at most 100 slabs per pool before releasing excess to OS
- **ReleaseUnused()**: Forces release of excess pooled slabs back to OS

```go
func GetSlab(capacity int) []byte
func PutSlab(b []byte)
func (p *SlabPool) ReleaseUnused() int
func (p *SlabPool) ActiveCount() int64
func (p *SlabPool) PooledCount() int64
```

**Memory Pressure Handling:**
- When pool reaches `maxPooled` limit, excess slabs are released via `madvise(MADV_DONTNEED)` (Unix/Linux) or `VirtualFree(MEM_DECOMMIT)` (Windows)
- This prevents unbounded growth while maintaining a warm pool of reusable slabs

### Global Arena Registry

`arena_registry.go` maintains a global list of all `SlabArena` instances for GC tuner awareness.

```go
var globalArenas sync.Map[*SlabArena]bool

func RegisterArena(a *SlabArena)
func GetGlobalArenas() []*SlabArena
```

**Purpose:**
- Enables the GC tuner to calculate "effective" memory usage by accounting for unused arena capacity
- Allows the tuner to distinguish between:
  - **Used memory**: Active vectors, indices, graph data
  - **Reserved but unused**: Fragmented space in arenas that can be compacted

## Compaction Strategies

### Arena Compaction

**When to Trigger:**
- Fragmentation > 30% (configurable via `CompactionConfig.FragmentationThreshold`)
- After bulk deletes or significant churn

**How it Works:**
1. Identify live data references (e.g., active vectors, graph nodes)
2. Allocate new arena with tighter memory layout
3. Copy live data to new arena
4. Release old arena memory back to OS

**Expected Savings:**
- 20-30% memory reduction in fragmented workloads

**Implementation:**
```go
func CompactArena[T](ta *TypedArena[T], liveRefs []SliceRef) (*CompactionStats, error)
```

### HNSW Graph Compaction

**When to Trigger:**
- >30% of graph nodes are deleted (via `ArrowHNSW.NeedsCompaction()`)
- Graph size >2x expected size for the dataset

**How it Works:**
1. Identify live nodes (not in deleted bitset)
2. Create new compacted graph with renumbered node IDs
3. Copy vector data and remap neighbor connections
4. Release old graph memory

**Expected Savings:**
- 15-25% memory reduction after significant deletions

**Implementation:**
```go
func (h *ArrowHNSW) CompactGraph(ctx context.Context) (*HNSWCompactionStats, error)
func (h *ArrowHNSW) NeedsCompaction() bool
```

### Vacuum (Connection Pruning)

**Purpose:**
- Clean up neighbor connections that point to deleted nodes
- Performed periodically by compaction worker
- Does not reclaim memory but improves search efficiency

## Memory Release

### Platform-Specific Release

**Linux/Darwin:**
```go
func ReleaseSlab(b []byte) error {
    ptr := unsafe.Pointer(&b[0])
    length := uintptr(cap(b))
    syscall.Syscall(
        syscall.SYS_MADVISE,
        uintptr(ptr),
        length,
        syscall.MADV_DONTNEED,
    )
}
```

**Windows:**
```go
func ReleaseSlab(b []byte) error {
    ptr := unsafe.Pointer(&b[0])
    length := uintptr(cap(b))
    ret, _, _ := procVirtualFree.Call(
        uintptr(ptr),
        length,
        MEM_DECOMMIT,
    )
    if ret == 0 {
        return fmt.Errorf("VirtualFree(MEM_DECOMMIT) failed")
    }
    return nil
}
```

## GC Tuning

### Aggressive GC Tuner

The GC tuner dynamically adjusts `GOGC` based on heap usage patterns.

**Key Features:**
- **Arena-aware**: Subtracts unused arena capacity from heap usage before calculating pressure
- **Aggressive mode**: Sets lower GOGC thresholds when `IsAggressive=true`
- **Tunable frequency**: Default 100ms interval (configurable)
- **Smart thresholds**: 
  - <50% heap usage → GOGC=80 (relaxed)
  - >90% heap usage → GOGC=10 (aggressive)
  - Linear interpolation for 50-90% range

```go
tuner := NewGCTuner(limitBytes, highGOGC, lowGOGC, logger)
tuner.IsAggressive = true
tuner.Start(ctx, 100*time.Millisecond)
```

**Effect on Memory:**
- Frequent GC when approaching memory limit
- Reduced GC pressure when headroom is available
- Accounts for arena fragmentation (doesn't over-GC on reserved-but-unused memory)

## Memory Subsystem Metrics

### Arena Metrics

- `SlabPoolAllocationsTotal`: Counter of slab allocations by size and status (hit/miss)
- `GCTunerHeapUtilization`: Gauge of effective heap usage ratio
- `GCTunerTargetGOGC`: Gauge of current GOGC setting

### Compaction Metrics

- `CompactionEventsTotal`: Counter of compaction operations
- `CompactionDurationSeconds`: Histogram of compaction runtimes

## Best Practices

### When to Use TypedArena vs Raw Allocation

| Scenario | Recommended Approach | Reason |
|----------|-------------------|---------|
| Vector storage | TypedArena | Type-safe, handles variable-length vectors |
| Graph data | SlabArena (direct) | Control over memory layout, no overhead |
| Temporary buffers | sync.Pool or heap allocation | Short lifetime, GC is appropriate |
| Large fixed structures | TypedArena with Compact() | Periodically compacted, amortizes cost |

### Memory Leak Prevention

1. **Always use proper lifecycle management**:
   - All arenas must be closed via `Release()` when no longer needed
   - Background workers must check `stopChan` and use `workerWg`

2. **Monitor goroutine counts**:
   - Use `runtime.NumGoroutine()` before/after critical operations
   - Use `go.uber.org/goleak` in tests to detect leaks

3. **Avoid permanent references**:
   - Do not store arena references in global variables
   - Use dependency injection to manage arena lifetimes

4. **Implement graceful shutdown**:
   - All long-running operations accept `context.Context`
   - Check `ctx.Done()` in loops and during blocking operations
   - Set reasonable timeouts for operations

## Configuration

### Memory Tuning

**Recommended Settings:**
```yaml
max_memory: 51200000000  # 48GB (adjust based on available RAM)
GOGC: 80                 # Default, let aggressive tuner adjust
GOGC_aggressive: 10       # Tuner will use this when memory pressure is high
compaction_interval: 30s    # Check for compaction every 30 seconds
compaction_enabled: true     # Enable automatic background compaction
```

### Performance Tuning

**For high-throughput ingestion:**
```yaml
batch_size: 4096           # Optimize for Arrow record batch size
prefetch_enabled: true       # Enable vector prefetching
compaction_threshold: 0.3   # Compact when >30% fragmentation
```

## Monitoring

### Key Metrics to Watch

1. **Memory Growth**: `process_resident_memory_bytes` - should stabilize after warmup
2. **GC Pauses**: `go_gc_pause_duration_seconds` - high values may indicate pressure
3. **Arena Usage**: Monitor slab counts and fragmentation ratios
4. **Goroutine Count**: `go_goroutines` - leaks will show as upward trend
5. **Compaction Events**: Track frequency and effectiveness

### Alerts

**High Memory Usage (>80% of limit):**
- Alert: "High memory usage detected"
- Action: Check for goroutine leaks, consider increasing `max_memory`

**Memory Growth (>1GB/hour):**
- Alert: "Unbounded memory growth detected"
- Action: Investigate for arena fragmentation, check compaction is running

## References

- [`internal/memory/slab_pool.go`](../internal/memory/slab_pool.go) - Slab allocation and pooling
- [`internal/memory/arena_compaction.go`](../internal/memory/arena_compaction.go) - Arena compaction
- [`internal/store/hnsw_compaction.go`](../internal/store/hnsw_compaction.go) - HNSW graph compaction
- [`internal/memory/gc_tuner.go`](../internal/memory/gc_tuner.go) - GC tuning logic
