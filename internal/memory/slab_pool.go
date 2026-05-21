package memory

import (
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
)

// numaBindAlloc wraps off-heap allocation with optional NUMA node memory binding.
// On Linux with multiple NUMA nodes, this binds allocated memory to the current
// CPU socket to reduce remote memory access latency. On non-Linux or single-node
// systems, MbindMemory is a no-op.
func numaBindAlloc(capacity int) []byte {
	b := offHeapAlloc.Allocate(capacity)
	if len(b) > 0 {
		_ = MbindMemory(unsafe.Pointer(&b[0]), len(b), -1) // -1 = bind to current node
	}
	return b
}

// SlabPool recycles byte slices of a fixed size to avoid repeated allocations and OS zeroing costs.
// It is specifically designed for 1MB slabs used by standard SlabArena configurations.
type SlabPool struct {
	pool        sync.Pool
	size        int
	activeCount int64 // Number of slabs currently in use (not in pool)
	pooledCount int64 // Number of slabs currently in the pool
	maxPooled   int64 // Maximum slabs to keep in pool before releasing to OS
	peakCount   int64 // Highest activeCount ever observed – used for leak-probability estimation
	hits        int64 // Running pool hits
	misses      int64 // Running pool misses
}

var (
	// Standard bucket sizes to cover common high-dim and standard workloads
	size4MB  = 4 * 1024 * 1024
	size8MB  = 8 * 1024 * 1024
	size16MB = 16 * 1024 * 1024
	size32MB = 32 * 1024 * 1024

	global4MBPool  = newSlabPool(size4MB)
	global8MBPool  = newSlabPool(size8MB)
	global16MBPool = newSlabPool(size16MB)
	global32MBPool = newSlabPool(size32MB)

	offHeapAlloc = NewOffHeapAllocator()
)

func newSlabPool(size int) *SlabPool {
	sizeLabel := strconv.Itoa(size)
	return &SlabPool{
		size:      size,
		maxPooled: 100, // Keep at most 100 slabs in pool before releasing
		pool: sync.Pool{
			New: func() any {
				b := numaBindAlloc(size)
				if err := AdviseHugePage(b); err == nil {
					metrics.SlabHugePageCount.Inc()
				}
				metrics.SlabPoolGrowthTotal.WithLabelValues(sizeLabel).Inc()
				return &b
			},
		},
	}
}

// roundUpSlabCapacity rounds non-standard capacities to the next standard pool size.
// This ensures slabs are recycled through the standard size-class pools instead of
// being individually allocated/freed, which reduces OS-level allocation churn.
func roundUpSlabCapacity(capacity int) int {
	switch {
	case capacity <= size4MB:
		return size4MB
	case capacity <= size8MB:
		return size8MB
	case capacity <= size16MB:
		return size16MB
	case capacity <= size32MB:
		return size32MB
	default:
		return capacity
	}
}

// GetSlab retrieves a slab of capacity 'cap'.
// If cap matches standard sizes, it returns a pooled slice.
// Non-standard capacities are rounded up to the next pool size for recycling.
// The returned slice has len=cap (fully usable buffer).
// NOTE: buffer content is DIRTY (not zeroed) if reused.
func GetSlab(capacity int) []byte {
	rounded := roundUpSlabCapacity(capacity)
	switch rounded {
	case size4MB:
		b := global4MBPool.Get()
		return b[:capacity]
	case size8MB:
		b := global8MBPool.Get()
		return b[:capacity]
	case size16MB:
		b := global16MBPool.Get()
		return b[:capacity]
	case size32MB:
		b := global32MBPool.Get()
		return b[:capacity]
	}
	// Fallback to off-heap alloc for oversized capacities
	sizeStr := strconv.Itoa(capacity)
	metrics.SlabPoolAllocationsTotal.WithLabelValues(sizeStr, "miss").Inc()
	metrics.SlabPoolGrowthTotal.WithLabelValues(sizeStr).Inc()
	if capacity >= 1024*1024 {
		return numaBindAlloc(capacity)
	}
	return make([]byte, capacity)
}

// PutSlab returns a slab to the pool for reuse.
func PutSlab(b []byte) {
	b = b[:cap(b)]
	c := cap(b)
	switch c {
	case size4MB:
		global4MBPool.Put(b)
	case size8MB:
		global8MBPool.Put(b)
	case size16MB:
		global16MBPool.Put(b)
	case size32MB:
		global32MBPool.Put(b)
	default:
		// If it's a large non-standard slab (>= 1MB), it was likely allocated
		// via offHeapAlloc.Allocate in GetSlab. We must free it to avoid leaks.
		if c >= 1024*1024 {
			sizeStr := strconv.Itoa(c)
			offHeapAlloc.Free(b)
			metrics.SlabPoolShrinkTotal.WithLabelValues(sizeStr, "non_standard_free").Inc()
		} else {
			metrics.SlabPoolBoundaryViolationsTotal.WithLabelValues(strconv.Itoa(c), "put_slab").Inc()
		}
	}
}

// Get retrieves a slab from the pool or allocates a new one.
func (p *SlabPool) Get() []byte {
	active := atomic.AddInt64(&p.activeCount, 1)
	sizeStr := strconv.Itoa(p.size)

	// Check if we have pooled items
	pooled := atomic.LoadInt64(&p.pooledCount)
	if pooled > 0 {
		atomic.AddInt64(&p.pooledCount, -1)
		atomic.AddInt64(&p.hits, 1)
		metrics.SlabPoolAllocationsTotal.WithLabelValues(sizeStr, "hit").Inc()
	} else {
		atomic.AddInt64(&p.misses, 1)
		metrics.SlabPoolAllocationsTotal.WithLabelValues(sizeStr, "miss").Inc()

		// Dynamic scale up: if we miss and our active usage exceeds current maxPooled,
		// expand maxPooled capacity to reduce future allocation/zeroing costs.
		for {
			currMax := atomic.LoadInt64(&p.maxPooled)
			if active <= currMax {
				break
			}
			newMax := currMax + 50 // Expand in steps of 50 slabs
			if newMax > 2000 {     // Cap at a sensible limit (e.g., 2000 slabs)
				newMax = 2000
			}
			if newMax == currMax {
				break
			}
			if atomic.CompareAndSwapInt64(&p.maxPooled, currMax, newMax) {
				metrics.SlabPoolResizesTotal.WithLabelValues(sizeStr, "scale_up").Inc()
				break
			}
		}
	}

	// Track peak active count for leak-probability estimation
	for {
		peak := atomic.LoadInt64(&p.peakCount)
		if active <= peak {
			break
		}
		if atomic.CompareAndSwapInt64(&p.peakCount, peak, active) {
			break
		}
	}

	// Update metrics
	p.updateMetrics()

	slab := p.pool.Get()
	b := *slab.(*[]byte)
	if cap(b) != p.size {
		metrics.SlabPoolBoundaryViolationsTotal.WithLabelValues(sizeStr, "get").Inc()
		newB := offHeapAlloc.Allocate(p.size)
		if err := AdviseHugePage(newB); err == nil {
			metrics.SlabHugePageCount.Inc()
		}
		b = newB
	}

	return b
}

// Put returns a slab to the pool for reuse.
func (p *SlabPool) Put(b []byte) {
	b = b[:cap(b)]
	if cap(b) != p.size {
		metrics.SlabPoolBoundaryViolationsTotal.WithLabelValues(strconv.Itoa(p.size), "put").Inc()
		return
	}

	atomic.AddInt64(&p.activeCount, -1)

	// Observe refcount distribution. For standard single-owner use this will
	// always be 1. Values >1 indicate unexpected sharing.
	sizeStr := strconv.Itoa(p.size)
	metrics.SlabRefCountDistribution.WithLabelValues(sizeStr).Observe(1)

	// Check if we should release this slab instead of pooling it
	pooled := atomic.LoadInt64(&p.pooledCount)
	if pooled >= atomic.LoadInt64(&p.maxPooled) {
		// Release memory back to OS instead of pooling
		// We MUST call Free to unmap and decrement the allocator's counter
		offHeapAlloc.Free(b)
		metrics.SlabPoolShrinkTotal.WithLabelValues(sizeStr, "max_pooled").Inc()
		// Update metrics after releasing
		p.updateMetrics()
		return
	}

	atomic.AddInt64(&p.pooledCount, 1)
	p.pool.Put(&b)

	// Update metrics
	p.updateMetrics()
}

// ReleaseUnused forces the pool to release excess slabs back to the OS.
// This is useful for explicit memory management after heavy workloads.
func (p *SlabPool) ReleaseUnused() int {
	released := 0
	pooled := atomic.LoadInt64(&p.pooledCount)
	sizeStr := strconv.Itoa(p.size)

	// Scale down capacity: set maxPooled back to baseline (100) or active count, whichever is larger,
	// to allow garbage collection and release excess pooled capacity.
	for {
		currMax := atomic.LoadInt64(&p.maxPooled)
		baseline := int64(100)
		if active := atomic.LoadInt64(&p.activeCount); active > baseline {
			baseline = active
		}
		if currMax <= baseline {
			break
		}
		if atomic.CompareAndSwapInt64(&p.maxPooled, currMax, baseline) {
			metrics.SlabPoolResizesTotal.WithLabelValues(sizeStr, "scale_down").Inc()
			break
		}
	}

	// Recalculate threshold based on updated maxPooled
	currMax := atomic.LoadInt64(&p.maxPooled)
	threshold := currMax / 2 // Keep 50% of max as buffer

	for i := int64(0); i < pooled-threshold; i++ {
		if slab := p.pool.Get(); slab != nil {
			b := *slab.(*[]byte)
			if err := ReleaseSlab(b); err == nil {
				released++
				atomic.AddInt64(&p.pooledCount, -1)
				metrics.SlabPoolShrinkTotal.WithLabelValues(sizeStr, "manual_release").Inc()
			} else {
				// Put it back if release failed
				p.pool.Put(slab)
				break
			}
		} else {
			break
		}
	}

	return released
}

// ActiveCount returns the number of slabs currently in use
func (p *SlabPool) ActiveCount() int64 {
	return atomic.LoadInt64(&p.activeCount)
}

// PooledCount returns the number of slabs currently in the pool
func (p *SlabPool) PooledCount() int64 {
	return atomic.LoadInt64(&p.pooledCount)
}

// updateMetrics updates Prometheus metrics for this slab pool
func (p *SlabPool) updateMetrics() {
	active := atomic.LoadInt64(&p.activeCount)
	pooled := atomic.LoadInt64(&p.pooledCount)
	peak := atomic.LoadInt64(&p.peakCount)
	hits := atomic.LoadInt64(&p.hits)
	misses := atomic.LoadInt64(&p.misses)

	// Calculate size label
	sizeLabel := strconv.Itoa(p.size)

	// Update arena memory bytes (active slabs * size)
	arenaBytes := float64(active * int64(p.size))
	metrics.ArenaMemoryBytes.WithLabelValues(sizeLabel).Set(arenaBytes)
	metrics.SlabActiveArenas.WithLabelValues(sizeLabel).Set(float64(active))

	// Calculate fragmentation ratio (pooled/active)
	// Higher ratio means more fragmentation (more slabs sitting idle in pool)
	fragmentation := float64(0)
	if active > 0 {
		fragmentation = float64(pooled) / float64(active)
	}
	metrics.SlabFragmentationRatio.WithLabelValues(sizeLabel).Set(fragmentation)

	// Leak probability: ratio of current active to historical peak.
	// A sustained value near 1.0 after a workload completes suggests slabs
	// are not being returned and may be leaking.
	leakProb := float64(0)
	if peak > 0 {
		leakProb = float64(active) / float64(peak)
	}
	metrics.SlabLeakProbability.WithLabelValues(sizeLabel).Set(leakProb)

	// Calculate and set running slab pool hit-to-total allocation ratio
	totalAlloc := hits + misses
	hitRatio := float64(0)
	if totalAlloc > 0 {
		hitRatio = float64(hits) / float64(totalAlloc)
	}
	metrics.SlabPoolBufferHitRatio.WithLabelValues(sizeLabel).Set(hitRatio)

	// Update the dynamically configured maxPooled capacity
	metrics.SlabPoolMaxPooled.WithLabelValues(sizeLabel).Set(float64(atomic.LoadInt64(&p.maxPooled)))
}

// GetGlobalSlabPoolUnusedMemory returns the total memory sitting idle in all global slab pools.
func GetGlobalSlabPoolUnusedMemory() int64 {
	var total int64
	total += atomic.LoadInt64(&global4MBPool.pooledCount) * int64(global4MBPool.size)
	total += atomic.LoadInt64(&global8MBPool.pooledCount) * int64(global8MBPool.size)
	total += atomic.LoadInt64(&global16MBPool.pooledCount) * int64(global16MBPool.size)
	total += atomic.LoadInt64(&global32MBPool.pooledCount) * int64(global32MBPool.size)
	return total
}

// ReleaseGlobalSlabPoolsUnused forces all global slab pools to release excess slabs back to the OS.
func ReleaseGlobalSlabPoolsUnused() int {
	released := 0
	released += global4MBPool.ReleaseUnused()
	released += global8MBPool.ReleaseUnused()
	released += global16MBPool.ReleaseUnused()
	released += global32MBPool.ReleaseUnused()
	return released
}
