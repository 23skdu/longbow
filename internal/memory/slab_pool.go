package memory

import (
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
)

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
				b := offHeapAlloc.Allocate(size)
				if err := AdviseHugePage(b); err == nil {
					metrics.SlabHugePageCount.Inc()
				}
				metrics.SlabPoolGrowthTotal.WithLabelValues(sizeLabel).Inc()
				return &b
			},
		},
	}
}

// GetSlab retrieves a slab of capacity 'cap'.
// If cap matches standard sizes, it returns a pooled slice.
// The returned slice has len=cap (fully usable buffer).
// NOTE: buffer content is DIRTY (not zeroed) if reused.
func GetSlab(capacity int) []byte {
	switch capacity {
	case size4MB:
		return global4MBPool.Get()
	case size8MB:
		return global8MBPool.Get()
	case size16MB:
		return global16MBPool.Get()
	case size32MB:
		return global32MBPool.Get()
	}
	// Fallback to off-heap alloc for non-standard large sizes
	sizeStr := strconv.Itoa(capacity)
	metrics.SlabPoolAllocationsTotal.WithLabelValues(sizeStr, "miss").Inc()
	metrics.SlabPoolGrowthTotal.WithLabelValues(sizeStr).Inc()
	if capacity >= 1024*1024 {
		return offHeapAlloc.Allocate(capacity)
	}
	return make([]byte, capacity)
}

// PutSlab returns a slab to the pool for reuse.
func PutSlab(b []byte) {
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

	return *p.pool.Get().(*[]byte)
}

// Put returns a slab to the pool for reuse.
func (p *SlabPool) Put(b []byte) {
	if cap(b) != p.size {
		return
	}

	atomic.AddInt64(&p.activeCount, -1)

	// Observe refcount distribution. For standard single-owner use this will
	// always be 1. Values >1 indicate unexpected sharing.
	sizeStr := strconv.Itoa(p.size)
	metrics.SlabRefCountDistribution.WithLabelValues(sizeStr).Observe(1)

	// Check if we should release this slab instead of pooling it
	pooled := atomic.LoadInt64(&p.pooledCount)
	if pooled >= p.maxPooled {
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

	// Release slabs beyond a reasonable threshold
	threshold := p.maxPooled / 2 // Keep 50% of max as buffer
	sizeStr := strconv.Itoa(p.size)

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
