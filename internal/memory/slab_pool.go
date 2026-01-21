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
)

func newSlabPool(size int) *SlabPool {
	return &SlabPool{
		size:      size,
		maxPooled: 100, // Keep at most 100 slabs in pool before releasing
		pool: sync.Pool{
			New: func() any {
				b := make([]byte, size)
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
	sizeStr := strconv.Itoa(capacity)
	switch capacity {
	case size4MB:
		metrics.SlabPoolAllocationsTotal.WithLabelValues(sizeStr, "hit").Inc()
		return global4MBPool.Get()
	case size8MB:
		metrics.SlabPoolAllocationsTotal.WithLabelValues(sizeStr, "hit").Inc()
		return global8MBPool.Get()
	case size16MB:
		metrics.SlabPoolAllocationsTotal.WithLabelValues(sizeStr, "hit").Inc()
		return global16MBPool.Get()
	case size32MB:
		metrics.SlabPoolAllocationsTotal.WithLabelValues(sizeStr, "hit").Inc()
		return global32MBPool.Get()
	}
	// Fallback to make (or add more pools logic here)
	metrics.SlabPoolAllocationsTotal.WithLabelValues(sizeStr, "miss").Inc()
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
	}
	// Else drop it
}

func (p *SlabPool) Get() []byte {
	atomic.AddInt64(&p.activeCount, 1)
	// Only decrement pooledCount if there was actually something in the pool
	// sync.Pool.Get() may call New() which creates a new slab
	if atomic.LoadInt64(&p.pooledCount) > 0 {
		atomic.AddInt64(&p.pooledCount, -1)
	}
	return *p.pool.Get().(*[]byte)
}

func (p *SlabPool) Put(b []byte) {
	if cap(b) != p.size {
		return
	}

	atomic.AddInt64(&p.activeCount, -1)

	// Check if we should release this slab instead of pooling it
	pooled := atomic.LoadInt64(&p.pooledCount)
	if pooled >= p.maxPooled {
		// Release memory back to OS instead of pooling
		_ = ReleaseSlab(b) // Ignore error, worst case we just don't release
		return
	}

	atomic.AddInt64(&p.pooledCount, 1)
	p.pool.Put(&b)
}

// ReleaseUnused forces the pool to release excess slabs back to the OS.
// This is useful for explicit memory management after heavy workloads.
func (p *SlabPool) ReleaseUnused() int {
	released := 0
	pooled := atomic.LoadInt64(&p.pooledCount)

	// Release slabs beyond a reasonable threshold
	threshold := p.maxPooled / 2 // Keep 50% of max as buffer

	for i := int64(0); i < pooled-threshold; i++ {
		if slab := p.pool.Get(); slab != nil {
			b := *slab.(*[]byte)
			if err := ReleaseSlab(b); err == nil {
				released++
				atomic.AddInt64(&p.pooledCount, -1)
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
