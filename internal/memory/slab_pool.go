package memory

import (
	"strconv"
	"sync"

	"github.com/23skdu/longbow/internal/metrics"
)

// SlabPool recycles byte slices of a fixed size to avoid repeated allocations and OS zeroing costs.
// It is specifically designed for 1MB slabs used by standard SlabArena configurations.
type SlabPool struct {
	pool sync.Pool
	size int
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
		size: size,
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
	return *p.pool.Get().(*[]byte)
}

func (p *SlabPool) Put(b []byte) {
	if cap(b) != p.size {
		return
	}
	p.pool.Put(&b)
}
