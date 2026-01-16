package memory

import (
	"sync"
)

// SlabPool recycles byte slices of a fixed size to avoid repeated allocations and OS zeroing costs.
// It is specifically designed for 1MB slabs used by standard SlabArena configurations.
type SlabPool struct {
	pool sync.Pool
	size int
}

var (
	// global4MBPool is a shared pool for standard 4MB slabs.
	// 4MB fits ~2 chunks of 1024 vectors (384 dims).
	global4MBPool = &SlabPool{
		size: 4 * 1024 * 1024,
		pool: sync.Pool{
			New: func() any {
				b := make([]byte, 4*1024*1024)
				return &b
			},
		},
	}
)

// Get retrieves a slab of capacity 'cap'.
// If cap matches standard sizes, it returns a pooled slice.
// The returned slice has len=cap (fully usable buffer).
// NOTE: buffer content is DIRTY (not zeroed) if reused.
func GetSlab(capacity int) []byte {
	if capacity == 4*1024*1024 {
		return global4MBPool.Get()
	}
	// Fallback to make (or add more pools logic here)
	return make([]byte, capacity)
}

// PutSlab returns a slab to the pool for reuse.
func PutSlab(b []byte) {
	if cap(b) == 4*1024*1024 {
		global4MBPool.Put(b)
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
