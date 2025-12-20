package memory

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// SlabSize is the size of each memory block (1MB)
const SlabSize = 1024 * 1024

// SlabAllocator manages memory in large chunks (slabs) to reduce GC pressure.
// It is optimized for append-only or long-lived structures like HNSW graphs.
type SlabAllocator struct {
	slabs          [][]byte
	curSlab        []byte
	curOffset      int
	mu             sync.Mutex
	totalAllocated atomic.Int64
}

// NewSlabAllocator creates a new allocator.
func NewSlabAllocator() *SlabAllocator {
	return &SlabAllocator{
		slabs: make([][]byte, 0, 16),
	}
}

// Alloc allocates a slice of size n bytes.
// The returned slice is valid until Reset() is called.
// NOT thread-safe for allocation; usually protected by structure's lock (e.g. HNSW lock).
func (a *SlabAllocator) Alloc(n int) []byte {
	a.totalAllocated.Add(int64(n))

	// If current slab has space, use it
	if len(a.curSlab)-a.curOffset >= n {
		ptr := a.curSlab[a.curOffset : a.curOffset+n]
		a.curOffset += n
		return ptr
	}

	// Allocate new slab (max of SlabSize or n if n > SlabSize)
	size := SlabSize
	if n > SlabSize {
		size = n
	}

	newSlab := make([]byte, size)
	a.slabs = append(a.slabs, newSlab)
	a.curSlab = newSlab
	a.curOffset = n
	return newSlab[:n]
}

// AllocStruct allocates space for a struct of type T and returns a pointer to it.
// T must be a plain old data type (no pointers that need GC scanning ideally,
// though Go GC will scan it if typed correctly, but this is for optimization).
func AllocStruct[T any](a *SlabAllocator) *T {
	var zero T
	size := int(unsafe.Sizeof(zero))
	bytes := a.Alloc(size)
	return (*T)(unsafe.Pointer(&bytes[0]))
}

// Reset frees all allocated memory by dropping references to slabs.
// This allows GC to reclaim huge chunks at once.
func (a *SlabAllocator) Reset() {
	a.mu.Lock()
	defer a.mu.Unlock() // Lock just in case, though usually single-thread controlled
	a.slabs = nil
	a.curSlab = nil
	a.curOffset = 0
	a.totalAllocated.Store(0)
}

// TotalAllocated returns bytes allocated by this arena.
func (a *SlabAllocator) TotalAllocated() int64 {
	return a.totalAllocated.Load()
}
