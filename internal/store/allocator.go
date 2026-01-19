package store

import (
	"sync"
)

// ChunkAllocator manages allocation of large byte slices (chunks).
// It uses a sync.Pool to recycle chunks and reduce GC pressure.
type ChunkAllocator struct {
	pool sync.Pool
	size int
}

// NewChunkAllocator creates a new allocator for chunks of dimensions: numItems * itemSizeBytes.
// NewChunkAllocator creates a new allocator for chunks of dimensions: numItems * itemSizeBytes.
func NewChunkAllocator(numItems, itemSizeBytes int) *ChunkAllocator {
	totalSize := numItems * itemSizeBytes
	return &ChunkAllocator{
		size: totalSize,
		pool: sync.Pool{
			New: func() interface{} {
				// Allocate a new slice
				return make([]byte, totalSize)
			},
		},
	}
}

// Alloc returns a slice of bytes of the configured size.
// The slice contents are not guaranteed to be zeroed (dirty).
func (a *ChunkAllocator) Alloc() []byte {
	return a.pool.Get().([]byte)
}

// Free returns a slice to the pool.
// The caller must assume the slice is invalid after calling Free.
func (a *ChunkAllocator) Free(b []byte) {
	if cap(b) < a.size {
		return // Ignore too small/invalid chunks
	}
	// Reslice to full capacity to ensure we don't leak capacity drift?
	// Just put it back.
	a.pool.Put(b[:a.size]) //nolint:staticcheck // SA6002: slice is pointer-like enough for us
}
