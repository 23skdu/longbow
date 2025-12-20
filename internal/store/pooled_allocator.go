// Package store provides a pooled memory allocator for Arrow operations.
package store

import (
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// PooledAllocator implements memory.Allocator with buffer pooling
// to reduce GC pressure during high-throughput ingestion.
// It uses size-bucketed sync.Pools for efficient buffer reuse.
type PooledAllocator struct {
	pools      [20]*sync.Pool // Powers of 2 from 64B to 32MB
	allocated  atomic.Int64
	reused     atomic.Int64
	underlying memory.Allocator
}

// Bucket sizes: 64, 128, 256, 512, 1K, 2K, 4K, 8K, 16K, 32K,
// 64K, 128K, 256K, 512K, 1M, 2M, 4M, 8M, 16M, 32M
const (
	minBucketShift = 6  // 64 bytes minimum
	maxBucketShift = 25 // 32MB maximum
	numBuckets     = maxBucketShift - minBucketShift + 1
)

// NewPooledAllocator creates a new pooled allocator.
// The underlying allocator is used for allocations larger than 32MB.
func NewPooledAllocator() *PooledAllocator {
	p := &PooledAllocator{
		underlying: memory.NewGoAllocator(),
	}

	// Initialize pools for each bucket size
	for i := 0; i < numBuckets; i++ {
		size := 1 << (i + minBucketShift)
		p.pools[i] = &sync.Pool{
			New: func(s int) func() interface{} {
				return func() interface{} {
					b := make([]byte, s)
					return &b
				}
			}(size),
		}
	}

	return p
}

// bucketIndex returns the pool index for a given size.
// Returns -1 if size exceeds maximum bucket size.
func bucketIndex(size int) int {
	if size <= 0 {
		return 0
	}
	if size > (1 << maxBucketShift) {
		return -1
	}

	// Find the smallest power of 2 >= size
	shift := minBucketShift
	for (1 << shift) < size {
		shift++
	}

	return shift - minBucketShift
}

// Allocate returns a buffer of at least the requested size.
// Buffers may be larger than requested due to bucketing.
func (p *PooledAllocator) Allocate(size int) []byte {
	idx := bucketIndex(size)
	if idx < 0 {
		// Too large for pooling, use underlying allocator
		p.allocated.Add(int64(size))
		return p.underlying.Allocate(size)
	}

	bp := p.pools[idx].Get().(*[]byte)
	p.reused.Add(1)

	// Return slice of exact requested size (capacity may be larger)
	return (*bp)[:size]
}

// Reallocate resizes a buffer, potentially reusing pooled memory.
func (p *PooledAllocator) Reallocate(size int, b []byte) []byte {
	if size <= cap(b) {
		return b[:size]
	}

	newBuf := p.Allocate(size)
	copy(newBuf, b)
	p.Free(b)

	return newBuf
}

// Free returns a buffer to the appropriate pool.
func (p *PooledAllocator) Free(b []byte) {
	if b == nil {
		return
	}

	idx := bucketIndex(cap(b))
	if idx < 0 {
		// Was allocated with underlying, let GC handle it
		return
	}

	// Reset slice to full capacity before returning to pool
	b = b[:cap(b)]
	p.pools[idx].Put(&b)
}

// Stats returns allocator statistics.
type PooledAllocatorStats struct {
	AllocatedBytes int64
	ReusedBuffers  int64
}

// Stats returns current allocator statistics.
func (p *PooledAllocator) Stats() PooledAllocatorStats {
	return PooledAllocatorStats{
		AllocatedBytes: p.allocated.Load(),
		ReusedBuffers:  p.reused.Load(),
	}
}

// Verify interface compliance
var _ memory.Allocator = (*PooledAllocator)(nil)
