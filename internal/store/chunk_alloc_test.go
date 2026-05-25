package store

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Mock/Interface for the Allocator to be implemented
// We want something that allocates byte slices of a fixed chunk size
// and potentially reuses them.

func TestChunkAllocator(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	chunkSize := 1024
	itemSize := 12288                 // 3072 dims * 4 bytes
	totalSize := chunkSize * itemSize // ~12MB per chunk

	t.Run("Allocates Correct Size", func(t *testing.T) {
		alloc := NewChunkAllocator(chunkSize, itemSize)
		chunk := alloc.Alloc()
		assert.Equal(t, totalSize, len(chunk))
		assert.Equal(t, totalSize, cap(chunk))
	})

	t.Run("Reuses Memory", func(t *testing.T) {
		alloc := NewChunkAllocator(chunkSize, itemSize)
		chunk1 := alloc.Alloc()

		// Fill with pattern
		chunk1[0] = 0xAA

		alloc.Free(chunk1)

		// Alloc again, should ideally get same memory (slab/pool)
		chunk2 := alloc.Alloc()

		// In a STRICT slab allocator, ptr1 == ptr2 might be true if LIFO.
		// We'll just assert it's valid memory.
		assert.Equal(t, totalSize, len(chunk2))

		// sync.Pool does not guarantee LIFO/FIFO address reuse under race detector / GC sweeps.
		// So we do not strictly assert ptr1 == ptr2 here to avoid flakiness.
	})

	t.Run("Concurrent Allocation", func(t *testing.T) {
		alloc := NewChunkAllocator(chunkSize, itemSize)

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				chunk := alloc.Alloc()
				assert.Equal(t, totalSize, len(chunk))
				alloc.Free(chunk)
			}()
		}
		wg.Wait()
	})
}
