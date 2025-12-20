package store

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPooledAllocator_Allocate(t *testing.T) {
	p := NewPooledAllocator()

	// Test various sizes
	sizes := []int{32, 64, 100, 256, 1000, 4096, 65536, 1024 * 1024}
	for _, size := range sizes {
		buf := p.Allocate(size)
		assert.Equal(t, size, len(buf), "Allocated buffer should have exact length")
		assert.GreaterOrEqual(t, cap(buf), size, "Capacity should be >= size")
		p.Free(buf)
	}
}

func TestPooledAllocator_Reallocate(t *testing.T) {
	p := NewPooledAllocator()

	// Allocate initial buffer
	buf := p.Allocate(100)
	for i := range buf {
		buf[i] = byte(i)
	}

	// Reallocate to smaller size (should reuse)
	buf2 := p.Reallocate(50, buf)
	assert.Equal(t, 50, len(buf2))
	for i := 0; i < 50; i++ {
		assert.Equal(t, byte(i), buf2[i])
	}

	// Reallocate to larger size (should allocate new)
	buf3 := p.Reallocate(200, buf2)
	assert.Equal(t, 200, len(buf3))
	for i := 0; i < 50; i++ {
		assert.Equal(t, byte(i), buf3[i])
	}

	p.Free(buf3)
}

func TestPooledAllocator_Free(t *testing.T) {
	p := NewPooledAllocator()

	// Free should not panic on nil
	p.Free(nil)

	// Allocate, free, reallocate should reuse
	buf1 := p.Allocate(128)
	p.Free(buf1)

	buf2 := p.Allocate(128)
	// Can't guarantee same pointer but stats should show reuse
	stats := p.Stats()
	assert.GreaterOrEqual(t, stats.ReusedBuffers, int64(2))
	p.Free(buf2)
}

func TestPooledAllocator_BucketIndex(t *testing.T) {
	tests := []struct {
		size     int
		expected int
	}{
		{0, 0},
		{1, 0},
		{64, 0},
		{65, 1},
		{128, 1},
		{129, 2},
		{256, 2},
		{1024, 4},
		{1 << 25, 19},       // 32MB - max bucket
		{(1 << 25) + 1, -1}, // > 32MB - too large
	}

	for _, tt := range tests {
		result := bucketIndex(tt.size)
		assert.Equal(t, tt.expected, result, "bucketIndex(%d)", tt.size)
	}
}

func TestPooledAllocator_LargeAllocation(t *testing.T) {
	p := NewPooledAllocator()

	// Allocate larger than max bucket (32MB)
	largeSize := (1 << 25) + 1000
	buf := p.Allocate(largeSize)
	assert.Equal(t, largeSize, len(buf))

	stats := p.Stats()
	assert.Equal(t, int64(largeSize), stats.AllocatedBytes)

	p.Free(buf) // Should not panic
}

func TestPooledAllocator_Concurrent(t *testing.T) {
	p := NewPooledAllocator()
	var wg sync.WaitGroup

	numGoroutines := 100
	numAllocations := 1000

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numAllocations; j++ {
				size := (j%10 + 1) * 100
				buf := p.Allocate(size)
				// Write to buffer to catch data races
				for k := range buf {
					buf[k] = byte(k)
				}
				p.Free(buf)
			}
		}()
	}

	wg.Wait()

	stats := p.Stats()
	assert.GreaterOrEqual(t, stats.ReusedBuffers, int64(numGoroutines*numAllocations))
}

func BenchmarkPooledAllocator_Allocate(b *testing.B) {
	p := NewPooledAllocator()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := p.Allocate(4096)
		p.Free(buf)
	}
}

func BenchmarkPooledAllocator_Allocate_Parallel(b *testing.B) {
	p := NewPooledAllocator()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := p.Allocate(4096)
			p.Free(buf)
		}
	})
}

func BenchmarkGoAllocator_Allocate(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buf := make([]byte, 4096)
		_ = buf
	}
}

func BenchmarkPooledAllocator_VectorSize(b *testing.B) {
	p := NewPooledAllocator()
	vectorSize := 1536 * 4 // 1536 float32s = 6144 bytes (typical embedding)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := p.Allocate(vectorSize)
		p.Free(buf)
	}
}
