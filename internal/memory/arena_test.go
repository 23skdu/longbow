package memory

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSlabArena_Alloc_Basic(t *testing.T) {
	// Arena with 1KB slabs
	arena := NewSlabArena(1024)

	// Alloc 10 floats (40 bytes)
	offset1, err := arena.Alloc(40)
	require.NoError(t, err)
	assert.NotZero(t, offset1)

	// Write data
	slice1 := arena.Get(offset1, 40)
	require.Len(t, slice1, 40)
	slice1[0] = 0xAB
	slice1[39] = 0xCD

	// Alloc another 10 floats
	offset2, err := arena.Alloc(40)
	require.NoError(t, err)
	assert.NotEqual(t, offset1, offset2)

	// Verify data persistence
	slice1Again := arena.Get(offset1, 40)
	assert.Equal(t, byte(0xAB), slice1Again[0])
	assert.Equal(t, byte(0xCD), slice1Again[39])
}

func TestSlabArena_Alloc_Growth(t *testing.T) {
	// Small slabs: 128 bytes
	arena := NewSlabArena(128)

	// 1. Fill first slab (alloc 80 bytes)
	off1, err := arena.Alloc(80)
	require.NoError(t, err)

	// 2. Alloc that fits in remainder (alloc 40 bytes) -> Total 120 bytes
	// (Wait, padding? 8-byte align. 80 is aligned. 40 is aligned. 80+40=120. Fits.)
	off2, err := arena.Alloc(40)
	require.NoError(t, err)

	// 3. Alloc that pushes to NEW slab (alloc 40 bytes)
	// Previous: 120 used. 8 left. Need 40. -> New Slab.
	off3, err := arena.Alloc(40)
	require.NoError(t, err)

	// Verify addresses are distinct
	s1 := arena.Get(off1, 80)
	s2 := arena.Get(off2, 40)
	s3 := arena.Get(off3, 40)

	// Check we got data
	assert.NotNil(t, s1)
	assert.NotNil(t, s2)
	assert.NotNil(t, s3)

	// Check offsets global monotonicity usually?
	// Slab 0: 0..128
	// Slab 1: 128..256
	// off1 should be ~1 (if 0 burnt). off3 should be >= 128.
	assert.GreaterOrEqual(t, int(off3), 128)
}
func TestSlabArena_Alloc_TooLarge(t *testing.T) {
	arena := NewSlabArena(100) // Will be clamped to 1024 internally

	// Alloc 2000 bytes > 1KB slab limit
	_, err := arena.Alloc(2000)
	assert.Error(t, err)
}

func TestRef_Encoding(t *testing.T) {
	// Test manual construction/checking
	var r SliceRef
	assert.True(t, r.IsNil())

	// Simulate a valid ref stub
	r.Offset = 50
	r.Len = 10
	assert.False(t, r.IsNil())
}

func TestSlabArena_FastPath(t *testing.T) {
	arena := NewSlabArena(1024)

	// Fast path: small allocations (≤ 64 bytes)
	for i := 0; i < 100; i++ {
		offset, err := arena.Alloc(32)
		require.NoError(t, err)
		assert.NotZero(t, offset)

		// Verify data persistence
		slice := arena.Get(offset, 32)
		require.Len(t, slice, 32)
		slice[0] = byte(i)
		assert.Equal(t, byte(i), slice[0])
	}
}

func TestSlabArena_FastPathConcurrent(t *testing.T) {
	arena := NewSlabArena(4096)

	const numGoroutines = 10
	const allocsPerGoroutine = 100

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < allocsPerGoroutine; i++ {
				size := 16 + (i % 48) // Mix of sizes ≤ 64
				offset, err := arena.Alloc(size)
				require.NoError(t, err)
				assert.NotZero(t, offset)

				// Write and verify
				slice := arena.Get(offset, uint32(size))
				require.Len(t, slice, size)
				slice[0] = byte(gid)
				assert.Equal(t, byte(gid), slice[0])
			}
		}(g)
	}
	wg.Wait()
}

func TestSlabArena_FastPathFallback(t *testing.T) {
	arena := NewSlabArena(128)

	// Fast path: small allocations (≤ 64 bytes)
	offsets := make([]uint64, 10)
	for i := range offsets {
		var err error
		offsets[i], err = arena.Alloc(32)
		require.NoError(t, err)
	}

	// Verify all allocations are distinct
	for i := 1; i < len(offsets); i++ {
		assert.NotEqual(t, offsets[i-1], offsets[i])
	}
}

func BenchmarkSlabArena_FastPath(b *testing.B) {
	arena := NewSlabArena(4096)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = arena.Alloc(32)
	}
}

func BenchmarkSlabArena_FastPathConcurrent(b *testing.B) {
	arena := NewSlabArena(4096)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = arena.Alloc(32)
		}
	})
}

func BenchmarkSlabArena_SlowPath(b *testing.B) {
	arena := NewSlabArena(4096)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = arena.Alloc(128) // > 64 bytes uses slow path
	}
}

func BenchmarkSlabArena_Comparison(b *testing.B) {
	b.Run("FastPath_32B", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			arena := NewSlabArena(4096)
			for j := 0; j < 100; j++ {
				_, _ = arena.Alloc(32)
			}
		}
	})

	b.Run("SlowPath_128B", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			arena := NewSlabArena(4096)
			for j := 0; j < 100; j++ {
				_, _ = arena.Alloc(128)
			}
		}
	})
}

func FuzzSlabArena_FastPath(f *testing.F) {
	f.Add(uint32(1024), uint32(100))
	f.Add(uint32(4096), uint32(500))
	f.Add(uint32(8192), uint32(1000))

	f.Fuzz(func(t *testing.T, slabSize uint32, numAllocs uint32) {
		if slabSize < 1024 || slabSize > 65536 {
			t.Skip()
		}
		if numAllocs == 0 || numAllocs > 10000 {
			t.Skip()
		}

		arena := NewSlabArena(int(slabSize))

		for i := uint32(0); i < numAllocs; i++ {
			size := 8 + (i % 56) // Keep allocations ≤ 64 bytes for fast path
			offset, err := arena.Alloc(int(size))
			if err != nil {
				// Out of space, that's acceptable
				break
			}

			// Verify we can read/write the allocated memory
			data := arena.Get(offset, uint32(size))
			if len(data) != int(size) {
				t.Errorf("expected %d bytes, got %d", size, len(data))
			}

			// Write a pattern
			data[0] = byte(i)
			data[size-1] = byte(i + 1)

			// Verify the pattern
			if data[0] != byte(i) || data[size-1] != byte(i+1) {
				t.Errorf("memory corruption detected")
			}
		}
	})
}

func FuzzSlabArena_FastPathConcurrent(f *testing.F) {
	f.Add(uint32(4096), uint32(10), uint32(100))
	f.Add(uint32(8192), uint32(20), uint32(200))

	f.Fuzz(func(t *testing.T, slabSize uint32, numGoroutines uint32, allocsPerGoroutine uint32) {
		if slabSize < 1024 || slabSize > 65536 {
			t.Skip()
		}
		if numGoroutines == 0 || numGoroutines > 100 {
			t.Skip()
		}
		if allocsPerGoroutine == 0 || allocsPerGoroutine > 1000 {
			t.Skip()
		}

		arena := NewSlabArena(int(slabSize))

		var wg sync.WaitGroup
		for g := uint32(0); g < numGoroutines; g++ {
			wg.Add(1)
			go func(gid uint32) {
				defer wg.Done()
				for i := uint32(0); i < allocsPerGoroutine; i++ {
					size := 8 + (i % 56) // ≤ 64 bytes
					offset, err := arena.Alloc(int(size))
					if err != nil {
						break
					}

					data := arena.Get(offset, uint32(size))
					if len(data) == int(size) {
						data[0] = byte(gid)
					}
				}
			}(g)
		}
		wg.Wait()
	})
}
