package store

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestArenaPool_GetReturnsArena tests that GetArena returns a valid arena
func TestArenaPool_GetReturnsArena(t *testing.T) {
	arena := GetArena()
	require.NotNil(t, arena, "GetArena should return non-nil arena")
	assert.Greater(t, arena.Cap(), 0, "Arena should have capacity")
	PutArena(arena)
}

// TestArenaPool_PutAndReuseArena tests that arenas are reused from pool
func TestArenaPool_PutAndReuseArena(t *testing.T) {
	arena1 := GetArena()
	require.NotNil(t, arena1)
	arena1.Alloc(100)
	assert.Equal(t, 100, arena1.Offset(), "Arena should have 100 bytes used")
	PutArena(arena1)
	arena2 := GetArena()
	require.NotNil(t, arena2)
	assert.Equal(t, 0, arena2.Offset(), "Reused arena should be reset")
	PutArena(arena2)
}

// TestArenaPool_ConcurrentAccess tests thread-safe pool access
func TestArenaPool_ConcurrentAccess(t *testing.T) {
	const goroutines = 100
	const iterations = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				arena := GetArena()
				require.NotNil(t, arena)
				_ = arena.Alloc(64)
				PutArena(arena)
			}
		}()
	}
	wg.Wait()
}

// TestArenaPool_NUMAAwareAllocation tests that GetArenaForNode works correctly
func TestArenaPool_NUMAAwareAllocation(t *testing.T) {
	arena := GetArenaForNode(1)
	require.NotNil(t, arena, "GetArenaForNode should return non-nil arena")
	assert.Greater(t, arena.Cap(), 0, "Arena should have capacity")
	assert.Equal(t, 1, arena.NUMANode(), "Arena should have correct NUMA node")
	PutArena(arena)
}

// FuzzNUMAAllocation fuzzes NUMA node requests to ensure thread safety and no panics
func FuzzNUMAAllocation(f *testing.F) {
	f.Add(0, 1024)
	f.Add(1, 4096)
	f.Add(2, 65536)
	
	f.Fuzz(func(t *testing.T, node int, allocSize int) {
		if node < 0 || node > 8 { // Bound reasonable node limits
			t.Skip()
		}
		if allocSize <= 0 || allocSize > 64*1024 {
			t.Skip()
		}
		
		arena := GetArenaForNode(node)
		require.NotNil(t, arena)
		assert.Equal(t, node, arena.NUMANode())
		
		buf := arena.Alloc(allocSize)
		if buf != nil {
			assert.Equal(t, allocSize, len(buf))
		}
		PutArena(arena)
	})
}
