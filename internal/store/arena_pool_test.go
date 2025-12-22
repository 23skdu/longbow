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
