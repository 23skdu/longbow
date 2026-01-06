package store


import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewSearchArena verifies arena creation with specified capacity
func TestNewSearchArena(t *testing.T) {
	arena := NewSearchArena(1024)
	require.NotNil(t, arena, "arena should not be nil")
	assert.Equal(t, 1024, arena.Cap(), "arena capacity should match requested size")
	assert.Equal(t, 0, arena.Offset(), "initial offset should be 0")
}

// TestSearchArenaAlloc verifies basic allocation works
func TestSearchArenaAlloc(t *testing.T) {
	arena := NewSearchArena(1024)
	buf := arena.Alloc(100)
	require.NotNil(t, buf, "allocated buffer should not be nil")
	assert.Equal(t, 100, len(buf), "allocated buffer should have requested size")
	assert.Equal(t, 100, arena.Offset(), "offset should advance by allocation size")
}

// TestSearchArenaAlloc_Multiple verifies multiple allocations work correctly
func TestSearchArenaAlloc_Multiple(t *testing.T) {
	arena := NewSearchArena(1024)

	buf1 := arena.Alloc(100)
	buf2 := arena.Alloc(200)
	buf3 := arena.Alloc(300)

	assert.Equal(t, 100, len(buf1))
	assert.Equal(t, 200, len(buf2))
	assert.Equal(t, 300, len(buf3))
	assert.Equal(t, 600, arena.Offset(), "offset should be sum of all allocations")
}

// TestSearchArenaAlloc_NoOverlap verifies allocations don't overlap
func TestSearchArenaAlloc_NoOverlap(t *testing.T) {
	arena := NewSearchArena(1024)

	buf1 := arena.Alloc(100)
	buf2 := arena.Alloc(100)

	// Write to buf1
	for i := range buf1 {
		buf1[i] = 0xAA
	}
	// Write to buf2
	for i := range buf2 {
		buf2[i] = 0xBB
	}

	// Verify buf1 wasn't overwritten
	for i := range buf1 {
		assert.Equal(t, byte(0xAA), buf1[i], "buf1 should not be overwritten by buf2")
	}
	// Verify buf2 has correct values
	for i := range buf2 {
		assert.Equal(t, byte(0xBB), buf2[i], "buf2 should have its own values")
	}
}

// TestSearchArenaReset verifies reset clears offset but retains capacity
func TestSearchArenaReset(t *testing.T) {
	arena := NewSearchArena(1024)

	_ = arena.Alloc(500)
	assert.Equal(t, 500, arena.Offset())

	arena.Reset()
	assert.Equal(t, 0, arena.Offset(), "offset should be 0 after reset")
	assert.Equal(t, 1024, arena.Cap(), "capacity should remain unchanged after reset")
}

// TestSearchArenaReset_AllowsReuse verifies arena can be reused after reset
func TestSearchArenaReset_AllowsReuse(t *testing.T) {
	arena := NewSearchArena(1024)

	// First use cycle
	buf1 := arena.Alloc(512)
	for i := range buf1 {
		buf1[i] = 0xFF
	}

	arena.Reset()

	// Second use cycle - should reuse same memory
	buf2 := arena.Alloc(512)
	assert.Equal(t, 512, len(buf2), "should allocate same size after reset")
	assert.Equal(t, 512, arena.Offset())
}

// TestSearchArenaAlloc_ZeroSize verifies zero-size allocation
func TestSearchArenaAlloc_ZeroSize(t *testing.T) {
	arena := NewSearchArena(1024)
	buf := arena.Alloc(0)
	assert.Equal(t, 0, len(buf), "zero-size allocation should return empty slice")
	assert.Equal(t, 0, arena.Offset(), "zero-size allocation should not advance offset")
}

// TestSearchArenaAlloc_ExactCapacity verifies allocation using full capacity
func TestSearchArenaAlloc_ExactCapacity(t *testing.T) {
	arena := NewSearchArena(1024)
	buf := arena.Alloc(1024)
	require.NotNil(t, buf)
	assert.Equal(t, 1024, len(buf))
	assert.Equal(t, 1024, arena.Offset())
}

// TestSearchArenaAlloc_ExceedsCapacity verifies behavior when capacity exceeded
func TestSearchArenaAlloc_ExceedsCapacity(t *testing.T) {
	arena := NewSearchArena(100)

	// First allocation uses some space
	_ = arena.Alloc(80)

	// This allocation would exceed capacity
	buf := arena.Alloc(50)
	assert.Nil(t, buf, "allocation exceeding capacity should return nil")
}

// TestSearchArenaRemaining verifies remaining capacity reporting
func TestSearchArenaRemaining(t *testing.T) {
	arena := NewSearchArena(1024)
	assert.Equal(t, 1024, arena.Remaining())

	_ = arena.Alloc(300)
	assert.Equal(t, 724, arena.Remaining())

	_ = arena.Alloc(200)
	assert.Equal(t, 524, arena.Remaining())

	arena.Reset()
	assert.Equal(t, 1024, arena.Remaining(), "remaining should reset to full capacity")
}

// TestSearchArenaAllocFloat32Slice verifies typed allocation helper for float32
func TestSearchArenaAllocFloat32Slice(t *testing.T) {
	arena := NewSearchArena(1024)

	floats := arena.AllocFloat32Slice(10)
	require.NotNil(t, floats)
	assert.Equal(t, 10, len(floats))
	assert.Equal(t, 10, cap(floats))

	// Verify we can write to it
	for i := range floats {
		floats[i] = float32(i)
	}
	assert.Equal(t, float32(5), floats[5])
}

// TestSearchArenaAllocFloat32Slice_ExceedsCapacity verifies typed allocation failure
func TestSearchArenaAllocFloat32Slice_ExceedsCapacity(t *testing.T) {
	arena := NewSearchArena(32) // 32 bytes = 8 float32s

	floats := arena.AllocFloat32Slice(100) // Would need 400 bytes
	assert.Nil(t, floats, "allocation exceeding capacity should return nil")
}

// BenchmarkSearchArenaAlloc benchmarks arena allocation vs make()
func BenchmarkSearchArenaAlloc(b *testing.B) {
	arena := NewSearchArena(64 * 1024) // 64KB arena
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		arena.Reset()
		// Simulate typical search allocations
		_ = arena.Alloc(1024) // distances
		_ = arena.Alloc(1024) // indices
		_ = arena.Alloc(256)  // candidates
	}
}

// BenchmarkStandardAlloc benchmarks standard Go allocation for comparison
func BenchmarkStandardAlloc(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = make([]byte, 1024) // distances
		_ = make([]byte, 1024) // indices
		_ = make([]byte, 256)  // candidates
	}
}

// BenchmarkSearchArenaAllocFloat32Slice benchmarks typed float32 allocation
func BenchmarkSearchArenaAllocFloat32Slice(b *testing.B) {
	arena := NewSearchArena(64 * 1024)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		arena.Reset()
		_ = arena.AllocFloat32Slice(256) // typical vector size
	}
}

// BenchmarkStandardFloat32Alloc benchmarks standard float32 slice allocation
func BenchmarkStandardFloat32Alloc(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = make([]float32, 256)
	}
}
