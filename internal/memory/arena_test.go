package memory

import (
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
