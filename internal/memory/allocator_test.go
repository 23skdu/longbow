package memory

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestArenaAllocator(t *testing.T) {
	alloc := NewArenaAllocator()
	defer alloc.Release()

	// 1. Basic Allocation
	buf := alloc.Allocate(1024)
	assert.Equal(t, 1024, len(buf))
	assert.True(t, cap(buf) >= 1024)
	// Note: Cap might be larger if slicing from larger chunk, but strictly len is what matters.

	// Write to it
	buf[0] = 1
	buf[1023] = 2

	// 2. Multiple Allocations fitting in one chunk
	buf2 := alloc.Allocate(1024)
	buf2[0] = 3

	// Addresses should be distinct
	assert.NotEqual(t, &buf[0], &buf2[0])

	// 3. Reallocation
	buf3 := alloc.Reallocate(2048, buf)
	assert.Equal(t, 2048, len(buf3))
	assert.Equal(t, byte(1), buf3[0])
	assert.Equal(t, byte(2), buf3[1023])

	// 4. Force new chunk
	largeBuf := alloc.Allocate(DefaultArenaChunkSize) // This might trigger large alloc path or fill chunk
	assert.Equal(t, DefaultArenaChunkSize, len(largeBuf))

	// 5. Huge allocation (bypass pool)
	hugeBuf := alloc.Allocate(DefaultArenaChunkSize * 2)
	assert.Equal(t, DefaultArenaChunkSize*2, len(hugeBuf))
}

func TestArenaReuse(t *testing.T) {
	alloc := NewArenaAllocator()

	// Allocate
	_ = alloc.Allocate(1024)
	assert.Equal(t, int64(1024), alloc.Allocated())

	// Release
	alloc.Release()
	assert.Equal(t, int64(0), alloc.Allocated())

	// Re-use same object
	_ = alloc.Allocate(512)
	assert.Equal(t, int64(512), alloc.Allocated())
}
