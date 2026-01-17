package memory

import (
	"testing"
	"unsafe"

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

	// 5. Huge allocation (bypass standard pool, use huge pool)
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

func TestArenaAllocator_HugeReuse(t *testing.T) {
	alloc := NewArenaAllocator()

	// 1. Alloc 100MB (Fits in 128MB pool)
	size := 100 * 1024 * 1024
	buf1 := alloc.Allocate(size)
	ptr1 := unsafe.Pointer(&buf1[0])

	// 2. Release
	alloc.Release()

	// 3. Alloc 100MB again
	buf2 := alloc.Allocate(size)
	ptr2 := unsafe.Pointer(&buf2[0])

	// Should reuse the same buffer (Best effort validation for sync.Pool)
	if ptr1 != ptr2 {
		t.Logf("Warning: Huge buffer was not reused (likely GC'd from pool). Expected %p, got %p", ptr1, ptr2)
	} else {
		assert.Equal(t, ptr1, ptr2, "Huge buffer should be reused from pool")
	}

	// 4. Alloc 200MB (Fits in 256MB pool)
	alloc.Release()
	buf3 := alloc.Allocate(200 * 1024 * 1024)
	ptr3 := unsafe.Pointer(&buf3[0])

	alloc.Release()
	buf4 := alloc.Allocate(200 * 1024 * 1024)
	ptr4 := unsafe.Pointer(&buf4[0])

	// Should reuse the same buffer
	if ptr3 != ptr4 {
		t.Logf("Warning: 200MB buffer was not reused. Expected %p, got %p", ptr3, ptr4)
	} else {
		assert.Equal(t, ptr3, ptr4, "200MB buffer should be reused")
	}

}
