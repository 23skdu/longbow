package memory

import (
	"runtime"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArena_HugePageAlignment(t *testing.T) {
	// 4MB slab arena (>= 2MB triggers transparent hugepage madvise)
	slabSize := 4 * 1024 * 1024
	arena := NewSlabArena(slabSize)
	defer arena.Release()

	// Allocate a slice across the arena
	off, err := arena.Alloc(1024 * 1024)
	require.NoError(t, err)
	assert.NotZero(t, off)

	slice := arena.Get(off, 1024*1024)
	require.Len(t, slice, 1024*1024)

	// Write and read pattern
	slice[0] = 0x5A
	slice[len(slice)-1] = 0xA5
	assert.Equal(t, byte(0x5A), slice[0])
	assert.Equal(t, byte(0xA5), slice[len(slice)-1])

	// Direct test of MadviseHugePages
	alloc := NewOffHeapAllocator()
	buf := alloc.Allocate(2 * 1024 * 1024)
	require.Len(t, buf, 2*1024*1024)
	defer alloc.Free(buf)

	err = MadviseHugePages(unsafe.Pointer(&buf[0]), len(buf)) // #nosec G103
	if runtime.GOOS == "linux" {
		// On Linux, MadviseHugePages should not error
		assert.NoError(t, err)
	} else {
		assert.NoError(t, err)
	}

	// Verify TypedArena works with hugepage-backed SlabArena
	typed := NewTypedArena[float32](arena)
	defer typed.Release()

	floatRef, err := typed.AllocSlice(128)
	require.NoError(t, err)
	assert.False(t, floatRef.IsNil())

	floats := typed.Get(floatRef)
	require.Len(t, floats, 128)
	floats[0] = 3.14159
	floats[127] = 2.71828

	floatsAgain := typed.Get(floatRef)
	assert.Equal(t, float32(3.14159), floatsAgain[0])
	assert.Equal(t, float32(2.71828), floatsAgain[127])
}
