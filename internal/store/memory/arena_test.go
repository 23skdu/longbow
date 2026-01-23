package memory

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestSlabAllocator_Alloc(t *testing.T) {
	allocator := NewSlabAllocator()

	// Test small allocation
	b1 := allocator.Alloc(10)
	assert.Len(t, b1, 10)
	assert.Equal(t, int64(10), allocator.TotalAllocated())

	// Test sequential allocation (should fit in same slab)
	b2 := allocator.Alloc(20)
	assert.Len(t, b2, 20)
	assert.Equal(t, int64(30), allocator.TotalAllocated())

	// Verify pointers are distinct but close (same slab)
	// This relies on implementation detail but good for "whitebox" testing
	p1 := unsafe.Pointer(&b1[0])
	p2 := unsafe.Pointer(&b2[0])
	diff := uintptr(p2) - uintptr(p1)
	assert.Equal(t, uintptr(10), diff)
}

func TestSlabAllocator_AllocLarge(t *testing.T) {
	allocator := NewSlabAllocator()

	// Alloc larger than slab size (1MB)
	size := SlabSize + 1024
	b := allocator.Alloc(size)
	assert.Len(t, b, size)
	assert.Equal(t, int64(size), allocator.TotalAllocated())
}

func TestSlabAllocator_AllocStruct(t *testing.T) {
	allocator := NewSlabAllocator()

	type TestStruct struct {
		A int64
		B float64
	}

	ts := AllocStruct[TestStruct](allocator)
	assert.NotNil(t, ts)
	assert.Equal(t, int64(0), ts.A)

	ts.A = 123
	ts.B = 456.789

	assert.Equal(t, int64(123), ts.A)
	assert.Equal(t, int64(unsafe.Sizeof(TestStruct{})), allocator.TotalAllocated())
}

func TestSlabAllocator_Reset(t *testing.T) {
	allocator := NewSlabAllocator()
	allocator.Alloc(100)
	assert.Equal(t, int64(100), allocator.TotalAllocated())

	allocator.Reset()
	assert.Equal(t, int64(0), allocator.TotalAllocated())

	// Should be able to alloc again
	allocator.Alloc(50)
	assert.Equal(t, int64(50), allocator.TotalAllocated())
}
