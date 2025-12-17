package store

import "unsafe"

// SearchArena is a per-request arena allocator that provides O(1) allocations
// with zero GC pressure. It uses a simple bump allocator strategy where
// allocations advance an offset through a pre-allocated buffer.
// The arena should be Reset() after each search request to reuse memory.
type SearchArena struct {
	buf    []byte
	offset int
}

// NewSearchArena creates a new arena with the specified capacity in bytes.
// The entire buffer is pre-allocated to avoid runtime allocations.
func NewSearchArena(capacity int) *SearchArena {
	return &SearchArena{
		buf:    make([]byte, capacity),
		offset: 0,
	}
}

// Alloc allocates size bytes from the arena and returns a slice.
// Returns nil if the allocation would exceed capacity.
// This is O(1) - just a pointer bump with no GC involvement.
func (a *SearchArena) Alloc(size int) []byte {
	if size == 0 {
		return []byte{}
	}
	if a.offset+size > len(a.buf) {
		return nil
	}
	result := a.buf[a.offset : a.offset+size]
	a.offset += size
	return result
}

// Reset resets the arena for reuse without deallocating the underlying buffer.
// Call this after each search request to recycle memory.
func (a *SearchArena) Reset() {
	a.offset = 0
}

// Cap returns the total capacity of the arena in bytes.
func (a *SearchArena) Cap() int {
	return len(a.buf)
}

// Offset returns the current allocation offset (bytes used).
func (a *SearchArena) Offset() int {
	return a.offset
}

// Remaining returns the number of bytes still available for allocation.
func (a *SearchArena) Remaining() int {
	return len(a.buf) - a.offset
}

// AllocFloat32Slice allocates a slice of float32 values from the arena.
// Returns nil if the allocation would exceed capacity.
// This is useful for allocating distance/score arrays in search operations.
func (a *SearchArena) AllocFloat32Slice(count int) []float32 {
	if count == 0 {
		return []float32{}
	}

	// Calculate bytes needed (4 bytes per float32)
	const float32Size = 4
	bytesNeeded := count * float32Size

	// Ensure proper alignment for float32 (4-byte alignment)
	alignment := float32Size
	alignedOffset := (a.offset + alignment - 1) &^ (alignment - 1)

	if alignedOffset+bytesNeeded > len(a.buf) {
		return nil
	}

	// Update offset to aligned position plus allocation
	a.offset = alignedOffset + bytesNeeded

	// Convert byte slice to float32 slice using unsafe
	ptr := unsafe.Pointer(&a.buf[alignedOffset])
	return unsafe.Slice((*float32)(ptr), count)
}
