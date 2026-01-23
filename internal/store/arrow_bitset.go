package store

import "github.com/23skdu/longbow/internal/metrics"

// ArrowBitset is a fast, fixed-size bitset for HNSW search visited tracking.
// It is NOT thread-safe (intended for thread-local use in SearchContext).
// It supports uint32 logic natively to match HNSW IDs.
type ArrowBitset struct {
	data []uint64
	size int
}

// NewArrowBitset creates a new bitset with the given capacity.
func NewArrowBitset(capacity int) *ArrowBitset {
	n := (capacity + 63) / 64
	return &ArrowBitset{
		data: make([]uint64, n),
		size: capacity,
	}
}

// Set sets the bit at index i.
func (b *ArrowBitset) Set(i uint32) {
	idx := int(i)
	if idx >= b.size {
		b.Grow(idx + 1)
	}
	b.data[idx/64] |= 1 << (idx % 64)
}

// Unset clears the bit at index i.
func (b *ArrowBitset) Unset(i uint32) {
	idx := int(i)
	if idx >= b.size {
		return
	}
	b.data[idx/64] &^= 1 << (idx % 64)
}

// IsSet checks if the bit at index i is set.
func (b *ArrowBitset) IsSet(i uint32) bool {
	idx := int(i)
	if idx >= b.size {
		return false
	}
	return (b.data[idx/64] & (1 << (idx % 64))) != 0
}

// Clear clears all bits.
func (b *ArrowBitset) Clear() {
	for i := range b.data {
		b.data[i] = 0
	}
}

// ClearSIMD is a placeholder for SIMD optimized clear (currently same as Clear).
func (b *ArrowBitset) ClearSIMD() {
	b.Clear()
}

// Grow ensures the bitset has at least newCap bits.
func (b *ArrowBitset) Grow(newCap int) {
	if newCap <= b.size {
		return
	}
	n := (newCap + 63) / 64
	if n > cap(b.data) {
		metrics.HNSWBitsetGrowTotal.Inc()
		// Calculate new capacity: double existing or use required if much larger
		newAlloc := cap(b.data) * 2
		if newAlloc < n {
			newAlloc = n
		}
		newData := make([]uint64, newAlloc)
		copy(newData, b.data)
		b.data = newData
	}
	// Re-slice to new required word count
	b.data = b.data[:n]

	// Clear the newly added bits (from old size to new size)
	startWord := (b.size + 63) / 64
	for i := startWord; i < n; i++ {
		b.data[i] = 0
	}

	b.size = newCap
}

// Size returns the capacity of the bitset.
func (b *ArrowBitset) Size() int {
	return b.size
}

// Contains alias for IsSet (convenience)
func (b *ArrowBitset) Contains(i uint32) bool {
	return b.IsSet(i)
}

// FilterVisited takes a list of IDs, checks if they are visited, sets them if not,
// and returns a new slice containing only the previously UNVISITED IDs.
// This allows batching the check-and-set operation.
func (b *ArrowBitset) FilterVisited(ids []uint32) []uint32 {
	out := make([]uint32, 0, len(ids))

	for _, id := range ids {
		idx := int(id)
		if idx >= b.size {
			b.Grow(idx + 1)
		}

		wordIdx := idx / 64
		bitMask := uint64(1) << (idx % 64)

		// Check
		if (b.data[wordIdx] & bitMask) == 0 {
			// Not visited: Set it and append
			b.data[wordIdx] |= bitMask
			out = append(out, id)
		}
	}
	return out
}

// FilterVisitedInto filters unvisited IDs into the provided 'out' slice.
func (b *ArrowBitset) FilterVisitedInto(ids, out []uint32) []uint32 {
	// Don't reset out, append to it?
	// Usually caller explicitly passes out[:0] if they want reset.
	// We should just append.
	for _, id := range ids {
		idx := int(id)
		if idx >= b.size {
			b.Grow(idx + 1)
		}

		wordIdx := idx / 64
		bitMask := uint64(1) << (idx % 64)

		if (b.data[wordIdx] & bitMask) == 0 {
			b.data[wordIdx] |= bitMask
			out = append(out, id)
		}
	}
	return out
}
