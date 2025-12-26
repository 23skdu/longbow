package hnsw2

import "fmt"

// Bitset provides a compact, zero-allocation visited tracking structure for HNSW search.
// It uses a fixed-size bit array to track which nodes have been visited during traversal.
type Bitset struct {
	bits []uint64
	size int
}

// NewBitset creates a new bitset capable of tracking up to maxSize elements.
func NewBitset(maxSize int) *Bitset {
	numWords := (maxSize + 63) / 64
	return &Bitset{
		bits: make([]uint64, numWords),
		size: maxSize,
	}
}

// Set marks the given index as visited.
func (b *Bitset) Set(idx uint32) {
	if int(idx) >= b.size {
		panic(fmt.Sprintf("Bitset.Set: index %d out of bounds (size %d)", idx, b.size))
	}
	word := idx / 64
	bit := idx % 64
	b.bits[word] |= 1 << bit
}

// IsSet returns true if the given index has been visited.
func (b *Bitset) IsSet(idx uint32) bool {
	if int(idx) >= b.size {
		return false
	}
	word := idx / 64
	bit := idx % 64
	return (b.bits[word] & (1 << bit)) != 0
}

// Clear resets all bits to unvisited.
func (b *Bitset) Clear() {
	for i := range b.bits {
		b.bits[i] = 0
	}
}

// Size returns the maximum number of elements this bitset can track.
func (b *Bitset) Size() int {
	return b.size
}
