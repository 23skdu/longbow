package types

import (
	"math/bits"
)

// ArrowBitset is a memory-efficient bitset implementation for tracking visited nodes
// during HNSW search operations. It's optimized for Arrow-based vector operations.
type ArrowBitset struct {
	bits   []uint64
	size   int
	blocks int
	growth int // How much to grow when capacity is exceeded
}

// NewArrowBitset creates a new ArrowBitset with the specified initial capacity.
func NewArrowBitset(initialSize int) *ArrowBitset {
	if initialSize <= 0 {
		initialSize = 64
	}

	blocks := (initialSize + 63) / 64
	return &ArrowBitset{
		bits:   make([]uint64, blocks),
		size:   0,
		blocks: blocks,
		growth: 64,
	}
}

// Set marks the bit at position pos as 1 (visited).
func (b *ArrowBitset) Set(pos int) {
	if pos < 0 {
		return
	}

	// Grow if necessary
	if pos >= b.blocks*64 {
		b.growInternal(pos + 1)
	}

	block := pos / 64
	bit := pos % 64
	b.bits[block] |= 1 << bit

	if pos > b.size {
		b.size = pos
	}
}

// IsSet returns true if the bit at position pos is 1 (visited).
func (b *ArrowBitset) IsSet(pos int) bool {
	if pos < 0 {
		return false
	}

	if pos >= b.blocks*64 {
		return false
	}

	block := pos / 64
	bit := pos % 64
	return (b.bits[block] & (1 << bit)) != 0
}

// Clear resets all bits to 0.
func (b *ArrowBitset) Clear() {
	for i := range b.bits {
		b.bits[i] = 0
	}
	b.size = 0
}

// Grow ensures the bitset can accommodate positions up to newSize.
func (b *ArrowBitset) Grow(newSize int) {
	b.growInternal(newSize)
}

// growInternal is the internal grow method that assumes the mutex is held.
func (b *ArrowBitset) growInternal(newSize int) {
	if newSize < b.blocks*64 {
		return
	}

	newBlocks := (newSize + 63) / 64
	newBits := make([]uint64, newBlocks)
	copy(newBits, b.bits)

	b.bits = newBits
	b.blocks = newBlocks
}

// Count returns the number of set bits in the bitset.
func (b *ArrowBitset) Count() int {
	count := 0
	for _, block := range b.bits {
		count += bits.OnesCount64(block)
	}
	return count
}

// Size returns the maximum position that has been set.
func (b *ArrowBitset) Size() int {
	return b.size
}

// Capacity returns the current capacity in bits.
func (b *ArrowBitset) Capacity() int {
	return b.blocks * 64
}

// Clear resets all bits to 0. Consistent with HNSW usage.
func (b *ArrowBitset) ClearSIMD() {
	b.Clear()
}

// FilterAndMarkUnvisited filters candidate IDs against the bitset, appends
// previously unvisited IDs that satisfy the commit threshold to dst, and marks
// them as visited. It minimizes branching and eliminates individual Set/IsSet overhead.
func (b *ArrowBitset) FilterAndMarkUnvisited(ids []uint32, maxCommitted int64, allowUncommitted bool, dst []uint32) []uint32 {
	n := len(ids)
	if n == 0 {
		return dst
	}

	maxCap := b.blocks * 64

	for i := 0; i < n; i++ {
		id := ids[i]
		if !allowUncommitted && int64(id) >= maxCommitted {
			continue
		}
		pos := int(id)
		if pos >= maxCap {
			b.growInternal(pos + 1)
			maxCap = b.blocks * 64
		}

		block := pos / 64
		bitMask := uint64(1) << (pos % 64)
		if (b.bits[block] & bitMask) == 0 {
			b.bits[block] |= bitMask
			dst = append(dst, id)
			if pos > b.size {
				b.size = pos
			}
		}
	}
	return dst
}
