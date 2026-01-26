package store

import (
	"math/bits"
	"sync"
)

// ArrowBitset is a memory-efficient bitset implementation for tracking visited nodes
// during HNSW search operations. It's optimized for Arrow-based vector operations.
type ArrowBitset struct {
	bits   []uint64
	size   int
	mu     sync.RWMutex
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

	b.mu.Lock()
	defer b.mu.Unlock()

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

	b.mu.RLock()
	defer b.mu.RUnlock()

	if pos >= b.blocks*64 {
		return false
	}

	block := pos / 64
	bit := pos % 64
	return (b.bits[block] & (1 << bit)) != 0
}

// Clear resets all bits to 0.
func (b *ArrowBitset) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i := range b.bits {
		b.bits[i] = 0
	}
	b.size = 0
}

// Grow ensures the bitset can accommodate positions up to newSize.
func (b *ArrowBitset) Grow(newSize int) {
	b.mu.Lock()
	defer b.mu.Unlock()
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
	b.mu.RLock()
	defer b.mu.RUnlock()

	count := 0
	for _, block := range b.bits {
		count += bits.OnesCount64(block)
	}
	return count
}

// Size returns the maximum position that has been set.
func (b *ArrowBitset) Size() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.size
}

// Capacity returns the current capacity in bits.
func (b *ArrowBitset) Capacity() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.blocks * 64
}
