package query

import (
	"sync"

	"github.com/23skdu/longbow/internal/pool"
	"github.com/RoaringBitmap/roaring/v2"
)

// Bitset is a thread-safe wrapper around a Roaring Bitmap
type Bitset struct {
	bitmap *roaring.Bitmap
	mu     sync.RWMutex
}

func NewBitset() *Bitset {
	return &Bitset{
		bitmap: pool.GetBitmap(),
	}
}

func NewBitsetFromRoaring(bm *roaring.Bitmap) *Bitset {
	return &Bitset{
		bitmap: bm,
	}
}

func (b *Bitset) And(other *roaring.Bitmap) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.bitmap != nil && other != nil {
		b.bitmap.And(other)
	}
}

func (b *Bitset) Set(i int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.bitmap != nil {
		b.bitmap.Add(uint32(i))
	}
}

func (b *Bitset) Clear(i int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.bitmap != nil {
		b.bitmap.Remove(uint32(i))
	}
}

func (b *Bitset) Contains(i int) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.bitmap == nil {
		return false
	}
	return b.bitmap.Contains(uint32(i))
}

// Clone creates a thread-safe copy of the bitset.
func (b *Bitset) Clone() *Bitset {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.bitmap == nil {
		return &Bitset{bitmap: nil}
	}
	return &Bitset{
		bitmap: b.bitmap.Clone(),
	}
}

// Count returns the number of set bits.
func (b *Bitset) Count() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.bitmap == nil {
		return 0
	}
	return b.bitmap.GetCardinality()
}

// ToUint32Array returns the set bits as a slice of uint32.
func (b *Bitset) ToUint32Array() []uint32 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.bitmap.ToArray()
}

// Release releases the underlying bitmap to the pool
func (b *Bitset) Release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.bitmap != nil {
		pool.PutBitmap(b.bitmap)
		b.bitmap = nil
	}
}
