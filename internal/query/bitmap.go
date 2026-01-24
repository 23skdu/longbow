package query

import (
	"sync"

	"sync/atomic"

	"github.com/23skdu/longbow/internal/pool"
	"github.com/RoaringBitmap/roaring/v2"
)

// Bitset is a thread-safe wrapper around a Roaring Bitmap
type Bitset struct {
	bitmap *roaring.Bitmap
	mu     sync.RWMutex
}

func (b *Bitset) AsRoaring() *roaring.Bitmap {
	if b == nil {
		return nil
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.bitmap
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

// Slice returns a new Bitset containing bits in range [offset, offset+length)
// shifted by -offset. Used for splitting batches with tombstones.
func (b *Bitset) Slice(offset, length int) *Bitset {
	b.mu.RLock()
	defer b.mu.RUnlock()

	newBm := NewBitset()
	if b.bitmap == nil {
		return newBm
	}

	// Optimize: Using iterator with skip
	iter := b.bitmap.Iterator()
	iter.AdvanceIfNeeded(uint32(offset))

	for iter.HasNext() {
		val := iter.Next()
		if val >= uint32(offset+length) {
			break
		}
		newBm.Set(int(val) - offset)
	}

	return newBm
}

// AtomicBitset implements a lock-free (for readers) bitset using Copy-On-Write logic.
// It is optimized for scenarios with frequent reads (Contains) and infrequent writes (Set/Delete).
type AtomicBitset struct {
	bitmap atomic.Pointer[roaring.Bitmap]
	// We use a mutex to serialize writers to avoid excessive CAS retries under high contention,
	// effectively making it Wait-Free for Readers and Blocking for Writers.
	// Pure CAS loop without mutex is possible but might livelock under heavy write pressure.
	// Given this is for Tombstones (deletes), write throughput is likely lower than read.
	writeMu sync.Mutex
}

func NewAtomicBitset() *AtomicBitset {
	ab := &AtomicBitset{}
	// Initialize with empty bitmap
	ab.bitmap.Store(roaring.New())
	return ab
}

func (b *AtomicBitset) Contains(i int) bool {
	bm := b.bitmap.Load()
	if bm == nil {
		return false
	}
	return bm.Contains(uint32(i))
}

func (b *AtomicBitset) Set(i int) {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()

	oldBm := b.bitmap.Load()
	var newBm *roaring.Bitmap
	if oldBm == nil {
		newBm = roaring.New()
	} else {
		newBm = oldBm.Clone()
	}
	newBm.Add(uint32(i))
	b.bitmap.Store(newBm)
}

func (b *AtomicBitset) Clear(i int) {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()

	oldBm := b.bitmap.Load()
	if oldBm == nil {
		return
	}
	if !oldBm.Contains(uint32(i)) {
		return
	}

	newBm := oldBm.Clone()
	newBm.Remove(uint32(i))
	b.bitmap.Store(newBm)
}

func (b *AtomicBitset) Count() uint64 {
	bm := b.bitmap.Load()
	if bm == nil {
		return 0
	}
	return bm.GetCardinality()
}

func (b *AtomicBitset) ToUint32Array() []uint32 {
	bm := b.bitmap.Load()
	if bm == nil {
		return nil
	}
	return bm.ToArray()
}

func (b *AtomicBitset) Reset() {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	b.bitmap.Store(roaring.New())
}

func (b *AtomicBitset) Clone() *AtomicBitset {
	bm := b.bitmap.Load()
	newAb := NewAtomicBitset()
	if bm != nil {
		newAb.bitmap.Store(bm.Clone())
	}
	return newAb
}

func (b *AtomicBitset) Release() {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()

	_ = b.bitmap.Swap(nil)
	// We rely on GC to collect the old bitmap since we can't safely pool it
	// without knowing if other readers have a reference.
}
