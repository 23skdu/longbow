package store

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"
)

// IndexJob represents a job for the indexing worker
type IndexJob struct {
	DatasetName string
	Record      arrow.RecordBatch
	BatchIdx    int
	RowIdx      int
	CreatedAt   time.Time
}

// Dataset wraps records with metadata for eviction and tombstones
type Dataset struct {
	Records    []arrow.RecordBatch
	lastAccess int64 // UnixNano
	Version    int64
	Index      VectorIndex  // Use common interface (Item 3)
	dataMu     sync.RWMutex // Protects Records slice (append-only)
	Name       string
	Schema     *arrow.Schema

	// Tombstones map BatchIdx -> Bitset of deleted RowIdxs
	Tombstones map[int]*Bitset

	// Memory tracking
	SizeBytes atomic.Int64
}

// IsSharded returns true if the dataset uses ShardedHNSW.
func (d *Dataset) IsSharded() bool {
	d.dataMu.RLock()
	defer d.dataMu.RUnlock()
	_, ok := d.Index.(*ShardedHNSW)
	return ok
}

// IndexLen returns the number of vectors in the index.
func (d *Dataset) IndexLen() int {
	d.dataMu.RLock()
	defer d.dataMu.RUnlock()
	if d.Index != nil {
		return d.Index.Len()
	}
	return 0
}

func NewDataset(name string, schema *arrow.Schema) *Dataset {
	return &Dataset{
		Name:       name,
		Records:    make([]arrow.RecordBatch, 0),
		Schema:     schema,
		Tombstones: make(map[int]*Bitset),
	}
}

func (d *Dataset) LastAccess() time.Time {
	return time.Unix(0, atomic.LoadInt64(&d.lastAccess))
}

func (d *Dataset) SetLastAccess(t time.Time) {
	atomic.StoreInt64(&d.lastAccess, t.UnixNano())
}

// Bitset is a thread-safe wrapper around a Roaring Bitmap (Item 10)
type Bitset struct {
	bitmap *roaring.Bitmap
	mu     sync.RWMutex
}

func NewBitset() *Bitset {
	return &Bitset{
		bitmap: roaring.New(),
	}
}

func (b *Bitset) Set(i int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bitmap.Add(uint32(i))
}

func (b *Bitset) Clear(i int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bitmap.Remove(uint32(i))
}

func (b *Bitset) Contains(i int) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.bitmap.Contains(uint32(i))
}

// Clone creates a thread-safe copy of the bitset.
func (b *Bitset) Clone() *Bitset {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return &Bitset{
		bitmap: b.bitmap.Clone(),
	}
}

// Count returns the number of set bits.
func (b *Bitset) Count() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.bitmap.GetCardinality()
}
