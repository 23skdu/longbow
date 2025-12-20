package store

import (
	"sync"
	"sync/atomic"
	"time"

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
	Index      Index        // Abstract Interface
	dataMu     sync.RWMutex // Protects Records slice (append-only)
	Name       string
	Schema     *arrow.Schema

	// Tombstones map BatchIdx -> Bitset of deleted RowIdxs
	Tombstones map[int]*Bitset

	// Memory tracking
	SizeBytes atomic.Int64
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

// Bitset is a thread-safe simple bitmap implementation
type Bitset struct {
	data []uint64
	mu   sync.RWMutex
}

func NewBitset() *Bitset {
	return &Bitset{
		data: make([]uint64, 0),
	}
}

func (b *Bitset) Set(i int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	word, bit := i/64, i%64
	for word >= len(b.data) {
		b.data = append(b.data, 0)
	}
	b.data[word] |= 1 << bit
}

func (b *Bitset) Clear(i int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	word, bit := i/64, i%64
	if word < len(b.data) {
		b.data[word] &^= 1 << bit
	}
}

func (b *Bitset) Contains(i int) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	word, bit := i/64, i%64
	if word >= len(b.data) {
		return false
	}
	return (b.data[word] & (1 << bit)) != 0
}
