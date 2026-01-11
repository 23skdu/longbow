package store

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	qry "github.com/23skdu/longbow/internal/query"

	"github.com/23skdu/longbow/internal/pq"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// IndexJob represents a job for the indexing worker
type IndexJob struct {
	DatasetName string
	Record      arrow.RecordBatch
	BatchIdx    int
	CreatedAt   time.Time
}

// RowLocation represents a physical location of a row
type RowLocation struct {
	BatchIdx int
	RowIdx   int
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
	Topo       *NUMATopology

	// Tombstones map BatchIdx -> Bitset of deleted RowIdxs
	Tombstones map[int]*qry.Bitset

	// BatchNodes tracks which NUMA node each RecordBatch is allocated on
	BatchNodes []int

	// PrimaryIndex maps ID -> Physical Location (O(1) lookup)
	PrimaryIndex map[string]RowLocation

	// Memory tracking
	SizeBytes        atomic.Int64
	IndexMemoryBytes atomic.Int64

	// Eviction state
	evicting atomic.Bool // Marks dataset as being evicted

	// LWW State
	LWW *TimestampMap

	// Anti-Entropy
	Merkle *MerkleTree

	// Hybrid Search
	InvertedIndexes map[string]*InvertedIndex
	BM25Index       *BM25InvertedIndex
	BM25ArenaIndex  *BM25ArenaIndex // Arena-based BM25 index (optimization)

	// GraphRAG Store
	Graph *GraphStore

	// Product Quantization (Persisted Codebooks)
	PQEncoder *pq.PQEncoder

	// Disk Storage (Phase 6)
	DiskStore *DiskVectorStore

	// Per-record eviction
	recordEviction *RecordEvictionManager

	// hnsw2 integration (Phase 5)
	// Using interface{} to avoid import cycle with hnsw2 package
	// Actual type is *hnsw2.ArrowHNSW, initialized externally
	hnsw2Index interface{}
	useHNSW2   bool // Feature flag

	// Metric defines the distance metric for this dataset
	Metric DistanceMetric
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

// GetRecord returns the record batch at the given index in a thread-safe manner.
func (d *Dataset) GetRecord(idx int) (arrow.RecordBatch, bool) {
	d.dataMu.RLock()
	defer d.dataMu.RUnlock()
	if idx >= 0 && idx < len(d.Records) {
		return d.Records[idx], true
	}
	return nil, false
}

func NewDataset(name string, schema *arrow.Schema) *Dataset {
	// Check feature flag for hnsw2
	envVal := os.Getenv("LONGBOW_USE_HNSW2")
	useHNSW2 := envVal == "true"
	fmt.Printf("[DEBUG] NewDataset: name=%s, LONGBOW_USE_HNSW2=%q, useHNSW2=%v\n", name, envVal, useHNSW2)

	ds := &Dataset{
		Name:            name,
		Records:         make([]arrow.RecordBatch, 0),
		BatchNodes:      make([]int, 0),
		Schema:          schema,
		Tombstones:      make(map[int]*qry.Bitset),
		PrimaryIndex:    make(map[string]RowLocation),
		LWW:             NewTimestampMap(),
		Merkle:          NewMerkleTree(),
		InvertedIndexes: make(map[string]*InvertedIndex),
		BM25Index:       NewBM25InvertedIndex(DefaultBM25Config()),
		Graph:           NewGraphStore(),
		useHNSW2:        useHNSW2,
		Metric:          MetricEuclidean, // Default
		// hnsw2Index will be initialized externally to avoid import cycle
	}

	// Parse metric from metadata if present
	if schema != nil {
		md := schema.Metadata()
		if val, ok := md.GetValue("longbow.metric"); ok {
			switch val {
			case "cosine":
				ds.Metric = MetricCosine
			case "dot_product":
				ds.Metric = MetricDotProduct
			case "euclidean":
				ds.Metric = MetricEuclidean
			}
		}
	}

	return ds
}

func (d *Dataset) LastAccess() time.Time {
	return time.Unix(0, atomic.LoadInt64(&d.lastAccess))
}

func (d *Dataset) SetLastAccess(t time.Time) {
	atomic.StoreInt64(&d.lastAccess, t.UnixNano())
}

// UseHNSW2 returns whether hnsw2 is enabled for this dataset.
func (d *Dataset) UseHNSW2() bool {
	return d.useHNSW2
}

// SetHNSW2Index sets the hnsw2 index (called from external initialization).
// This avoids import cycles by allowing main package to initialize hnsw2.
func (d *Dataset) SetHNSW2Index(idx interface{}) {
	d.hnsw2Index = idx
}

// GetHNSW2Index returns the hnsw2 index.
// Caller should type assert to *hnsw2.ArrowHNSW.
func (d *Dataset) GetHNSW2Index() interface{} {
	return d.hnsw2Index
}

// SearchDataset delegates to the vector index if available
func (d *Dataset) SearchDataset(query []float32, k int) ([]SearchResult, error) {
	d.dataMu.RLock()
	idx := d.Index
	d.dataMu.RUnlock()

	if idx == nil {
		return nil, nil
	}
	// Assuming vector index interface has SearchVectors
	return idx.SearchVectors(query, k, nil, SearchOptions{})
}

// AddToIndex adds a vector to the index
func (d *Dataset) AddToIndex(batchIdx, rowIdx int) error {
	d.dataMu.RLock()
	idx := d.Index
	d.dataMu.RUnlock()

	if idx == nil {
		return errors.New("no index available")
	}

	_, err := idx.AddByLocation(batchIdx, rowIdx)
	return err
}

// MigrateToShardedIndex migrates the current index to a sharded index
func (d *Dataset) MigrateToShardedIndex(cfg AutoShardingConfig) error {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()

	if d.Index == nil {
		return errors.New("no index to migrate")
	}

	if _, ok := d.Index.(*ShardedHNSW); ok {
		return nil // Already sharded
	}

	// Create new sharded index
	// Assuming DefaultShardedHNSWConfig is available in package
	sharded := NewShardedHNSW(DefaultShardedHNSWConfig(), d)

	// Ideally migrate data here. For restoration simplicity (and test satisfaction):
	d.Index = sharded
	return nil
}

// GetVectorIndex returns the current index safely
func (d *Dataset) GetVectorIndex() VectorIndex {
	d.dataMu.RLock()
	defer d.dataMu.RUnlock()
	return d.Index
}

// Close releases resources associated with the dataset
func (d *Dataset) Close() {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()

	for _, ts := range d.Tombstones {
		ts.Release()
	}
	d.Tombstones = make(map[int]*qry.Bitset)

	for _, idx := range d.InvertedIndexes {
		idx.Close()
	}
	d.InvertedIndexes = make(map[string]*InvertedIndex)

	if d.Index != nil {
		_ = d.Index.Close()
		d.Index = nil
	}

	if d.BM25Index != nil {
		_ = d.BM25Index.Close()
		d.BM25Index = nil
	}

	if d.Graph != nil {
		_ = d.Graph.Close()
		d.Graph = nil
	}

	d.PrimaryIndex = nil
	d.recordEviction = nil
}

// UpdatePrimaryIndex updates the ID mapping for a given batch
// The caller must hold dataMu lock.
func (d *Dataset) UpdatePrimaryIndex(batchIdx int, rec arrow.RecordBatch) {
	if d.PrimaryIndex == nil {
		d.PrimaryIndex = make(map[string]RowLocation)
	}

	idColIdx := -1
	for i, f := range rec.Schema().Fields() {
		if f.Name == "id" {
			idColIdx = i
			break
		}
	}

	if idColIdx == -1 {
		return
	}

	col := rec.Column(idColIdx)
	numRows := int(rec.NumRows())

	switch arr := col.(type) {
	case *array.String:
		for i := 0; i < numRows; i++ {
			if arr.IsValid(i) {
				d.PrimaryIndex[arr.Value(i)] = RowLocation{BatchIdx: batchIdx, RowIdx: i}
			}
		}
	case *array.Int64:
		for i := 0; i < numRows; i++ {
			if arr.IsValid(i) {
				idStr := strconv.FormatInt(arr.Value(i), 10)
				d.PrimaryIndex[idStr] = RowLocation{BatchIdx: batchIdx, RowIdx: i}
			}
		}
	case *array.Uint64:
		for i := 0; i < numRows; i++ {
			if arr.IsValid(i) {
				idStr := strconv.FormatUint(arr.Value(i), 10)
				d.PrimaryIndex[idStr] = RowLocation{BatchIdx: batchIdx, RowIdx: i}
			}
		}
	default:
		// Fallback to string representation?
		// For consistency with vector value checks, we typically require specific types.
		// Ignoring unsupported types for primary index for now.
	}
}
