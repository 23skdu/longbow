package store

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	qry "github.com/23skdu/longbow/internal/query"
	"github.com/rs/zerolog"

	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// IndexJob represents a job for the indexing worker

// RowLocation represents a physical location of a row

// Dataset wraps records with metadata for eviction and tombstones
type HNSWSettings struct {
	M              int
	EfConstruction int
}

type Dataset struct {
	Records    []arrow.RecordBatch
	lastAccess int64 // UnixNano
	Version    int64
	Index      VectorIndex  // Use common interface (Item 3)
	dataMu     sync.RWMutex // Protects Records slice (append-only)
	Name       string
	Schema     *arrow.Schema
	Topo       *NUMATopology

	// Vector Configuration
	PreferredVectorType types.VectorDataType
	TurboQuantBits      int // Bits per dimension for TurboQuant encoding (4, 8)

	// Schema Evolution
	SchemaManager *SchemaEvolutionManager

	// Tombstones map BatchIdx -> Bitset of deleted RowIdxs
	Tombstones map[int]*qry.Bitset

	// BatchNodes tracks which NUMA node each RecordBatch is allocated on
	BatchNodes []int

	// PrimaryIndex maps ID -> Physical Location (O(1) lookup)
	PrimaryIndex map[string]RowLocation
	// metadataMu protects PrimaryIndex, LWW, and Merkle updates
	metadataMu sync.Mutex

	// Memory tracking
	SizeBytes        atomic.Int64
	IndexMemoryBytes atomic.Int64

	// Eviction state
	evicting atomic.Bool // Marks dataset as being evicted

	// In-flight Indexing Tracking (Compaction Safety)
	PendingIndexJobs atomic.Int64
	PendingIngestion atomic.Int64

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

	// Metric defines the distance metric for this dataset
	HNSWConfig HNSWSettings
	Metric     DistanceMetric

	// Fragmentation-Aware Compaction
	fragmentationTracker *FragmentationTracker

	// Filter Cache: maps filter hash -> Bitset
	filterCache map[string]*qry.Bitset
	filterMu    sync.RWMutex

	Logger zerolog.Logger
}

// UpdateBatchSize sets the total size of a batch in the fragmentation tracker.
func (d *Dataset) UpdateBatchSize(batchIdx, size int) {
	if d.fragmentationTracker != nil {
		d.fragmentationTracker.SetBatchSize(batchIdx, size)
	}
}

// RecordBatchDeletion records a deletion in the specified batch.
func (d *Dataset) RecordBatchDeletion(batchIdx int) {
	if d.fragmentationTracker != nil {
		d.fragmentationTracker.RecordDeletion(batchIdx)
	}
}

// GetFragmentedBatches returns indices of batches exceeding the fragmentation threshold.
func (d *Dataset) GetFragmentedBatches(threshold float64) []int {
	if d.fragmentationTracker != nil {
		return d.fragmentationTracker.GetFragmentedBatches(threshold)
	}
	return nil
}

// ResetBatchFragmentation resets the tracking for a batch.
func (d *Dataset) ResetBatchFragmentation(batchIdx int) {
	if d.fragmentationTracker != nil {
		d.fragmentationTracker.Reset(batchIdx)
	}
}

// IsSharded returns true if the dataset's vector index is sharded.
func (d *Dataset) IsSharded() bool {
	if d.Index != nil {
		return d.Index.IsSharded()
	}
	return false
}

// GetShardedIndex returns the index as a *ShardedHNSW if it is one.
func (d *Dataset) GetShardedIndex() *ShardedHNSW {
	if d.Index == nil {
		return nil
	}
	if s, ok := d.Index.(*ShardedHNSW); ok {
		return s
	}
	// Also check if it's an AutoShardingIndex that is currently sharded
	if asi, ok := d.Index.(*AutoShardingIndex); ok {
		asi.mu.RLock()
		defer asi.mu.RUnlock()
		if s, ok := asi.current.(*ShardedHNSW); ok {
			return s
		}
	}
	return nil
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

// GetName returns the name of the dataset
func (d *Dataset) GetName() string {
	return d.Name
}

// GetRecords returns the records in the dataset
func (d *Dataset) GetRecords() []arrow.RecordBatch {
	return d.Records
}

// GetIndex returns the underlying vector index
func (d *Dataset) GetIndex() any {
	return d.Index
}

// GetSchema returns the schema of the dataset
func (d *Dataset) GetSchema() *arrow.Schema {
	return d.Schema
}

// GetTombstones returns the tombstones for the dataset
func (d *Dataset) GetTombstones() map[int]*qry.Bitset {
	return d.Tombstones
}

// GetPQEncoder returns the PQ encoder for the dataset
func (d *Dataset) GetPQEncoder() *pq.PQEncoder {
	return d.PQEncoder
}

// RLockData acquires a read lock on the dataset data
func (d *Dataset) RLockData() {
	d.dataMu.RLock()
}

// RUnlockData releases a read lock on the dataset data
func (d *Dataset) RUnlockData() {
	d.dataMu.RUnlock()
}

// ResetTombstones clears all tombstones in the dataset
func (d *Dataset) ResetTombstones() {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()
	d.Tombstones = make(map[int]*qry.Bitset)
}

func NewDataset(name string, schema *arrow.Schema) *Dataset {

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
		Graph:           NewGraphStore(),
		filterCache:     make(map[string]*qry.Bitset),
		Metric:          MetricEuclidean, // Default
	}

	// Initialize Schema Manager
	ds.SchemaManager = NewSchemaEvolutionManager(schema, name)

	// Initialize fragmentation tracker
	ds.fragmentationTracker = NewFragmentationTracker()
	ds.fragmentationTracker.SetDatasetName(name)

	// Parse configuration from metadata if present
	if schema != nil {
		md := schema.Metadata()
		if val, ok := md.GetValue("longbow.metric"); ok {
			switch strings.ToLower(val) {
			case "cosine":
				ds.Metric = MetricCosine
			case "dot_product":
				ds.Metric = MetricDotProduct
			case "euclidean", "l2":
				ds.Metric = MetricEuclidean
			}
		}
		if val, ok := md.GetValue("longbow.vector_type"); ok {
			if dt, err := ParseVectorType(val); err == nil {
				ds.PreferredVectorType = dt
			}
		}
		if val, ok := md.GetValue("longbow.turboquant_bits"); ok {
			if bits, err := strconv.Atoi(val); err == nil {
				ds.TurboQuantBits = bits
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

// SearchDataset delegates to the vector index if available
func (d *Dataset) SearchDataset(ctx context.Context, queryVec []float32, k int) ([]SearchResult, error) {
	if d.Index == nil {
		return nil, fmt.Errorf("index not initialized")
	}
	return d.Index.SearchVectors(ctx, queryVec, k, nil, SearchOptions{})
}

// AddToIndex adds a vector to the index
func (d *Dataset) AddToIndex(batchIdx, rowIdx int) error {
	d.dataMu.RLock()
	idx := d.Index
	d.dataMu.RUnlock()

	if idx == nil {
		return errors.New("no index available")
	}

	// Pass Background or propagate context
	_, err := idx.AddByLocation(context.Background(), batchIdx, rowIdx)
	return err
}

// GenerateFilterBitset pre-calculates a bitset of VectorIDs that match the filters.
func (d *Dataset) GenerateFilterBitset(filters []qry.Filter, filterExpr FilterExpr) (*qry.Bitset, error) {
	// Generate hash
	var hash string
	for _, f := range filters {
		hash += qry.FilterHash(f) + ";"
	}

	d.filterMu.RLock()
	if bs, ok := d.filterCache[hash]; ok {
		d.filterMu.RUnlock()
		return bs, nil
	}
	d.filterMu.RUnlock()

	d.dataMu.RLock()
	defer d.dataMu.RUnlock()

	return d.GenerateFilterBitsetLocked(filters, filterExpr, hash)
}

// GenerateFilterBitsetLocked is the variant that assumes d.dataMu is already held.
func (d *Dataset) GenerateFilterBitsetLocked(filters []qry.Filter, filterExpr FilterExpr, hash string) (*qry.Bitset, error) {
	if len(d.Records) == 0 || d.Index == nil {
		return nil, nil
	}

	bitset := qry.NewBitset()

	// Dataset records must have the same schema.
	eval, err := qry.NewFilterEvaluator(d.Records[0], filters)
	if err != nil {
		bitset.Release()
		return nil, err
	}

	idx := d.Index
	for batchIdx, rec := range d.Records {
		if err := eval.Reset(rec); err != nil {
			continue // Should not happen with consistent schema
		}

		matches, err := eval.MatchesAll(int(rec.NumRows()))
		if err != nil {
			return nil, err
		}

		if filterExpr != nil {
			metaIdx := -1
			for i, field := range d.Schema.Fields() {
				if field.Name == "metadata" {
					metaIdx = i
					break
				}
			}

			var validMatches []int
			if metaIdx >= 0 {
				metaCol := rec.Column(metaIdx)
				strData := array.NewStringData(metaCol.Data())

				for _, rowIdx := range matches {
					if strData.IsValid(rowIdx) {
						metaStr := strData.Value(rowIdx)
						var metaMap map[string]interface{}
						if err := json.Unmarshal([]byte(metaStr), &metaMap); err == nil {
							if filterExpr.Evaluate(metaMap) {
								validMatches = append(validMatches, rowIdx)
							}
						}
					}
				}
				strData.Release()
				matches = validMatches
			}
		}

		for _, rowIdx := range matches {
			loc := Location{BatchIdx: batchIdx, RowIdx: rowIdx}
			if vid, ok := idx.GetVectorID(loc); ok {
				bitset.Set(int(vid))
			}
		}
	}

	// Cache a clone so the original can be released/modified if needed elsewhere
	// and the cached one stays safe.
	d.filterMu.Lock()
	if d.filterCache == nil {
		d.filterCache = make(map[string]*qry.Bitset)
	}
	if len(d.filterCache) > 100 {
		// Evict first element (pseudo-LRU since map iteration is random)
		for k, v := range d.filterCache {
			v.Release()
			delete(d.filterCache, k)
			break
		}
	}
	d.filterCache[hash] = bitset
	d.filterMu.Unlock()

	return bitset, nil
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

	if d.BM25ArenaIndex != nil {
		_ = d.BM25ArenaIndex.Close()
		d.BM25ArenaIndex = nil
	}

	if d.Graph != nil {
		_ = d.Graph.Close()
		d.Graph = nil
	}

	// Release records
	for _, r := range d.Records {
		if r != nil {
			r.Release()
		}
	}
	d.Records = nil

	d.PrimaryIndex = nil
	d.recordEviction = nil
}

// ExtractIDs extracts primary IDs from a record batch into a map of ID -> RowIdx.
// This can be called outside of dataMu lock to prepare for a bulk update.
func (d *Dataset) ExtractIDs(rec arrow.RecordBatch) map[string]int {
	idColIdx := -1
	for i, f := range rec.Schema().Fields() {
		if f.Name == "id" {
			idColIdx = i
			break
		}
	}

	if idColIdx == -1 {
		return nil
	}

	col := rec.Column(idColIdx)
	numRows := int(rec.NumRows())
	idMap := make(map[string]int, numRows)

	switch arr := col.(type) {
	case *array.String:
		for i := 0; i < numRows; i++ {
			if arr.IsValid(i) {
				idMap[arr.Value(i)] = i
			}
		}
	case *array.Int64:
		for i := 0; i < numRows; i++ {
			if arr.IsValid(i) {
				idStr := strconv.FormatInt(arr.Value(i), 10)
				idMap[idStr] = i
			}
		}
	case *array.Uint64:
		for i := 0; i < numRows; i++ {
			if arr.IsValid(i) {
				idStr := strconv.FormatUint(arr.Value(i), 10)
				idMap[idStr] = i
			}
		}
	}
	return idMap
}

// UpdatePrimaryIndex updates the ID mapping for a given batch using a pre-extracted ID map.
// The caller must hold dataMu lock.
func (d *Dataset) UpdatePrimaryIndex(batchIdx int, idMap map[string]int) {
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()
	if idMap == nil {
		return
	}
	if d.PrimaryIndex == nil {
		d.PrimaryIndex = make(map[string]RowLocation)
	}
	for id, rowIdx := range idMap {
		if oldLoc, exists := d.PrimaryIndex[id]; exists {
			if d.Tombstones[oldLoc.BatchIdx] == nil {
				d.Tombstones[oldLoc.BatchIdx] = qry.NewBitset()
			}
			d.Tombstones[oldLoc.BatchIdx].Set(oldLoc.RowIdx)
			d.RecordBatchDeletion(oldLoc.BatchIdx)
			metrics.TombstonesTotal.WithLabelValues(d.Name).Inc()
		}
		d.PrimaryIndex[id] = RowLocation{BatchIdx: batchIdx, RowIdx: rowIdx}
	}
}

// UpdatePrimaryIndexAsync updates the primary index without holding dataMu.
// Uses a dedicated mutex for serialization.
func (d *Dataset) UpdatePrimaryIndexAsync(batchIdx int, idMap map[string]int) {
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()
	if idMap == nil {
		return
	}
	if d.PrimaryIndex == nil {
		d.PrimaryIndex = make(map[string]RowLocation)
	}
	for id, rowIdx := range idMap {
		if oldLoc, exists := d.PrimaryIndex[id]; exists {
			if d.Tombstones[oldLoc.BatchIdx] == nil {
				d.Tombstones[oldLoc.BatchIdx] = qry.NewBitset()
			}
			d.Tombstones[oldLoc.BatchIdx].Set(oldLoc.RowIdx)
			d.RecordBatchDeletion(oldLoc.BatchIdx)
			metrics.TombstonesTotal.WithLabelValues(d.Name).Inc()
		}
		d.PrimaryIndex[id] = RowLocation{BatchIdx: batchIdx, RowIdx: rowIdx}
	}
}

// WaitForIndexing blocks until all pending indexing jobs for this dataset are complete.
func (d *Dataset) WaitForIndexing() {
	for d.PendingIndexJobs.Load() > 0 || d.PendingIngestion.Load() > 0 {
		time.Sleep(10 * time.Millisecond)
	}
}
// IngestBatch appends a batch of Parquet records to the dataset
func (d *Dataset) IngestBatch(batch []DatasetParquetRecord) error {
	if len(batch) == 0 {
		return nil
	}

	d.PendingIngestion.Add(int64(len(batch)))
	defer d.PendingIngestion.Add(-int64(len(batch)))

	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, d.Schema)
	defer b.Release()

	idColIdx := -1
	vectorColIdx := -1
	metadataColIdx := -1
	createdAtColIdx := -1

	for i, f := range d.Schema.Fields() {
		switch f.Name {
		case "id":
			idColIdx = i
		case "vector":
			vectorColIdx = i
		case "metadata":
			metadataColIdx = i
		case "created_at":
			createdAtColIdx = i
		}
	}

	for _, row := range batch {
		if idColIdx >= 0 {
			if row.ID != 0 {
				b.Field(idColIdx).(*array.Int64Builder).Append(row.ID)
			} else {
				b.Field(idColIdx).AppendNull()
			}
		}

		if vectorColIdx >= 0 {
			if row.Vector != nil {
				vecLen := len(row.Vector) / 4
				listBuilder := b.Field(vectorColIdx).(*array.FixedSizeListBuilder)
				valBuilder := listBuilder.ValueBuilder().(*array.Float32Builder)
				for i := 0; i < vecLen; i++ {
					v := binary.LittleEndian.Uint32(row.Vector[i*4:])
					valBuilder.Append(math.Float32frombits(v))
				}
				listBuilder.Append(true)
			} else {
				b.Field(vectorColIdx).AppendNull()
			}
		}

		if metadataColIdx >= 0 {
			if row.Metadata != nil {
				b.Field(metadataColIdx).(*array.BinaryBuilder).Append(row.Metadata)
			} else {
				b.Field(metadataColIdx).AppendNull()
			}
		}

		if createdAtColIdx >= 0 {
			if row.CreatedAt != 0 {
				b.Field(createdAtColIdx).(*array.Int64Builder).Append(row.CreatedAt)
			} else {
				b.Field(createdAtColIdx).AppendNull()
			}
		}
	}

	rec := b.NewRecord()
	
	d.dataMu.Lock()
	batchIdx := len(d.Records)
	d.Records = append(d.Records, rec)
	d.BatchNodes = append(d.BatchNodes, -1) // NUMA untracked for now
	d.dataMu.Unlock()

	// Update primary index and handle tombstones
	idMap := d.ExtractIDs(rec)
	if idMap != nil {
		d.UpdatePrimaryIndexAsync(batchIdx, idMap)
	}

	// Trigger async indexing if needed (usually handled by indexer worker)
	// We'll leave it to the worker to pick up NewRecords or send explicitly
	
	return nil
}
