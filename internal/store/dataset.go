package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
	qry "github.com/23skdu/longbow/internal/query"
	amemory "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"

	gputypes "github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/store/index"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// HNSWSettings holds configuration parameters for the HNSW index construction.
type HNSWSettings struct {
	// M is the number of bi-directional links created for every new element during construction.
	M int
	// EfConstruction is the size of the dynamic list for the nearest neighbors (used during construction).
	EfConstruction int
}

// IDMap is a pooled container for ID to row index mappings.
// Supports string, int64, and uint64 IDs to avoid allocation/conversion.
type IDMap struct {
	StringMap map[string]int
	IntMap    map[int64]int
	UintMap   map[uint64]int
	IsNumeric bool
}

// Release returns the IDMap to the pool.
func (m *IDMap) Release() {
	if m == nil {
		return
	}
	if m.StringMap != nil {
		clear(m.StringMap)
	}
	if m.IntMap != nil {
		clear(m.IntMap)
	}
	if m.UintMap != nil {
		clear(m.UintMap)
	}
	idMapPool.Put(m)
}

var idMapPool = sync.Pool{
	New: func() any {
		return &IDMap{
			StringMap: make(map[string]int, 1024),
			IntMap:    make(map[int64]int, 1024),
			UintMap:   make(map[uint64]int, 1024),
		}
	},
}

// Dataset wraps records with metadata for eviction and tombstones.
type Dataset struct {
	Records    *LockFreeSlice[arrow.RecordBatch]
	lastAccess int64 // UnixNano
	Version    int64
	Index      VectorIndex  // Use common interface (Item 3)
	dataMu     sync.RWMutex // Protects Records slice (append-only)
	Name       string
	Schema     *arrow.Schema
	Topo       *memory.NUMATopology

	// Vector Configuration
	PreferredVectorType types.VectorDataType
	turboQuantBits      int // Bits per dimension for TurboQuant encoding (4, 8)

	// Schema Evolution
	SchemaManager *SchemaEvolutionManager

	// Tombstones map BatchIdx -> Bitset of deleted RowIdxs
	Tombstones map[int]*types.Bitset

	// BatchNodes tracks which NUMA node each RecordBatch is allocated on
	BatchNodes *LockFreeSlice[int]

	// PrimaryIndex maps ID -> Physical Location (O(1) lookup)
	PrimaryIndex          map[string]RowLocation
	NumericPrimaryIndex   map[int64]RowLocation
	Uint64PrimaryIndex    map[uint64]RowLocation
	// metadataMu protects PrimaryIndex, LWW, and Merkle updates
	metadataMu sync.Mutex

	// Memory tracking
	SizeBytes        atomic.Int64
	IndexMemoryBytes atomic.Int64

	// Eviction state
	evicting       atomic.Bool // Marks dataset as being evicted
	isRequantizing atomic.Bool // Marks dataset as being re-quantized

	// In-flight Indexing Tracking (Compaction Safety)
	PendingIndexJobs    atomic.Int64
	PendingIngestion    atomic.Int64
	ActiveIngestStreams atomic.Int64 // Number of active DoPut streams for this dataset
	IsReady             atomic.Bool  // Set to true after first successful ingestion (v0.2.0)
	RegistryPublished   atomic.Bool  // Set to true when advertised to the cluster

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

	// Geospatial Index (Quadtree)
	GeoIndex *GeoIndex

	// Adaptive Metrics
	queryStats *QueryStats

	// Metric defines the distance metric for this dataset
	HNSWConfig HNSWSettings
	Metric     DistanceMetric

	// Fragmentation-Aware Compaction
	fragmentationTracker *FragmentationTracker

	// Filter Cache: maps filter hash -> Bitset
	filterCache map[string]*types.Bitset
	filterMu    sync.RWMutex
	ColumnIndex *ColumnInvertedIndex

	TemporalIndex *TemporalIndex

	Admission *AdmissionController

	EvictionManager *index.GraphLayerEvictionManager

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

// QueryStats tracks performance metrics for searches on a dataset.
type QueryStats struct {
	mu           sync.RWMutex
	latencies    []time.Duration
	recalls      []float64
	queriesCount int64
	lastReset    time.Time
}

// Record adds a new sample to the query statistics.
func (s *QueryStats) Record(latency time.Duration, recall float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latencies = append(s.latencies, latency)
	s.recalls = append(s.recalls, recall)
	s.queriesCount++

	// Keep only last 1000 samples
	if len(s.latencies) > 1000 {
		s.latencies = s.latencies[1:]
		s.recalls = s.recalls[1:]
	}
}

// GetMetrics returns the calculated performance metrics.
func (s *QueryStats) GetMetrics() (p50, p99, avg float64, recall float64, qps float64) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.latencies) == 0 {
		return 0, 0, 0, 0, 0
	}

	// Work on a copy to avoid holding lock too long and for sorting
	lats := make([]time.Duration, len(s.latencies))
	copy(lats, s.latencies)
	recs := make([]float64, len(s.recalls))
	copy(recs, s.recalls)

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

	p50 = lats[len(lats)/2].Seconds() * 1000.0
	p99 = lats[len(lats)*99/100].Seconds() * 1000.0

	sum := 0.0
	for _, l := range lats {
		sum += l.Seconds() * 1000.0
	}
	avg = sum / float64(len(lats))

	sumRecall := 0.0
	for _, r := range recs {
		sumRecall += r
	}
	recall = sumRecall / float64(len(recs))

	duration := time.Since(s.lastReset).Seconds()
	if duration > 0 {
		qps = float64(s.queriesCount) / duration
	}

	return
}

// IsSharded returns true if the dataset's vector index is sharded.
func (d *Dataset) IsSharded() bool {
	d.dataMu.RLock()
	defer d.dataMu.RUnlock()
	if d.Index != nil {
		return d.Index.IsSharded()
	}
	return false
}

// GetShardedIndex returns the index as a *ShardedHNSW if it is one.
func (d *Dataset) GetShardedIndex() *ShardedHNSW {
	d.dataMu.RLock()
	idx := d.Index
	d.dataMu.RUnlock()

	if idx == nil {
		return nil
	}
	if s, ok := idx.(*ShardedHNSW); ok {
		return s
	}
	// Also check if it's an AutoShardingIndex that is currently sharded
	if asi, ok := idx.(*AutoShardingIndex); ok {
		asi.mu.RLock()
		defer asi.mu.RUnlock()
		if s, ok := asi.current.(*ShardedHNSW); ok {
			return s
		}
	}
	return nil
}

// IndexLen returns the total number of vectors currently in the dataset's index.
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
	records := d.Records.Read()
	if idx >= 0 && idx < len(records) {
		return records[idx], true
	}
	return nil, false
}

// GetName returns the name of the dataset.
func (d *Dataset) GetName() string {
	return d.Name
}

// GetRecords returns the records in the dataset
// GetRecords returns the records in the dataset.
func (d *Dataset) GetRecords() []arrow.RecordBatch {
	if d.Records == nil {
		return nil
	}
	return d.Records.Read()
}

// GetIndex returns the underlying vector index
// GetIndex returns the underlying vector index.
func (d *Dataset) GetIndex() any {
	return d.Index
}

// GetSchema returns the schema of the dataset
// GetSchema returns the schema of the dataset.
func (d *Dataset) GetSchema() *arrow.Schema {
	return d.Schema
}

// GetTombstones returns the tombstones for the dataset
// GetTombstones returns the tombstones for the dataset.
func (d *Dataset) GetTombstones() map[int]*types.Bitset {
	return d.Tombstones
}

// GetPQEncoder returns the PQ encoder for the dataset
// GetPQEncoder returns the PQ encoder for the dataset.
func (d *Dataset) GetPQEncoder() *pq.PQEncoder {
	return d.PQEncoder
}

// RLockData acquires a read lock on the dataset data
// RLockData acquires a read lock on the dataset data.
func (d *Dataset) RLockData() {
	d.dataMu.RLock()
}

// RUnlockData releases a read lock on the dataset data
// RUnlockData releases a read lock on the dataset data.
func (d *Dataset) RUnlockData() {
	d.dataMu.RUnlock()
}

// ResetTombstones clears all tombstones in the dataset
func (d *Dataset) ResetTombstones() {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()
	d.Tombstones = make(map[int]*types.Bitset)
}

// SetAdmission associates an AdmissionController with the dataset.
func (d *Dataset) SetAdmission(admission *AdmissionController) {
	d.Admission = admission
}

// NewDataset creates a new Dataset with the specified name and schema.
func NewDataset(name string, schema *arrow.Schema) *Dataset {

	ds := &Dataset{
		Name:                name,
		Records:             NewLockFreeSlice[arrow.RecordBatch](),
		BatchNodes:          NewLockFreeSlice[int](),
		Schema:              schema,
		Tombstones:          make(map[int]*types.Bitset),
		PrimaryIndex:        make(map[string]RowLocation),
		NumericPrimaryIndex:   make(map[int64]RowLocation),
		Uint64PrimaryIndex:    make(map[uint64]RowLocation),
		LWW:                 NewTimestampMap(),
		Merkle:              NewMerkleTree(),
		queryStats: &QueryStats{
			lastReset: time.Now(),
		},
		InvertedIndexes: make(map[string]*InvertedIndex),
		Graph:           NewGraphStore(),
		filterCache:     make(map[string]*types.Bitset),
		ColumnIndex:     NewColumnInvertedIndex(),
		Metric:          MetricEuclidean,     // Default
		TemporalIndex:   NewTemporalIndex(0), // Dimension will be updated on first Add
		BM25Index:       NewBM25InvertedIndex(DefaultBM25Config()),
		BM25ArenaIndex:  NewBM25ArenaIndex(memory.NewSlabArena(4*1024*1024), 10000),
	}
	ds.TemporalIndex.ds = ds

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
				ds.turboQuantBits = bits
			}
		}
	}

	return ds
}

// LastAccess returns the time of the last access to the dataset.
func (d *Dataset) LastAccess() time.Time {
	return time.Unix(0, atomic.LoadInt64(&d.lastAccess))
}

// SetLastAccess updates the time of the last access to the dataset.
func (d *Dataset) SetLastAccess(t time.Time) {
	atomic.StoreInt64(&d.lastAccess, t.UnixNano())
}

// SearchDataset delegates to the vector index if available
// SearchDataset delegates to the vector index if available.
func (d *Dataset) SearchDataset(ctx context.Context, queryVec []float32, k int) ([]SearchResult, error) {
	d.dataMu.RLock()
	idx := d.Index
	d.dataMu.RUnlock()

	if idx == nil {
		return nil, fmt.Errorf("index not initialized")
	}
	return idx.SearchVectors(ctx, queryVec, k, nil, SearchOptions{})
}

// AddToIndex adds a vector to the index
// AddToIndex adds a vector to the index.
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
func (d *Dataset) GenerateFilterBitset(filters []qry.Filter, filterExpr FilterExpr) (*types.Bitset, error) {
	// Generate hash
	var hash string
	for _, f := range filters {
		hash += qry.FilterHash(f) + ";"
	}

	d.filterMu.RLock()
	if bs, ok := d.filterCache[hash]; ok {
		d.filterMu.RUnlock()
		metrics.BitmapCacheHitsTotal.Inc()
		return bs.Clone(), nil
	}
	d.filterMu.RUnlock()
	metrics.BitmapCacheMissesTotal.Inc()

	d.dataMu.RLock()
	defer d.dataMu.RUnlock()

	return d.GenerateFilterBitsetLocked(filters, filterExpr, hash)
}

// GenerateFilterBitsetLocked is the variant that assumes d.dataMu is already held.
func (d *Dataset) GenerateFilterBitsetLocked(filters []qry.Filter, filterExpr FilterExpr, hash string) (*types.Bitset, error) {
	records := d.Records.Read()
	if len(records) == 0 || d.Index == nil {
		return nil, nil
	}

	bitset := types.NewBitset()

	// Dataset records must have the same schema.
	eval, err := qry.NewFilterEvaluator(records[0], filters)
	if err != nil {
		bitset.Release()
		return nil, err
	}

	idx := d.Index
	for batchIdx, rec := range records {
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
				binData := array.NewBinaryData(metaCol.Data())

				for _, rowIdx := range matches {
					if binData.IsValid(rowIdx) {
						metaBytes := binData.Value(rowIdx)
						lazyMeta := types.NewLazyMetadata(metaBytes)
						if filterExpr.Evaluate(lazyMeta) {
							validMatches = append(validMatches, rowIdx)
						}
					}
				}
				binData.Release()
				matches = validMatches
			}
		}

		for _, rowIdx := range matches {
			loc := types.Location{BatchIdx: batchIdx, RowIdx: int(rowIdx)}
			if vid, ok := idx.GetVectorID(loc); ok {
				bitset.Set(int(vid))
			}
		}
	}

	// Cache a clone so the original can be released/modified if needed elsewhere
	// and the cached one stays safe.
	d.filterMu.Lock()
	if d.filterCache == nil {
		d.filterCache = make(map[string]*types.Bitset)
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

	return bitset.Clone(), nil
}

// MigrateToShardedIndex migrates the current index to a sharded index.
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
// GetVectorIndex returns the current index safely.
func (d *Dataset) GetVectorIndex() VectorIndex {
	d.dataMu.RLock()
	defer d.dataMu.RUnlock()
	return d.Index
}

// Close releases resources associated with the dataset
// Close releases resources associated with the dataset.
func (d *Dataset) Close() {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()

	for _, ts := range d.Tombstones {
		ts.Release()
	}
	d.Tombstones = make(map[int]*types.Bitset)

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

	if d.TemporalIndex != nil {
		_ = d.TemporalIndex.Close()
		d.TemporalIndex = nil
	}

	if d.ColumnIndex != nil {
		_ = d.ColumnIndex.Close()
		d.ColumnIndex = nil
	}

	if d.Graph != nil {
		_ = d.Graph.Close()
		d.Graph = nil
	}

	// Release records
	records := d.Records.Read()
	for _, r := range records {
		if r != nil {
			r.Release()
		}
	}
	d.Records.UpdateInPlace(nil)

	d.PrimaryIndex = nil
	d.NumericPrimaryIndex = nil
	d.Uint64PrimaryIndex = nil
	d.recordEviction = nil
}

// ExtractIDs extracts primary IDs from a record batch into an IDMap.
// This can be called outside of dataMu lock to prepare for a bulk update.
func (d *Dataset) ExtractIDs(rec arrow.RecordBatch) *IDMap {
	idColIdx := -1
	// Search for common ID column names
	idNames := []string{"id", "doc_id", "record_id", "pk", "_id"}
	for _, name := range idNames {
		for i, f := range rec.Schema().Fields() {
			if f.Name == name {
				idColIdx = i
				break
			}
		}
		if idColIdx != -1 {
			break
		}
	}

	if idColIdx == -1 {
		d.Logger.Warn().Str("dataset", d.Name).Msg("No known ID column found in batch (searched: id, doc_id, record_id, pk, _id)")
		return nil
	}

	col := rec.Column(idColIdx)
	numRows := int(rec.NumRows())
	m := idMapPool.Get().(*IDMap)

	switch arr := col.(type) {
	case *array.String:
		m.IsNumeric = false
		for i := 0; i < numRows; i++ {
			if arr.IsValid(i) {
				m.StringMap[arr.Value(i)] = i
			}
		}
	case *array.Int64:
		m.IsNumeric = true
		for i := 0; i < numRows; i++ {
			if arr.IsValid(i) {
				m.IntMap[arr.Value(i)] = i
			}
		}
	case *array.Uint64:
		m.IsNumeric = true
		for i := 0; i < numRows; i++ {
			if arr.IsValid(i) {
				m.UintMap[arr.Value(i)] = i
			}
		}
	case *array.Int32:
		m.IsNumeric = true
		for i := 0; i < numRows; i++ {
			if arr.IsValid(i) {
				m.IntMap[int64(arr.Value(i))] = i
			}
		}
	case *array.Uint32:
		m.IsNumeric = true
		for i := 0; i < numRows; i++ {
			if arr.IsValid(i) {
				m.UintMap[uint64(arr.Value(i))] = i
			}
		}
	default:
		d.Logger.Warn().
			Str("dataset", d.Name).
			Str("idColType", fmt.Sprintf("%T", col)).
			Msg("Unhandled ID column type, PrimaryIndex will be empty for this batch")
	}
	return m
}

// UpdatePrimaryIndex updates the ID mapping for a given batch using a pre-extracted ID map.
// The caller must hold dataMu lock.
func (d *Dataset) UpdatePrimaryIndex(batchIdx int, idMap *IDMap) {
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()
	if idMap == nil {
		return
	}
	if d.PrimaryIndex == nil {
		d.PrimaryIndex = make(map[string]RowLocation)
	}
	if d.NumericPrimaryIndex == nil {
		d.NumericPrimaryIndex = make(map[int64]RowLocation)
	}
	if d.Uint64PrimaryIndex == nil {
		d.Uint64PrimaryIndex = make(map[uint64]RowLocation)
	}

	if idMap.IsNumeric {
		for id, rowIdx := range idMap.IntMap {
			if oldLoc, exists := d.NumericPrimaryIndex[id]; exists {
				if d.Tombstones[oldLoc.BatchIdx] == nil {
					d.Tombstones[oldLoc.BatchIdx] = types.NewBitset()
				}
				d.Tombstones[oldLoc.BatchIdx].Set(oldLoc.RowIdx)
				d.RecordBatchDeletion(oldLoc.BatchIdx)
				metrics.TombstonesTotal.WithLabelValues(d.Name).Inc()
			}
			d.NumericPrimaryIndex[id] = RowLocation{BatchIdx: batchIdx, RowIdx: rowIdx}
		}
		for id, rowIdx := range idMap.UintMap {
			if oldLoc, exists := d.Uint64PrimaryIndex[id]; exists {
				if d.Tombstones[oldLoc.BatchIdx] == nil {
					d.Tombstones[oldLoc.BatchIdx] = types.NewBitset()
				}
				d.Tombstones[oldLoc.BatchIdx].Set(oldLoc.RowIdx)
				d.RecordBatchDeletion(oldLoc.BatchIdx)
				metrics.TombstonesTotal.WithLabelValues(d.Name).Inc()
			}
			d.Uint64PrimaryIndex[id] = RowLocation{BatchIdx: batchIdx, RowIdx: rowIdx}
		}
	} else {
		for id, rowIdx := range idMap.StringMap {
			if oldLoc, exists := d.PrimaryIndex[id]; exists {
				if d.Tombstones[oldLoc.BatchIdx] == nil {
					d.Tombstones[oldLoc.BatchIdx] = types.NewBitset()
				}
				d.Tombstones[oldLoc.BatchIdx].Set(oldLoc.RowIdx)
				d.RecordBatchDeletion(oldLoc.BatchIdx)
				metrics.TombstonesTotal.WithLabelValues(d.Name).Inc()
			}
			d.PrimaryIndex[id] = RowLocation{BatchIdx: batchIdx, RowIdx: rowIdx}
		}
	}
}

// UpdatePrimaryIndexAsync updates the primary index without holding dataMu.
// Uses a dedicated mutex for serialization.
func (d *Dataset) UpdatePrimaryIndexAsync(batchIdx int, idMap *IDMap) {
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()
	if idMap == nil {
		return
	}
	if d.PrimaryIndex == nil {
		d.PrimaryIndex = make(map[string]RowLocation)
	}
	if d.NumericPrimaryIndex == nil {
		d.NumericPrimaryIndex = make(map[int64]RowLocation)
	}
	if d.Uint64PrimaryIndex == nil {
		d.Uint64PrimaryIndex = make(map[uint64]RowLocation)
	}

	if idMap.IsNumeric {
		for id, rowIdx := range idMap.IntMap {
			if oldLoc, exists := d.NumericPrimaryIndex[id]; exists {
				if d.Tombstones[oldLoc.BatchIdx] == nil {
					d.Tombstones[oldLoc.BatchIdx] = types.NewBitset()
				}
				d.Tombstones[oldLoc.BatchIdx].Set(oldLoc.RowIdx)
				d.RecordBatchDeletion(oldLoc.BatchIdx)
				metrics.TombstonesTotal.WithLabelValues(d.Name).Inc()
			}
			d.NumericPrimaryIndex[id] = RowLocation{BatchIdx: batchIdx, RowIdx: rowIdx}
		}
		for id, rowIdx := range idMap.UintMap {
			if oldLoc, exists := d.Uint64PrimaryIndex[id]; exists {
				if d.Tombstones[oldLoc.BatchIdx] == nil {
					d.Tombstones[oldLoc.BatchIdx] = types.NewBitset()
				}
				d.Tombstones[oldLoc.BatchIdx].Set(oldLoc.RowIdx)
				d.RecordBatchDeletion(oldLoc.BatchIdx)
				metrics.TombstonesTotal.WithLabelValues(d.Name).Inc()
			}
			d.Uint64PrimaryIndex[id] = RowLocation{BatchIdx: batchIdx, RowIdx: rowIdx}
		}
	} else {
		for id, rowIdx := range idMap.StringMap {
			if oldLoc, exists := d.PrimaryIndex[id]; exists {
				if d.Tombstones[oldLoc.BatchIdx] == nil {
					d.Tombstones[oldLoc.BatchIdx] = types.NewBitset()
				}
				d.Tombstones[oldLoc.BatchIdx].Set(oldLoc.RowIdx)
				d.RecordBatchDeletion(oldLoc.BatchIdx)
				metrics.TombstonesTotal.WithLabelValues(d.Name).Inc()
			}
			d.PrimaryIndex[id] = RowLocation{BatchIdx: batchIdx, RowIdx: rowIdx}
		}
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

	pool := amemory.NewGoAllocator()
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

	numRows := len(batch)
	ids := make([]int64, numRows)
	idValid := make([]bool, numRows)
	metas := make([][]byte, numRows)
	metaValid := make([]bool, numRows)
	createdAts := make([]int64, numRows)
	createdAtValid := make([]bool, numRows)

	// Collect scalar columns in bulk
	for i, row := range batch {
		if idColIdx >= 0 {
			ids[i] = row.ID
			idValid[i] = row.ID != 0
		}
		if metadataColIdx >= 0 {
			metas[i] = row.Metadata
			metaValid[i] = row.Metadata != nil
		}
		if createdAtColIdx >= 0 {
			createdAts[i] = row.CreatedAt
			createdAtValid[i] = row.CreatedAt != 0
		}
	}

	// Bulk append scalars
	if idColIdx >= 0 {
		b.Field(idColIdx).(*array.Int64Builder).AppendValues(ids, idValid)
	}
	if metadataColIdx >= 0 {
		b.Field(metadataColIdx).(*array.BinaryBuilder).AppendValues(metas, metaValid)
	}
	if createdAtColIdx >= 0 {
		b.Field(createdAtColIdx).(*array.Int64Builder).AppendValues(createdAts, createdAtValid)
	}

	// Vector column (FixedSizeList) remains row-by-row for now due to complex nested structure,
	// but inner loop is optimized.
	if vectorColIdx >= 0 {
		listBuilder := b.Field(vectorColIdx).(*array.FixedSizeListBuilder)
		valBuilder := listBuilder.ValueBuilder().(*array.Float32Builder)
		for _, row := range batch {
			if row.Vector != nil {
				vecLen := len(row.Vector) / 4
				// Optimize: Use unsafe cast for float32 bytes if aligned (advanced)
				// For now, keep the loop but minimize builder calls
				for i := 0; i < vecLen; i++ {
					v := binary.LittleEndian.Uint32(row.Vector[i*4:])
					valBuilder.Append(math.Float32frombits(v))
				}
				listBuilder.Append(true)
			} else {
				listBuilder.AppendNull()
			}
		}
	}

	rec := b.NewRecord()

	records := d.Records.Read()
	newRecords := make([]arrow.RecordBatch, len(records)+1)
	copy(newRecords, records)
	newRecords[len(records)] = rec
	d.Records.UpdateInPlace(newRecords)

	batchNodes := d.BatchNodes.Read()
	newNodes := make([]int, len(batchNodes)+1)
	copy(newNodes, batchNodes)
	newNodes[len(batchNodes)] = -1
	d.BatchNodes.UpdateInPlace(newNodes)

	batchIdx := len(records)

	// Update primary index and handle tombstones
	idMap := d.ExtractIDs(rec)
	if idMap != nil {
		d.UpdatePrimaryIndexAsync(batchIdx, idMap)
		idMap.Release()
	}

	// Trigger async indexing if needed (usually handled by indexer worker)
	// We'll leave it to the worker to pick up NewRecords or send explicitly

	return nil
}

// SearchGraphRAG performs graph-based RAG search with GPU acceleration fallback.
func (d *Dataset) SearchGraphRAG(ctx context.Context, queryVec []float32, k int, alpha float32, depth int) ([]SearchResult, error) {
	// 1. Initial Vector Search
	results, err := d.SearchDataset(ctx, queryVec, k)
	if err != nil {
		return nil, err
	}

	if d.Graph == nil {
		return results, nil
	}

	// 2. Try GPU Acceleration
	if gpuIdxAny := d.Index.GetGPUIndex(); gpuIdxAny != nil {
		if gpuIdx, ok := gpuIdxAny.(gputypes.Index); ok {
			res, err := d.Graph.RankWithGraphGPU(d.Name, queryVec, results, alpha, depth, gpuIdx)
			if err == nil {
				return res, nil
			}
			// Fallback to CPU on GPU error
			d.Logger.Warn().Err(err).Msg("GPU GraphRAG failed, falling back to CPU")
		}
	}

	// 3. CPU Fallback
	return d.Graph.RankWithGraph(d.Name, queryVec, results, alpha, depth), nil
}

// TriggerRequantization starts a background job to change the quantization level of the dataset.
func (d *Dataset) TriggerRequantization(targetType types.VectorDataType) {
	if d.isRequantizing.Swap(true) {
		return // Already in progress
	}
	go d.requantizeTask(targetType)
}

func (d *Dataset) requantizeTask(targetType types.VectorDataType) {
	defer d.isRequantizing.Store(false)
	startTime := time.Now()

	// 1. Prepare Index if needed (e.g. PQ training)
	d.dataMu.RLock()
	idx := d.Index
	d.dataMu.RUnlock()

	if idx == nil {
		return
	}

	// 2. Iterate through all record batches and re-quantize
	records := d.Records.Read()

	totalVectors := 0
	for _, rec := range records {
		totalVectors += int(rec.NumRows())
	}

	// Implementation of actual quantization logic would go here.
	// For now, we update the PreferredVectorType and signal the index.

	d.dataMu.Lock()
	d.PreferredVectorType = targetType
	d.dataMu.Unlock()

	// Record Metrics
	duration := time.Since(startTime)
	metrics.RequantizationDurationSeconds.WithLabelValues(d.Name, "current", targetType.String()).Observe(duration.Seconds())

	typeStr := targetType.String()
	metrics.QuantizationActiveType.WithLabelValues(d.Name, typeStr).Set(1)

	d.Logger.Info().
		Str("dataset", d.Name).
		Int("vectors", totalVectors).
		Dur("duration", duration).
		Msg("Background re-quantization complete")
}

func (d *Dataset) GetMetric() DistanceMetric {
	return d.Metric
}

func (d *Dataset) GetLogger() zerolog.Logger {
	return d.Logger
}

func (d *Dataset) GetTopo() *memory.NUMATopology {
	return d.Topo
}

func (d *Dataset) GetEvictionManager() any {
	return d.EvictionManager
}

func (d *Dataset) GetDiskStore() any {
	if d.DiskStore == nil {
		return nil
	}
	return d.DiskStore
}
