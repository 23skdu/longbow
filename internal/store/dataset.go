package store

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/rs/zerolog"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/pq"
	qry "github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/index"
	"github.com/23skdu/longbow/internal/store/types"
)

// HNSWSettings holds configuration parameters for the HNSW index construction.
type HNSWSettings struct {
	// M is the number of bi-directional links created for every new element during construction.
	M int
	// EfConstruction is the size of the dynamic list for the nearest neighbors (used during construction).
	EfConstruction int
}
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
	pvtMu               sync.RWMutex
	initMu              sync.Mutex
	PreferredVectorType types.VectorDataType
	turboQuantBits      int // Bits per dimension for TurboQuant encoding (4, 8)

	// Schema Evolution
	SchemaManager *SchemaEvolutionManager

	// Tombstones map BatchIdx -> Bitset of deleted RowIdxs
	Tombstones map[int]*types.Bitset

	// BatchNodes tracks which NUMA node each RecordBatch is allocated on
	BatchNodes *LockFreeSlice[int]

	// TotalIngestedRows is the running sum of NumRows over Records. It is
	// maintained by appendRecordLocked/replaceRecords and exists so that the
	// global row offset of a newly arriving batch - the base ID hybrid search
	// and text indexing derive from it - can be read without walking every
	// historical batch on every ingest.
	TotalIngestedRows atomic.Int64

	// PrimaryIndex maps ID -> Physical Location (O(1) lookup)
	PrimaryIndex        map[string]RowLocation
	NumericPrimaryIndex map[int64]RowLocation
	Uint64PrimaryIndex  map[uint64]RowLocation
	// metadataMu protects PrimaryIndex, LWW, and Merkle updates
	metadataMu sync.Mutex

	// Memory tracking
	SizeBytes        atomic.Int64
	IndexMemoryBytes atomic.Int64

	// Eviction state
	evicting       atomic.Bool // Marks dataset as being evicted
	isRequantizing atomic.Bool // Marks dataset as being re-quantized

	// In-flight Indexing Tracking (Compaction Safety)
	PendingIndexJobs        atomic.Int64
	PendingIngestion        atomic.Int64
	LastIngestionCompletion atomic.Int64 // Unix timestamp of last successful ingestion job completion (for stale-PendingIngestion watchdog)
	ActiveIngestStreams     atomic.Int64 // Number of active DoPut streams for this dataset
	IsReady                 atomic.Bool  // Set to true after first successful ingestion (v0.2.0)
	RegistryPublished       atomic.Bool  // Set to true when advertised to the cluster

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

	stringDictMu     sync.RWMutex
	StringDicts      map[string]*qry.StringDictionary
	BatchStringCodes map[string]map[int][]uint16
	// nonCategoricalCols records string columns whose distinct-value count
	// exceeded the dictionary's useful range; they are skipped from then on.
	nonCategoricalCols map[string]bool

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

// GetTombstones returns a copy of the tombstones for the dataset.
func (d *Dataset) GetTombstones() map[int]*types.Bitset {
	d.dataMu.RLock()
	defer d.dataMu.RUnlock()
	cp := make(map[int]*types.Bitset, len(d.Tombstones))
	for k, v := range d.Tombstones {
		cp[k] = v
	}
	return cp
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
		NumericPrimaryIndex: make(map[int64]RowLocation),
		Uint64PrimaryIndex:  make(map[uint64]RowLocation),
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
	ds.LastIngestionCompletion.Store(time.Now().Unix())
	ds.TemporalIndex.SetVectorIndexProvider(func() VectorIndex { return ds.Index })
	// Name the temporal cache's metrics after this dataset, so cache behaviour is
	// attributable rather than a single unlabeled global (roadmap R11a).
	ds.TemporalIndex.SetDatasetLabel(name)

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

// GetRowCount returns the total number of rows across all record batches in the dataset.
func (d *Dataset) GetRowCount() int64 {
	if d == nil || d.Records == nil {
		return 0
	}
	return d.TotalIngestedRows.Load()
}

// appendRecordLocked publishes a new record batch and returns its batch index.
//
// It is the single place that grows Dataset.Records / Dataset.BatchNodes, so
// the O(1) row counter cannot drift away from the batch list. Callers must
// hold d.dataMu (write) or otherwise guarantee that no other append is in
// flight; the counter itself is atomic so readers never observe a torn value.
//
// The append is amortised O(1): LockFreeSlice.Append grows geometrically and
// writes past the published length rather than reallocating the whole slice.
// The previous "make(len+1) + copy" shape cost O(N) per batch, which is
// O(N^2) over a dataset's lifetime and held the dataset lock for the duration.
func (d *Dataset) appendRecordLocked(rec arrow.RecordBatch, numaNode int) int {
	batchIdx := d.Records.Append(rec) - 1
	d.BatchNodes.Append(numaNode)
	if rec != nil {
		d.TotalIngestedRows.Add(rec.NumRows())
	}
	return batchIdx
}

// replaceRecords installs a wholly new batch list (compaction, eviction,
// restore) and recomputes the row counter from it.
//
// This is the O(N) counterpart to appendRecordLocked and is only correct
// because it runs under d.dataMu, which serialises it against appends.
func (d *Dataset) replaceRecords(records []arrow.RecordBatch) {
	d.Records.UpdateInPlace(records)

	var rows int64
	for _, r := range records {
		if r != nil {
			rows += r.NumRows()
		}
	}
	d.TotalIngestedRows.Store(rows)
}

// SetLastAccess updates the time of the last access to the dataset.
func (d *Dataset) SetLastAccess(t time.Time) {
	atomic.StoreInt64(&d.lastAccess, t.UnixNano())
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
	d.TotalIngestedRows.Store(0)

	d.PrimaryIndex = nil
	d.NumericPrimaryIndex = nil
	d.Uint64PrimaryIndex = nil
	d.recordEviction = nil
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

// GetPreferredVectorType returns the preferred vector data type in a thread-safe manner.
func (d *Dataset) GetPreferredVectorType() types.VectorDataType {
	if d == nil {
		return types.VectorTypeUnknown
	}
	d.pvtMu.RLock()
	defer d.pvtMu.RUnlock()
	return d.PreferredVectorType
}

// SetPreferredVectorType updates the preferred vector data type in a thread-safe manner.
func (d *Dataset) SetPreferredVectorType(t types.VectorDataType) {
	if d == nil {
		return
	}
	d.pvtMu.Lock()
	defer d.pvtMu.Unlock()
	d.PreferredVectorType = t
}
