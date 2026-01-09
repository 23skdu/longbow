package store

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"

	"github.com/23skdu/longbow/internal/mesh"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/storage"
)

// VectorStore implements flight.FlightServer with minimal logic
type VectorStore struct {
	flight.BaseFlightServer
	mem           memory.Allocator
	logger        zerolog.Logger
	maxMemory     atomic.Int64
	currentMemory atomic.Int64
	memoryConfig  MemoryConfig

	sequence atomic.Uint64 // Global operation sequence

	// Persistence
	dataPath      string
	engine        *storage.StorageEngine // Manages WAL and Snapshots
	snapshotReset chan time.Duration

	indexQueue *IndexJobQueue // Integrated HNSW

	// Lifecycle
	stopChan chan struct{}
	indexWg  sync.WaitGroup // For background workers
	// mu       sync.RWMutex   // DEPRECATED: Replaced by RCU
	datasets atomic.Pointer[map[string]*Dataset]

	// configMu protects configuration fields
	configMu sync.RWMutex

	// Mesh integration
	Mesh            *mesh.Gossip
	meshStatusCache *MeshStatusCache // Cache for mesh status serialization

	// NUMA integration (Phase 4/5)
	numaTopology *NUMATopology

	// Hybrid search (Phase 20)
	hybridSearchConfig HybridSearchConfig
	bm25Index          *BM25InvertedIndex

	// DoGet pipeline subsystem
	doGetPipelinePool *DoGetPipelinePool
	pipelineThreshold int

	// Column-based inverted index for O(1) equality filter lookups
	columnIndex    *ColumnInvertedIndex
	indexedColumns []string // columns to index for fast equality lookups

	// Compaction (Phase 11/14)
	compactionConfig CompactionConfig
	compactionWorker *CompactionWorker

	// Auto-sharding (Phase 13)
	autoShardingConfig AutoShardingConfig

	// Namespace management
	nsManager *namespaceManager

	// GPU acceleration (optional)

	// Shutdown and lifecycle (Phase 6/21)
	shutdownState int32
	workerWg      sync.WaitGroup

	// hnsw2 integration hook (Phase 5)
	// Called after dataset creation to initialize hnsw2 (avoids import cycle)
	datasetInitHook func(*Dataset)

	// Distributed search coordinator (shared between Data/Meta servers)
	coordinator *GlobalSearchCoordinator
}

func NewVectorStore(mem memory.Allocator, logger zerolog.Logger, maxMemoryBytes int64, _ int64, _ time.Duration) *VectorStore {
	memCfg := DefaultMemoryConfig()
	memCfg.MaxMemory = maxMemoryBytes

	s := &VectorStore{
		mem:          mem,
		logger:       logger,
		memoryConfig: memCfg,
		stopChan:     make(chan struct{}),
	}
	// Initialize empty datasets map
	emptyMap := make(map[string]*Dataset)
	s.datasets.Store(&emptyMap)

	s.maxMemory.Store(maxMemoryBytes)
	s.indexQueue = NewIndexJobQueue(DefaultIndexJobQueueConfig())

	s.nsManager = newNamespaceManager()
	s.columnIndex = NewColumnInvertedIndex()
	return s
}

// TrackMemory adds delta to current usage and logs if large
func (s *VectorStore) TrackMemory(delta int64) {
	if delta > 100*1024*1024 {
		s.logger.Warn().
			Int64("delta", delta).
			Int64("current", s.currentMemory.Load()).
			Str("stack", stackTrace()).
			Msg("Large memory addition detected")
	}
	s.currentMemory.Add(delta)
}

func stackTrace() string {
	buf := make([]byte, 1024)
	n := runtime.Stack(buf, false)
	return string(buf[:n])
}

// RCU Helpers

func (s *VectorStore) loadDatasets() map[string]*Dataset {
	return *s.datasets.Load()
}

func (s *VectorStore) getDataset(name string) (*Dataset, bool) {
	m := s.loadDatasets()
	ds, ok := m[name]
	return ds, ok
}

// updateDatasets executes a CAS loop to update the map.
// fn receives a COPY of the map to modify.
func (s *VectorStore) updateDatasets(fn func(map[string]*Dataset)) {
	for {
		oldPtr := s.datasets.Load()
		oldMap := *oldPtr

		newMap := make(map[string]*Dataset, len(oldMap)+1)
		for k, v := range oldMap {
			newMap[k] = v
		}

		fn(newMap)

		if s.datasets.CompareAndSwap(oldPtr, &newMap) {
			return
		}
		// Contention, retry
		metrics.DatasetUpdateRetriesTotal.Inc()

		runtime.Gosched()
	}
}

// IterateDatasets safely iterates over all datasets.
func (s *VectorStore) IterateDatasets(fn func(string, *Dataset)) {
	m := s.loadDatasets()
	for name, ds := range m {
		fn(name, ds)
	}
}

// getOrCreateDataset atomically gets an existing dataset or creates a new one using the provider.
// The provider is only called if creation is needed (lazy).
func (s *VectorStore) getOrCreateDataset(name string, createFn func() *Dataset) (*Dataset, bool) {
	// 1. Optimistic Read
	if ds, ok := s.getDataset(name); ok {
		return ds, false
	}

	// 2. CAS Loop
	var result *Dataset
	var created bool
	s.updateDatasets(func(m map[string]*Dataset) {
		// Double-check existence in the new copy
		if ds, ok := m[name]; ok {
			result = ds
			created = false
			return
		}

		// Create
		newDs := createFn()
		m[name] = newDs
		result = newDs
		created = true
	})

	return result, created
}

func (s *VectorStore) SetCoordinator(c *GlobalSearchCoordinator) {
	s.coordinator = c
}

func (s *VectorStore) SetMesh(m *mesh.Gossip) {
	s.Mesh = m
}

// SetDatasetInitHook sets a hook function called after dataset creation.
// This allows external initialization (e.g., hnsw2) without import cycles.
// The hook is called from main package which can import both store and hnsw2.
func (s *VectorStore) SetDatasetInitHook(hook func(*Dataset)) {
	s.datasetInitHook = hook
}

// SetIndexedColumns updates columns that should be indexed for fast equality lookups
func (s *VectorStore) SetIndexedColumns(cols []string) {
	s.indexedColumns = cols
}

// GetIndexedColumns returns columns currently being indexed
func (s *VectorStore) GetIndexedColumns() []string {
	return s.indexedColumns
}

// IndexRecordColumns indexes specific columns for fast equality lookups
func (s *VectorStore) IndexRecordColumns(datasetName string, rec arrow.RecordBatch, batchIdx int) {
	if s.columnIndex == nil || len(s.indexedColumns) == 0 {
		return
	}
	s.columnIndex.IndexRecord(datasetName, batchIdx, rec, s.indexedColumns)
}

// SetAutoShardingConfig updates the auto-sharding configuration
func (s *VectorStore) SetAutoShardingConfig(cfg AutoShardingConfig) {
	s.autoShardingConfig = cfg
}

// GetAutoShardingConfig returns the current auto-sharding configuration
func (s *VectorStore) GetAutoShardingConfig() AutoShardingConfig {
	return s.autoShardingConfig
}

func (s *VectorStore) checkAndMigrateToSharded(_ *Dataset) {
	// Placeholder logic: check if dataset size exceeds threshold and migrate index to sharded
	if !s.autoShardingConfig.Enabled {
		return
	}
	// Migration logic would go here
}

// WarmupStats holds statistics about the warmup operation
type WarmupStats struct {
	DatasetsWarmed   int
	DatasetsSkipped  int
	TotalNodesWarmed int
	Duration         time.Duration
}

func (w WarmupStats) String() string {
	return fmt.Sprintf("Warmed %d datasets (%d skipped), touched %d nodes in %v",
		w.DatasetsWarmed, w.DatasetsSkipped, w.TotalNodesWarmed, w.Duration)
}

// Warmup iterates through all datasets and warms up their indexes
func (s *VectorStore) Warmup() WarmupStats {
	start := time.Now()
	stats := WarmupStats{}
	datasets := make([]*Dataset, 0)
	s.IterateDatasets(func(_ string, ds *Dataset) {
		datasets = append(datasets, ds)
	})

	for _, ds := range datasets {
		ds.dataMu.RLock()
		idx := ds.Index
		ds.dataMu.RUnlock()

		if idx != nil {
			nodes := idx.Warmup()
			stats.TotalNodesWarmed += nodes
			stats.DatasetsWarmed++
		} else {
			stats.DatasetsSkipped++
		}
	}

	stats.Duration = time.Since(start)
	return stats
}

func (s *VectorStore) GetWALQueueDepth() (int, int) {
	if s.engine == nil {
		return 0, 0
	}
	return s.engine.GetWALQueueDepth()
}

func (s *VectorStore) updateLWWAndMerkle(ds *Dataset, rec arrow.RecordBatch, ts int64) {
	idColIdx := -1
	for i, f := range rec.Schema().Fields() {
		if f.Name == "id" {
			idColIdx = i
			break
		}
	}

	if idColIdx >= 0 {
		column := rec.Column(idColIdx)
		if ids, ok := column.(*array.Uint32); ok {
			for i := 0; i < int(rec.NumRows()); i++ {
				vid := VectorID(ids.Value(i))
				if ds.LWW.Update(vid, ts) {
					if ds.Merkle != nil {
						ds.Merkle.Update(vid, ts)
					}
				}
			}
		}
	}
}

func (s *VectorStore) MerkleRoot(name string) [32]byte {
	ds, ok := s.getDataset(name)
	if !ok {
		return [32]byte{}
	}
	return ds.Merkle.RootHash()
}

// IndexJob is defined in dataset.go
