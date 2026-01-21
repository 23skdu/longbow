package store

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"context"
	"errors"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"

	"github.com/23skdu/longbow/internal/cache"
	"github.com/23skdu/longbow/internal/gc"
	"github.com/23skdu/longbow/internal/mesh"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/storage"
)

// VectorStore implements flight.FlightServer with minimal logic
type VectorStore struct {
	flight.BaseFlightServer
	mem           memory.Allocator
	pooledMem     memory.Allocator // Pooled allocator for transient ingestion buffers
	logger        zerolog.Logger
	maxMemory     atomic.Int64
	currentMemory atomic.Int64
	memoryConfig  MemoryConfig

	sequence atomic.Uint64 // Global operation sequence

	// Persistence
	dataPath      string
	engine        *storage.StorageEngine // Manages WAL and Snapshots
	snapshotReset chan time.Duration

	indexQueue          *IndexJobQueue       // Integrated HNSW
	ingestionQueue      *IngestionRingBuffer // Lock-free ring buffer
	persistenceQueue    chan persistenceJob  // Async persistence queue
	pendingOverflowJobs atomic.Int64         // Jobs spinning in applyBatchToMemory

	// Lifecycle
	stopChan           chan struct{}
	stopOnce           sync.Once      // Protects stopChan closure
	indexWg            sync.WaitGroup // For background workers
	startIndexingOnce  sync.Once      // Ensure background workers start only once
	ingestionStartOnce sync.Once      // Ensure ingestion workers start only once
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
	rateLimiter      *RateLimiter

	// Auto-sharding (Phase 13)
	autoShardingConfig AutoShardingConfig

	// Namespace management
	nsManager *namespaceManager

	// GPU acceleration (optional)

	// Shutdown and lifecycle (Phase 6/21)
	shutdownState int32
	workerWg      sync.WaitGroup
	cleanupWg     sync.WaitGroup

	// hnsw2 integration hook (Phase 5)
	// Called after dataset creation to initialize hnsw2 (avoids import cycle)
	datasetInitHook func(*Dataset)

	// Distributed search coordinator (shared between Data/Meta servers)
	// Distributed search coordinator (shared between Data/Meta servers)
	coordinator *GlobalSearchCoordinator

	// Query Cache (Phase 23)
	queryCache *cache.QueryCache[[]SearchResult]

	// Adaptive GC Controller (optional)
	gcController *gc.AdaptiveGCController

	// Parser pool for vector search
	vectorSearchParserPool sync.Pool
}

type ingestionJob struct {
	datasetName string
	batch       arrow.RecordBatch
	ts          int64
	// We might add more metadata here (e.g. span context)
}

type persistenceJob struct {
	datasetName string
	batch       arrow.RecordBatch
	ts          int64
}

//nolint:gocritic // Logger passed by value for simplicity
func NewVectorStore(mem memory.Allocator, logger zerolog.Logger, maxMemoryBytes int64, _ int64, _ time.Duration) *VectorStore {
	memCfg := DefaultMemoryConfig()
	memCfg.MaxMemory = maxMemoryBytes

	s := &VectorStore{
		mem:          mem,
		pooledMem:    NewPooledAllocator(),
		logger:       logger,
		memoryConfig: memCfg,
		stopChan:     make(chan struct{}),
	}
	// Initialize empty datasets map
	emptyMap := make(map[string]*Dataset)
	s.datasets.Store(&emptyMap)

	s.maxMemory.Store(maxMemoryBytes)
	s.indexQueue = NewIndexJobQueue(DefaultIndexJobQueueConfig())
	s.ingestionQueue = NewIngestionRingBuffer(64)      // Reduced from 256 to prevent OOM with large batches
	s.persistenceQueue = make(chan persistenceJob, 64) // Reduced from 10000 to prevent OOM

	s.nsManager = newNamespaceManager()
	s.columnIndex = NewColumnInvertedIndex()

	// Default Cache: 1024 entries, 60s TTL

	// In future, make this configurable per dataset or global
	s.queryCache = cache.NewQueryCache[[]SearchResult](1024, 60*time.Second, "global")

	// Initialize Adaptive GC Controller (disabled by default)
	s.gcController = gc.NewAdaptiveGCController(gc.DefaultAdaptiveGCConfig())

	// Initialize Compaction
	s.compactionConfig = DefaultCompactionConfig()
	s.compactionWorker = NewCompactionWorker(s, s.compactionConfig)
	if s.compactionConfig.Enabled {
		s.compactionWorker.Start()
	}

	s.workerWg.Add(1)
	go s.runPersistenceWorker()

	// Start default index worker (1 thread)
	s.StartIndexingWorkers(1)
	s.StartIngestionWorkers(1)

	// Initialize parser pool
	s.vectorSearchParserPool = sync.Pool{
		New: func() interface{} {
			return query.NewZeroAllocVectorSearchParser(768, s.logger)
		},
	}

	return s
}

// CheckIngestionBackpressure checks if the system is under heavy load and
// should throttle incoming requests.
// Returns true if backpressure should be applied.
func (s *VectorStore) CheckIngestionBackpressure() bool {
	// 1. Memory Pressure
	// If current memory > 90% of max memory, throttle.
	maxMem := s.maxMemory.Load()
	if maxMem > 0 {
		currMem := s.currentMemory.Load()
		if float64(currMem) > float64(maxMem)*0.9 {
			return true
		}
	}

	// 2. Queue Pressure
	// If ingestion queue is > 80% full, throttle.
	// Capacity is 16384 (hardcoded in NewVectorStore).
	// Capacity is 256
	const queueCap = 256
	if s.ingestionQueue != nil && s.ingestionQueue.Len() > (queueCap*80)/100 {
		return true
	}

	return false
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
	if ds, ok := s.getDataset(name); ok && ds != nil {
		return ds, false
	}

	// 2. CAS Loop
	var result *Dataset
	var created bool
	s.updateDatasets(func(m map[string]*Dataset) {
		// Double-check existence in the new copy
		if ds, ok := m[name]; ok && ds != nil {
			result = ds
			created = false
			return
		}

		// Create
		newDs := createFn()
		if newDs != nil {
			m[name] = newDs
			result = newDs
			created = true
		}
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

// EnableAdaptiveGC starts the adaptive GC controller with the given configuration.
// This is optional and disabled by default. Call this after NewVectorStore if you want
// dynamic GOGC adjustment based on allocation rate and memory pressure.
func (s *VectorStore) EnableAdaptiveGC(config gc.AdaptiveGCConfig) {
	if s.gcController != nil {
		s.gcController.Stop() // Stop existing controller if any
	}

	config.Enabled = true // Force enabled
	s.gcController = gc.NewAdaptiveGCController(config)
	s.gcController.Start()

	s.logger.Info().
		Int("min_gogc", config.MinGOGC).
		Int("max_gogc", config.MaxGOGC).
		Dur("adjust_interval", config.AdjustInterval).
		Msg("Adaptive GC controller enabled")
}

// DisableAdaptiveGC stops the adaptive GC controller
func (s *VectorStore) DisableAdaptiveGC() {
	if s.gcController != nil {
		s.gcController.Stop()
		s.logger.Info().Msg("Adaptive GC controller disabled")
	}
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

func (s *VectorStore) GetWALQueueDepth() (count, size int) {
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

// DropDataset removes a dataset from the store immediately (Fast Path).
// It unlinks the dataset from the map (RCU) and schedules cleanup asynchronously.
func (s *VectorStore) DropDataset(ctx context.Context, name string) error {
	for {
		oldMapPtr := s.datasets.Load()
		if oldMapPtr == nil {
			return errors.New("store not initialized")
		}

		oldMap := *oldMapPtr
		if _, ok := oldMap[name]; !ok {
			return fmt.Errorf("dataset %s not found", name)
		}

		// Copy-On-Write
		newMap := make(map[string]*Dataset, len(oldMap)-1)
		for k, v := range oldMap {
			if k != name {
				newMap[k] = v
			}
		}

		if s.datasets.CompareAndSwap(oldMapPtr, &newMap) {
			// Unlink successful - Resource is ostensibly "gone" from new readers.
			// Schedule Async Cleanup
			droppedDS := oldMap[name]
			metrics.StoreDroppedDatasets.Inc()
			metrics.StoreActiveDatasets.Set(float64(len(newMap)))

			s.cleanupWg.Add(1)
			go func() {
				// Defer cleanup to background to avoid blocking DropDataset call (Fast Path)
				defer s.cleanupWg.Done()
				defer func() {
					if r := recover(); r != nil {
						s.logger.Error().Msgf("Panic during dataset cleanup: %v", r)
					}
				}()

				// Ensure no active readers? RCU guarantees new readers won't see it.
				// Old readers might still hold the reference.
				// Closing immediately *might* panic concurrent readers if not careful,
				// but usually Dataset struct uses locks or is robust.
				// Ideally we wait for refcount or just close underlying resources which are safe to close.
				// Dataset.Close() typically releases Arrow memory.
				droppedDS.Close()
				s.logger.Info().Str("dataset", name).Msg("Dataset dropped and resources released (async)")
			}()

			return nil
		}
		// CAS failed, retry
		runtime.Gosched()
	}
}

// WaitForIndexing blocks until all pending indexing jobs for the given dataset are complete.
func (s *VectorStore) WaitForIndexing(name string) {
	// First wait for any global congestion to clear
	start := time.Now()
	for s.pendingOverflowJobs.Load() > 0 {
		if time.Since(start) > 5*time.Second {
			// Don't block forever if something is stuck, let dataset check proceed
			s.logger.Warn().Msg("WaitForIndexing timed out waiting for global overflow jobs")
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if ds, ok := s.getDataset(name); ok {
		ds.WaitForIndexing()
	}
}
