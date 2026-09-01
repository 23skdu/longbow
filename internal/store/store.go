package store

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"errors"

	"github.com/23skdu/longbow/pkg/loadbalancing"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"

	"github.com/23skdu/longbow/internal/autoscale"
	"github.com/23skdu/longbow/internal/cache"
	"github.com/23skdu/longbow/internal/gc"
	"github.com/23skdu/longbow/internal/gpu"
	gputypes "github.com/23skdu/longbow/internal/gpu/types"
	lbmem "github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/mesh"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/storage"
	lbcore "github.com/23skdu/longbow/internal/store/index"
)

// VectorStore implements flight.FlightServer and provides vector storage and search with support for HNSW, IVF, and learned indexing.
type VectorStore struct {
	flight.BaseFlightServer
	mem                   memory.Allocator
	replicator            *PeerReplicator
	pooledMem             memory.Allocator // Pooled allocator for transient ingestion buffers
	logger                zerolog.Logger
	maxMemory             atomic.Int64
	currentMemory         atomic.Int64
	lastThrottlingLogTime atomic.Int64 // Unix timestamp in seconds of last throttling log
	memoryConfig          MemoryConfig

	sequence atomic.Uint64 // Global operation sequence

	// Persistence
	dataPath string
	engine   atomic.Pointer[storage.StorageEngine] // Manages WAL and Snapshots

	indexQueue          *IndexJobQueueLockFree // Integrated HNSW background indexing queue.
	ingestionQueue      *IngestionRingBuffer   // Lock-free ring buffer for high-throughput ingestion.
	persistenceQueue    chan persistenceJob    // Async persistence queue
	pendingOverflowJobs atomic.Int64           // Jobs spinning in applyBatchToMemory

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

	// Worker management (Part 1.2)
	workerMu               sync.Mutex
	indexingWorkerCancels  []context.CancelFunc
	ingestionWorkerCancels []context.CancelFunc

	// Mesh integration
	Mesh            *mesh.Gossip
	meshStatusCache *MeshStatusCache // Cache for mesh status serialization

	// NUMA integration (Phase 4/5)
	numaTopology *lbmem.NUMATopology

	// Hybrid search (Phase 20)
	hybridSearchConfig HybridSearchConfig

	// DoGet pipeline subsystem
	doGetPipelinePool *DoGetPipelinePool
	pipelineThreshold int

	indexedColumns []string // columns to index for fast equality lookups

	// Compaction (Phase 11/14)
	compactionConfig CompactionConfig
	compactionWorker *CompactionWorker

	// learnedIndexAdapter manages the online learning and index adaptation loop.
	indexAdapter *RuntimeIndexAdapter

	// Auto-sharding (Phase 13)
	autoShardingConfig AutoShardingConfig

	// Node monitoring for load balancing hints
	nodeMonitor  *NodeMonitor
	metadataPool sync.Pool

	// Search pooling (Phase 6 Optimization)
	resultPool *SearchResultPool

	// Namespace management
	nsManager *namespaceManager

	// Circuit Breakers for fault tolerance
	Breakers *CircuitBreakerRegistry

	// Disk IO Scheduler for background tasks
	DiskIO *DiskIOScheduler

	// Rate limiting per namespace
	rateLimiterManager *RateLimiterManager

	// Version management for Part 8
	versionManager *VersionManager

	// Backup management for Part 9
	backupManager          *BackupManager
	backupScheduleInterval time.Duration
	activeSwitches         sync.Map // collection -> bool

	// Auto-scale configuration for Part 1.5
	autoScaleEnabled       bool
	autoScaleMinWorkers    int
	autoScaleMaxWorkers    int
	autoScaleTargetQPS     float64
	autoScaleUpThreshold   float64
	autoScaleDownThreshold float64

	// GPU acceleration (optional)
	gpuBackend   gpu.GPUBackend
	gpuDeviceID  int32
	gpuMemPool   *gpu.GPUMemPool
	gpuEnabled   bool
	gpuIndexPool *gpu.GPUIndexPool // Pool for reusable GPU indexes

	// NUMA support
	numaEnabled bool

	// Shutdown and lifecycle (Phase 6/21)
	shutdownState int32
	ctx           context.Context
	cancel        context.CancelFunc
	workerWg      sync.WaitGroup
	cleanupWg     sync.WaitGroup

	// hnsw2 integration hook (Phase 5)
	// Called after dataset creation to initialize hnsw2 (avoids import cycle)
	hnsw2Config *ArrowHNSWConfig //nolint:unused

	// Memory Tuner
	tuner atomic.Pointer[lbmem.GCTuner]

	// AutoScaler (Part 1.1)
	scaler    *autoscale.AutoScaler
	admission *AdmissionController

	// Distributed search coordinator (shared between Data/Meta servers)
	coordinator *GlobalSearchCoordinator
	pool        *FlightClientPool

	// Query Cache (Phase 23)
	queryCache *cache.QueryCache[[]SearchResult]

	// Adaptive GC Controller (optional)
	gcController *gc.AdaptiveGCController

	// Parser pool for vector search
	vectorSearchParserPool sync.Pool
	temporalParserPool     sync.Pool

	// Change Data Capture (CDC)
	cdc            *ChangeDataCapture
	cdcMu          sync.RWMutex
	cdcSubscribers map[string][]chan arrow.RecordBatch

	evictionManager *lbcore.GraphLayerEvictionManager

	// Learned Index Predictor (Part 16)
	indexPredictor *IndexPerformancePredictor
	// activeEmbeddingProvider and activeEmbeddingModel track the currently active
	// EmbeddingGenerator backend (set via SetActiveEmbedding). These are propagated
	// into QueryFeatures by RecordQueryPerformance for learned index training.
	activeEmbeddingProvider string
	activeEmbeddingModel    string

	temporalConfig TemporalConfig

	// Quantization auto-tuner (v0.1.9)
	quantTuner *QuantizationTuner

	// Learned index rate limiter
	rateLimiter *LearnedIndexRateLimiter
}

// IngestionJob represents a unit of work for the ingestion pipeline, containing a record batch and timestamp.
type IngestionJob struct {
	DS    *Dataset
	Batch arrow.RecordBatch
	TS    int64
	// We might add more metadata here (e.g. span context)
}

type persistenceJob struct {
	datasetName string
	batch       arrow.RecordBatch
	ts          int64
}

// NewVectorStore creates a new VectorStore instance.
func NewVectorStore(mem memory.Allocator, logger zerolog.Logger, maxMemoryBytes int64, _ int64, _ time.Duration) *VectorStore {
	memCfg := DefaultMemoryConfig()
	memCfg.MaxMemory = maxMemoryBytes

	vs := &VectorStore{
		mem:          mem,
		pooledMem:    NewPooledAllocator(),
		logger:       logger,
		memoryConfig: memCfg,
		stopChan:     make(chan struct{}),
		resultPool:   NewSearchResultPool(),
	}
	vs.ctx, vs.cancel = context.WithCancel(context.Background()) // #nosec G118
	vs.evictionManager = lbcore.NewGraphLayerEvictionManager(0.60, logger)

	// Initialize NUMA topology if on Linux
	vs.initNUMA(logger)

	// Initialize empty datasets map
	emptyMap := make(map[string]*Dataset)
	vs.datasets.Store(&emptyMap)

	vs.maxMemory.Store(maxMemoryBytes)
	vs.indexQueue = NewIndexJobQueueLockFree(DefaultIndexJobQueueConfig())
	vs.ingestionQueue = NewIngestionRingBuffer(4096)    // Absorbs burst traffic without blocking DoPut
	vs.persistenceQueue = make(chan persistenceJob, 64) // Reduced from 10000 to prevent OOM

	vs.nsManager = newNamespaceManager()
	vs.versionManager = NewVersionManager()

	// Default Cache: 1024 entries, 60s TTL

	// In future, make this configurable per dataset or global
	vs.queryCache = cache.NewQueryCache[[]SearchResult](1024, 60*time.Second, "global")

	// Initialize Adaptive GC Controller (disabled by default)
	vs.gcController = gc.NewAdaptiveGCController(gc.DefaultAdaptiveGCConfig())

	// Initialize Compaction
	vs.compactionConfig = *DefaultCompactionConfig()
	vs.compactionWorker = NewCompactionWorker(vs, &vs.compactionConfig)
	if vs.compactionConfig.Enabled {
		vs.compactionWorker.Start()
	}

	vs.Breakers = NewCircuitBreakerRegistry(DefaultCircuitBreakerConfig())

	vs.workerWg.Add(1)
	go vs.runPersistenceWorker()

	// Initialize parser pools
	vs.vectorSearchParserPool = sync.Pool{
		New: func() any {
			return query.NewZeroAllocVectorSearchParser(768, &vs.logger)
		},
	}
	vs.temporalParserPool = sync.Pool{
		New: func() any {
			return query.NewZeroAllocTemporalParser()
		},
	}

	vs.replicator = NewPeerReplicator(DefaultReplicatorConfig())
	_ = vs.replicator.Start()

	// Initialize Learned Index Predictor (Part 16/v0.1.9)
	predictorCfg := LearnedIndexConfig{
		EnableAutoSelection: true,
		MinTrainingSamples:  100,
		ConfidenceThreshold: 0.7,
		UpdateInterval:      time.Hour,
	}
	vs.indexPredictor = NewIndexPerformancePredictor(vs.logger, predictorCfg)
	vs.rateLimiter = NewLearnedIndexRateLimiter(vs.indexPredictor, vs.logger)

	// Initialize and start the Adaptive Learned Index Adapter
	adaptConfig := IndexAdaptationConfig{
		EnableRollback:     true,
		RollbackWindow:     30 * time.Minute,
		LatencyThresholdMs: 200.0,
		RecallThreshold:    0.90,
		CheckInterval:      5 * time.Minute,
	}
	vs.indexAdapter = NewRuntimeIndexAdapter(vs.logger, vs.indexPredictor, adaptConfig, vs)
	vs.indexAdapter.WithIndexSwitcher(vs)
	vs.indexAdapter.Start()

	// Initialize Flight client pool for distributed coordination
	vs.pool = NewFlightClientPool(DefaultFlightClientPoolConfig())

	vs.admission = NewAdmissionController(&vs.maxMemory, &vs.currentMemory, nil, vs.logger)

	// Initialize Quantization Auto-Tuner (v0.1.9)
	vs.quantTuner = NewQuantizationTuner(vs.logger, vs)
	vs.workerWg.Add(1)
	go func() {
		defer vs.workerWg.Done()
		vs.quantTuner.Start(vs.ctx)
	}()

	vs.metadataPool = sync.Pool{
		New: func() any {
			b := make([]byte, loadbalancing.LoadHintsSize)
			return &b
		},
	}

	vs.nodeMonitor = NewNodeMonitor()
	return vs
}

func (vs *VectorStore) getPooledMetadataBuffer(size int) []byte {
	if size > loadbalancing.LoadHintsSize {
		return make([]byte, size)
	}
	bufPtr := vs.metadataPool.Get().(*[]byte)
	buf := *bufPtr
	// Zero the buffer for clean reuse
	for i := range buf {
		buf[i] = 0
	}
	return buf
}

// initNUMA initializes NUMA topology detection and enables NUMA-aware allocations
// when multiple NUMA nodes are detected on the system.
func (vs *VectorStore) initNUMA(logger zerolog.Logger) {
	topo, err := lbmem.DetectNUMATopology()
	if err != nil {
		logger.Warn().Err(err).Msg("Failed to detect NUMA topology")
		vs.numaEnabled = false
		metrics.NUMAEnabled.Set(0)
		metrics.NUMANodeCount.Set(0)
		return
	}

	vs.numaTopology = topo
	metrics.NUMANodeCount.Set(float64(topo.NumNodes))

	if topo.NumNodes > 1 {
		vs.numaEnabled = true
		metrics.NUMAEnabled.Set(1)
		logger.Info().
			Int("nodes", topo.NumNodes).
			Str("topology", topo.String()).
			Msg("NUMA topology detected")
	} else {
		vs.numaEnabled = false
		metrics.NUMAEnabled.Set(0)
		logger.Debug().Msg("Single NUMA node detected (no NUMA)")
	}
}

// CheckIngestionBackpressure checks if the system is under heavy load and
// should throttle incoming requests.
// Returns true if backpressure should be applied.
func (vs *VectorStore) CheckIngestionBackpressure() bool {
	// 1. Queue Pressure
	// If ingestion queue is > 95% full, hard block.
	if vs.ingestionQueue != nil {
		queueCap := vs.ingestionQueue.Capacity()
		if vs.ingestionQueue.Len() > (queueCap*95)/100 {
			return true
		}
	}

	// 2. Global Heap Pressure (Hard Threshold)
	tuner := vs.tuner.Load()
	if tuner != nil {
		ratio := tuner.GetUtilizationRatio()
		if ratio > 0.98 {
			return true
		}
	}

	// 3. WAL Pressure (Hard Threshold: > 90% queue depth)
	engine := vs.engine.Load()
	if engine != nil {
		pending, capacity := engine.GetWALQueueDepth()
		if capacity > 0 && pending > (capacity*90)/100 {
			return true
		}
	}

	return false
}

// IngestionBackpressureDelay returns a delay duration if the system is under moderate load.
// This implements "Soft Backpressure" to prevent the ingestion cliff.
func (vs *VectorStore) IngestionBackpressureDelay() time.Duration {
	// 1. Queue Pressure (Soft Threshold: 70% to 95%)
	if vs.ingestionQueue != nil {
		queueCap := vs.ingestionQueue.Capacity()
		length := vs.ingestionQueue.Len()
		if length > (queueCap*70)/100 {
			// Linear delay from 0 to 50ms
			p := float64(length-(queueCap*70)/100) / float64((queueCap*95)/100-(queueCap*70)/100)
			if p > 1.0 {
				p = 1.0
			}
			return time.Duration(p * float64(50*time.Millisecond))
		}
	}

	// 2. Global Heap Pressure (Soft Threshold: 85% to 98%)
	tuner := vs.tuner.Load()
	if tuner != nil {
		ratio := tuner.GetUtilizationRatio()
		if ratio > 0.85 {
			// Linear delay from 0 to 100ms
			p := (ratio - 0.85) / (0.98 - 0.85)
			if p > 1.0 {
				p = 1.0
			}
			return time.Duration(p * float64(100*time.Millisecond))
		}
	}

	// 3. WAL Pressure (Soft Threshold: 60% to 90% queue depth)
	engine := vs.engine.Load()
	if engine != nil {
		pending, capacity := engine.GetWALQueueDepth()
		if capacity > 0 && pending > (capacity*60)/100 {
			p := float64(pending-(capacity*60)/100) / float64((capacity*90)/100-(capacity*60)/100)
			if p > 1.0 {
				p = 1.0
			}
			// Linear delay from 0 to 75ms
			return time.Duration(p * float64(75*time.Millisecond))
		}
	}

	return 0
}

// TrackMemory adds delta to current usage and logs if large.
func (vs *VectorStore) TrackMemory(delta int64) {
	if delta > 100*1024*1024 {
		vs.logger.Warn().
			Int64("delta", delta).
			Int64("current", vs.currentMemory.Load()).
			Str("stack", stackTrace()).
			Msg("Large memory addition detected")
	}
	vs.currentMemory.Add(delta)
}

func stackTrace() string {
	buf := make([]byte, 1024)
	n := runtime.Stack(buf, false)
	return string(buf[:n])
}

// IngestionQueueLen returns the current length of the ingestion queue.
func (vs *VectorStore) IngestionQueueLen() int {
	if vs.ingestionQueue == nil {
		return 0
	}
	return vs.ingestionQueue.Len()
}

// IngestionQueueCap returns the capacity of the ingestion queue.
func (vs *VectorStore) IngestionQueueCap() int {
	if vs.ingestionQueue == nil {
		return 0
	}
	return vs.ingestionQueue.Capacity()
}

// GetActiveDatasets returns a list of active dataset names.
func (vs *VectorStore) GetActiveDatasets() []string {
	var list []string
	vs.IterateDatasets(func(name string, ds *Dataset) {
		list = append(list, name)
	})
	return list
}

// GetDatasetIndexStats returns core progress statistics for a dataset's index.
func (vs *VectorStore) GetDatasetIndexStats(name string) (nodeCount int64, maxLevel int32, isOffHeap bool, exists bool) {
	ds, ok := vs.getDataset(name)
	if !ok || ds.Index == nil {
		return 0, 0, false, false
	}
	hnsw, ok := ds.Index.(*lbcore.ArrowHNSW)
	if !ok {
		return 0, 0, false, true
	}
	snap := hnsw.GetMetadataSnapshot()
	isOff := false
	if hnsw.GetData() != nil && len(hnsw.GetData().PackedNeighbors) > 0 {
		isOff = true
	}
	return snap.NodeCount, snap.MaxLevel, isOff, true
}

// RCU Helpers

func (vs *VectorStore) loadDatasets() map[string]*Dataset {
	return *vs.datasets.Load()
}

func (vs *VectorStore) getDataset(name string) (*Dataset, bool) {
	m := vs.loadDatasets()
	ds, ok := m[name]
	return ds, ok
}

// UpdateDatasets executes a CAS loop to update the map.
// fn receives a COPY of the map to modify.
func (vs *VectorStore) UpdateDatasets(fn func(map[string]*Dataset)) {
	for {
		oldPtr := vs.datasets.Load()
		oldMap := *oldPtr

		newMap := make(map[string]*Dataset, len(oldMap)+1)
		for k, v := range oldMap {
			newMap[k] = v
		}

		fn(newMap)

		if vs.datasets.CompareAndSwap(oldPtr, &newMap) {
			return
		}
		// Contention, retry
		metrics.DatasetUpdateRetriesTotal.Inc()

		runtime.Gosched()
	}
}

// IterateDatasets safely iterates over all datasets.
func (vs *VectorStore) IterateDatasets(fn func(string, *Dataset)) {
	m := vs.loadDatasets()
	for name, ds := range m {
		fn(name, ds)
	}
}

// getOrCreateDataset atomically gets an existing dataset or creates a new one using the provider.
// The provider is only called if creation is needed (lazy).
func (vs *VectorStore) getOrCreateDataset(name string, createFn func() *Dataset) (*Dataset, bool) {
	// 1. Optimistic Read
	if ds, ok := vs.getDataset(name); ok && ds != nil {
		return ds, false
	}

	// 2. CAS Loop
	var result *Dataset
	var created bool
	vs.UpdateDatasets(func(m map[string]*Dataset) {
		// Double-check existence in the new copy
		if ds, ok := m[name]; ok && ds != nil {
			result = ds
			created = false
			return
		}

		// Create
		newDs := createFn()
		if newDs != nil {
			newDs.EvictionManager = vs.evictionManager
			newDs.SetAdmission(vs.admission)
			if vs.hybridSearchConfig.Enabled {
				newDs.BM25Index = NewBM25InvertedIndex(vs.hybridSearchConfig.BM25)
				// Also initialize the arena-based BM25 index for reduced GC pressure
				slabArena := lbmem.NewSlabArena(4 * 1024 * 1024)
				newDs.BM25ArenaIndex = NewBM25ArenaIndex(slabArena, 10000)
			}
			if vs.temporalConfig.Enabled {
				newDs.TemporalIndex = NewTemporalIndex(0)
				// Apply history/retention settings from global config if needed
				// For now, the global config is used by the VectorStore to check if enabled
			}
			m[name] = newDs
			result = newDs
			created = true
		}
	})

	// 3. Register dataset in namespace
	if result != nil {
		nsName, _ := ParseNamespacedPath(name)
		if ns := vs.GetNamespace(nsName); ns != nil {
			if !ns.HasDataset(name) {
				ns.AddDataset(name)
			}
		}
	}

	return result, created
}

// IndexRecordColumns indexes specific columns for fast equality lookups
func (vs *VectorStore) IndexRecordColumns(datasetName string, rec arrow.RecordBatch, batchIdx int) {
	ds, ok := vs.getDataset(datasetName)
	if !ok || ds.ColumnIndex == nil || len(vs.indexedColumns) == 0 {
		return
	}
	ds.ColumnIndex.IndexRecord(batchIdx, rec, vs.indexedColumns)
}

// GetLoadHints returns the current load balancing hints for this node.
func (vs *VectorStore) GetLoadHints() loadbalancing.LoadHints {
	var hints loadbalancing.LoadHints

	// 1. CPU Load (Approximate using Go metrics or OS)
	// For now, use a simple proxy or 0 if not implemented

	// 2. Memory Load
	tuner := vs.tuner.Load()
	if tuner != nil {
		hints.MemLoad = uint32(tuner.GetUtilizationRatio() * 100)
	}

	// 3. Queue Depth (Indexing + Ingestion)
	if vs.indexQueue != nil {
		hints.QueueDepth += int64(vs.indexQueue.Len())
	}
	if vs.ingestionQueue != nil {
		hints.QueueDepth += int64(vs.ingestionQueue.Len())
	}

	// 4. Health
	hints.Health = 100 // Default healthy
	if vs.CheckIngestionBackpressure() {
		hints.Health = 50 // Degraded
	}

	return hints
}

// getGPUIndex returns a GPU index handle for the current backend and device.
func (vs *VectorStore) getGPUIndex(dim int) (gputypes.Index, error) {
	vs.configMu.RLock()
	gpuEnabled := vs.gpuEnabled
	gpuBackend := vs.gpuBackend
	gpuDeviceID := vs.gpuDeviceID
	vs.configMu.RUnlock()

	if !gpuEnabled || gpuBackend == gpu.BackendCPU {
		return nil, fmt.Errorf("GPU acceleration not enabled")
	}

	cfg := gpu.GPUConfig{
		DeviceID:  gpuDeviceID,
		Dimension: dim,
		Enabled:   true,
		Backend:   gpuBackend,
	}
	return gpu.NewIndexWithBackend(cfg, gpuBackend)
}

// GetGPUIndexPool returns the GPU index pool for this store
func (vs *VectorStore) GetGPUIndexPool() *gpu.GPUIndexPool {
	return vs.gpuIndexPool
}

// GetGPUIndexPoolStats returns statistics about the GPU index pool
func (vs *VectorStore) GetGPUIndexPoolStats() gpu.GPUIndexPoolStats {
	if vs.gpuIndexPool == nil {
		return gpu.GPUIndexPoolStats{}
	}
	return vs.gpuIndexPool.Stats()
}

// CleanupGPUIndexPool removes expired idle indexes from the pool
func (vs *VectorStore) CleanupGPUIndexPool() int {
	if vs.gpuIndexPool == nil {
		return 0
	}
	return vs.gpuIndexPool.Cleanup()
}

func (vs *VectorStore) checkAndMigrateToSharded(ds *Dataset) {
	if !vs.autoShardingConfig.Enabled {
		return
	}
	if ds == nil {
		return
	}

	threshold := vs.autoShardingConfig.ShardThreshold
	if threshold <= 0 {
		threshold = 10000
	}

	size := ds.IndexLen()

	if size >= threshold {
		if err := ds.MigrateToShardedIndex(vs.autoShardingConfig); err != nil {
			vs.logger.Warn().Err(err).Str("dataset", ds.Name).Msg("Auto-sharding migration failed")
		} else {
			vs.logger.Info().Str("dataset", ds.Name).Int("size", size).Int("threshold", threshold).Msg("Migrated dataset to sharded index")
		}
	}
}

// WarmupStats holds statistics about the warmup operation, including duration and node count.
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
func (vs *VectorStore) Warmup() WarmupStats {
	start := time.Now()
	stats := WarmupStats{}
	datasets := make([]*Dataset, 0)
	vs.IterateDatasets(func(_ string, ds *Dataset) {
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

// GetWALQueueDepth returns the number of jobs and total size in bytes currently in the WAL queue.
func (vs *VectorStore) GetWALQueueDepth() (count, size int) {
	engine := vs.engine.Load()
	if engine == nil {
		return 0, 0
	}
	return engine.GetWALQueueDepth()
}

func (vs *VectorStore) updateLWWAndMerkle(ds *Dataset, rec arrow.RecordBatch, ts int64) {
	ds.metadataMu.Lock()
	defer ds.metadataMu.Unlock()

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

// MerkleRoot returns the root hash of the Merkle tree for a given dataset.
func (vs *VectorStore) MerkleRoot(name string) [32]byte {
	ds, ok := vs.getDataset(name)
	if !ok {
		return [32]byte{}
	}
	return ds.Merkle.RootHash()
}

// IndexJob is defined in dataset.go

// DropDataset removes a dataset from the store immediately (Fast Path).
// It unlinks the dataset from the map (RCU) and schedules cleanup asynchronously.
func (vs *VectorStore) DropDataset(ctx context.Context, name string) error {
	for {
		oldMapPtr := vs.datasets.Load()
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

		if vs.datasets.CompareAndSwap(oldMapPtr, &newMap) {
			// Unlink successful - Resource is ostensibly "gone" from new readers.
			// Schedule Async Cleanup
			droppedDS := oldMap[name]
			metrics.StoreDroppedDatasets.Inc()
			metrics.StoreActiveDatasets.Set(float64(len(newMap)))

			// Ensure all pending indexing/ingestion for this dataset is finished
			// before we decrement the global memory counter and release resources.
			droppedDS.WaitForIndexing()

			// Decrement both record batch memory AND index memory
			totalMemory := droppedDS.SizeBytes.Load() + droppedDS.IndexMemoryBytes.Load()
			vs.currentMemory.Add(-totalMemory)
			droppedDS.Close()

			// Synchronous Memory Reclamation for Benchmarking stability
			// 1. Trigger GC to finalize all unreachable objects and arenas
			runtime.GC()
			// 2. Force the runtime to return all freed memory to the OS
			debug.FreeOSMemory()

			vs.logger.Info().Str("dataset", name).Int64("freed_bytes", totalMemory).Msg("Dataset dropped and resources released synchronously")

			return nil
		}
		// CAS failed, retry
		runtime.Gosched()
	}
}

// WaitForIndexing blocks until all pending indexing jobs for the given dataset are complete.
func (vs *VectorStore) WaitForIndexing(name string) {
	// First wait for any global congestion to clear
	start := time.Now()
	for vs.pendingOverflowJobs.Load() > 0 {
		if time.Since(start) > 5*time.Second {
			// Don't block forever if something is stuck, let dataset check proceed
			vs.logger.Warn().Msg("WaitForIndexing timed out waiting for global overflow jobs")
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if ds, ok := vs.getDataset(name); ok {
		ds.WaitForIndexing()
	}
}

// ClosePersistence closes the persistence engine.
func (vs *VectorStore) ClosePersistence() error {
	engine := vs.engine.Load()
	if engine != nil {
		return engine.Close()
	}
	return nil
}

func (vs *VectorStore) runPersistenceWorker() {
	defer vs.workerWg.Done()

	for {
		select {
		case <-vs.stopChan:
			return
		case job := <-vs.persistenceQueue:
			vs.processPersistenceJob(job)
		}
	}
}

func (vs *VectorStore) processPersistenceJob(job persistenceJob) {
	defer job.batch.Release()

	// Assign Sequence atomically
	seq := vs.sequence.Add(1)

	// Write to WAL if engine is initialized
	// Note: We access vs.engine racily if InitPersistence is called concurrently,
	// but usage model implies Init happens before heavy load.

	engine := vs.engine.Load()

	if engine != nil {
		engine.RLockSnapshot()
		err := engine.WriteWAL(job.datasetName, job.batch, seq, job.ts)
		engine.RUnlockSnapshot()
		if err != nil {
			vs.logger.Error().
				Str("dataset", job.datasetName).
				Uint64("seq", seq).
				Err(err).
				Msg("Failed to write to WAL")
		}
	}
}

// broadcastCDC safely dispatches a copy of the incoming batch to all registered observers
func (vs *VectorStore) broadcastCDC(dataset string, batches []arrow.RecordBatch) {
	vs.cdcMu.RLock()
	subs, ok := vs.cdcSubscribers[dataset]
	vs.cdcMu.RUnlock()
	if !ok || len(subs) == 0 {
		return
	}

	for _, batch := range batches {
		for _, sub := range subs {
			batch.Retain()
			select {
			case sub <- batch:
				// successfully queued
			default:
				// channel full, drop CDC event to avoid blocking ingestion
				batch.Release()
			}
		}
	}
}

// GetNeighborsBulk retrieves the adjacency lists for multiple nodes in the vector index.
func (vs *VectorStore) GetNeighborsBulk(ctx context.Context, datasetName string, nodeIDs []uint32) (map[uint32][]uint32, error) {
	ds, ok := vs.getDataset(datasetName)
	if !ok {
		return nil, fmt.Errorf("dataset %s not found", datasetName)
	}

	ds.dataMu.RLock()
	idx := ds.Index
	ds.dataMu.RUnlock()

	if idx == nil {
		return nil, fmt.Errorf("index for dataset %s not initialized", datasetName)
	}

	results := make(map[uint32][]uint32, len(nodeIDs))
	for _, id := range nodeIDs {
		neighbors, err := idx.GetRawNeighbors(id)
		if err != nil {
			continue // Or log error
		}
		results[id] = neighbors
	}

	return results, nil
}

func (d *Dataset) TurboQuantBits() int {
	return d.turboQuantBits
}

// GetLogger returns the logger
func (s *VectorStore) GetLogger() zerolog.Logger {
	return s.logger
}

// GetMesh returns the mesh
func (s *VectorStore) GetMesh() *mesh.Gossip {
	return s.Mesh
}

// RegisterCDCSubscriber registers a subscriber for CDC events
func (s *VectorStore) RegisterCDCSubscriber(dataset string, sub chan arrow.RecordBatch) {
	// If store has CDC enabled/instantiated
	// But to avoid cyclical dependencies, we just log unimplemented
	s.logger.Warn().Msg("RegisterCDCSubscriber unimplemented directly on store due to package refactor")
}

// UnregisterCDCSubscriber unregisters a subscriber for CDC events
func (s *VectorStore) UnregisterCDCSubscriber(dataset string, sub chan arrow.RecordBatch) {
}
