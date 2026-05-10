package core

// nosec G404 - math/rand is used for HNSW operations, not security-sensitive
import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu"
	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"

	"github.com/apache/arrow-go/v18/arrow"
	arrowarray "github.com/apache/arrow-go/v18/arrow/array"
	arrowmemory "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// HNSWMetadata contains core index state that must be updated atomically
// to ensure consistent views during concurrent ingestion.
type HNSWMetadata struct {
	EntryPoint uint32
	MaxLevel   int32
	NodeCount  int64
	Version    uint64
	Generation uint64
}


// ArrowHNSW implements a Hierarchical Navigable Small World (HNSW) index
// optimized for Apache Arrow data structures with zero-copy operations.
type ArrowHNSW struct {
	config types.ArrowHNSWConfig

	dataset       types.IndexDataProvider
	data          atomic.Pointer[types.GraphData]
	locationStore *ChunkedLocationStore

	nodeCount      atomic.Int64 // Number of nodes ready for search
	nextID         atomic.Int64 // Next ID to allocate for insertion
	dims           atomic.Int32
	entryPoint     atomic.Uint32
	maxLevel       atomic.Int32
	efConstruction atomic.Int32

	metadataRegistry atomic.Pointer[HNSWMetadata]
	// entryPointPools provides multiple entry points per layer to reduce contention
	entryPointPools [types.ArrowMaxLayers]*ConcurrentSkipList

	m     atomic.Int32
	mMax  atomic.Int32
	mMax0 atomic.Int32

	// Mutexes for thread-safe concurrent writes (sharded to 1024 to reduce contention)
	insertMus [ShardedLockCount]types.PaddedMutex

	// Insert context pool for reducing allocations (Issue 2: PoolMetrics)
	insertPool *InsertContextPool

	// DiskGraph backing
	diskGraph atomic.Pointer[DiskGraph]

	quantizer  *ScalarQuantizer
	sq8Ready   atomic.Bool
	bqEncoder  *types.BQEncoder
	oopqEncoder any // Accepts *pq.PQEncoder or *pq.OPQEncoder (Issue 4: Use new OPQ)
	tqEncoder  *TurboQuantEncoder
	searchPool    *ArrowSearchContextPool
	candidatePool sync.Pool

	name                   string
	disableNodeCountMetric atomic.Bool
	metricsSampleCounter   atomic.Uint64
	topLayerManager        *TopLayerManager

	distFunc     func([]float32, []float32) (float32, error)
	distFuncSquared  func([]float32, []float32) (float32, error)
	distFuncF64  func([]float64, []float64) (float32, error)
	distFuncF16  func([]float16.Num, []float16.Num) (float32, error)
	distFuncC64  func([]complex64, []complex64) (float32, error)
	distFuncC128 func([]complex128, []complex128) (float32, error)
	distFuncInt8 func([]int8, []int8) (float32, error)
	distFuncInt8Squared func([]int8, []int8) (float32, error)
	distFuncUint8 func([]uint8, []uint8) (float32, error)
	distFuncUint8Squared func([]uint8, []uint8) (float32, error)
	distFuncInt16 func([]int16, []int16) (float32, error)
	distFuncUint16 func([]uint16, []uint16) (float32, error)
	distFuncInt32 func([]int32, []int32) (float32, error)
	distFuncUint32 func([]uint32, []uint32) (float32, error)
	distFuncInt64 func([]int64, []int64) (float32, error)
	distFuncUint64 func([]uint64, []uint64) (float32, error)

	adaptiveMTriggered atomic.Bool

	initMu sync.Mutex
	growMu sync.RWMutex
	bulkMu sync.Mutex
	epMu       sync.Mutex
	commitMu   sync.Mutex
	commitCond *sync.Cond

	deleted   *roaring.Bitmap
	deletedMu sync.RWMutex

	repairAgent *RepairAgent

	// Parallel Search Config
	parallelConfig types.ParallelSearchConfig

	// GPU Support
	gpuMu          sync.RWMutex
	gpuEnabled     bool
	gpuFallback    bool
	gpuIndex       gpu.Index
	gpuConfig      GPUConfig
	gpuResultCache *gpuResultCache

	// GPU Batch Sync Support
	gpuBatchIDs     []int64
	gpuBatchVectors []float32
	gpuBatchMu      sync.Mutex
	gpuLastSyncTime time.Time
	gpuSyncTicker   *time.Ticker
	gpuStopSync     chan struct{}

	// GPU Circuit Breaker
	gpuCircuitBreaker *gpu.CircuitBreaker

	sq8TrainingBuffer [][]float32
	levelMultiplier   float64

	// Graph Navigation
	navigator *GraphNavigator
	tqCompute *TurboQuantCompute
	gpuTrained atomic.Bool
	topo       *memory.NUMATopology
	efTuner     *PIDTuner

	// MetadataRegistry for pre-cached field lookups
	metadata struct {
		vecColIdx atomic.Int32
		tqBits    atomic.Int32
		metric    atomic.Int32
		vecType   atomic.Int32 // stores types.VectorDataType
		isComplex atomic.Bool
		cached    atomic.Bool
		fieldMap  sync.Map // map[string]int
	}
}




// GetRecords is a helper to get records from the dataset.
func (h *ArrowHNSW) GetRecords() []arrow.RecordBatch {
	if h.dataset == nil {
		return nil
	}
	return h.dataset.GetRecords()
}

// GetDataset returns the underlying dataset provider.
func (h *ArrowHNSW) GetDataset() types.IndexDataProvider {
	return h.dataset
}
// NewArrowHNSW creates a new ArrowHNSW index.
func NewArrowHNSW(dataset types.IndexDataProvider, config *types.ArrowHNSWConfig, topo *memory.NUMATopology) *ArrowHNSW {
	return NewArrowHNSWWithConfig(dataset, *config, topo)
}

// NewArrowHNSWWithConfig creates a new ArrowHNSW index with the given configuration.
func NewArrowHNSWWithConfig(dataset types.IndexDataProvider, config types.ArrowHNSWConfig, topo *memory.NUMATopology) *ArrowHNSW {
	h := &ArrowHNSW{
		config:          config,
		dataset:         dataset,
		m:               atomic.Int32{},
		mMax:            atomic.Int32{},
		mMax0:           atomic.Int32{},
		searchPool:      NewArrowSearchContextPool(),
		insertPool:     NewInsertContextPool(), // Issue 2: Pool metrics
		candidatePool: sync.Pool{
			New: func() any {
				s := make([]types.Candidate, 0, config.EfConstruction)
				return &s
			},
		},
		locationStore:   NewChunkedLocationStore(),
		deleted:         roaring.New(),
		levelMultiplier: 1.0 / math.Log(float64(config.M)),
		topLayerManager: NewTopLayerManager(config.LockFreeThreshold),
		topo:            topo,
	}
	// Ensure config parameters fit in int32 and are valid for HNSW
	if config.M > math.MaxInt32 || config.M <= 1 {
		config.M = 16 // Default safe value
	}
	if config.MMax > math.MaxInt32 || config.MMax <= 0 {
		config.MMax = config.M * 2
	}
	if config.MMax0 > math.MaxInt32 || config.MMax0 <= 0 {
		config.MMax0 = config.M * 2
	}

	h.m.Store(int32(config.M))     // #nosec G115
	h.mMax.Store(int32(config.MMax))   // #nosec G115
	h.mMax0.Store(int32(config.MMax0)) // #nosec G115
	// Initialize metadata cache to -1 to avoid defaulting to column 0
	h.metadata.vecColIdx.Store(-1)
	h.metadata.tqBits.Store(-1)
	h.metadata.metric.Store(-1)
	h.metadata.vecType.Store(-1)

	if h.topLayerManager.threshold == 0 {
		h.topLayerManager.threshold = 2 // Default to layer 2+
	}

	if dataset != nil {
		h.name = dataset.GetName()
	} else {
		h.name = "unnamed"
	}

	if dataset != nil {
		// Restore PQ Encoder if present in dataset (e.g. from snapshot)
		if encoder := dataset.GetPQEncoder(); encoder != nil {
			h.oopqEncoder = encoder
			h.config.PQEnabled = true
		}
	}

	// Initialize atomic values
	if config.EfConstruction <= 0 {
		config.EfConstruction = 200
	}
	h.efConstruction.Store(int32(config.EfConstruction)) // #nosec G115
	h.maxLevel.Store(-1)
	h.entryPoint.Store(math.MaxUint32)
	if config.Dims > math.MaxInt32 {
		fmt.Printf("Error: dimensions %d exceed MaxInt32, returning nil index\n", config.Dims)
		return nil
	}
	h.dims.Store(int32(config.Dims)) // #nosec G115

	// Initialize distance functions using resolvers
	h.resolveAllDistanceFuncs()

	// Initialize quantization if enabled
	if config.SQ8Enabled {
		// Initialize with config dims if available, otherwise lazy init will handle it
		if config.Dims > 0 {
			h.quantizer = NewScalarQuantizer(config.Dims)
		}
		// Do not set sq8Ready to true until trained
	}

	h.efTuner = NewPIDTuner(0.95, int(config.EfSearch)) // Target 0.95 recall

	// Ensure initial capacity
	capacity := config.InitialCapacity
	if capacity < 1000 {
		capacity = 1000
	}

	// Determine DataType
	dt := config.DataType
	if config.Float16Enabled {
		dt = types.VectorTypeFloat16
	}
	h.config.DataType = dt

	// Optimize HNSW Dimension Index Parameters for All Scalar Types at High Scale
	if config.InitialCapacity >= 10000 && config.Dims >= 384 &&
		(dt == types.VectorTypeFloat32 ||
			dt == types.VectorTypeFloat64 ||
			dt == types.VectorTypeInt8 ||
			dt == types.VectorTypeInt16 ||
			dt == types.VectorTypeInt32 ||
			dt == types.VectorTypeComplex64 ||
			dt == types.VectorTypeComplex128 ||
			dt == types.VectorTypeTQ) {
		if h.m.Load() < 24 {
			h.m.Store(24)
		}
		if h.mMax.Load() < int32(h.m.Load()*2) {
			h.mMax.Store(int32(h.m.Load() * 2)) // #nosec G115
		}
		if h.mMax0.Load() < int32(h.m.Load()*2) {
			h.mMax0.Store(int32(h.m.Load() * 2)) // #nosec G115
		}
		h.levelMultiplier = 1.0 / math.Log(float64(h.m.Load()))
	}

	// Initialize types.GraphData with NUMA awareness if configured
	var numaAlloc arrowmemory.Allocator
	if config.NUMANode >= 0 && topo != nil {
		numaAlloc = memory.NewNUMAAllocator(topo, config.NUMANode)
	}

	gd := types.NewGraphData(
		capacity,
		config.Dims,
		false, // mmap
		config.UseDisk,
		0, // fd
		config.Quantization,
		config.SQ8Enabled,
		config.UseDisk, // persistent
		dt,
		config.BQEnabled,
		config.PQEnabled,
		config.TurboQuantEnabled,
		config.TurboQuantBits,
		h.name,
		numaAlloc,
	)
	if h.oopqEncoder != nil {
		switch enc := h.oopqEncoder.(type) {
		case *pq.PQEncoder:
			gd.PQM = enc.CodeSize()
		case *pq.OPQEncoder:
			gd.PQM = enc.CodeSize()
		}
	}

	// Initialize Lock-Free Adjacency for all layers ([#11] Lock-Free Adjacency)
	gd.PackedNeighbors = make([]types.PackedNeighbors, types.ArrowMaxLayers)
	
	for l := 0; l < types.ArrowMaxLayers; l++ {
		var adjArena *memory.SlabArena
		// We use a dedicated SlabArena for each layer to minimize contention
		if config.NUMANode >= 0 && topo != nil {
			numaAlloc := memory.NewNUMAAllocator(topo, config.NUMANode)
			// Lower layers are larger, use larger slabs
			slabSize := 1024 * 1024 * 64
			if l > 0 {
				slabSize = 1024 * 1024 * 8 // Upper layers are smaller but still benefit from larger slabs
			}
			adjArena = memory.NewSlabArenaWithAllocator(slabSize, numaAlloc)
		} else {
			slabSize := 1024 * 1024 * 32
			if l > 0 {
				slabSize = 1024 * 1024 * 4
			}
			adjArena = memory.NewSlabArena(slabSize)
		}
		gd.PackedNeighbors[l] = NewPackedAdjacency(adjArena, capacity)
	}

	for i := range h.entryPointPools {
		h.entryPointPools[i] = NewConcurrentSkipList()
	}

	h.metadataRegistry.Store(&HNSWMetadata{
		EntryPoint: math.MaxUint32,
		MaxLevel:   -1,
		NodeCount:  0,
		Version:    0,
		Generation: 0,
	})
	h.data.Store(gd)

	// Initialize Graph Navigator
	navConfig := NavigatorConfig{
		MaxHops:           10, // Default
		Concurrency:       config.Workers,
		EarlyTerminate:    true,
		DistanceThreshold: 0, // No threshold by default
	}
	dsName := "unknown"
	if dataset != nil {
		dsName = dataset.GetName()
	}
	h.navigator = NewGraphNavigator(dsName, h.GetData, navConfig, config.Registerer)
	_ = h.navigator.Initialize()

	if config.DataType == types.VectorTypeTQ {
		bits := 3
		h.tqEncoder = NewTurboQuantEncoder(config.Dims, bits, 42)
		h.data.Load().TurboQuantEnabled = true
		h.data.Load().TurboQuantBits = bits
		h.tqCompute = NewTurboQuantCompute(h)
	}

	h.commitCond = sync.NewCond(&h.commitMu)
	return h
}

// getCandidateSlice retrieves a pooled candidate slice.
func (h *ArrowHNSW) getCandidateSlice(capacity int) []types.Candidate {
	ptr := h.candidatePool.Get().(*[]types.Candidate)
	s := *ptr
	if cap(s) < capacity {
		s = make([]types.Candidate, 0, capacity)
	} else {
		s = s[:0]
	}
	return s
}


// SetDisableNodeCountMetric prevents this ArrowHNSW from reporting HNSWNodeCount.
func (h *ArrowHNSW) SetDisableNodeCountMetric(disable bool) {
	h.disableNodeCountMetric.Store(disable)
}

// SetData sets the graph data for the index
func (h *ArrowHNSW) SetData(data *types.GraphData) {
	h.data.Store(data)
}

// GetData returns the current graph data
func (h *ArrowHNSW) GetData() *types.GraphData {
	return h.data.Load()
}

// IsSharded returns whether the index is sharded (currently always false).
func (h *ArrowHNSW) IsSharded() bool {
	return false
}


// GetConfig returns the current configuration
func (h *ArrowHNSW) GetConfig() types.ArrowHNSWConfig {
	return h.config
}


// GetShardedLocks returns the sharded mutex for concurrent access

// GetDiskGraph returns the disk graph if enabled
func (h *ArrowHNSW) GetDiskGraph() *DiskGraph {
	return h.diskGraph.Load()
}

// SetDiskGraph sets the disk graph
func (h *ArrowHNSW) SetDiskGraph(disk *DiskGraph) {
	h.diskGraph.Store(disk)
}







// SetOPQEncoder sets the OPQ encoder (accepts both OPQ and legacy PQ for backward compatibility)

// setDims sets the vector dimensions
func (h *ArrowHNSW) setDims(dims int32) {
	h.dims.Store(dims)
	h.updateMetadata(func(meta *HNSWMetadata) {
		// Dims are not in HNSWMetadata currently, should we add them?
		// The plan mentions entryPoint, maxLevel, and nodeCount.
	})
}

// GetMetadataSnapshot returns a consistent snapshot of the index metadata.
func (h *ArrowHNSW) GetMetadataSnapshot() *HNSWMetadata {
	return h.metadataRegistry.Load()
}

// SetDimension sets the absolute dimension of the index.
func (h *ArrowHNSW) SetDimension(dim int) error {
	if dim > math.MaxInt32 {
		return fmt.Errorf("dimension %d exceeds MaxInt32", dim)
	}
	h.setDims(int32(dim)) // #nosec G115
	// Also ensure types.GraphData is updated to reflect this dimension
	// This is critical if the index was initialized with 0 dims but large capacity (default config).
	h.initMu.Lock()
	defer h.initMu.Unlock()
	h.resolveAllDistanceFuncs()
	data := h.data.Load()
	if data != nil {
		if err := h.Grow(data.Capacity, dim); err != nil {
			return err
		}
	}
	return nil
}

// Delete invokes Delete for a single id.
func (h *ArrowHNSW) Delete(id uint32) error {
	h.deletedMu.Lock()
	defer h.deletedMu.Unlock()
	if h.deleted == nil {
		h.deleted = roaring.New()
	}
	h.deleted.Add(id)
	h.locationStore.Delete(types.VectorID(id))
	return nil
}





// DeleteBatch invokes Delete for each id.
func (h *ArrowHNSW) DeleteBatch(ctx context.Context, ids []uint32) error {
	for _, id := range ids {
		if err := h.Delete(id); err != nil {
			return err
		}
	}
	return nil
}

func (h *ArrowHNSW) commitID(id uint32) {
	h.commitMu.Lock()
	for h.GetMetadataSnapshot().NodeCount < int64(id) {
		h.commitCond.Wait()
	}

	h.updateMetadata(func(meta *HNSWMetadata) {
		if meta.NodeCount == int64(id) {
			meta.NodeCount++

			// Entry Point Promotion: Only promote if this node reached a higher level than current EP
			data := h.data.Load()
			if data != nil {
				cID := int(id) / types.ChunkSize
				cOff := int(id) % types.ChunkSize
				levels := data.GetLevelsChunk(cID)
				nodeLevel := 0
				if levels != nil {
					nodeLevel = int(atomic.LoadUint32(&levels[cOff]))
				}

				epLevel := -1
				ep := meta.EntryPoint
				if ep != math.MaxUint32 {
					epCID := int(ep) / types.ChunkSize
					epCOff := int(ep) % types.ChunkSize
					epLevels := data.GetLevelsChunk(epCID)
					if epLevels != nil {
						epLevel = int(atomic.LoadUint32(&epLevels[epCOff]))
					}
				}

				if ep == math.MaxUint32 || nodeLevel > epLevel {
					meta.EntryPoint = id
					meta.MaxLevel = int32(nodeLevel)
				}
			}
		}
	})
	
	// Sync atomics with the newly committed metadata
	meta := h.GetMetadataSnapshot()
	h.nodeCount.Store(meta.NodeCount)
	h.entryPoint.Store(meta.EntryPoint)
	h.maxLevel.Store(meta.MaxLevel)

	h.commitCond.Broadcast()
	h.commitMu.Unlock()
}

func (h *ArrowHNSW) updateMetadata(update func(*HNSWMetadata)) {
	for {
		oldMeta := h.metadataRegistry.Load()
		newMeta := &HNSWMetadata{}
		if oldMeta != nil {
			*newMeta = *oldMeta
		}
		update(newMeta)
		newMeta.Version++
		if h.metadataRegistry.CompareAndSwap(oldMeta, newMeta) {
			// Backwards compatibility: keep legacy atomics in sync
			h.entryPoint.Store(newMeta.EntryPoint)
			h.maxLevel.Store(newMeta.MaxLevel)
			h.nodeCount.Store(newMeta.NodeCount)
			break
		}
	}
}

// updateMetadataIfHigher updates the entry point and max level if the provided node has a higher level.
func (h *ArrowHNSW) updateMetadataIfHigher(id uint32, level int32) {
	h.updateMetadata(func(meta *HNSWMetadata) {
		if meta.EntryPoint == math.MaxUint32 || level > meta.MaxLevel {
			meta.MaxLevel = level
			meta.EntryPoint = id
		}
	})
}

// AddByLocation adds a vector by its location in the dataset.
func (h *ArrowHNSW) AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	next := h.nextID.Add(1)
	if next > math.MaxUint32 {
		return 0, fmt.Errorf("index overflow: nextID %d exceeds uint32 max", next)
	}
	id := uint32(next - 1) // #nosec G115
	defer h.commitID(id)

	var vec any
	if h.dataset != nil {
		records := h.dataset.GetRecords()
		if batchIdx < len(records) {
			record := records[batchIdx]
			// Find vector column
			vecColIdx := h.getVectorColumnIndex(record)
			if vecColIdx != -1 {
				vec = h.extractVector(record, vecColIdx, rowIdx)
			}
		}
	}

	h.SetLocation(types.VectorID(id), types.Location{BatchIdx: batchIdx, RowIdx: rowIdx})

	shard := id % ShardedLockCount
	lockStart := time.Now()
	h.insertMus[shard].Lock()
	metrics.InsertMuWaitDurationSeconds.WithLabelValues(h.name).Observe(time.Since(lockStart).Seconds())
	err := h.InsertWithVector(id, vec, h.generateLevel())
	h.insertMus[shard].Unlock()
	if err != nil {
		return 0, err
	}

	return id, nil
}

// AddByRecord implements VectorIndex.
func (h *ArrowHNSW) AddByRecord(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	next := h.nextID.Add(1)
	if next > math.MaxUint32 {
		return 0, fmt.Errorf("index overflow: nextID %d exceeds uint32 max", next)
	}
	id := uint32(next - 1) // #nosec G115
	defer h.commitID(id)

	var vec any
	// Find vector column
	vecColIdx := h.getVectorColumnIndex(rec)
	if vecColIdx != -1 {
		vec = h.extractVector(rec, vecColIdx, rowIdx)
	}

	h.SetLocation(types.VectorID(id), types.Location{BatchIdx: batchIdx, RowIdx: rowIdx})

	shard := id % ShardedLockCount
	lockStart := time.Now()
	h.insertMus[shard].Lock()
	metrics.InsertMuWaitDurationSeconds.WithLabelValues(h.name).Observe(time.Since(lockStart).Seconds())
	err := h.InsertWithVector(id, vec, h.generateLevel())
	h.insertMus[shard].Unlock()
	if err != nil {
		return 0, err
	}

	return id, nil
}


// MinCandidateHeap for exploration (closest first)
// Uses store.Candidate (ID, Dist) to match ArrowSearchContext





// PreWarm proactively loads chunks of data into memory based on target size.
func (h *ArrowHNSW) PreWarm(targetSize int) {
	start := time.Now()
	defer func() {
		metrics.HNSWPreWarmDuration.Observe(time.Since(start).Seconds())
		metrics.HNSWPreWarmTotal.Inc()
	}()

	data := h.data.Load()
	if data == nil {
		return
	}

	targetChunks := (targetSize + types.ChunkSize - 1) / types.ChunkSize
	pageSize := 4096

	h.growMu.RLock()
	defer h.growMu.RUnlock()

	var dummy uint32

	// Prewarm Neighbors (Layer 0) - this is usually the largest memory block
	if len(data.Neighbors) > 0 {
		for i := 0; i < targetChunks && i < len(data.Neighbors[0]); i++ {
			chunk := data.GetNeighborsChunk(0, i)
			if chunk == nil {
				continue
			}
			stride := pageSize / 4
			for j := 0; j < len(chunk); j += stride {
				dummy += chunk[j] // Force read
			}
			if len(chunk) > 0 {
				dummy += chunk[len(chunk)-1]
			}
		}
	}

	// Prewarm Vectors (Float32 is the most common use-case)
	if data.Type == types.VectorTypeFloat32 || data.Type == types.VectorTypeUnknown {
		for i := 0; i < targetChunks && i < len(data.VectorsF32); i++ {
			chunk := data.GetVectorsChunk(i)
			if chunk == nil {
				continue
			}
			stride := pageSize / 4
			for j := 0; j < len(chunk); j += stride {
				dummy += uint32(chunk[j]) // Force read
			}
			if len(chunk) > 0 {
				dummy += uint32(chunk[len(chunk)-1])
			}
		}
	}

	_ = dummy
}

// CleanupTombstones removes deleted nodes that exceed the specified threshold.
func (h *ArrowHNSW) CleanupTombstones(threshold int) int {
	h.dataset.RLockData()
	
	shouldReset := false
	totalPruned := 0
	for _, ts := range h.dataset.GetTombstones() {
		if ts == nil {
			continue
		}
		count := int(ts.Count()) // #nosec G115
		if count > threshold {
			shouldReset = true
			totalPruned = count
			break
		}
	}
	h.dataset.RUnlockData()

	if shouldReset {
		h.dataset.ResetTombstones()
	}
	return totalPruned
}

// NeedsCompaction returns true if the index has accumulated significant tombstoned entries.
func (h *ArrowHNSW) NeedsCompaction() bool {
	if h.dataset == nil {
		return false
	}
	var total int64
	for _, ts := range h.dataset.GetTombstones() {
		if ts != nil {
			total += int64(ts.Count()) // #nosec G115
		}
	}
	nodeCount := h.GetMetadataSnapshot().NodeCount
	return total > 5000 || (nodeCount > 0 && float64(total)/float64(nodeCount) > 0.1)
}

// SetIndexedColumns is a no-op as column indexing is managed at a higher level.
func (h *ArrowHNSW) SetIndexedColumns(columns []string) {
	// No-op: Column indexing is handled at the VectorStore level, not ArrowHNSW.
	// VectorStore.SetIndexedColumns() stores columns in s.indexedColumns,
	// and VectorStore.applyBatchToIndex() calls columnIndex.IndexRecord().
	// ArrowHNSW doesn't maintain its own column index - it's managed by VectorStore.
}

func (h *ArrowHNSW) generateLevel() int {
	l := int(math.Floor(-math.Log(rand.Float64()) * h.levelMultiplier)) // #nosec G404
	if l >= types.ArrowMaxLayers {
		l = types.ArrowMaxLayers - 1
	}
	return l
}

// AddBatch implements VectorIndex.
func (h *ArrowHNSW) AddBatch(ctx context.Context, recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	h.bulkMu.Lock()
	defer h.bulkMu.Unlock()
	n := len(rowIdxs)
	if n == 0 {
		return nil, nil
	}

	// Discover vector column
	var schemaSource arrow.RecordBatch
	for _, r := range recs {
		if r != nil {
			schemaSource = r
			break
		}
	}

	vecColIdx := h.getVectorColumnIndex(schemaSource)

	if vecColIdx == -1 {
		var colNames []string
		if schemaSource != nil {
			for i := 0; i < int(schemaSource.NumCols()); i++ {
				colNames = append(colNames, schemaSource.ColumnName(i))
			}
		}
		return nil, fmt.Errorf("no vector column found (looked for 'vector', 'embedding', 'vec'); available columns: %v", colNames)
	}

	var startID uint32
	// Allocate local IDs for the entire batch to ensure monotonic assignment and avoid overwrites
	startID = uint32(h.nextID.Add(int64(n)) - int64(n)) // #nosec G115

	// Ensure the index is grown to accommodate the new batch before parallel ingestion.
	if n > 0 && vecColIdx != -1 {
		h.growMu.Lock()
		data := h.data.Load()
		if data == nil || int(startID)+n > data.Capacity || h.dims.Load() == 0 {
			// Extract first vector to determine dimensions
			var recFirst arrow.RecordBatch
			if len(recs) == 1 {
				recFirst = recs[0]
			} else if batchIdxs[0] >= 0 && batchIdxs[0] < len(recs) {
				recFirst = recs[batchIdxs[0]]
			}
			
			if recFirst != nil {
				v := h.extractVector(recFirst, vecColIdx, rowIdxs[0])
				if v != nil {
					dims := 0
					switch vt := v.(type) {
					case []float32: dims = len(vt)
					case []float16.Num: dims = len(vt)
					case []float64: dims = len(vt)
					case []int32: dims = len(vt)
					case []uint32: dims = len(vt)
					case []int16: dims = len(vt)
					case []uint16: dims = len(vt)
					case []int8: dims = len(vt)
					case []uint8: dims = len(vt)
					case []int64: dims = len(vt)
					case []uint64: dims = len(vt)
					case []complex64: dims = len(vt)
					case []complex128: dims = len(vt)
					}
					
					if dims > 0 {
						newCap := int(startID) + n
						if data != nil && data.Capacity > 0 {
							newCap = int(math.Max(float64(newCap), float64(data.Capacity*2)))
						}
						newCap = (newCap + types.ChunkSize - 1) & ^(types.ChunkSize - 1)
						_ = h.growInternal(newCap, dims)
					}
				}
			}
		}
		h.growMu.Unlock()
	}

	// Bulk optimization path - only use for large batches.
	// AddBatchBulk handles its own bootstrap sequentially if the index is empty.
	if n >= 1000 && !h.IsSharded() {

		if vecColIdx != -1 {
			// Extract all vectors into a typed slice for bulk processing
			var vecs any
			supported := true
			switch h.config.DataType {
			case types.VectorTypeFloat32:
				f32s := make([][]float32, n)
				// Cache raw slices per record batch to avoid expensive column calls
				valuesCache := make(map[arrow.RecordBatch][]float32)
				// Pre-populate valuesCache
				for i := range rowIdxs {
					// Robust record resolution: if we only have one record, use it regardless of batchIdx
					var rec arrow.RecordBatch
					bIdx := batchIdxs[i]
					if bIdx >= 0 && bIdx < len(recs) && recs[bIdx] != nil {
						rec = recs[bIdx]
					} else if len(recs) == 1 {
						rec = recs[0]
					}

					if rec != nil {
						if _, ok := valuesCache[rec]; !ok {
							col := rec.Column(vecColIdx)
							if f32Arr, okCol := col.(*arrowarray.Float32); okCol {
								valuesCache[rec] = f32Arr.Float32Values()
							}
						}
					}
				}
				physicalDims := int(h.dims.Load())
				
				// Parallel extraction
				pool := GetSharedPool()
				var supportedAtomic atomic.Bool
				supportedAtomic.Store(true)
				
				pool.ParallelFor(n, max(256, (n+runtime.NumCPU()-1)/runtime.NumCPU()), func(start, end int) {
					if !supportedAtomic.Load() {
						return
					}
					
					for i := start; i < end; i++ {
					// Robust record resolution
					var rec arrow.RecordBatch
					bIdx := batchIdxs[i]
					if bIdx >= 0 && bIdx < len(recs) && recs[bIdx] != nil {
						rec = recs[bIdx]
					} else if len(recs) == 1 {
						rec = recs[0]
					}

						if rec == nil {
							supportedAtomic.Store(false)
							return
						}

						values := valuesCache[rec]
						
						if values != nil {
							off := rowIdxs[i] * physicalDims
							if off+physicalDims <= len(values) {
								f32s[i] = values[off : off+physicalDims]
								h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]}) // #nosec G115
							} else {
								supportedAtomic.Store(false)
								return
							}
						} else {
							// Fallback to slow path if type mismatch
							if v, okC := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]float32); okC {
								f32s[i] = v
								h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]}) // #nosec G115
							} else {
								supportedAtomic.Store(false)
								return
							}
						}
					}
				})
				supported = supportedAtomic.Load()
				vecs = f32s

				// Zero-Copy Direct Mapping Optimization
				// If we are ingesting a full contiguous block that aligns with HNSW chunks,
				// we map the Arrow memory instead of copying into arenas.
				if len(recs) == 1 && startID%uint32(types.ChunkSize) == 0 && n >= types.ChunkSize {
					isContiguous := rowIdxs[0]%types.ChunkSize == 0
					if isContiguous {
						for j := 1; j < n; j++ {
							if rowIdxs[j] != rowIdxs[j-1]+1 {
								isContiguous = false
								break
							}
						}
					}

					if isContiguous {
						rec := recs[0]
						values := valuesCache[rec]
						if values != nil {
							data := h.data.Load()
							numFullChunks := n / types.ChunkSize
							for c := 0; c < numFullChunks; c++ {
								cID := int(startID)/types.ChunkSize + c
								rowOffset := rowIdxs[0] + (c * types.ChunkSize)
								
								offset := rowOffset * physicalDims
								dataSize := types.ChunkSize * physicalDims
								if offset+dataSize <= len(values) {
									chunkData := values[offset : offset+dataSize]
									col := rec.Column(vecColIdx)
									_ = data.SetZeroCopyMapping(cID, chunkData, col)
								}
							}
						}
					}
				}
			case types.VectorTypeFloat16:
				f16s := make([][]float16.Num, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]float16.Num); ok {
						f16s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = f16s
			case types.VectorTypeInt8:
				i8s := make([][]int8, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]int8); ok {
						i8s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = i8s
			case types.VectorTypeFloat64:
				f64s := make([][]float64, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]float64); ok {
						f64s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = f64s
			case types.VectorTypeComplex64:
				c64s := make([][]complex64, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]complex64); ok {
						c64s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = c64s
			case types.VectorTypeComplex128:
				c128s := make([][]complex128, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]complex128); ok {
						c128s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = c128s
			case types.VectorTypeUint32:
				u32s := make([][]uint32, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]uint32); ok {
						u32s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = u32s
			case types.VectorTypeInt32:
				i32s := make([][]int32, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]int32); ok {
						i32s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = i32s
			case types.VectorTypeInt16:
				i16s := make([][]int16, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]int16); ok {
						i16s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = i16s
			case types.VectorTypeUint16:
				u16s := make([][]uint16, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]uint16); ok {
						u16s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = u16s
			case types.VectorTypeInt64:
				i64s := make([][]int64, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]int64); ok {
						i64s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = i64s
			case types.VectorTypeUint64:
				u64s := make([][]uint64, n)
				for i := range rowIdxs {
					rec := recs[batchIdxs[i]]
					if v, ok := h.extractVector(rec, vecColIdx, rowIdxs[i]).([]uint64); ok {
						u64s[i] = v
						h.SetLocation(types.VectorID(startID+uint32(i)), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
					} else {
						supported = false
						break
					}
				}
				vecs = u64s
			default:
				supported = false
			}

			if supported && vecs != nil {
				err := h.addBatchBulkInternal(ctx, startID, n, vecs)
				if err == nil {
					ids := make([]uint32, n)
					for i := 0; i < n; i++ {
						ids[i] = startID + uint32(i)
					}
					return ids, nil
				}
				fmt.Printf("AddBatchBulk failed for %s: %v\n", h.config.DataType.String(), err)
			}
		}
	}

	// Use optimized bulk ingestion path
	err := h.addBatchBulkInternal(ctx, startID, len(rowIdxs), recs)
	if err == nil {
		ids := make([]uint32, len(rowIdxs))
		for i := range rowIdxs {
			ids[i] = startID + uint32(i)
		}
		return ids, nil
	}
	
	// Fallback to sequential insertion if bulk fails (rare)
	ids := make([]uint32, len(rowIdxs))
	maxID := startID + uint32(len(rowIdxs)) - 1 // #nosec G115
	fmt.Printf("AddBatch startID=%d n=%d\n", startID, len(rowIdxs))
	data, err := h.EnsureChunks(int(types.ChunkID(startID)), int(types.ChunkID(maxID)), int(h.dims.Load()))
	if err == nil {
		data = data.Clone()
	}
	if err != nil {
		return nil, err
	}
	// h.data.Store(data)

	// Phase 1: Sequential Vector Ingestion
	// Ensures all vectors are persistent in arenas before we start linking nodes.
	for i := 0; i < len(rowIdxs); i++ {
		id := startID + uint32(i) // #nosec G115
		
		// Resolve record batch
		var rec arrow.RecordBatch
		bIdx := batchIdxs[i]
		if bIdx >= 0 && bIdx < len(recs) && recs[bIdx] != nil {
			rec = recs[bIdx]
		} else if len(recs) == 1 {
			rec = recs[0]
		}
		
		if rec == nil {
			continue
		}

		v := h.extractVector(rec, vecColIdx, rowIdxs[i])
		if v != nil {
			if err := data.SetVector(id, v); err != nil {
				return nil, err
			}
			h.SetLocation(types.VectorID(id), types.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
		}
	}

	// Publish the populated snapshot
	h.compareAndSwapData(h.data.Load(), data.Clone())

	// Phase 1.5: Sequential Bootstrap
	// If the index is empty or very small, we must insert some nodes sequentially
	// to establish an entry point and basic graph structure before parallel insertion.
	bootstrapEnd := 0
	nodeCount := h.GetMetadataSnapshot().NodeCount
	seedCount := 256
	if nodeCount < int64(seedCount) {
		bootstrapEnd = seedCount - int(nodeCount)
		if bootstrapEnd > len(rowIdxs) {
			bootstrapEnd = len(rowIdxs)
		}
		if bootstrapEnd < 0 {
			bootstrapEnd = 0
		}
	}

	for i := 0; i < len(rowIdxs); i++ {
		id := startID + uint32(i) // #nosec G115
		
		var rec arrow.RecordBatch
		bIdx := batchIdxs[i]
		if bIdx >= 0 && bIdx < len(recs) && recs[bIdx] != nil {
			rec = recs[bIdx]
		} else if len(recs) == 1 {
			rec = recs[0]
		}
		
		if rec == nil {
			return nil, fmt.Errorf("could not resolve record batch for index %d", i)
		}

		vec := h.extractVector(rec, vecColIdx, rowIdxs[i])
		if vec == nil {
			return nil, fmt.Errorf("vector missing for row %d", rowIdxs[i])
		}

		// Insert node-by-node. InsertWithVector uses in-place arena updates (lock-free)
		// and commitID for sequential metadata commitment (serialized).
		if err := h.InsertWithVector(id, vec, -1); err != nil {
			return nil, err
		}
		ids[i] = id
	}

	return ids, nil
}

// EstimateMemory returns an estimated memory usage of the index in bytes.
func (h *ArrowHNSW) EstimateMemory() int64 {
	if h == nil {
		return 0
	}
	
	var total int64
	gd := h.data.Load()
	if gd != nil {
		total += gd.EstimateMemory()
	}

	var locMemory int64
	if h.locationStore != nil {
		locCount := h.locationStore.Len()
		locMemory = int64(locCount) * 8
	}

	return total + locMemory
}




// GetParallelSearchConfig implements VectorIndex.
func (h *ArrowHNSW) GetParallelSearchConfig() types.ParallelSearchConfig {
	return h.parallelConfig
}

// SetParallelSearchConfig implements VectorIndex.
func (h *ArrowHNSW) SetParallelSearchConfig(cfg types.ParallelSearchConfig) {
	h.parallelConfig = cfg
}

// GetLocationForParallel retrieves a vector location for parallel processing.
func (h *ArrowHNSW) GetLocationForParallel(id uint32) (types.Location, bool) {
	return h.locationStore.Get(types.VectorID(id))
}

// ExtractVectorForParallel extracts a vector for parallel search refinement.
func (h *ArrowHNSW) ExtractVectorForParallel(rec arrow.RecordBatch, rowIdx int) ([]float32, error) {
	if rec == nil {
		return nil, fmt.Errorf("record is nil")
	}
	vecColIdx := h.getVectorColumnIndex(rec)
	if vecColIdx == -1 {
		if rec.NumCols() == 1 {
			vecColIdx = 0
		} else {
			return nil, fmt.Errorf("vector column not found in record")
		}
	}

	vec, err := ExtractVectorRaw(rec, rowIdx, vecColIdx)
	if err != nil {
		return nil, err
	}

	// Complex128 stored in Arrow as FixedSizeList<float64>. Convert to []float32 for the SIMD path.
	if h.config.DataType == types.VectorTypeComplex128 {
		vf64, ok := vec.([]float64)
		if !ok {
			return nil, fmt.Errorf("expected []float64 for Complex128, got %T", vec)
		}
		f32 := make([]float32, len(vf64))
		for i, v := range vf64 {
			f32[i] = float32(v)
		}
		return f32, nil
	}

	// Convert other types to float32 for parallel refinement
	switch v := vec.(type) {
	case []float32:
		return v, nil
	case []float64:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []float16.Num:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = val.Float32()
		}
		return res, nil
	case []int32:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []uint32:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []int8:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []uint8:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	}

	return nil, fmt.Errorf("unsupported vector type %T for parallel search refinement", vec)
}


// ExtractVectorByIDForParallel extracts a vector by ID as a float32 slice.
func (h *ArrowHNSW) ExtractVectorByIDForParallel(id uint32) ([]float32, error) {
	vecAny, err := h.GetVector(id)
	if err != nil {
		return nil, err
	}
	if v, ok := vecAny.([]float32); ok {
		return v, nil
	}

	// Handle Complex128/64 and other types by flattening to float32
	switch v := vecAny.(type) {
	case []complex128:
		res := make([]float32, len(v)*2)
		for i, val := range v {
			res[i*2] = float32(real(val))
			res[i*2+1] = float32(imag(val))
		}
		return res, nil
	case []complex64:
		// Complex64 is 2x float32 in memory, treat it as a flattened float32 slice
		if len(v) == 0 {
			return nil, nil
		}
		res := unsafe.Slice((*float32)(unsafe.Pointer(&v[0])), len(v)*2) // #nosec G103
		// We should return a copy since the parallel worker might use it concurrently
		resCopy := make([]float32, len(res))
		copy(resCopy, res)
		return resCopy, nil
	case []float64:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []float16.Num:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = val.Float32()
		}
		return res, nil
	case []int8:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []uint8:
		if h.quantizer != nil && h.sq8Ready.Load() {
			return h.quantizer.Decode(v), nil
		}
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []int32:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []uint32:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	}

	return nil, fmt.Errorf("unsupported vector type %T for parallel search", vecAny)
}



// RemapLocations updates the vector ID to location mapping.
func (h *ArrowHNSW) RemapLocations(ctx context.Context, mapping map[uint32]any) error {
	if h.locationStore == nil {
		return fmt.Errorf("location store not initialized")
	}

	for id, locAny := range mapping {
		if loc, ok := locAny.(types.Location); ok {
			h.locationStore.Set(types.VectorID(id), loc)
		} else if loc, ok := locAny.(*types.Location); ok && loc != nil {
			h.locationStore.Set(types.VectorID(id), *loc)
		}
	}
	return nil
}

// SetEfConstruction sets the efConstruction parameter.
func (h *ArrowHNSW) SetEfConstruction(ef int32) {
	h.efConstruction.Store(ef)
}

// GetNUMANode returns the NUMA node and topology for the index.
func (h *ArrowHNSW) GetNUMANode() (int, *memory.NUMATopology) {
	return h.config.NUMANode, h.topo
}
