package index

// nosec G404 - math/rand is used for HNSW operations, not security-sensitive
import (
	"context"
	"fmt"
	"math"
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
	"github.com/apache/arrow-go/v18/arrow/float16"
	arrowmemory "github.com/apache/arrow-go/v18/arrow/memory"
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

	quantizer     *ScalarQuantizer
	sq8Ready      atomic.Bool
	bqEncoder     *types.BQEncoder
	oopqEncoder   any // Accepts *pq.PQEncoder or *pq.OPQEncoder (Issue 4: Use new OPQ)
	tqEncoder     *TurboQuantEncoder
	searchPool    *ArrowSearchContextPool
	candidatePool sync.Pool

	name                   string
	disableNodeCountMetric atomic.Bool
	metricsSampleCounter   atomic.Uint64
	topLayerManager        *TopLayerManager
	neighborCache          [types.ArrowMaxLayers]*LockFreeNeighborCache

	distFunc             func([]float32, []float32) (float32, error)
	distFuncSquared      func([]float32, []float32) (float32, error)
	distFuncF64          func([]float64, []float64) (float32, error)
	distFuncF16          func([]float16.Num, []float16.Num) (float32, error)
	distFuncC64          func([]complex64, []complex64) (float32, error)
	distFuncC128         func([]complex128, []complex128) (float32, error)
	distFuncInt8         func([]int8, []int8) (float32, error)
	distFuncInt8Squared  func([]int8, []int8) (float32, error)
	distFuncUint8        func([]uint8, []uint8) (float32, error)
	distFuncUint8Squared func([]uint8, []uint8) (float32, error)
	distFuncInt16        func([]int16, []int16) (float32, error)
	distFuncUint16       func([]uint16, []uint16) (float32, error)
	distFuncInt32        func([]int32, []int32) (float32, error)
	distFuncUint32       func([]uint32, []uint32) (float32, error)
	distFuncInt64        func([]int64, []int64) (float32, error)
	distFuncUint64       func([]uint64, []uint64) (float32, error)

	sharedVectorSpace  atomic.Bool
	adaptiveMTriggered atomic.Bool

	initMu     sync.Mutex
	growMu     sync.RWMutex
	bulkMu     sync.Mutex
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
	navigator  *GraphNavigator
	tqCompute  *TurboQuantCompute
	gpuTrained atomic.Bool
	topo       *memory.NUMATopology
	efTuner    *PIDTuner

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
		config:     config,
		dataset:    dataset,
		m:          atomic.Int32{},
		mMax:       atomic.Int32{},
		mMax0:      atomic.Int32{},
		searchPool: NewArrowSearchContextPool(),
		insertPool: NewInsertContextPool(), // Issue 2: Pool metrics
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
	for i := 0; i < types.ArrowMaxLayers; i++ {
		h.neighborCache[i] = NewLockFreeNeighborCache()
	}
	h.metadataRegistry.Store(&HNSWMetadata{
		EntryPoint: math.MaxUint32,
		MaxLevel:   -1,
		NodeCount:  0,
		Version:    0,
		Generation: 0,
	})

	h.m.Store(int32(config.M))         // #nosec G115
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
	h.sharedVectorSpace.Store(config.SharedVectorSpace)

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

	// Autonomous efSearch Tuning based on DataType
	baseEfSearch := int(config.EfSearch)
	// Low-precision and quantized data types are heavily memory-bound optimized, so they
	// can handle a massively expanded search buffer with very little latency penalty.
	// Higher efSearch improves recall for these types without significant throughput loss.
	switch config.DataType {
	case types.VectorTypeInt8, types.VectorTypeUint8,
		types.VectorTypeInt16, types.VectorTypeUint16,
		types.VectorTypeTQ:
		if baseEfSearch < 600 {
			baseEfSearch = 600
		}
	case types.VectorTypeInt32, types.VectorTypeUint32,
		types.VectorTypeFloat16:
		if baseEfSearch < 300 {
			baseEfSearch = 300
		}
	}
	if config.TurboQuantEnabled && baseEfSearch < 600 {
		baseEfSearch = 600
	}
	h.efTuner = NewPIDTuner(0.95, baseEfSearch) // Target 0.95 recall

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
		h.sharedVectorSpace.Load(),
	)
	if h.oopqEncoder != nil {
		switch enc := h.oopqEncoder.(type) {
		case *pq.PQEncoder:
			gd.PQM = enc.CodeSize()
		case *pq.OPQEncoder:
			gd.PQM = enc.CodeSize()
		}
	}
	gd.SharedVectorSpace = config.SharedVectorSpace
	h.sharedVectorSpace.Store(config.SharedVectorSpace)

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
			offHeapAlloc := memory.NewOffHeapAllocator()
			adjArena = memory.NewSlabArenaWithAllocator(slabSize, offHeapAlloc)
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
		bits := config.TurboQuantBits
		if bits == 0 {
			bits = 8
		}
		h.config.TurboQuantEnabled = true
		h.config.TurboQuantBits = bits
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
	meta := h.metadataRegistry.Load()
	if meta == nil {
		return &HNSWMetadata{
			EntryPoint: math.MaxUint32,
			MaxLevel:   -1,
			NodeCount:  0,
		}
	}
	return meta
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

	if h.config.DataType == types.VectorTypeTQ || h.config.TurboQuantEnabled {
		bits := h.config.TurboQuantBits
		if bits == 0 {
			bits = 8
		}
		h.tqEncoder = NewTurboQuantEncoder(dim, bits, 42)
		h.tqCompute = NewTurboQuantCompute(h)
	}

	data := h.data.Load()
	if data != nil {
		if h.config.DataType == types.VectorTypeTQ || h.config.TurboQuantEnabled {
			data.TurboQuantEnabled = true
			if h.config.TurboQuantBits > 0 {
				data.TurboQuantBits = h.config.TurboQuantBits
			} else {
				data.TurboQuantBits = 8
			}
		}
		if err := h.Grow(data.Capacity, dim); err != nil {
			return err
		}
	}
	return nil
}

// Delete invokes Delete for a single id.

// DeleteBatch invokes Delete for each id.

// updateMetadataIfHigher updates the entry point and max level if the provided node has a higher level.

// AddByLocation adds a vector by its location in the dataset.

// AddByRecord implements VectorIndex.

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

// ReleaseMonolithicChunk releases the storage for a chunk of the index.
// This is used during incremental handover to shards.
func (h *ArrowHNSW) ReleaseMonolithicChunk(cID int) error {
	gd := h.data.Load()
	if gd != nil {
		gd.ReleaseChunk(cID)
		// Also release neighbors for all layers for this chunk
		for l := 0; l < types.ArrowMaxLayers; l++ {
			gd.ReleaseNeighborsChunk(l, cID)
		}
	}
	return nil
}

// CleanupTombstones removes deleted nodes that exceed the specified threshold.

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

// AddBatch implements VectorIndex.

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

func (h *ArrowHNSW) RelocateToOffHeap() error {
	gd := h.data.Load()
	if gd == nil {
		return fmt.Errorf("no graph data to relocate")
	}
	return gd.RelocateToOffHeap()
}
