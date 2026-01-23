package store

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

const (
	ChunkShift = 10
	ChunkSize  = 1 << ChunkShift
	ChunkMask  = ChunkSize - 1

	ShardedLockCount = 1024
)

func chunkID(id uint32) uint32 {
	return id >> ChunkShift
}

func chunkOffset(id uint32) uint32 {
	return id & ChunkMask
}

type ArrowHNSWConfig struct {
	M                       int
	MMax                    int
	MMax0                   int
	EfConstruction          int
	Metric                  DistanceMetric
	InitialCapacity         int
	SQ8Enabled              bool
	SQ8TrainingThreshold    int
	PQEnabled               bool
	PQM                     int
	PQK                     int
	AdaptiveEf              bool
	AdaptiveEfMin           int
	AdaptiveEfThreshold     int
	AdaptiveMEnabled        bool
	AdaptiveMThreshold      int
	RepairInterval          time.Duration
	RepairBatchSize         int
	SelectionHeuristicLimit int
	Alpha                   float32
	RefinementFactor        float64 // Changed to float64 to match usage
	KeepPrunedConnections   bool
	BQEnabled               bool
	Float16Enabled          bool // Enable native float16 storage
	PackedAdjacencyEnabled  bool // Enable optimized packed adjacency storage

	Dims int // Explicit dimension size if known

	IndexedColumns []string // Columns to build bitmap index for

	DataType VectorDataType // underlying vector element type

	Logger zerolog.Logger
}

func DefaultArrowHNSWConfig() ArrowHNSWConfig {
	return ArrowHNSWConfig{
		M:                      32,
		MMax:                   64,
		MMax0:                  128,
		EfConstruction:         400,
		Metric:                 MetricEuclidean,
		Alpha:                  1.0,
		RefinementFactor:       2.0,
		PackedAdjacencyEnabled: true,
		DataType:               VectorTypeFloat32,
	}
}

type ArrowSearchContext struct {
	visited          *ArrowBitset
	candidates       *FixedHeap
	resultSet        *MaxHeap
	results          []SearchResult // Added for compatibility
	scratchIDs       []uint32
	scratchDists     []float32
	scratchNeighbors []uint32
	scratchResults   []Candidate
	scratchSelected  []Candidate
	scratchRemaining []Candidate
	// scratchSelectedVecs  [][]float32 // Replaced by selectedBatch
	// scratchRemainingVecs [][]float32 // Replaced by remainingBatch
	selectedBatch    VectorBatch
	remainingBatch   VectorBatch
	scratchDiscarded []Candidate
	scratchVecs      [][]float32
	scratchVecsSQ8   [][]byte
	scratchVecsF16   [][]float16.Num

	querySQ8       []byte
	queryBQ        []uint64
	queryF16       []float16.Num
	queryF32       []float32
	adcTable       []float32
	scratchPQCodes []byte
	pruneDepth     int
	visitedList    []uint32 // Track visited IDs for sparse clearing
}

func (c *ArrowSearchContext) EnsureCapacity(dim int) {
	// Only resize if current capacity is insufficient
	if cap(c.querySQ8) < dim {
		c.querySQ8 = make([]byte, 0, dim)
		metrics.HNSWSearchScratchSpaceResizesTotal.WithLabelValues("query_sq8").Inc()
	}
	if cap(c.queryF16) < dim {
		c.queryF16 = make([]float16.Num, 0, dim)
		metrics.HNSWSearchScratchSpaceResizesTotal.WithLabelValues("query_f16").Inc()
	}
	if cap(c.queryF32) < dim {
		c.queryF32 = make([]float32, 0, dim)
		metrics.HNSWSearchScratchSpaceResizesTotal.WithLabelValues("query_f32").Inc()
	}
	// For Float32 query (scratch space for distance calculations if needed)
	if cap(c.adcTable) < dim {
		c.adcTable = make([]float32, 0, dim)
		metrics.HNSWSearchScratchSpaceResizesTotal.WithLabelValues("adc_table").Inc()
	}
}

func NewArrowSearchContext() *ArrowSearchContext {
	return &ArrowSearchContext{
		visited:          NewArrowBitset(128000),
		candidates:       NewFixedHeap(400),
		resultSet:        NewMaxHeap(400),
		scratchIDs:       make([]uint32, 0, MaxNeighbors),
		scratchDists:     make([]float32, 0, MaxNeighbors),
		scratchNeighbors: make([]uint32, 0, MaxNeighbors),
		scratchResults:   make([]Candidate, 0, 400),
		scratchSelected:  make([]Candidate, 0, 100),
		scratchDiscarded: make([]Candidate, 0, 100),
		scratchVecs:      make([][]float32, 0, MaxNeighbors),
		scratchVecsSQ8:   make([][]byte, 0, MaxNeighbors),
		querySQ8:         make([]byte, 0, 1536), // Common high-dim size
		queryF16:         make([]float16.Num, 0, 1536),
		queryF32:         make([]float32, 0, 1536),
		adcTable:         make([]float32, 0, 1536),
		visitedList:      make([]uint32, 0, 2048),
	}
}

func NewArrowSearchContextPool() *sync.Pool {
	return &sync.Pool{
		New: func() any {
			metrics.HNSWSearchPoolNewTotal.Inc()
			return NewArrowSearchContext()
		},
	}
}

func (c *ArrowSearchContext) Reset() {
	if c.visited != nil {
		c.visited.ClearSIMD()
	}
	if c.candidates != nil {
		c.candidates.Clear()
	}
	if c.resultSet != nil {
		c.resultSet.Clear()
	}
	c.results = c.results[:0]
	c.scratchIDs = c.scratchIDs[:0]
	c.scratchDists = c.scratchDists[:0]
	c.scratchNeighbors = c.scratchNeighbors[:0]
	c.scratchResults = c.scratchResults[:0]
	c.scratchSelected = c.scratchSelected[:0]
	c.scratchRemaining = c.scratchRemaining[:0]
	// 2D slices: Reset length to 0. NOTE: Inner slices are not reclaimed, but we overwrite them anyway
	c.scratchVecs = c.scratchVecs[:0]
	c.scratchVecsSQ8 = c.scratchVecsSQ8[:0]
	c.scratchVecsF16 = c.scratchVecsF16[:0]

	// Reset batches if they exist
	if c.selectedBatch != nil {
		c.selectedBatch.Reset()
	}
	if c.remainingBatch != nil {
		c.remainingBatch.Reset()
	}

	c.scratchDiscarded = c.scratchDiscarded[:0]
	c.scratchVecs = c.scratchVecs[:0]
	c.scratchVecsSQ8 = c.scratchVecsSQ8[:0]
	// Note: query fields (querySQ8, queryBQ, queryF16, adcTable) are NOT cleared here
	// because Reset() is called between layers during the same search.
	// They are overwritten/resized at the start of Search() if needed.

	c.scratchPQCodes = c.scratchPQCodes[:0]
	c.querySQ8 = c.querySQ8[:0]
	c.queryBQ = c.queryBQ[:0]
	c.queryF16 = c.queryF16[:0]
	c.queryF32 = c.queryF32[:0]
	c.adcTable = c.adcTable[:0]
	c.pruneDepth = 0
}

// ResetVisited performs a sparse clear of the visited bitset.
func (c *ArrowSearchContext) ResetVisited() {
	if len(c.visitedList) == 0 {
		return // Already clean
	}

	// Sparse clear: only unset bits that were set
	for _, id := range c.visitedList {
		c.visited.Unset(id)
	}
	c.visitedList = c.visitedList[:0]
}

// Visit marks an ID as visited and tracks it for sparse clearing.
func (c *ArrowSearchContext) Visit(id uint32) {
	if !c.visited.IsSet(id) {
		c.visited.Set(id)
		c.visitedList = append(c.visitedList, id)
	}
}

// VisitedCount returns the number of nodes visited in the current context.
func (c *ArrowSearchContext) VisitedCount() int {
	return len(c.visitedList)
}

type ArrowHNSW struct {
	config  ArrowHNSWConfig
	dataset *Dataset

	dims       atomic.Int32
	ml         float64
	maxLevel   atomic.Int32
	entryPoint atomic.Uint32
	nodeCount  atomic.Int64

	initMu sync.Mutex
	growMu sync.RWMutex

	shardedLocks *AlignedShardedMutex

	data       atomic.Pointer[GraphData]
	deleted    *query.AtomicBitset
	searchPool *sync.Pool

	quantizer          *ScalarQuantizer
	sq8TrainingBuffer  [][]float32
	pqEncoder          *pq.PQEncoder
	bqEncoder          *BQEncoder
	adaptiveMTriggered atomic.Bool
	sq8Ready           atomic.Bool

	locationStore *ChunkedLocationStore

	metric        DistanceMetric
	distFunc      func([]float32, []float32) (float32, error)
	distFuncF16   func([]float16.Num, []float16.Num) (float32, error)
	distFuncF64   func([]float64, []float64) (float32, error)
	distFuncC64   func([]complex64, []complex64) (float32, error)
	distFuncC128  func([]complex128, []complex128) (float32, error)
	batchDistFunc func([]float32, [][]float32, []float32) error
	batchComputer any

	m              int
	mMax           int
	mMax0          int
	efConstruction atomic.Int32

	backend      atomic.Pointer[GraphData]
	diskGraph    atomic.Pointer[DiskGraph] // Read-only mmap backing store
	vectorColIdx int

	// Cached Metrics (Curried)
	metricInsertDuration prometheus.Observer
	metricNodeCount      prometheus.Gauge
	metricGraphHeight    prometheus.Gauge
	metricActiveReaders  prometheus.Gauge
	metricLockWait       prometheus.ObserverVec
	// Bulk path metrics
	metricBulkInsertDuration prometheus.Observer
	metricBulkVectors        prometheus.Counter
	metricBQVectors          prometheus.Gauge
	metricBitmapEntries      prometheus.Gauge
	metricBitmapFilterDelta  prometheus.Observer
	metricEarlyTermination   *prometheus.CounterVec

	bitmapIndex *BitmapIndex

	// Connectivity Repair Agent (optional)
	repairAgent *RepairAgent
	// ... (rest of fields)
}

type GraphData struct {
	Capacity   int
	Dims       int
	PaddedDims int // Dims padded for 64-byte memory alignment (stride)
	PQDims     int
	Type       VectorDataType

	// Context (Optional, for memory tracking)
	// We might store *Dataset or similar if needed, but keeping it simple.

	// Arenas
	SlabArena       *memory.SlabArena
	Uint8Arena      *memory.TypedArena[uint8]
	Int8Arena       *memory.TypedArena[int8]
	Int16Arena      *memory.TypedArena[int16]
	Uint16Arena     *memory.TypedArena[uint16]
	Float32Arena    *memory.TypedArena[float32]
	Float16Arena    *memory.TypedArena[float16.Num]
	Float64Arena    *memory.TypedArena[float64]
	Uint32Arena     *memory.TypedArena[uint32]
	Int32Arena      *memory.TypedArena[int32]
	Uint64Arena     *memory.TypedArena[uint64]
	Int64Arena      *memory.TypedArena[int64]
	Complex64Arena  *memory.TypedArena[complex64]
	Complex128Arena *memory.TypedArena[complex128]

	// Offsets (Atomic Uint64)
	// 0 means unallocated (nil).
	Levels         []uint64 // []*[]uint8 -> []offset
	Vectors        []uint64 // []*[]float32 -> []offset
	VectorsInt8    []uint64 // []*[]int8 -> []offset
	VectorsUint8   []uint64 // []*[]uint8 -> []offset
	VectorsInt16   []uint64 // []*[]int16 -> []offset
	VectorsUint16  []uint64 // []*[]uint16 -> []offset
	VectorsInt32   []uint64 // []*[]int32 -> []offset
	VectorsUint32  []uint64 // []*[]uint32 -> []offset
	VectorsInt64   []uint64 // []*[]int64 -> []offset
	VectorsUint64  []uint64 // []*[]uint64 -> []offset
	VectorsFloat64 []uint64 // []*[]float64 -> []offset
	VectorsF16     []uint64 // []*[]float16.Num -> []offset
	VectorsSQ8     []uint64 // []*[]byte -> []offset
	VectorsPQ      []uint64 // []*[]byte -> []offset
	VectorsBQ      []uint64 // []*[]uint64 -> []offset
	VectorsC64     []uint64 // []*[]complex64 -> []offset
	VectorsC128    []uint64 // []*[]complex128 -> []offset

	// Disk Storage (Phase 6)
	DiskStore *DiskVectorStore

	// Read-only Mmap Backing Graph (Phase 3 - Zero-Copy)
	BackingGraph *DiskGraph

	Neighbors [ArrowMaxLayers][]uint64 // [][]*[]uint32 -> [][]offset
	Counts    [ArrowMaxLayers][]uint64 // [][]*[]int32 -> [][]offset
	Versions  [ArrowMaxLayers][]uint64 // [][]*[]uint32 -> [][]offset

	// Packed Neighbors (v0.1.4-rc1 optimization)
	PackedNeighbors [ArrowMaxLayers]*PackedAdjacency

	// Lock-free neighbor caches (v0.1.4-rc5 optimization)
	// Provides zero-lock reads for hot search paths
	LockFreeNeighborCaches [ArrowMaxLayers]*LockFreeNeighborCache
}

const (
	ArrowMaxLayers = 10
	MaxNeighbors   = 448
)

func NewGraphData(capacity, dims int, sq8Enabled, pqEnabled bool, pqDims int, bqEnabled, float16Enabled, packedAdjacencyEnabled bool, dataType VectorDataType) *GraphData {
	numChunks := (capacity + ChunkSize - 1) / ChunkSize
	if numChunks < 1 {
		numChunks = 1
	}

	// Calculate padded dimensions (stride)
	// Align byte size to 64 bytes (cache line)
	elementSize := dataType.ElementSize()
	rawBytes := dims * elementSize
	paddedBytes := (rawBytes + 63) & ^63
	paddedDims := dims
	if elementSize > 0 {
		paddedDims = paddedBytes / elementSize
	}

	// Use standard slabs to enable pooling and fast allocation.
	// Ensure slab size is large enough to hold at least a full chunk of ALL data types
	// (vectors, neighbors, counts, levels, etc) to ensure we can always satisfy
	// a full chunk allocation even from a single slab.
	slabSize := 4 * 1024 * 1024 // Default 4MB
	requiredForVectors := ChunkSize * paddedBytes
	// Neighbors: MaxLayers * MaxNeighbors * uint32 (4 bytes) * ChunkSize
	requiredForNeighbors := ArrowMaxLayers * MaxNeighbors * 4 * ChunkSize

	// Total required for one full chunk of everything
	totalRequired := requiredForVectors + requiredForNeighbors + (ChunkSize * 32) // buffer for levels, counts etc
	if totalRequired > slabSize {
		// Use 1.5x for headroom to avoid tight fits
		slabSize = (totalRequired * 3) / 2
	}

	// Snap to standard bucket sizes to enable global pooling in SlabPool
	switch {
	case slabSize <= 4*1024*1024:
		slabSize = 4 * 1024 * 1024
	case slabSize <= 8*1024*1024:
		slabSize = 8 * 1024 * 1024
	case slabSize <= 16*1024*1024:
		slabSize = 16 * 1024 * 1024
	case slabSize <= 32*1024*1024:
		slabSize = 32 * 1024 * 1024
	}

	slab := memory.NewSlabArena(slabSize)

	var f16Arena *memory.TypedArena[float16.Num]
	if float16Enabled || dataType == VectorTypeFloat16 {
		f16Arena = memory.NewTypedArena[float16.Num](slab)
	}

	gd := &GraphData{
		Capacity:   numChunks * ChunkSize,
		Dims:       dims,
		PaddedDims: paddedDims,
	}
	gd.PQDims = pqDims
	gd.Type = dataType
	gd.SlabArena = slab
	gd.Uint8Arena = memory.NewTypedArena[uint8](slab)
	gd.Int8Arena = memory.NewTypedArena[int8](slab)
	gd.Int16Arena = memory.NewTypedArena[int16](slab)
	gd.Uint16Arena = memory.NewTypedArena[uint16](slab)
	gd.Float32Arena = memory.NewTypedArena[float32](slab)
	gd.Float16Arena = f16Arena
	gd.Float64Arena = memory.NewTypedArena[float64](slab)
	gd.Uint32Arena = memory.NewTypedArena[uint32](slab)
	gd.Int32Arena = memory.NewTypedArena[int32](slab)
	gd.Uint64Arena = memory.NewTypedArena[uint64](slab)
	gd.Int64Arena = memory.NewTypedArena[int64](slab)
	gd.Complex64Arena = memory.NewTypedArena[complex64](slab)
	gd.Complex128Arena = memory.NewTypedArena[complex128](slab)
	gd.Levels = make([]uint64, numChunks)
	if dims > 0 {
		// Allocate primary vector storage based on data type
		switch dataType {
		case VectorTypeFloat32:
			gd.Vectors = make([]uint64, numChunks)
		case VectorTypeInt8:
			gd.VectorsInt8 = make([]uint64, numChunks)
		case VectorTypeUint8:
			gd.VectorsUint8 = make([]uint64, numChunks)
		case VectorTypeInt16:
			gd.VectorsInt16 = make([]uint64, numChunks)
		case VectorTypeUint16:
			gd.VectorsUint16 = make([]uint64, numChunks)
		case VectorTypeInt32:
			gd.VectorsInt32 = make([]uint64, numChunks)
		case VectorTypeUint32:
			gd.VectorsUint32 = make([]uint64, numChunks)
		case VectorTypeInt64:
			gd.VectorsInt64 = make([]uint64, numChunks)
		case VectorTypeUint64:
			gd.VectorsUint64 = make([]uint64, numChunks)
		case VectorTypeFloat64:
			gd.VectorsFloat64 = make([]uint64, numChunks)
		case VectorTypeComplex64:
			gd.VectorsC64 = make([]uint64, numChunks)
		case VectorTypeComplex128:
			gd.VectorsC128 = make([]uint64, numChunks)
		}

		// Allocate auxiliary storage
		if float16Enabled || dataType == VectorTypeFloat16 {
			gd.VectorsF16 = make([]uint64, numChunks)
		}

		if sq8Enabled {
			gd.VectorsSQ8 = make([]uint64, numChunks)
		}
		if pqEnabled && pqDims > 0 {
			gd.VectorsPQ = make([]uint64, numChunks)
		}
		if bqEnabled {
			gd.VectorsBQ = make([]uint64, numChunks)
		}
	}

	for i := 0; i < ArrowMaxLayers; i++ {
		gd.Neighbors[i] = make([]uint64, numChunks)
		gd.Counts[i] = make([]uint64, numChunks)
		gd.Versions[i] = make([]uint64, numChunks)

		if packedAdjacencyEnabled {
			gd.PackedNeighbors[i] = NewPackedAdjacency(slab, capacity)
		}

		// Initialize lock-free neighbor cache for this layer
		gd.LockFreeNeighborCaches[i] = NewLockFreeNeighborCache()
	}
	return gd
}

// CleanupTombstones iterates over the graph and removes connections to deleted nodes.
// It returns the total number of connections prone.
// maxNodes is a limit on how many nodes to scan (0 = all).
func (h *ArrowHNSW) CleanupTombstones(maxNodes int) int {
	totalPruned := 0

	// Snapshot max node ID to iterate up to
	maxID := int(h.nodeCount.Load())
	if maxNodes > 0 && maxNodes < maxID {
		maxID = maxNodes
	}

	data := h.data.Load()
	if data == nil {
		return 0
	}

	// Iterate all potential node IDs
	for i := 0; i < maxID; i++ {
		nodeID := uint32(i)

		// If node itself is deleted, we don't need to clean its neighbors right now
		// (it's unreachable from entry point eventually), OR we might want to clean them to help GC.
		// For Search performance, we care about LIVE nodes pointing to DEAD nodes.
		if h.deleted.Contains(int(nodeID)) {
			continue
		}

		// Lock this node to safely modify its neighbor list
		lockID := nodeID % 1024
		h.shardedLocks.Lock(uint64(lockID))

		// Re-check deleted status after lock? Unlikely to change from false -> true without lock?
		// Delete() uses lock? No, Delete() typically atomic bitset.
		// If it became deleted while we waited, we can skip.
		if h.deleted.Contains(int(nodeID)) {
			h.shardedLocks.Unlock(uint64(lockID))
			continue
		}

		// Iterate Levels
		for lvl := 0; lvl < ArrowMaxLayers; lvl++ {
			cID := chunkID(nodeID)
			cOff := chunkOffset(nodeID)

			// Check if chunk exists (0 offset means nil)
			if lvl >= len(data.Neighbors) || int(cID) >= len(data.Neighbors[lvl]) || data.Neighbors[lvl][cID] == 0 {
				continue
			}

			countsChunk := data.GetCountsChunk(lvl, cID)
			if countsChunk == nil {
				continue
			}
			countAddr := &countsChunk[cOff]
			count := int(atomic.LoadInt32(countAddr))
			if count == 0 {
				continue
			}

			neighborsChunk := data.GetNeighborsChunk(lvl, cID)
			if neighborsChunk == nil {
				continue
			}

			baseIdx := int(cOff) * MaxNeighbors

			// 1. Scan first to see if we NEED to prune (avoid dirtying cache lines/versions if clean)
			needsPrune := false
			for r := 0; r < count; r++ {
				if h.deleted.Contains(int(neighborsChunk[baseIdx+r])) {
					needsPrune = true
					break
				}
			}

			if !needsPrune {
				continue
			}

			versionsChunk := data.GetVersionsChunk(lvl, cID)
			if versionsChunk == nil {
				continue
			}

			// 2. Seqlock: Begin Write (Odd)
			verAddr := &versionsChunk[cOff]
			atomic.AddUint32(verAddr, 1)

			// 3. Compact the list
			writeIdx := 0
			prunedInLevel := 0
			for r := 0; r < count; r++ {
				neighborID := neighborsChunk[baseIdx+r]
				if h.deleted.Contains(int(neighborID)) {
					prunedInLevel++
				} else {
					if writeIdx != r {
						// Atomic store to avoid race with readers
						atomic.StoreUint32(&neighborsChunk[baseIdx+writeIdx], neighborID)
					}
					writeIdx++
				}
			}

			// Zero out remainder
			for k := writeIdx; k < count; k++ {
				atomic.StoreUint32(&neighborsChunk[baseIdx+k], 0)
			}

			// Update count
			atomic.StoreInt32(countAddr, int32(writeIdx))
			totalPruned += prunedInLevel

			// 4. Seqlock: End Write (Even)
			atomic.AddUint32(verAddr, 1)
		}

		h.shardedLocks.Unlock(uint64(lockID))
	}

	return totalPruned
}

func (g *GraphData) Size() int {
	return g.Capacity
}

func (g *GraphData) GetCapacity() int {
	return g.Capacity
}

func (g *GraphData) GetPaddedDims() int {
	if g.PaddedDims > 0 {
		return g.PaddedDims
	}
	return g.GetPaddedDimsForType(g.Type)
}

// GetPaddedDimsForType returns the 64-byte aligned stride for a specific type
func (g *GraphData) GetPaddedDimsForType(dt VectorDataType) int {
	elementSize := dt.ElementSize()
	if elementSize <= 0 {
		elementSize = 4 // Float32 default
	}
	rawBytes := g.Dims * elementSize
	paddedBytes := (rawBytes + 63) & ^63
	return paddedBytes / elementSize
}

func (g *GraphData) Close() error {
	// SlabArena freeing is now handled by the index owner (ArrowHNSW.Close)
	// to avoid premature freeing during Grow operations where arenas are shared.
	// We no longer nil out fields here because concurrent readers might still have
	// a reference to the old GraphData during a Grow transition.
	return nil
}

func (h *ArrowHNSW) Grow(minCap, dims int) {
	start := time.Now()
	h.growMu.Lock()
	defer h.growMu.Unlock()
	h.growNoLock(minCap, dims)
	metrics.HNSWIndexGrowthDuration.Observe(time.Since(start).Seconds())
}

func (h *ArrowHNSW) growNoLock(minCap, dims int) {
	data := h.data.Load()
	// Check if we need to upgrade schema (enable PQ/SQ8/BQ)
	needsUpgrade := (h.config.PQEnabled && data.VectorsPQ == nil) ||
		(h.config.SQ8Enabled && data.VectorsSQ8 == nil) ||
		(h.config.BQEnabled && data.VectorsBQ == nil) || (h.config.Float16Enabled && data.VectorsF16 == nil)

	if !needsUpgrade && data.Capacity >= minCap && data.Dims == dims && data.PQDims == h.config.PQM {
		return
	}

	// Determine new capacity with geometric growth
	currentCap := data.Capacity
	newCap := currentCap

	// If current capacity is small, grow aggressively (2x)
	// If large (>1M vectors), grow more conservatively (1.5x) to save memory
	const LargeCapacityThreshold = 1_000_000

	if minCap > currentCap {
		if currentCap < LargeCapacityThreshold {
			newCap = currentCap * 2
		} else {
			newCap = currentCap + (currentCap / 2) // 1.5x
		}

		// Ensure we satisfy the request
		if newCap < minCap {
			newCap = minCap
		}

		// Round up to ChunkSize
		if newCap%ChunkSize != 0 {
			newCap = ((newCap / ChunkSize) + 1) * ChunkSize
		}
	}

	newGD := data.Clone(newCap, dims, h.config.SQ8Enabled, h.config.PQEnabled, h.config.PQM, h.config.BQEnabled, h.config.Float16Enabled, h.config.PackedAdjacencyEnabled)

	oldGD := h.data.Swap(newGD)
	oldBackend := h.backend.Swap(newGD)

	if oldGD != nil && oldGD != newGD {
		_ = oldGD.Close()
	}
	if oldBackend != nil && oldBackend != newGD && oldBackend != oldGD {
		_ = oldBackend.Close()
	}
	if dims > 0 {
		h.dims.Store(int32(dims))
	}
}

// Helper to safely get a pointer to the slice header for a chunk.
// WARNING: The returned slice is only valid until the arena is reallocated/moved (though SlabArena handles stability).
func (gd *GraphData) GetNeighborsChunk(layer int, chunkID uint32) []uint32 {
	if layer >= len(gd.Neighbors) || int(chunkID) >= len(gd.Neighbors[layer]) {
		return nil
	}
	offset := atomic.LoadUint64(&gd.Neighbors[layer][chunkID])
	if offset == 0 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    ChunkSize * MaxNeighbors,
		Cap:    ChunkSize * MaxNeighbors,
	}
	return gd.Uint32Arena.Get(ref)
}

func (gd *GraphData) GetCountsChunk(layer int, chunkID uint32) []int32 {
	if layer >= len(gd.Counts) || int(chunkID) >= len(gd.Counts[layer]) {
		return nil
	}
	offset := atomic.LoadUint64(&gd.Counts[layer][chunkID])
	if offset == 0 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    ChunkSize,
		Cap:    ChunkSize,
	}
	return gd.Int32Arena.Get(ref)
}

func (gd *GraphData) GetVersionsChunk(layer int, chunkID uint32) []uint32 {
	if layer >= len(gd.Versions) || int(chunkID) >= len(gd.Versions[layer]) {
		return nil
	}
	offset := atomic.LoadUint64(&gd.Versions[layer][chunkID])
	if offset == 0 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    ChunkSize,
		Cap:    ChunkSize,
	}
	return gd.Uint32Arena.Get(ref)
}

func (gd *GraphData) GetLevelsChunk(chunkID uint32) []uint8 {
	if int(chunkID) >= len(gd.Levels) {
		return nil
	}
	offset := atomic.LoadUint64(&gd.Levels[chunkID])
	if offset == 0 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    ChunkSize,
		Cap:    ChunkSize,
	}
	return gd.Uint8Arena.Get(ref)
}

// SetNeighbors sets the neighbor list for a node at a specific layer.
// This is a low-level method that directly manipulates the graph data structure.
// For high-level operations, use ArrowHNSW.AddConnection instead.
func (gd *GraphData) SetNeighbors(layer int, id uint32, neighbors []uint32) {
	if len(neighbors) > MaxNeighbors {
		neighbors = neighbors[:MaxNeighbors] // Truncate if too many
	}

	cID := chunkID(id)
	cOff := chunkOffset(id)

	// Ensure chunks exist
	gd.EnsureChunk(cID, int(id)+1)

	versionsChunk := gd.GetVersionsChunk(layer, cID)
	countsChunk := gd.GetCountsChunk(layer, cID)
	neighborsChunk := gd.GetNeighborsChunk(layer, cID)

	if versionsChunk == nil || countsChunk == nil || neighborsChunk == nil {
		return // Cannot set neighbors if chunks don't exist
	}

	// Update version for consistency
	atomic.AddUint32(&versionsChunk[cOff], 1)

	// Set count
	atomic.StoreInt32(&countsChunk[cOff], int32(len(neighbors)))

	// Set neighbors
	baseIdx := int(cOff) * MaxNeighbors
	for i, neighbor := range neighbors {
		if baseIdx+i < len(neighborsChunk) {
			neighborsChunk[baseIdx+i] = neighbor
		}
	}
}

func (gd *GraphData) GetNeighbors(layer int, id uint32, buffer []uint32) []uint32 {
	cID := chunkID(id)
	cOff := chunkOffset(id)

	// We need Versions, Counts, Neighbors for consistent read
	versionsChunk := gd.GetVersionsChunk(layer, cID)
	countsChunk := gd.GetCountsChunk(layer, cID)
	neighborsChunk := gd.GetNeighborsChunk(layer, cID)

	if versionsChunk == nil || countsChunk == nil || neighborsChunk == nil {
		return buffer[:0]
	}

	// Read version for consistency check
	version := atomic.LoadUint32(&versionsChunk[cOff])

	// Read count
	count := atomic.LoadInt32(&countsChunk[cOff])

	// Read neighbors
	baseIdx := int(cOff) * MaxNeighbors
	neighbors := neighborsChunk[baseIdx : baseIdx+int(count)]

	// Verify version hasn't changed
	if atomic.LoadUint32(&versionsChunk[cOff]) != version {
		// Inconsistent read, return empty
		return buffer[:0]
	}

	// Copy to buffer if provided, otherwise return slice
	if buffer != nil && len(buffer) >= len(neighbors) {
		copy(buffer, neighbors)
		return buffer[:len(neighbors)]
	}
	return neighbors
}

func (h *ArrowHNSW) ensureChunk(data *GraphData, cID, _ uint32, dims int) *GraphData {
	// Fast path check to avoid locking if chunk is already allocated.
	// This relies on Levels being the first thing allocated.
	if len(data.Levels) > int(cID) && atomic.LoadUint64(&data.Levels[cID]) != 0 {
		return data
	}

	// Slow path: lock to ensure atomic chunk allocation
	h.initMu.Lock()
	defer h.initMu.Unlock()

	// Double-check after lock
	if len(data.Levels) > int(cID) && atomic.LoadUint64(&data.Levels[cID]) != 0 {
		return data
	}

	// Need to grow data structure
	oldCap := data.Capacity
	newCap := oldCap
	if newCap <= int(cID) {
		newCap = int(cID) + 1
	}

	// Grow if needed
	if newCap > oldCap {
		h.growNoLock(newCap, dims)
		data = h.data.Load()
	}

	// Allocate chunk data if not already done
	// Use Double-Check Locking pattern with atomic CAS

	// Levels
	if atomic.LoadUint64(&data.Levels[cID]) == 0 {
		ref, err := data.Uint8Arena.AllocSlice(ChunkSize)
		if err == nil {
			if atomic.CompareAndSwapUint64(&data.Levels[cID], 0, ref.Offset) {
				if h.dataset != nil {
					h.dataset.IndexMemoryBytes.Add(int64(ChunkSize))
				}
			}
		}
	}

	// Vectors (based on data type)
	switch data.Type {
	case VectorTypeFloat32:
		if len(data.Vectors) > int(cID) && atomic.LoadUint64(&data.Vectors[cID]) == 0 {
			ref, err := data.Float32Arena.AllocSlice(ChunkSize * data.PaddedDims)
			if err == nil {
				if atomic.CompareAndSwapUint64(&data.Vectors[cID], 0, ref.Offset) {
					if h.dataset != nil {
						h.dataset.IndexMemoryBytes.Add(int64(ChunkSize * data.PaddedDims * 4)) // 4 bytes per float32
					}
				}
			}
		}
	case VectorTypeInt8:
		if len(data.VectorsInt8) > int(cID) && atomic.LoadUint64(&data.VectorsInt8[cID]) == 0 {
			ref, err := data.Int8Arena.AllocSlice(ChunkSize * data.PaddedDims)
			if err == nil {
				if atomic.CompareAndSwapUint64(&data.VectorsInt8[cID], 0, ref.Offset) {
					if h.dataset != nil {
						h.dataset.IndexMemoryBytes.Add(int64(ChunkSize * data.PaddedDims * 1)) // 1 byte per int8
					}
				}
			}
		}
	case VectorTypeUint8:
		if len(data.VectorsUint8) > int(cID) && atomic.LoadUint64(&data.VectorsUint8[cID]) == 0 {
			ref, err := data.Uint8Arena.AllocSlice(ChunkSize * data.PaddedDims)
			if err == nil {
				if atomic.CompareAndSwapUint64(&data.VectorsUint8[cID], 0, ref.Offset) {
					if h.dataset != nil {
						h.dataset.IndexMemoryBytes.Add(int64(ChunkSize * data.PaddedDims * 1)) // 1 byte per uint8
					}
				}
			}
		}
	case VectorTypeFloat16:
		if len(data.VectorsF16) > int(cID) && atomic.LoadUint64(&data.VectorsF16[cID]) == 0 {
			ref, err := data.Float16Arena.AllocSlice(ChunkSize * data.PaddedDims)
			if err == nil {
				if atomic.CompareAndSwapUint64(&data.VectorsF16[cID], 0, ref.Offset) {
					if h.dataset != nil {
						h.dataset.IndexMemoryBytes.Add(int64(ChunkSize * data.PaddedDims * 2)) // 2 bytes per float16
					}
				}
			}
		}
		// Add other types as needed...
	}

	return data
}

func (h *ArrowHNSW) GetPQEncoder() *pq.PQEncoder {
	// No lock needed if pqEncoder is immutable after training?
	// It is set during TrainPQ and never changed.
	return h.pqEncoder
}

func (h *ArrowHNSW) Size() int {
	return int(h.nodeCount.Load())
}

// GetEntryPoint returns the current entry point of the HNSW graph.
func (h *ArrowHNSW) GetEntryPoint() uint32 {
	return h.entryPoint.Load()
}

// GetMaxLevel returns the current maximum level of the HNSW graph.
func (h *ArrowHNSW) GetMaxLevel() int {
	return int(h.maxLevel.Load())
}

// Delete marks a vector as deleted in the index.
// This is a soft delete that will be cleaned up during compaction.
func (h *ArrowHNSW) Delete(id uint32) error {
	h.deleted.Set(int(id))
	return nil
}

// IsDeleted checks if a vector is marked as deleted.
// Checks both soft deletes (bitset) and hard deletes (tombstone marker).
func (h *ArrowHNSW) IsDeleted(id uint32) bool {
	// First check soft delete (bitset)
	if h.deleted.Contains(int(id)) {
		return true
	}

	// Then check hard delete (tombstone marker)
	data := h.data.Load()
	if data == nil || int(id) >= data.Capacity {
		return true
	}
	cID := chunkID(id)
	cOff := chunkOffset(id)
	levelsChunk := data.GetLevelsChunk(cID)
	if levelsChunk == nil {
		return true
	}
	return levelsChunk[cOff] == 255 // Tombstone marker
}

func (gd *GraphData) GetLevel(id uint32) int {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	levelsChunk := gd.GetLevelsChunk(cID)
	if levelsChunk != nil {
		return int(levelsChunk[cOff])
	}
	return 0
}

func (h *ArrowHNSW) NeedsCompaction() bool {
	// Heuristic: if deleted nodes > 30% of total nodes OR capacity > 2x expected size
	data := h.data.Load()
	if data == nil || data.Capacity == 0 {
		return false
	}
	totalNodes := int(h.nodeCount.Load())
	if totalNodes == 0 {
		return false
	}
	deletedCount := h.deleted.Count()
	deletedRatio := float64(deletedCount) / float64(totalNodes)
	sizeRatio := float64(data.Capacity) / float64(totalNodes)
	return deletedRatio > 0.3 || sizeRatio > 2.0
}
