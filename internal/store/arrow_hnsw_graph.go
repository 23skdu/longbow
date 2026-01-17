package store

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/prometheus/client_golang/prometheus"
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

	querySQ8       []byte
	queryBQ        []uint64
	queryF16       []float16.Num
	adcTable       []float32
	scratchPQCodes []byte
	pruneDepth     int
	visitedList    []uint32 // Track visited IDs for sparse clearing
}

func NewArrowSearchContext() *ArrowSearchContext {
	return &ArrowSearchContext{
		visited:          NewArrowBitset(10000),
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
		visitedList:      make([]uint32, 0, 2048),
	}
}

func NewArrowSearchContextPool() *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
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
	c.scratchRemaining = c.scratchRemaining[:0]

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
	growMu sync.Mutex

	shardedLocks []sync.Mutex

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
	distFunc      func([]float32, []float32) float32
	distFuncF16   func([]float16.Num, []float16.Num) float32
	distFuncF64   func([]float64, []float64) float32
	distFuncC64   func([]complex64, []complex64) float32
	distFuncC128  func([]complex128, []complex128) float32
	batchDistFunc func([]float32, [][]float32, []float32)
	batchComputer interface{}

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
	Levels      []uint64 // []*[]uint8 -> []offset
	Vectors     []uint64 // []*[]float32 -> []offset
	VectorsF16  []uint64 // []*[]float16.Num -> []offset
	VectorsSQ8  []uint64 // []*[]byte -> []offset
	VectorsPQ   []uint64 // []*[]byte -> []offset
	VectorsBQ   []uint64 // []*[]uint64 -> []offset
	VectorsC64  []uint64 // []*[]complex64 -> []offset
	VectorsC128 []uint64 // []*[]complex128 -> []offset

	// Disk Storage (Phase 6)
	DiskStore *DiskVectorStore

	// Read-only Mmap Backing Graph (Phase 3 - Zero-Copy)
	BackingGraph *DiskGraph

	Neighbors [ArrowMaxLayers][]uint64 // [][]*[]uint32 -> [][]offset
	Counts    [ArrowMaxLayers][]uint64 // [][]*[]int32 -> [][]offset
	Versions  [ArrowMaxLayers][]uint64 // [][]*[]uint32 -> [][]offset

	// Packed Neighbors (v0.1.4-rc1 optimization)
	PackedNeighbors [ArrowMaxLayers]*PackedAdjacency
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
		gd.Vectors = make([]uint64, numChunks)
		if float16Enabled {
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
		if dataType == VectorTypeComplex64 {
			gd.VectorsC64 = make([]uint64, numChunks)
		}
		if dataType == VectorTypeComplex128 {
			gd.VectorsC128 = make([]uint64, numChunks)
		}
	}

	for i := 0; i < ArrowMaxLayers; i++ {
		gd.Neighbors[i] = make([]uint64, numChunks)
		gd.Counts[i] = make([]uint64, numChunks)
		gd.Versions[i] = make([]uint64, numChunks)

		if packedAdjacencyEnabled {
			gd.PackedNeighbors[i] = NewPackedAdjacency(slab, capacity)
		}
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
		h.shardedLocks[lockID].Lock()

		// Re-check deleted status after lock? Unlikely to change from false -> true without lock?
		// Delete() uses lock? No, Delete() typically atomic bitset.
		// If it became deleted while we waited, we can skip.
		if h.deleted.Contains(int(nodeID)) {
			h.shardedLocks[lockID].Unlock()
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

		h.shardedLocks[lockID].Unlock()
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
	// Nil out all chunks to help GC even if GraphData persists in other references
	g.Levels = nil
	g.Vectors = nil
	g.VectorsSQ8 = nil
	g.VectorsPQ = nil
	g.VectorsBQ = nil

	for i := 0; i < ArrowMaxLayers; i++ {
		g.Neighbors[i] = nil
		g.Counts[i] = nil
		g.Versions[i] = nil
	}
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

	h.data.Store(newGD)
	h.backend.Store(newGD)
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

func (gd *GraphData) GetVectorsChunk(chunkID uint32) []float32 {
	if int(chunkID) >= len(gd.Vectors) {
		return nil
	}
	offset := atomic.LoadUint64(&gd.Vectors[chunkID])
	if offset == 0 || gd.Type != VectorTypeFloat32 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    uint32(ChunkSize * gd.GetPaddedDims()),
		Cap:    uint32(ChunkSize * gd.GetPaddedDims()),
	}
	return gd.Float32Arena.Get(ref)
}

func (gd *GraphData) GetVectorsInt8Chunk(chunkID uint32) []int8 {
	if int(chunkID) >= len(gd.Vectors) {
		return nil
	}
	offset := atomic.LoadUint64(&gd.Vectors[chunkID])
	if offset == 0 || gd.Type != VectorTypeInt8 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    uint32(ChunkSize * gd.GetPaddedDims()),
		Cap:    uint32(ChunkSize * gd.GetPaddedDims()),
	}
	return gd.Int8Arena.Get(ref)
}

func (gd *GraphData) GetVectorsInt16Chunk(chunkID uint32) []int16 {
	if int(chunkID) >= len(gd.Vectors) {
		return nil
	}
	offset := atomic.LoadUint64(&gd.Vectors[chunkID])
	if offset == 0 || gd.Type != VectorTypeInt16 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    uint32(ChunkSize * gd.GetPaddedDims()),
		Cap:    uint32(ChunkSize * gd.GetPaddedDims()),
	}
	return gd.Int16Arena.Get(ref)
}

func (gd *GraphData) GetVectorsInt32Chunk(chunkID uint32) []int32 {
	if int(chunkID) >= len(gd.Vectors) {
		return nil
	}
	offset := atomic.LoadUint64(&gd.Vectors[chunkID])
	if offset == 0 || gd.Type != VectorTypeInt32 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    uint32(ChunkSize * gd.GetPaddedDims()),
		Cap:    uint32(ChunkSize * gd.GetPaddedDims()),
	}
	return gd.Int32Arena.Get(ref)
}

func (gd *GraphData) GetVectorsFloat64Chunk(chunkID uint32) []float64 {
	if int(chunkID) >= len(gd.Vectors) {
		return nil
	}
	offset := atomic.LoadUint64(&gd.Vectors[chunkID])
	if offset == 0 || gd.Type != VectorTypeFloat64 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    uint32(ChunkSize * gd.GetPaddedDims()),
		Cap:    uint32(ChunkSize * gd.GetPaddedDims()),
	}
	return gd.Float64Arena.Get(ref)
}

func (gd *GraphData) GetVectorsComplex64Chunk(chunkID uint32) []complex64 {
	if int(chunkID) >= len(gd.Vectors) {
		return nil
	}
	offset := atomic.LoadUint64(&gd.Vectors[chunkID])
	if offset == 0 || gd.Type != VectorTypeComplex64 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    uint32(ChunkSize * gd.GetPaddedDims()),
		Cap:    uint32(ChunkSize * gd.GetPaddedDims()),
	}
	return gd.Complex64Arena.Get(ref)
}

func (gd *GraphData) GetVectorsComplex128Chunk(chunkID uint32) []complex128 {
	if int(chunkID) >= len(gd.Vectors) {
		return nil
	}
	offset := atomic.LoadUint64(&gd.Vectors[chunkID])
	if offset == 0 || gd.Type != VectorTypeComplex128 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    uint32(ChunkSize * gd.GetPaddedDims()),
		Cap:    uint32(ChunkSize * gd.GetPaddedDims()),
	}
	return gd.Complex128Arena.Get(ref)
}

func (gd *GraphData) GetVectorComplex64(id uint32) []complex64 {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	chunk := gd.GetVectorsComplex64Chunk(cID)
	if chunk == nil {
		return nil
	}
	start := int(cOff) * gd.GetPaddedDims()
	return chunk[start : start+gd.Dims]
}

func (gd *GraphData) GetVectorComplex128(id uint32) []complex128 {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	chunk := gd.GetVectorsComplex128Chunk(cID)
	if chunk == nil {
		return nil
	}
	start := int(cOff) * gd.GetPaddedDims()
	return chunk[start : start+gd.Dims]
}

func (gd *GraphData) GetVectorsF16Chunk(chunkID uint32) []float16.Num {
	// Try VectorsF16 (Aux) first
	if int(chunkID) < len(gd.VectorsF16) {
		if offset := atomic.LoadUint64(&gd.VectorsF16[chunkID]); offset != 0 {
			stride := gd.GetPaddedDimsForType(VectorTypeFloat16)
			return gd.Float16Arena.Get(memory.SliceRef{
				Offset: offset,
				Len:    uint32(ChunkSize * stride),
				Cap:    uint32(ChunkSize * stride),
			})
		}
	}
	// Try Primary Vectors if Type is Float16
	if gd.Type == VectorTypeFloat16 && int(chunkID) < len(gd.Vectors) {
		if offset := atomic.LoadUint64(&gd.Vectors[chunkID]); offset != 0 {
			stride := gd.GetPaddedDims()
			return gd.Float16Arena.Get(memory.SliceRef{
				Offset: offset,
				Len:    uint32(ChunkSize * stride),
				Cap:    uint32(ChunkSize * stride),
			})
		}
	}
	return nil
}

func (gd *GraphData) GetVectorsSQ8Chunk(chunkID uint32) []byte {
	if int(chunkID) >= len(gd.VectorsSQ8) {
		return nil
	}
	offset := atomic.LoadUint64(&gd.VectorsSQ8[chunkID])
	if offset == 0 {
		return nil
	}
	// SQ8 is always padded to 64 bytes
	paddedDims := (gd.Dims + 63) & ^63
	ref := memory.SliceRef{
		Offset: offset,
		Len:    uint32(ChunkSize * paddedDims),
		Cap:    uint32(ChunkSize * paddedDims),
	}
	return gd.Uint8Arena.Get(ref)
}

func (gd *GraphData) GetVectorsPQChunk(chunkID uint32) []byte {
	if int(chunkID) >= len(gd.VectorsPQ) {
		return nil
	}
	offset := atomic.LoadUint64(&gd.VectorsPQ[chunkID])
	if offset == 0 {
		return nil
	}
	if gd.PQDims == 0 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    uint32(ChunkSize * gd.PQDims),
		Cap:    uint32(ChunkSize * gd.PQDims),
	}
	return gd.Uint8Arena.Get(ref)
}

// GraphData doesn't track PQ metadata explicitly except Dims (which is vector dims).
// However, the chunk was allocated with a specific size.
// Arena (SliceRef) stores the length!
// But we only have offset. We LOST the length if we just store offset!
// Wait, SliceRef is {Offset, Len, Cap}. memory.TypedArena.AllocSlice returns SliceRef.
// GraphData ONLY stores Offset (uint64).
// WE LOST THE LENGTH INFO!
// Critical issue: TypedArena.Get(ref) requires Len.
// How do we recover Len from Offset?
//
// Solution A: Store SliceRef (16 bytes) instead of Offset (8 bytes).
// Solution B: Re-calculate len based on fixed ChunkSize * ElementSize.
//
// For Vectors, it is ChunkSize * Dims. Dims is known (gd.Dims).
// For Neighbors, it is ChunkSize * MaxNeighbors. Known.
// For Levels, ChunkSize. Known.

func (gd *GraphData) GetVectorsBQChunk(chunkID uint32) []uint64 {
	if int(chunkID) >= len(gd.VectorsBQ) {
		return nil
	}
	offset := atomic.LoadUint64(&gd.VectorsBQ[chunkID])
	if offset == 0 {
		return nil
	}
	numWords := (gd.Dims + 63) / 64
	ref := memory.SliceRef{
		Offset: offset,
		Len:    uint32(ChunkSize * numWords),
		Cap:    uint32(ChunkSize * numWords),
	}
	return gd.Uint64Arena.Get(ref)
}

// New API for tests
func (gd *GraphData) SetNeighbors(layer int, id uint32, neighbors []uint32) {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	chunk := gd.GetNeighborsChunk(layer, cID)
	if chunk == nil {
		return // Should verify ensureChunk called
	}

	// Copy
	base := int(cOff) * MaxNeighbors
	// Truncate if too long?
	count := len(neighbors)
	if count > MaxNeighbors {
		count = MaxNeighbors
	}

	copy(chunk[base:base+count], neighbors)

	// Update Count
	cChunk := gd.GetCountsChunk(layer, cID)
	if cChunk != nil {
		atomic.StoreInt32(&cChunk[cOff], int32(count))
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
		return nil
	}

	verAddr := &versionsChunk[cOff]
	countAddr := &countsChunk[cOff]
	baseIdx := int(cOff) * MaxNeighbors

	for {
		v1 := atomic.LoadUint32(verAddr)
		if v1%2 != 0 {
			runtime.Gosched()
			continue
		}

		count := atomic.LoadInt32(countAddr)
		need := int(count)
		if need == 0 {
			return nil
		}

		if cap(buffer) < need {
			buffer = make([]uint32, need)
		}
		res := buffer[:need]

		// Atomic copy
		for i := 0; i < need; i++ {
			res[i] = atomic.LoadUint32(&neighborsChunk[baseIdx+i])
		}

		v2 := atomic.LoadUint32(verAddr)
		if v1 == v2 {
			return res
		}
	}
}

func (gd *GraphData) GetVectorBQ(id uint32) []uint64 {
	cID := chunkID(id)
	chunk := gd.GetVectorsBQChunk(cID)
	if chunk == nil {
		return nil
	}
	cOff := chunkOffset(id)
	numWords := (gd.Dims + 63) / 64
	start := int(cOff) * numWords
	if start+numWords > len(chunk) {
		return nil
	}
	// Return copy
	res := make([]uint64, numWords)
	copy(res, chunk[start:start+numWords])
	return res
}

func (gd *GraphData) GetVectorPQ(id uint32) []byte {
	cID := chunkID(id)
	chunk := gd.GetVectorsPQChunk(cID)
	if chunk == nil {
		return nil
	}
	cOff := chunkOffset(id)
	m := gd.PQDims
	start := int(cOff) * m
	if start+m > len(chunk) {
		return nil
	}
	res := make([]byte, m)
	copy(res, chunk[start:start+m])
	return res
}

func (gd *GraphData) GetVectorSQ8(id uint32) []byte {
	cID := chunkID(id)
	chunk := gd.GetVectorsSQ8Chunk(cID)
	if chunk == nil {
		return nil
	}
	cOff := chunkOffset(id)
	// SQ8 is always padded to 64 bytes
	dims := (gd.Dims + 63) & ^63
	start := int(cOff) * dims
	if start+gd.Dims > len(chunk) {
		return nil
	}
	res := make([]byte, gd.Dims)
	copy(res, chunk[start:start+gd.Dims])
	return res
}

func (gd *GraphData) GetLevel(id uint32) int {
	cID := chunkID(id)
	chunk := gd.GetLevelsChunk(cID)
	if chunk == nil {
		return -1
	}
	cOff := chunkOffset(id)
	return int(chunk[cOff])
}
func (h *ArrowHNSW) ensureChunk(data *GraphData, cID, _ uint32, dims int) *GraphData {
	// Use Double-Check Locking pattern with atomic CAS

	// Levels
	if atomic.LoadUint64(&data.Levels[cID]) == 0 {
		ref, err := data.Uint8Arena.AllocSlice(ChunkSize)
		if err == nil {
			atomic.CompareAndSwapUint64(&data.Levels[cID], 0, ref.Offset)
		}
	}

	// Primary Vectors
	if dims > 0 && !h.config.Float16Enabled && len(data.Vectors) > int(cID) && atomic.LoadUint64(&data.Vectors[cID]) == 0 {
		var ref memory.SliceRef
		var err error

		// Use PaddedDims if available for alignment
		allocDims := dims
		if data.PaddedDims > 0 {
			allocDims = data.PaddedDims
		}
		count := ChunkSize * allocDims

		switch data.Type {
		// ... (other types omitted for brevity, logic applies generally if BackingGraph supports them)
		// For now we focus on Float32 as it's the default and failing test case.
		case VectorTypeFloat32:
			// OPTIMIZATION: Use Dirty allocation (no zeroing) for Vectors
			ref, err = data.Float32Arena.AllocSliceDirty(count)
		default:
			// Fallback generic allocator switch from original code
			switch data.Type {
			case VectorTypeFloat16:
				ref, err = data.Float16Arena.AllocSliceDirty(count)
			case VectorTypeInt8:
				ref, err = data.Int8Arena.AllocSliceDirty(count)
			case VectorTypeUint8:
				ref, err = data.Uint8Arena.AllocSliceDirty(count)
			case VectorTypeInt16:
				ref, err = data.Int16Arena.AllocSliceDirty(count)
			case VectorTypeUint16:
				ref, err = data.Uint16Arena.AllocSliceDirty(count)
			case VectorTypeInt32:
				ref, err = data.Int32Arena.AllocSliceDirty(count)
			case VectorTypeUint32:
				ref, err = data.Uint32Arena.AllocSliceDirty(count)
			case VectorTypeInt64:
				ref, err = data.Int64Arena.AllocSliceDirty(count)
			case VectorTypeUint64:
				ref, err = data.Uint64Arena.AllocSliceDirty(count)
			case VectorTypeFloat64:
				ref, err = data.Float64Arena.AllocSliceDirty(count)
			case VectorTypeComplex64:
				ref, err = data.Complex64Arena.AllocSliceDirty(count)
			case VectorTypeComplex128:
				ref, err = data.Complex128Arena.AllocSliceDirty(count)
			}
		}

		if err == nil && ref.Offset != 0 {
			// Try to set the vector offset. If we fail, it means someone else already set it.
			atomic.CompareAndSwapUint64(&data.Vectors[cID], 0, ref.Offset)
			metrics.HNSWArenaAllocationBytes.WithLabelValues(data.Type.String()).Add(float64(count * data.Type.ElementSize()))
		}
	}

	// SQ8
	if h.config.SQ8Enabled && dims > 0 && len(data.VectorsSQ8) > int(cID) && atomic.LoadUint64(&data.VectorsSQ8[cID]) == 0 {
		// SQ8 is always padded to 64 bytes
		paddedDims := (dims + 63) & ^63
		// SQ8 vectors are overwritten entirely
		ref, err := data.Uint8Arena.AllocSliceDirty(ChunkSize * paddedDims)
		if err == nil {
			if atomic.CompareAndSwapUint64(&data.VectorsSQ8[cID], 0, ref.Offset) {
				// Copy-On-Write logic remains same...
				// Copy-On-Write: If we have a backing graph, copy existing vectors into this new chunk
				if data.BackingGraph != nil {
					chunk := data.Uint8Arena.Get(ref)
					backingLimit := data.BackingGraph.Size()
					startID := int(cID) * ChunkSize
					endID := startID + ChunkSize
					if endID > backingLimit {
						endID = backingLimit
					}

					for id := startID; id < endID; id++ {
						vecBytes := data.BackingGraph.GetVectorSQ8(uint32(id))
						// DiskGraph stores DENSE (unpadded) vectors, but we allocate PADDED
						if vecBytes != nil && len(vecBytes) == dims {
							if len(vecBytes) > 0 {
								// Destination uses padded stride
								start := uint32(id-startID) * uint32(paddedDims)
								dest := chunk[start : start+uint32(dims)] // Copy only valid bytes
								copy(dest, vecBytes)
							}
						}
					}
				}
			}
		}
	}

	// PQ
	if h.config.PQEnabled && data.PQDims > 0 && len(data.VectorsPQ) > int(cID) && atomic.LoadUint64(&data.VectorsPQ[cID]) == 0 {
		ref, err := data.Uint8Arena.AllocSliceDirty(ChunkSize * data.PQDims)
		if err == nil {
			atomic.CompareAndSwapUint64(&data.VectorsPQ[cID], 0, ref.Offset)
		}
	}

	// BQ
	if h.config.BQEnabled && dims > 0 && len(data.VectorsBQ) > int(cID) && atomic.LoadUint64(&data.VectorsBQ[cID]) == 0 {
		numWords := (dims + 63) / 64
		ref, err := data.Uint64Arena.AllocSliceDirty(ChunkSize * numWords)
		if err == nil {
			atomic.CompareAndSwapUint64(&data.VectorsBQ[cID], 0, ref.Offset)
		}
	}

	// Float16 (Legacy/Special path)
	if h.config.Float16Enabled && dims > 0 && len(data.VectorsF16) > int(cID) && atomic.LoadUint64(&data.VectorsF16[cID]) == 0 {
		paddedDimsF16 := data.GetPaddedDimsForType(VectorTypeFloat16)
		ref, err := data.Float16Arena.AllocSliceDirty(ChunkSize * paddedDimsF16)
		if err == nil {
			atomic.CompareAndSwapUint64(&data.VectorsF16[cID], 0, ref.Offset)
		}
	}

	// Neighbors context
	for i := 0; i < ArrowMaxLayers; i++ {
		if atomic.LoadUint64(&data.Neighbors[i][cID]) == 0 {
			// Neighbors can be dirty (we rely on Counts)
			ref, err := data.Uint32Arena.AllocSliceDirty(ChunkSize * MaxNeighbors)
			if err == nil {
				atomic.CompareAndSwapUint64(&data.Neighbors[i][cID], 0, ref.Offset)
			}
		}
		if atomic.LoadUint64(&data.Counts[i][cID]) == 0 {
			// Counts MUST be zeroed
			ref, err := data.Int32Arena.AllocSlice(ChunkSize)
			if err == nil {
				atomic.CompareAndSwapUint64(&data.Counts[i][cID], 0, ref.Offset)
			}
		}
		if atomic.LoadUint64(&data.Versions[i][cID]) == 0 {
			// Versions MUST be zeroed
			ref, err := data.Uint32Arena.AllocSlice(ChunkSize)
			if err == nil {
				atomic.CompareAndSwapUint64(&data.Versions[i][cID], 0, ref.Offset)
			}
		}
	}
	return data
}

// GetPQEncoder returns the PQ encoder if one exists.
func (h *ArrowHNSW) GetPQEncoder() *pq.PQEncoder {
	// No lock needed if pqEncoder is immutable after training?
	// It is set during TrainPQ and never changed.
	return h.pqEncoder
}

// ArrowSearchContextPool manages reusable search contexts.
type ArrowSearchContextPool struct {
	pool sync.Pool
}

// CleanupTombstones iterates over the graph and removes connections to deleted nodes.
// It returns the total number of connections prone.
// maxNodes is a limit on how many nodes to scan (0 = all).

// Get retrieves a search context from the pool.
func (p *ArrowSearchContextPool) Get() *ArrowSearchContext {
	return p.pool.Get().(*ArrowSearchContext)
}

// Put returns a search context to the pool.
func (p *ArrowSearchContextPool) Put(ctx *ArrowSearchContext) {
	ctx.candidates.Clear()
	ctx.visited.Clear()
	ctx.results = ctx.results[:0]
	ctx.resultSet.Clear()
	// Clear scratch slices to avoid hanging onto memory
	for i := range ctx.scratchVecs {
		ctx.scratchVecs[i] = nil
	}
	ctx.querySQ8 = nil
	ctx.queryBQ = nil
	p.pool.Put(ctx)
}

// Size returns the number of vectors in the index.
func (h *ArrowHNSW) Size() int {
	return int(h.nodeCount.Load())
}

// IsDeleted checks if a node ID is marked as deleted.

func (h *ArrowHNSW) IsDeleted(id uint32) bool {
	return h.deleted.Contains(int(id))
}

// Delete marks a vector as deleted.
func (h *ArrowHNSW) Delete(id uint32) error {
	h.deleted.Set(int(id))
	return nil
}

// GetEntryPoint returns the current entry point node ID.

// GetEntryPoint returns the current entry point node ID.
func (h *ArrowHNSW) GetEntryPoint() uint32 {
	return h.entryPoint.Load()
}

// GetMaxLevel returns the current maximum level in the graph.
func (h *ArrowHNSW) GetMaxLevel() int {
	return int(h.maxLevel.Load())
}

func (gd *GraphData) GetVectorsUint16Chunk(chunkID uint32) []uint16 {
	if int(chunkID) >= len(gd.Vectors) {
		return nil
	}
	offset := atomic.LoadUint64(&gd.Vectors[chunkID])
	if offset == 0 || gd.Type != VectorTypeUint16 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    uint32(ChunkSize * gd.Dims),
		Cap:    uint32(ChunkSize * gd.Dims),
	}
	return gd.Uint16Arena.Get(ref)
}

func (gd *GraphData) GetVectorsUint32Chunk(chunkID uint32) []uint32 {
	if int(chunkID) >= len(gd.Vectors) {
		return nil
	}
	offset := atomic.LoadUint64(&gd.Vectors[chunkID])
	if offset == 0 || gd.Type != VectorTypeUint32 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    uint32(ChunkSize * gd.Dims),
		Cap:    uint32(ChunkSize * gd.Dims),
	}
	return gd.Uint32Arena.Get(ref)
}

func (gd *GraphData) GetVectorsUint64Chunk(chunkID uint32) []uint64 {
	if int(chunkID) >= len(gd.Vectors) {
		return nil
	}
	offset := atomic.LoadUint64(&gd.Vectors[chunkID])
	if offset == 0 || gd.Type != VectorTypeUint64 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    uint32(ChunkSize * gd.Dims),
		Cap:    uint32(ChunkSize * gd.Dims),
	}
	return gd.Uint64Arena.Get(ref)
}

func (gd *GraphData) GetVectorsInt64Chunk(chunkID uint32) []int64 {
	if int(chunkID) >= len(gd.Vectors) {
		return nil
	}
	offset := atomic.LoadUint64(&gd.Vectors[chunkID])
	if offset == 0 || gd.Type != VectorTypeInt64 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    uint32(ChunkSize * gd.Dims),
		Cap:    uint32(ChunkSize * gd.Dims),
	}
	return gd.Int64Arena.Get(ref)
}

func (gd *GraphData) GetVectorF16(id uint32) []float16.Num {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	vecF16Chunk := gd.GetVectorsF16Chunk(cID)
	if vecF16Chunk == nil {
		return nil
	}
	off := int(cOff) * gd.Dims
	if off+gd.Dims > len(vecF16Chunk) {
		return nil
	}
	return vecF16Chunk[off : off+gd.Dims]
}
