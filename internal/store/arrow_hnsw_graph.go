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

	QueryCacheEnabled  bool
	QueryCacheCapacity int
	QueryCacheTTL      time.Duration
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
	}
}

type ArrowSearchContext struct {
	visited              *ArrowBitset
	candidates           *FixedHeap
	resultSet            *MaxHeap
	results              []SearchResult // Added for compatibility
	scratchIDs           []uint32
	scratchDists         []float32
	scratchNeighbors     []uint32
	scratchResults       []Candidate
	scratchSelected      []Candidate
	scratchRemaining     []Candidate
	scratchSelectedVecs  [][]float32
	scratchRemainingVecs [][]float32
	scratchDiscarded     []Candidate
	scratchVecs          [][]float32
	scratchVecsSQ8       [][]byte

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
	c.scratchSelectedVecs = c.scratchSelectedVecs[:0]
	c.scratchRemainingVecs = c.scratchRemainingVecs[:0]
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
	batchDistFunc func([]float32, [][]float32, []float32)
	batchComputer interface{}

	m              int
	mMax           int
	mMax0          int
	efConstruction int

	backend      atomic.Pointer[GraphData]
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
	queryCache  *QueryCache
	// ... (rest of fields)
}

type GraphData struct {
	Capacity int
	Dims     int
	PQDims   int

	// Context (Optional, for memory tracking)
	// We might store *Dataset or similar if needed, but keeping it simple.

	// Arenas
	SlabArena    *memory.SlabArena
	Uint8Arena   *memory.TypedArena[uint8]
	Float32Arena *memory.TypedArena[float32]
	Float16Arena *memory.TypedArena[float16.Num]
	Uint32Arena  *memory.TypedArena[uint32]
	Int32Arena   *memory.TypedArena[int32]
	Uint64Arena  *memory.TypedArena[uint64]

	// Offsets (Atomic Uint64)
	// 0 means unallocated (nil).
	Levels     []uint64 // []*[]uint8 -> []offset
	Vectors    []uint64 // []*[]float32 -> []offset
	VectorsF16 []uint64 // []*[]float16.Num -> []offset
	VectorsSQ8 []uint64 // []*[]byte -> []offset
	VectorsPQ  []uint64 // []*[]byte -> []offset
	VectorsBQ  []uint64 // []*[]uint64 -> []offset

	// Disk Storage (Phase 6)
	DiskStore *DiskVectorStore

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

func NewGraphData(capacity, dims int, sq8Enabled, pqEnabled bool, pqDims int, bqEnabled, float16Enabled, packedAdjacencyEnabled bool) *GraphData {
	numChunks := (capacity + ChunkSize - 1) / ChunkSize
	if numChunks < 1 {
		numChunks = 1
	}

	estSize := capacity * dims * 4
	if estSize < 16*1024*1024 {
		estSize = 16 * 1024 * 1024
	}

	slab := memory.NewSlabArena(estSize)

	var f16Arena *memory.TypedArena[float16.Num]
	if float16Enabled {
		f16Arena = memory.NewTypedArena[float16.Num](slab)
	}

	gd := &GraphData{
		Capacity:     numChunks * ChunkSize,
		Dims:         dims,
		PQDims:       pqDims,
		SlabArena:    slab,
		Uint8Arena:   memory.NewTypedArena[uint8](slab),
		Float32Arena: memory.NewTypedArena[float32](slab),
		Float16Arena: f16Arena,

		Uint32Arena: memory.NewTypedArena[uint32](slab),
		Int32Arena:  memory.NewTypedArena[int32](slab),
		Uint64Arena: memory.NewTypedArena[uint64](slab),

		Levels: make([]uint64, numChunks),
	}
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

// Clone creates a deep copy of GraphData with new capacity.
// Note: This is expensive and involves full Arena copy or re-allocation.
// For SlabArena, we might just create new Arena and copy active data.
func (gd *GraphData) Clone(minCap, targetDims int, sq8Enabled, pqEnabled bool, pqDims int, bqEnabled bool) *GraphData {
	// Reuse the same arenas to keep offsets valid
	// gd.SlabArena.AddRef() // SlabArena is simple struct/pointer, no ref counting needed exposed.
	// Actually SlabArena is a pointer. Reusing it is fine.
	// We do NOT use NewGraphData because it creates a new SlabArena.

	numChunks := (minCap + ChunkSize - 1) / ChunkSize
	if numChunks < 1 {
		numChunks = 1
	}

	newGD := &GraphData{
		Capacity:     numChunks * ChunkSize,
		Dims:         targetDims,
		PQDims:       pqDims,
		SlabArena:    gd.SlabArena,
		Uint8Arena:   gd.Uint8Arena,
		Float32Arena: gd.Float32Arena,
		Float16Arena: gd.Float16Arena,

		Uint32Arena: gd.Uint32Arena,
		Int32Arena:  gd.Int32Arena,
		Uint64Arena: gd.Uint64Arena,

		Levels: make([]uint64, numChunks),
	}

	if targetDims > 0 {
		newGD.Vectors = make([]uint64, numChunks)
		if sq8Enabled {
			newGD.VectorsSQ8 = make([]uint64, numChunks)
		}
		if pqEnabled && pqDims > 0 {
			newGD.VectorsPQ = make([]uint64, numChunks)
		}
		if bqEnabled {
			newGD.VectorsBQ = make([]uint64, numChunks)
		}
	}

	for i := 0; i < ArrowMaxLayers; i++ {
		newGD.Neighbors[i] = make([]uint64, numChunks)
		newGD.Counts[i] = make([]uint64, numChunks)
		newGD.Versions[i] = make([]uint64, numChunks)
	}

	// Levels
	limit := len(gd.Levels)
	if len(newGD.Levels) < limit {
		limit = len(newGD.Levels)
	}
	for i := 0; i < limit; i++ {
		newGD.Levels[i] = atomic.LoadUint64(&gd.Levels[i])
	}

	// Vectors
	if len(gd.Vectors) > 0 {
		limit = len(gd.Vectors)
		if len(newGD.Vectors) < limit {
			limit = len(newGD.Vectors)
		}
		for i := 0; i < limit; i++ {
			newGD.Vectors[i] = atomic.LoadUint64(&gd.Vectors[i])
		}
	}

	// SQ8
	if len(gd.VectorsSQ8) > 0 {
		limit = len(gd.VectorsSQ8)
		if len(newGD.VectorsSQ8) < limit {
			limit = len(newGD.VectorsSQ8)
		}
		for i := 0; i < limit; i++ {
			newGD.VectorsSQ8[i] = atomic.LoadUint64(&gd.VectorsSQ8[i])
		}
	}

	// PQ
	if len(gd.VectorsPQ) > 0 {
		limit = len(gd.VectorsPQ)
		if len(newGD.VectorsPQ) < limit {
			limit = len(newGD.VectorsPQ)
		}
		for i := 0; i < limit; i++ {
			newGD.VectorsPQ[i] = atomic.LoadUint64(&gd.VectorsPQ[i])
		}
	}

	// BQ
	if len(gd.VectorsBQ) > 0 {
		limit = len(gd.VectorsBQ)
		if len(newGD.VectorsBQ) < limit {
			limit = len(newGD.VectorsBQ)
		}
		for i := 0; i < limit; i++ {
			newGD.VectorsBQ[i] = atomic.LoadUint64(&gd.VectorsBQ[i])
		}
	}

	// Float16
	if len(gd.VectorsF16) > 0 {
		limit = len(gd.VectorsF16)
		if len(newGD.VectorsF16) < limit {
			limit = len(newGD.VectorsF16)
		}
		for i := 0; i < limit; i++ {
			newGD.VectorsF16[i] = atomic.LoadUint64(&gd.VectorsF16[i])
		}
	}

	// Packed Neighbors (v0.1.4-rc1)
	for i := 0; i < ArrowMaxLayers; i++ {
		newGD.PackedNeighbors[i] = gd.PackedNeighbors[i]
	}

	// Neighbors / Counts / Versions
	for i := 0; i < ArrowMaxLayers; i++ {
		// Neighbors
		limit = len(gd.Neighbors[i])
		if len(newGD.Neighbors[i]) < limit {
			limit = len(newGD.Neighbors[i])
		}
		for j := 0; j < limit; j++ {
			newGD.Neighbors[i][j] = atomic.LoadUint64(&gd.Neighbors[i][j])
		}

		// Counts
		limit = len(gd.Counts[i])
		if len(newGD.Counts[i]) < limit {
			limit = len(newGD.Counts[i])
		}
		for j := 0; j < limit; j++ {
			newGD.Counts[i][j] = atomic.LoadUint64(&gd.Counts[i][j])
		}

		// Versions
		limit = len(gd.Versions[i])
		if len(newGD.Versions[i]) < limit {
			limit = len(newGD.Versions[i])
		}
		for j := 0; j < limit; j++ {
			newGD.Versions[i][j] = atomic.LoadUint64(&gd.Versions[i][j])
		}
	}

	// Transfer Arena ownership?
	// GraphData shares same Arena?
	// NewGraphData creates NEW Arenas.
	// We want to KEEP underlying data.
	// So newGD should reference OLD Arenas.
	newGD.SlabArena = gd.SlabArena
	newGD.Uint8Arena = gd.Uint8Arena
	newGD.Float32Arena = gd.Float32Arena
	newGD.Uint32Arena = gd.Uint32Arena
	newGD.Int32Arena = gd.Int32Arena
	newGD.Uint64Arena = gd.Uint64Arena
	newGD.Float16Arena = gd.Float16Arena

	return newGD
}

func (h *ArrowHNSW) Grow(minCap, dims int) {
	h.growMu.Lock()
	defer h.growMu.Unlock()
	h.growNoLock(minCap, dims)
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
	newCap := data.Capacity
	if newCap < minCap {
		newCap = minCap + ChunkSize
		if newCap%ChunkSize != 0 {
			newCap = ((newCap / ChunkSize) + 1) * ChunkSize
		}
	}
	newGD := data.Clone(newCap, dims, h.config.SQ8Enabled, h.config.PQEnabled, h.config.PQM, h.config.BQEnabled) // fixed config

	h.data.Store(newGD)
	h.backend.Store(newGD)
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
	if offset == 0 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    uint32(ChunkSize * gd.Dims),
		Cap:    uint32(ChunkSize * gd.Dims),
	}
	return gd.Float32Arena.Get(ref)
}

func (gd *GraphData) GetVectorsSQ8Chunk(chunkID uint32) []byte {
	if int(chunkID) >= len(gd.VectorsSQ8) {
		return nil
	}
	offset := atomic.LoadUint64(&gd.VectorsSQ8[chunkID])
	if offset == 0 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    uint32(ChunkSize * gd.Dims),
		Cap:    uint32(ChunkSize * gd.Dims),
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
	dims := gd.Dims
	start := int(cOff) * dims
	if start+dims > len(chunk) {
		return nil
	}
	res := make([]byte, dims)
	copy(res, chunk[start:start+dims])
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
		if err == nil { // Ignore OOM for now/panic
			atomic.CompareAndSwapUint64(&data.Levels[cID], 0, ref.Offset)
		}
	}

	// Vectors
	if !h.config.Float16Enabled && dims > 0 && len(data.Vectors) > int(cID) && atomic.LoadUint64(&data.Vectors[cID]) == 0 {
		ref, err := data.Float32Arena.AllocSlice(ChunkSize * dims)
		if err == nil {
			atomic.CompareAndSwapUint64(&data.Vectors[cID], 0, ref.Offset)
		}
	}

	// SQ8
	if h.config.SQ8Enabled && dims > 0 && len(data.VectorsSQ8) > int(cID) && atomic.LoadUint64(&data.VectorsSQ8[cID]) == 0 {
		ref, err := data.Uint8Arena.AllocSlice(ChunkSize * dims)
		if err == nil {
			atomic.CompareAndSwapUint64(&data.VectorsSQ8[cID], 0, ref.Offset)
		}
	}

	// PQ
	if h.config.PQEnabled && data.PQDims > 0 && len(data.VectorsPQ) > int(cID) && atomic.LoadUint64(&data.VectorsPQ[cID]) == 0 {
		ref, err := data.Uint8Arena.AllocSlice(ChunkSize * data.PQDims)
		if err == nil {
			atomic.CompareAndSwapUint64(&data.VectorsPQ[cID], 0, ref.Offset)
		}
	}

	// BQ
	if h.config.BQEnabled && dims > 0 && len(data.VectorsBQ) > int(cID) && atomic.LoadUint64(&data.VectorsBQ[cID]) == 0 {

		numWords := (dims + 63) / 64
		ref, err := data.Uint64Arena.AllocSlice(ChunkSize * numWords)
		if err == nil {
			atomic.CompareAndSwapUint64(&data.VectorsBQ[cID], 0, ref.Offset)
		}
	}

	// Float16
	if h.config.Float16Enabled && dims > 0 && len(data.VectorsF16) > int(cID) && atomic.LoadUint64(&data.VectorsF16[cID]) == 0 {
		ref, err := data.Float16Arena.AllocSlice(ChunkSize * dims)
		if err == nil {
			atomic.CompareAndSwapUint64(&data.VectorsF16[cID], 0, ref.Offset)
		}
	}

	// Neighbors context
	for i := 0; i < ArrowMaxLayers; i++ {
		if atomic.LoadUint64(&data.Neighbors[i][cID]) == 0 {
			ref, err := data.Uint32Arena.AllocSlice(ChunkSize * MaxNeighbors)
			if err == nil {
				atomic.CompareAndSwapUint64(&data.Neighbors[i][cID], 0, ref.Offset)
			}
		}
		if atomic.LoadUint64(&data.Counts[i][cID]) == 0 {
			ref, err := data.Int32Arena.AllocSlice(ChunkSize)
			if err == nil {
				atomic.CompareAndSwapUint64(&data.Counts[i][cID], 0, ref.Offset)
			}
		}
		if atomic.LoadUint64(&data.Versions[i][cID]) == 0 {
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
	for i := range ctx.scratchRemainingVecs {
		ctx.scratchRemainingVecs[i] = nil
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

func (gd *GraphData) GetVectorsF16Chunk(chunkID uint32) []float16.Num {
	if gd.VectorsF16 == nil || int(chunkID) >= len(gd.VectorsF16) {
		return nil
	}
	offset := atomic.LoadUint64(&gd.VectorsF16[chunkID])
	if offset == 0 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    uint32(ChunkSize * gd.Dims),
		Cap:    uint32(ChunkSize * gd.Dims),
	}
	return gd.Float16Arena.Get(ref)
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
