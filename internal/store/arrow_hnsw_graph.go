package store

import (
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/query"
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
	SelectionHeuristicLimit int
	Alpha                   float32
	RefinementFactor        float64 // Changed to float64 to match usage
	KeepPrunedConnections   bool
	BQEnabled               bool
}

func DefaultArrowHNSWConfig() ArrowHNSWConfig {
	return ArrowHNSWConfig{
		M:                32,
		MMax:             64,
		MMax0:            128,
		EfConstruction:   400,
		Metric:           MetricEuclidean,
		Alpha:            1.0,
		RefinementFactor: 1.0,
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

	querySQ8   []byte
	queryBQ    []uint64
	pruneDepth int
}

func NewArrowSearchContext() *ArrowSearchContext {
	return &ArrowSearchContext{
		visited:    NewArrowBitset(10000),
		candidates: NewFixedHeap(100),
		resultSet:  NewMaxHeap(100),
	}
}

func NewArrowSearchContextPool() *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			return NewArrowSearchContext()
		},
	}
}

func (c *ArrowSearchContext) Reset() {
	if c.visited != nil {
		c.visited.Clear()
	}
	if c.candidates != nil {
		c.candidates.Clear()
	}
	if c.resultSet != nil {
		c.resultSet.Clear()
	}
	c.scratchIDs = c.scratchIDs[:0]
	c.scratchDists = c.scratchDists[:0]
	c.scratchNeighbors = c.scratchNeighbors[:0]
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

	quantizer         *ScalarQuantizer
	sq8TrainingBuffer [][]float32
	pqEncoder         *PQEncoder
	bqEncoder         *BQEncoder

	locationStore *ChunkedLocationStore

	metric        DistanceMetric
	distFunc      func([]float32, []float32) float32
	batchDistFunc func([]float32, [][]float32, []float32)
	batchComputer interface{}

	m              int
	mMax           int
	mMax0          int
	efConstruction int

	backend      atomic.Pointer[GraphData]
	vectorColIdx int
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
	Uint32Arena  *memory.TypedArena[uint32]
	Int32Arena   *memory.TypedArena[int32]
	Uint64Arena  *memory.TypedArena[uint64]

	// Offsets (Atomic Uint64)
	// 0 means unallocated (nil).
	Levels     []uint64 // []*[]uint8 -> []offset
	Vectors    []uint64 // []*[]float32 -> []offset
	VectorsSQ8 []uint64 // []*[]byte -> []offset
	VectorsPQ  []uint64 // []*[]byte -> []offset
	VectorsBQ  []uint64 // []*[]uint64 -> []offset

	Neighbors [ArrowMaxLayers][]uint64 // [][]*[]uint32 -> [][]offset
	Counts    [ArrowMaxLayers][]uint64 // [][]*[]int32 -> [][]offset
	Versions  [ArrowMaxLayers][]uint64 // [][]*[]uint32 -> [][]offset
}

const (
	ArrowMaxLayers = 10
	MaxNeighbors   = 448
)

func NewGraphData(capacity, dims int, sq8Enabled, pqEnabled, bqEnabled bool) *GraphData {
	numChunks := (capacity + ChunkSize - 1) / ChunkSize
	if numChunks < 1 {
		numChunks = 1
	}

	estSize := capacity * dims * 4
	if estSize < 16*1024*1024 {
		estSize = 16 * 1024 * 1024
	}

	slab := memory.NewSlabArena(estSize)

	gd := &GraphData{
		Capacity:     numChunks * ChunkSize,
		Dims:         dims,
		SlabArena:    slab,
		Uint8Arena:   memory.NewTypedArena[uint8](slab),
		Float32Arena: memory.NewTypedArena[float32](slab),
		Uint32Arena:  memory.NewTypedArena[uint32](slab),
		Int32Arena:   memory.NewTypedArena[int32](slab),
		Uint64Arena:  memory.NewTypedArena[uint64](slab),

		Levels: make([]uint64, numChunks),
	}
	if dims > 0 {
		gd.Vectors = make([]uint64, numChunks)
		if sq8Enabled {
			gd.VectorsSQ8 = make([]uint64, numChunks)
		}
		if pqEnabled {
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
			countAddr := &(*countsChunk)[cOff]
			count := int(atomic.LoadInt32(countAddr))
			if count == 0 {
				continue
			}

			neighborsChunkPtr := data.GetNeighborsChunk(lvl, cID)
			if neighborsChunkPtr == nil {
				continue
			}
			neighborsChunk := *neighborsChunkPtr

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
			verAddr := &(*versionsChunk)[cOff]
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
func (gd *GraphData) Clone(minCap, targetDims int, sq8Enabled, pqEnabled, bqEnabled bool) *GraphData {
	// Refactored Clone for Arena:
	// Allocating new GraphData implies new Arena.
	newGD := NewGraphData(minCap, targetDims, sq8Enabled, pqEnabled, bqEnabled)

	// Copy data from old arenas to new arenas?
	// This is complex. For now, Grow() strategy might be:
	// 1. New GraphData struct
	// 2. Share same SlabArena? No, Resize implies larger arrays (Levels, Neighbors).
	// But Arenas are append-only.
	// We only need to grow the 'Directory' arrays (Levels, Neighbors, Vectors).
	// The Chunks themselves stay valid in the SAME Arena.
	// So we just copy the slice headers (Vectors, Neighbors) which contain Arena Offsets.

	// Copy Directory Arrays
	// Use atomic load to avoid race with concurrent ensureChunk (which uses CAS)
	// We simulate copy() semantics by iterating up to min(len(src), len(dst))

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

	return newGD
}

func (h *ArrowHNSW) Grow(minCap, dims int) {
	h.growMu.Lock()
	defer h.growMu.Unlock()
	data := h.data.Load()
	if data.Capacity >= minCap && data.Dims == dims {
		return
	}
	newCap := data.Capacity
	if newCap < minCap {
		newCap = minCap + ChunkSize
		if newCap%ChunkSize != 0 {
			newCap = ((newCap / ChunkSize) + 1) * ChunkSize
		}
	}
	newGD := data.Clone(newCap, dims, h.config.SQ8Enabled, h.config.PQEnabled, h.config.BQEnabled) // fixed config
	h.data.Store(newGD)
}

// Helper to safely get a pointer to the slice header for a chunk.
// WARNING: The returned slice is only valid until the arena is reallocated/moved (though SlabArena handles stability).
func (gd *GraphData) GetNeighborsChunk(layer int, chunkID uint32) *[]uint32 {
	offset := atomic.LoadUint64(&gd.Neighbors[layer][chunkID])
	if offset == 0 {
		return nil
	}
	// Reconstruct slice: ChunkSize * MaxNeighbors
	// Note: We use Unsafe Get from TypedArena.
	// We construct a SliceRef.
	ref := memory.SliceRef{
		Offset: offset,
		Len:    ChunkSize * MaxNeighbors,
		Cap:    ChunkSize * MaxNeighbors,
	}
	slice := gd.Uint32Arena.Get(ref)
	return &slice
}

func (gd *GraphData) GetCountsChunk(layer int, chunkID uint32) *[]int32 {
	offset := atomic.LoadUint64(&gd.Counts[layer][chunkID])
	if offset == 0 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    ChunkSize,
		Cap:    ChunkSize,
	}
	slice := gd.Int32Arena.Get(ref)
	return &slice
}

func (gd *GraphData) GetVersionsChunk(layer int, chunkID uint32) *[]uint32 {
	offset := atomic.LoadUint64(&gd.Versions[layer][chunkID])
	if offset == 0 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    ChunkSize,
		Cap:    ChunkSize,
	}
	slice := gd.Uint32Arena.Get(ref)
	return &slice
}

func (gd *GraphData) GetLevelsChunk(chunkID uint32) *[]uint8 {
	offset := atomic.LoadUint64(&gd.Levels[chunkID])
	if offset == 0 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    ChunkSize,
		Cap:    ChunkSize,
	}
	slice := gd.Uint8Arena.Get(ref)
	return &slice
}

func (gd *GraphData) GetVectorsChunk(chunkID uint32) *[]float32 {
	offset := atomic.LoadUint64(&gd.Vectors[chunkID])
	if offset == 0 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    uint32(ChunkSize * gd.Dims),
		Cap:    uint32(ChunkSize * gd.Dims),
	}
	slice := gd.Float32Arena.Get(ref)
	return &slice
}

func (gd *GraphData) GetVectorsSQ8Chunk(chunkID uint32) *[]byte {
	offset := atomic.LoadUint64(&gd.VectorsSQ8[chunkID])
	if offset == 0 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    uint32(ChunkSize * gd.Dims),
		Cap:    uint32(ChunkSize * gd.Dims),
	}
	slice := gd.Uint8Arena.Get(ref)
	return &slice
}

func (gd *GraphData) GetVectorsPQChunk(chunkID uint32) *[]byte {
	offset := atomic.LoadUint64(&gd.VectorsPQ[chunkID])
	if offset == 0 {
		return nil
	}
	// We use stored PQDims
	if gd.PQDims == 0 {
		return nil
	}
	ref := memory.SliceRef{
		Offset: offset,
		Len:    uint32(ChunkSize * gd.PQDims),
		Cap:    uint32(ChunkSize * gd.PQDims),
	}
	slice := gd.Uint8Arena.Get(ref)
	return &slice
}

// GetVectorPQ returns the PQ encoded vector for the given node ID.
func (gd *GraphData) GetVectorPQ(id uint32, pqDims int) []byte {
	cID := chunkID(id)
	chunk := gd.GetVectorsPQChunk(cID)
	if chunk == nil {
		return nil
	}

	cOff := chunkOffset(id)
	off := int(cOff) * pqDims
	// Should check against actual chunk length if available or trust math
	if off+pqDims > len(*chunk) {
		return nil
	}
	return (*chunk)[off : off+pqDims]
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

func (gd *GraphData) GetVectorSQ8(id uint32) []byte {
	cID := chunkID(id)
	chunk := gd.GetVectorsSQ8Chunk(cID)
	if chunk == nil {
		return nil
	}
	cOff := chunkOffset(id)
	dims := gd.Dims
	start := int(cOff) * dims
	if start+dims > len(*chunk) {
		return nil
	}
	return (*chunk)[start : start+dims]
}

func (gd *GraphData) GetVectorsBQChunk(chunkID uint32) *[]uint64 {
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
	slice := gd.Uint64Arena.Get(ref)
	return &slice
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

	copy((*chunk)[base:base+count], neighbors)

	// Update Count
	cChunk := gd.GetCountsChunk(layer, cID)
	if cChunk != nil {
		atomic.StoreInt32(&(*cChunk)[cOff], int32(count))
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

	verAddr := &(*versionsChunk)[cOff]
	countAddr := &(*countsChunk)[cOff]
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
			res[i] = atomic.LoadUint32(&(*neighborsChunk)[baseIdx+i])
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
	if start+numWords > len(*chunk) {
		return nil
	}
	// Return copy
	res := make([]uint64, numWords)
	copy(res, (*chunk)[start:start+numWords])
	return res
}

func (gd *GraphData) GetLevel(id uint32) int {
	cID := chunkID(id)
	chunk := gd.GetLevelsChunk(cID)
	if chunk == nil {
		return -1
	}
	cOff := chunkOffset(id)
	return int((*chunk)[cOff])
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
	if dims > 0 && len(data.Vectors) > int(cID) && atomic.LoadUint64(&data.Vectors[cID]) == 0 {
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

	// BQ
	if h.config.BQEnabled && dims > 0 && len(data.VectorsBQ) > int(cID) && atomic.LoadUint64(&data.VectorsBQ[cID]) == 0 {
		numWords := (dims + 63) / 64
		ref, err := data.Uint64Arena.AllocSlice(ChunkSize * numWords)
		if err == nil {
			atomic.CompareAndSwapUint64(&data.VectorsBQ[cID], 0, ref.Offset)
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
func (h *ArrowHNSW) GetPQEncoder() *PQEncoder {
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
