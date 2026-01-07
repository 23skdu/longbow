package store

import (
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

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

	data    atomic.Pointer[GraphData]
	deleted *query.Bitset

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

	backend      atomic.Value
	vectorColIdx int
}

type GraphData struct {
	Capacity   int
	Dims       int
	Levels     []*[]uint8
	Vectors    []*[]float32
	VectorsSQ8 []*[]byte
	VectorsPQ  []*[]byte
	VectorsBQ  []*[]uint64

	Neighbors [ArrowMaxLayers][]*[]uint32
	Counts    [ArrowMaxLayers][]*[]int32
	Versions  [ArrowMaxLayers][]*[]uint32
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
	gd := &GraphData{
		Capacity: numChunks * ChunkSize,
		Dims:     dims,
		Levels:   make([]*[]uint8, numChunks),
	}
	if dims > 0 {
		gd.Vectors = make([]*[]float32, numChunks)
		if sq8Enabled {
			gd.VectorsSQ8 = make([]*[]byte, numChunks)
		}
		if pqEnabled {
			gd.VectorsPQ = make([]*[]byte, numChunks)
		}
		if bqEnabled {
			gd.VectorsBQ = make([]*[]uint64, numChunks)
		}
	}
	for i := 0; i < ArrowMaxLayers; i++ {
		gd.Neighbors[i] = make([]*[]uint32, numChunks)
		gd.Counts[i] = make([]*[]int32, numChunks)
		gd.Versions[i] = make([]*[]uint32, numChunks)
	}
	return gd
}

func (g *GraphData) GetNeighbors(layer int, id uint32, buffer []uint32) []uint32 {
	cID := chunkID(id)
	cOff := chunkOffset(id)

	versionsChunk := g.LoadVersionsChunk(layer, cID)
	countsChunk := g.LoadCountsChunk(layer, cID)
	neighborsChunk := g.LoadNeighborsChunk(layer, cID)

	if versionsChunk == nil || countsChunk == nil || neighborsChunk == nil {
		return nil
	}

	verAddr := &(*versionsChunk)[cOff]
	countAddr := &(*countsChunk)[cOff]
	baseIdx := int(cOff) * MaxNeighbors

	for {
		v1 := atomic.LoadUint32(verAddr)
		if v1%2 != 0 {
			// Write in progress, spin
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

		// Atomic copy to avoid race reports
		for i := 0; i < need; i++ {
			res[i] = atomic.LoadUint32(&(*neighborsChunk)[baseIdx+i])
		}

		v2 := atomic.LoadUint32(verAddr)
		if v1 == v2 {
			return res
		}
	}
}

func (g *GraphData) GetVectorSQ8(id uint32) []byte {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	sq8Chunk := g.LoadSQ8Chunk(cID)
	if sq8Chunk == nil {
		return nil
	}
	dims := g.Dims
	baseIdx := int(cOff) * dims
	res := make([]byte, dims)

	// Since SQ8 is not yet protected by seqlocks (it's write-once currently),
	// but might race with Grow/Clone, we use atomic loads if needed.
	// However, SQ8 chunks are fixed after allocation.
	// The race reported was actually on Neighbors.
	// We'll keep this simple for now but use copy() which is generally safe
	// if we don't have concurrent writes to THESE bytes.
	copy(res, (*sq8Chunk)[baseIdx:baseIdx+dims])
	return res
}

func (g *GraphData) GetVectorPQ(id uint32) []byte {
	cID := chunkID(id)
	pqChunk := g.LoadPQChunk(cID)
	if pqChunk == nil {
		return nil
	}
	return nil
}

func (g *GraphData) GetVectorBQ(id uint32) []uint64 {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	bqChunk := g.LoadBQChunk(cID)
	if bqChunk == nil {
		return nil
	}
	// BQ uses (dims+63)/64 uint64s per vector
	numWords := (g.Dims + 63) / 64
	baseIdx := int(cOff) * numWords
	res := make([]uint64, numWords)
	copy(res, (*bqChunk)[baseIdx:baseIdx+numWords])
	return res
}

func (g *GraphData) GetLevel(id uint32) int {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	levels := g.LoadLevelChunk(cID)
	if levels == nil {
		return -1
	}
	return int((*levels)[cOff])
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

			// Check if chunk exists
			if lvl >= len(data.Neighbors) || int(cID) >= len(data.Neighbors[lvl]) || data.Neighbors[lvl][cID] == nil {
				continue
			}

			countAddr := &(*data.Counts[lvl][cID])[cOff]
			count := int(atomic.LoadInt32(countAddr))
			if count == 0 {
				continue
			}

			neighborsChunk := (*data.Neighbors[lvl][cID])
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

			// 2. Seqlock: Begin Write (Odd)
			verAddr := &(*data.Versions[lvl][cID])[cOff]
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
	return nil
}

func (g *GraphData) LoadVectorChunk(cID uint32) *[]float32 {
	if int(cID) >= len(g.Vectors) {
		return nil
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.Vectors[cID]))
	return (*[]float32)(atomic.LoadPointer(ptr))
}
func (g *GraphData) StoreVectorChunk(cID uint32, chunk *[]float32) {
	if int(cID) >= len(g.Vectors) {
		return
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.Vectors[cID]))
	atomic.StorePointer(ptr, unsafe.Pointer(chunk))
}
func (g *GraphData) LoadSQ8Chunk(cID uint32) *[]byte {
	if g.VectorsSQ8 == nil || int(cID) >= len(g.VectorsSQ8) {
		return nil
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.VectorsSQ8[cID]))
	return (*[]byte)(atomic.LoadPointer(ptr))
}
func (g *GraphData) StoreSQ8Chunk(cID uint32, chunk *[]byte) {
	if g.VectorsSQ8 == nil || int(cID) >= len(g.VectorsSQ8) {
		return
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.VectorsSQ8[cID]))
	atomic.StorePointer(ptr, unsafe.Pointer(chunk))
}
func (g *GraphData) LoadPQChunk(cID uint32) *[]byte {
	if g.VectorsPQ == nil || int(cID) >= len(g.VectorsPQ) {
		return nil
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.VectorsPQ[cID]))
	return (*[]byte)(atomic.LoadPointer(ptr))
}
func (g *GraphData) StorePQChunk(cID uint32, chunk *[]byte) {
	if g.VectorsPQ == nil || int(cID) >= len(g.VectorsPQ) {
		return
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.VectorsPQ[cID]))
	atomic.StorePointer(ptr, unsafe.Pointer(chunk))
}
func (g *GraphData) LoadBQChunk(cID uint32) *[]uint64 {
	if g.VectorsBQ == nil || int(cID) >= len(g.VectorsBQ) {
		return nil
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.VectorsBQ[cID]))
	return (*[]uint64)(atomic.LoadPointer(ptr))
}
func (g *GraphData) StoreBQChunk(cID uint32, chunk *[]uint64) {
	if g.VectorsBQ == nil || int(cID) >= len(g.VectorsBQ) {
		return
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.VectorsBQ[cID]))
	atomic.StorePointer(ptr, unsafe.Pointer(chunk))
}
func (g *GraphData) LoadLevelChunk(cID uint32) *[]uint8 {
	if int(cID) >= len(g.Levels) {
		return nil
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.Levels[cID]))
	return (*[]uint8)(atomic.LoadPointer(ptr))
}
func (g *GraphData) StoreLevelChunk(cID uint32, chunk *[]uint8) {
	if int(cID) >= len(g.Levels) {
		return
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.Levels[cID]))
	atomic.StorePointer(ptr, unsafe.Pointer(chunk))
}
func (g *GraphData) LoadCountsChunk(layer int, cID uint32) *[]int32 {
	if layer >= len(g.Counts) || int(cID) >= len(g.Counts[layer]) {
		return nil
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.Counts[layer][cID]))
	return (*[]int32)(atomic.LoadPointer(ptr))
}
func (g *GraphData) StoreCountsChunk(layer int, cID uint32, chunk *[]int32) {
	if layer >= len(g.Counts) || int(cID) >= len(g.Counts[layer]) {
		return
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.Counts[layer][cID]))
	atomic.StorePointer(ptr, unsafe.Pointer(chunk))
}
func (g *GraphData) LoadNeighborsChunk(layer int, cID uint32) *[]uint32 {
	if layer >= len(g.Neighbors) || int(cID) >= len(g.Neighbors[layer]) {
		return nil
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.Neighbors[layer][cID]))
	return (*[]uint32)(atomic.LoadPointer(ptr))
}
func (g *GraphData) StoreNeighborsChunk(layer int, cID uint32, chunk *[]uint32) {
	if layer >= len(g.Neighbors) || int(cID) >= len(g.Neighbors[layer]) {
		return
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.Neighbors[layer][cID]))
	atomic.StorePointer(ptr, unsafe.Pointer(chunk))
}
func (g *GraphData) LoadVersionsChunk(layer int, cID uint32) *[]uint32 {
	if layer >= len(g.Versions) || int(cID) >= len(g.Versions[layer]) {
		return nil
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.Versions[layer][cID]))
	return (*[]uint32)(atomic.LoadPointer(ptr))
}
func (g *GraphData) StoreVersionsChunk(layer int, cID uint32, chunk *[]uint32) {
	if layer >= len(g.Versions) || int(cID) >= len(g.Versions[layer]) {
		return
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.Versions[layer][cID]))
	atomic.StorePointer(ptr, unsafe.Pointer(chunk))
}

func (gd *GraphData) Clone(minCap, targetDims int, sq8Enabled, pqEnabled, bqEnabled bool) *GraphData {
	newGD := NewGraphData(minCap, targetDims, sq8Enabled, pqEnabled, bqEnabled)

	// Levels
	for i := 0; i < len(gd.Levels) && i < len(newGD.Levels); i++ {
		p := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&gd.Levels[i])))
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&newGD.Levels[i])), p)
	}

	// Vectors
	if len(gd.Vectors) > 0 {
		for i := 0; i < len(gd.Vectors) && i < len(newGD.Vectors); i++ {
			p := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&gd.Vectors[i])))
			atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&newGD.Vectors[i])), p)
		}
	}
	// SQ8
	if len(gd.VectorsSQ8) > 0 {
		for i := 0; i < len(gd.VectorsSQ8) && i < len(newGD.VectorsSQ8); i++ {
			p := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&gd.VectorsSQ8[i])))
			atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&newGD.VectorsSQ8[i])), p)
		}
	}
	// PQ
	if len(gd.VectorsPQ) > 0 {
		for i := 0; i < len(gd.VectorsPQ) && i < len(newGD.VectorsPQ); i++ {
			p := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&gd.VectorsPQ[i])))
			atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&newGD.VectorsPQ[i])), p)
		}
	}
	// BQ
	if len(gd.VectorsBQ) > 0 {
		for i := 0; i < len(gd.VectorsBQ) && i < len(newGD.VectorsBQ); i++ {
			p := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&gd.VectorsBQ[i])))
			atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&newGD.VectorsBQ[i])), p)
		}
	}

	for i := 0; i < ArrowMaxLayers; i++ {
		// Neighbors
		for j := 0; j < len(gd.Neighbors[i]) && j < len(newGD.Neighbors[i]); j++ {
			p := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&gd.Neighbors[i][j])))
			atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&newGD.Neighbors[i][j])), p)
		}
		// Counts
		for j := 0; j < len(gd.Counts[i]) && j < len(newGD.Counts[i]); j++ {
			p := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&gd.Counts[i][j])))
			atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&newGD.Counts[i][j])), p)
		}
		// Versions
		for j := 0; j < len(gd.Versions[i]) && j < len(newGD.Versions[i]); j++ {
			p := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&gd.Versions[i][j])))
			atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&newGD.Versions[i][j])), p)
		}
	}
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

func (h *ArrowHNSW) ensureChunk(data *GraphData, cID, _ uint32, dims int) *GraphData {
	if atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&data.Levels[cID]))) == nil {
		chunk := make([]uint8, ChunkSize)
		ptr := (*unsafe.Pointer)(unsafe.Pointer(&data.Levels[cID]))
		atomic.CompareAndSwapPointer(ptr, nil, unsafe.Pointer(&chunk))
	}
	if dims > 0 && data.Vectors != nil && atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&data.Vectors[cID]))) == nil {
		chunk := make([]float32, ChunkSize*dims)
		ptr := (*unsafe.Pointer)(unsafe.Pointer(&data.Vectors[cID]))
		atomic.CompareAndSwapPointer(ptr, nil, unsafe.Pointer(&chunk))
	}
	if h.config.SQ8Enabled && dims > 0 && data.VectorsSQ8 != nil && atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&data.VectorsSQ8[cID]))) == nil {
		chunk := make([]byte, ChunkSize*dims)
		ptr := (*unsafe.Pointer)(unsafe.Pointer(&data.VectorsSQ8[cID]))
		atomic.CompareAndSwapPointer(ptr, nil, unsafe.Pointer(&chunk))
	}
	if h.config.BQEnabled && dims > 0 && data.VectorsBQ != nil && atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&data.VectorsBQ[cID]))) == nil {
		numWords := (dims + 63) / 64
		chunk := make([]uint64, ChunkSize*numWords)
		ptr := (*unsafe.Pointer)(unsafe.Pointer(&data.VectorsBQ[cID]))
		atomic.CompareAndSwapPointer(ptr, nil, unsafe.Pointer(&chunk))
	}
	// Allocate metadata chunks for all layers
	for i := 0; i < ArrowMaxLayers; i++ {
		if atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&data.Neighbors[i][cID]))) == nil {
			chunk := make([]uint32, ChunkSize*MaxNeighbors)
			ptr := (*unsafe.Pointer)(unsafe.Pointer(&data.Neighbors[i][cID]))
			atomic.CompareAndSwapPointer(ptr, nil, unsafe.Pointer(&chunk))
		}
		if atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&data.Counts[i][cID]))) == nil {
			chunk := make([]int32, ChunkSize)
			ptr := (*unsafe.Pointer)(unsafe.Pointer(&data.Counts[i][cID]))
			atomic.CompareAndSwapPointer(ptr, nil, unsafe.Pointer(&chunk))
		}
		if atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&data.Versions[i][cID]))) == nil {
			chunk := make([]uint32, ChunkSize)
			ptr := (*unsafe.Pointer)(unsafe.Pointer(&data.Versions[i][cID]))
			atomic.CompareAndSwapPointer(ptr, nil, unsafe.Pointer(&chunk))
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

// Size returns the index capacity.
func (h *ArrowHNSW) Size() int {
	if data := h.data.Load(); data != nil {
		return data.Size()
	}
	return 0
}

// IsDeleted checks if a node ID is marked as deleted.

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
