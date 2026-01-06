package store

import (
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

	Neighbors [ArrowMaxLayers][]*[]uint32
	Counts    [ArrowMaxLayers][]*[]int32
	Versions  [ArrowMaxLayers][]*[]uint32
}

const (
	ArrowMaxLayers = 10
	MaxNeighbors   = 448
)

func NewGraphData(capacity, dims int, sq8Enabled bool, pqEnabled bool) *GraphData {
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

	countsChunk := g.LoadCountsChunk(layer, cID)
	if countsChunk == nil {
		return nil
	}
	count := atomic.LoadInt32(&(*countsChunk)[cOff])

	neighborsChunk := g.LoadNeighborsChunk(layer, cID)
	if neighborsChunk == nil {
		return nil
	}

	baseIdx := int(cOff) * MaxNeighbors

	need := int(count)
	if cap(buffer) < need {
		buffer = make([]uint32, need)
	}
	res := buffer[:need]
	copy(res, (*neighborsChunk)[baseIdx:baseIdx+need])
	return res
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

func (g *GraphData) GetLevel(id uint32) int {
	cID := chunkID(id)
	cOff := chunkOffset(id)
	levels := g.LoadLevelChunk(cID)
	if levels == nil {
		return -1
	}
	return int((*levels)[cOff])
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

func (gd *GraphData) Clone(minCap int, targetDims int, sq8Enabled bool, pqEnabled bool) *GraphData {
	newGD := NewGraphData(minCap, targetDims, sq8Enabled, pqEnabled)
	copy(newGD.Levels, gd.Levels)
	if len(gd.Vectors) > 0 {
		copy(newGD.Vectors, gd.Vectors)
	}
	if len(gd.VectorsSQ8) > 0 {
		copy(newGD.VectorsSQ8, gd.VectorsSQ8)
	}
	if len(gd.VectorsPQ) > 0 {
		copy(newGD.VectorsPQ, gd.VectorsPQ)
	}
	for i := 0; i < ArrowMaxLayers; i++ {
		copy(newGD.Neighbors[i], gd.Neighbors[i])
		copy(newGD.Counts[i], gd.Counts[i])
		copy(newGD.Versions[i], gd.Versions[i])
	}
	return newGD
}

func (h *ArrowHNSW) Grow(minCap int, dims int) {
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
	newGD := data.Clone(newCap, dims, h.config.SQ8Enabled, h.config.PQEnabled)
	h.data.Store(newGD)
}

func (h *ArrowHNSW) ensureChunk(data *GraphData, cID uint32, cOff uint32, dims int) (*GraphData, error) {
	if data.Levels[cID] == nil {
		chunk := make([]uint8, ChunkSize)
		ptr := (*unsafe.Pointer)(unsafe.Pointer(&data.Levels[cID]))
		atomic.CompareAndSwapPointer(ptr, nil, unsafe.Pointer(&chunk))
	}
	if dims > 0 && data.Vectors != nil && data.Vectors[cID] == nil {
		chunk := make([]float32, ChunkSize*dims)
		data.StoreVectorChunk(cID, &chunk)
	}
	if h.config.SQ8Enabled && dims > 0 && data.VectorsSQ8 != nil && data.VectorsSQ8[cID] == nil {
		chunk := make([]byte, ChunkSize*dims)
		data.StoreSQ8Chunk(cID, &chunk)
	}
	// Allocate metadata chunks for all layers
	for i := 0; i < ArrowMaxLayers; i++ {
		if data.Neighbors[i][cID] == nil {
			chunk := make([]uint32, ChunkSize*MaxNeighbors)
			data.StoreNeighborsChunk(i, cID, &chunk)
		}
		if data.Counts[i][cID] == nil {
			chunk := make([]int32, ChunkSize)
			data.StoreCountsChunk(i, cID, &chunk)
		}
		if data.Versions[i][cID] == nil {
			chunk := make([]uint32, ChunkSize)
			data.StoreVersionsChunk(i, cID, &chunk)
		}
	}
	return data, nil
}

func (h *ArrowHNSW) CleanupTombstones(maxScan int) int {
	return 0
}

func (h *ArrowHNSW) Size() int {
	return int(h.nodeCount.Load())
}

func (h *ArrowHNSW) GetPQEncoder() *PQEncoder {
	return h.pqEncoder
}

func (h *ArrowHNSW) IsDeleted(id uint32) bool {
	return h.deleted.Contains(int(id))
}

// Delete marks a vector as deleted.
func (h *ArrowHNSW) Delete(id uint32) error {
	h.deleted.Set(int(id))
	return nil
}

// Added accessors for tests
func (h *ArrowHNSW) GetEntryPoint() uint32 {
	return h.entryPoint.Load()
}
func (h *ArrowHNSW) GetMaxLevel() int {
	return int(h.maxLevel.Load())
}
