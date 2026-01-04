package store

import (
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
)

const (
	// ChunkShift determines the size of each chunk (1 << 10 = 1024).
	ChunkShift = 10
	ChunkSize  = 1 << ChunkShift
	ChunkMask  = ChunkSize - 1
)

// GraphData holds the SoA (Struct of Arrays) representation of the graph using a Chunked/Arena layout.
// This structure is designed for lock-free snapshot access and O(1) growth.
// We use pointers to chunks (*[]T) to allow atomic lazy allocation.
type GraphData struct {
	Capacity int
	Dims     int
	// SoA Slices of Chunks
	// Access: Levels[id >> ChunkShift] is *[]uint8. If nil, chunk not allocated.
	Levels []*[]uint8
	// Vectors holds dense packed float32 vectors.
	Vectors []*[]float32

	// VectorsSQ8 holds dense packed uint8 vectors forScalar Quantization.
	VectorsSQ8 []*[]byte

	// VectorsPQ holds dense packed byte vectors for Product Quantization.
	VectorsPQ []*[]byte

	// Neighbors flattened per layer and chunk
	Neighbors [ArrowMaxLayers][]*[]uint32
	// Counts per layer and chunk
	Counts [ArrowMaxLayers][]*[]int32

	// Versions per layer and chunk
	Versions [ArrowMaxLayers][]*[]uint32
}

// NewGraphData creates a new GraphData with the specified capacity.
// It allocates slice headers for chunks but does not allocate the chunks themselves (Lazy).
func NewGraphData(capacity, dims int, sq8Enabled bool, pqEnabled bool) *GraphData {
	// Calculate number of chunks needed
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

// (This part was handled in the previous replacement, just cleaning up the tail if needed)
// The previous tool call replaced up to line 48 of the original file, which was inside NewGraphData.
// I need to make sure the REST of NewGraphData is also updated or removed.

// Wait, the previous replacement ended at line 48 of ORIGINAL.
// Original NewGraphData went from 41 to 55+.
// I need to replace the loop that initializes Neighbors.
const (
	ArrowMaxLayers = 10  // Maximum number of layers in the graph
	MaxNeighbors   = 448 // Maximum neighbors per node per layer (sufficient for M up to 128, MMax up to 256)
)

// ArrowHNSW is the main HNSW index structure with Arrow integration.
// It uses atomic pointers for lock-free search and a mutex for serialized writes.
type ArrowHNSW struct {
	// Graph data snapshot (lock-free access)
	// data holds the mutable in-memory graph. It can be nil if running in Read-Only Disk mode.
	data atomic.Pointer[GraphData]

	// backend holds the GraphBackend interface (GraphData or DiskGraph).
	// Used for Search operations.
	backend atomic.Value

	// Locking strategy:
	// initMu protects the initialization of the first node (entry point) and global dimension changes.
	initMu sync.Mutex

	// growMu protects the GraphData pointer and Global growth operations.
	// Insert() does NOT take this lock (lock-free read). Grow() takes Lock (exclusive).
	growMu sync.Mutex

	// shardedLocks protects individual node neighbor lists during updates.
	// lockID = nodeID % 1024
	shardedLocks [1024]MeasuredMutex

	// Graph state (atomic for lock-free reads)
	entryPoint atomic.Uint32
	// Maximum level in the graph
	maxLevel atomic.Int32

	// Total number of nodes in the graph
	nodeCount atomic.Uint32

	// Component dependencies
	quantizer *ScalarQuantizer
	pqEncoder *PQEncoder

	// Training buffer for SQ8 auto-tuning
	// Protected by growMu (since training is a structural change)
	sq8TrainingBuffer [][]float32

	// Caches
	vectorColIdx int // Index of the vector column in the Arrow schema

	// Arrow integration - reference to parent dataset for vector access
	dataset *Dataset
	dims    atomic.Int32 // Vector dimensions, cached for lock-free access

	// HNSW parameters
	config         ArrowHNSWConfig // Store full config
	m              int             // Number of neighbors per layer
	mMax           int             // Maximum neighbors for layer 0
	mMax0          int             // Maximum neighbors for higher layers
	efConstruction int             // Size of dynamic candidate list during construction
	ml             float64         // Level multiplier for exponential decay

	// Pooled resources
	searchPool    *ArrowSearchContextPool
	batchComputer *BatchDistanceComputer // Vectorized distance computation

	// Location tracking for VectorIndex implementation
	locationStore *ChunkedLocationStore

	// Deleted nodes tracking
	deleted *Bitset
}

// ArrowHNSWConfig holds HNSW configuration parameters.
type ArrowHNSWConfig struct {
	M     int // Max neighbors for layer > 0
	MMax  int // Max neighbors for layer 0
	MMax0 int // Max connected components for layer 0 (actually MMax0 is layer > 0 max neighbors during construction?)
	// NOTE: In standard HNSW, M is the target, MMax is max buffer.
	// In this implementation:
	// M = target neighbors for new nodes? No, M is mainly Used for Ml calculation.
	// MMax = neighbors for layer 0.
	// MMax0 = neighbors for layer > 0.

	EfConstruction int     // Size of dynamic candidate list during construction
	Ml             float64 // Level generation multiplier

	// Alpha is the diversity factor for robust pruning (default 1.0).
	// Values > 1.0 relax the pruning, allowing more connections.
	Alpha float32

	// KeepPrunedConnections enables the "Robust Prune" backfill strategy.
	// If true, candidates discarded by the heuristic are used to ensure
	// the number of connections reaches M. This improves recall but may reduce performance.
	KeepPrunedConnections bool

	// RefinementFactor controls the number of candidates to re-rank.
	// Valid only if SQ8Enabled is true.
	// Candidates = k * RefinementFactor.
	// Default 1.0 (no refinement, just return SQ8 results).
	RefinementFactor float64

	// SelectionHeuristicLimit caps the number of candidates checked by the RobustPrune heuristic.
	// If 0, all candidates (up to EfConstruction) are checked.
	// Limiting this (e.g., to 4*M) drastically improves insertion speed when Alpha > 1.0.
	SelectionHeuristicLimit int

	// SQ8Enabled enables scalar quantization
	SQ8Enabled bool

	// SQ8TrainingThreshold is the number of vectors to accumulate before training SQ8 bounds.
	// Default: 1000.
	SQ8TrainingThreshold int

	// InitialCapacity is the starting size of the graph.
	InitialCapacity int

	// AdaptiveEf enables adaptive efConstruction scaling during graph build.
	// When enabled, efConstruction starts low and increases as the graph grows,
	// reducing build time by 30-40% while maintaining recall quality.
	AdaptiveEf bool

	// AdaptiveEfMin is the minimum efConstruction value (default: EfConstruction/4).
	// Only used when AdaptiveEf is true.
	AdaptiveEfMin int

	// AdaptiveEfThreshold is the node count at which ef reaches full EfConstruction.
	// Default: InitialCapacity/2. Only used when AdaptiveEf is true.
	AdaptiveEfThreshold int

	// Product Quantization
	PQEnabled bool
	PQM       int // Number of sub-vectors (must divide Dims)
	PQK       int // Number of centroids (default 256)
}

// DefaultArrowHNSWConfig returns sensible default HNSW parameters.
// Tuned for high recall (99.5%+) on 10K+ vector datasets.
func DefaultArrowHNSWConfig() ArrowHNSWConfig {
	return ArrowHNSWConfig{
		M:                    32,
		MMax:                 96, // Layer 0: 3 * M (Target is 2 * M = 64)
		MMax0:                48, // Layer > 0: 1.5 * M
		EfConstruction:       400,
		Ml:                   1.0 / math.Log(32),
		Alpha:                1.1,
		SQ8Enabled:           false,
		SQ8TrainingThreshold: 1000,
		RefinementFactor:     1.0,
		InitialCapacity:      1000,
		AdaptiveEf:           false, // Disabled by default (opt-in)
		AdaptiveEfMin:        0,     // Auto-calculate as EfConstruction/4
		AdaptiveEfThreshold:  0,     // Auto-calculate as InitialCapacity/2
		PQEnabled:            false,
		PQM:                  0, // Auto-calculate?
		PQK:                  256,
	}
}

// Grow ensures the graph has enough capacity for the requested ID.
// It uses a Chunked Storage strategy: new chunks are appended, making growth O(1).
// Grow ensures the graph has enough capacity for the requested ID.
// It uses a Chunked Storage strategy: new chunks are appended, making growth O(1).

// ensureChunk ensures that the chunk for the given ID is allocated.
// This handles the "Lazy Allocation" logic.
// ensureChunk ensures that the chunk for the given ID is allocated.
// This handles the "Lazy Allocation" logic.
// Returns the (potentially updated) GraphData pointer to ensure caller uses the valid snapshot.
func (h *ArrowHNSW) ensureChunk(data *GraphData, cID, _ uint32, dims int) (*GraphData, error) {
	// Optimization: check if chunks exist
	levelsExist := data.LoadLevelChunk(cID) != nil
	vectorsExist := true
	if dims > 0 {
		vectorsExist = data.Vectors != nil && data.LoadVectorChunk(cID) != nil
		if h.config.SQ8Enabled {
			vectorsExist = vectorsExist && data.LoadSQ8Chunk(cID) != nil
		}
		if h.config.PQEnabled {
			vectorsExist = vectorsExist && data.LoadPQChunk(cID) != nil
		}
	}

	if levelsExist && vectorsExist {
		return data, nil
	}

	// Chunk missing. Acquire lock to allocate.
	h.growMu.Lock()
	defer h.growMu.Unlock()

	// Critical Fix: Reload data to ensure we are modifying the authoritative snapshot.
	// If Grow run while we were waiting, 'data' is stale and modifications to it are lost.
	currentData := h.data.Load()

	// If data was stale, switch to current (which Grow guaranteed is large enough)
	if currentData != data {
		data = currentData
		// Re-check existence in new data
		levelsExist = data.LoadLevelChunk(cID) != nil
		vectorsExist = true
		if dims > 0 {
			vectorsExist = data.Vectors != nil && data.LoadVectorChunk(cID) != nil
			if h.config.SQ8Enabled {
				vectorsExist = vectorsExist && data.LoadSQ8Chunk(cID) != nil
			}
			if h.config.PQEnabled {
				vectorsExist = vectorsExist && data.LoadPQChunk(cID) != nil
			}
		}
		if levelsExist && vectorsExist {
			return data, nil
		}
	}

	// Re-check under lock (double-checked locking)
	if data.LoadLevelChunk(cID) == nil {
		levels := make([]uint8, ChunkSize)
		data.StoreLevelChunk(cID, &levels)

		// Allocate layers only if levels was nil (new chunk completely)
		for i := 0; i < ArrowMaxLayers; i++ {
			neighbors := make([]uint32, ChunkSize*MaxNeighbors)
			data.StoreNeighborsChunk(i, cID, &neighbors)

			counts := make([]int32, ChunkSize)
			data.StoreCountsChunk(i, cID, &counts)

			versions := make([]uint32, ChunkSize)
			data.StoreVersionsChunk(i, cID, &versions)
		}
	}

	if dims > 0 {
		if data.Vectors != nil && data.LoadVectorChunk(cID) == nil {
			vecs := make([]float32, ChunkSize*dims)
			data.StoreVectorChunk(cID, &vecs)
		}

		if h.config.SQ8Enabled && data.LoadSQ8Chunk(cID) == nil {
			sq8 := make([]byte, ChunkSize*dims)
			data.StoreSQ8Chunk(cID, &sq8)
		}

		if h.config.PQEnabled && data.LoadPQChunk(cID) == nil {
			// PQ storage size: ChunkSize * M
			m := h.config.PQM
			if m == 0 && h.pqEncoder != nil {
				m = h.pqEncoder.config.NumSubVectors
			}
			if m > 0 {
				pqVecs := make([]byte, ChunkSize*m)
				data.StorePQChunk(cID, &pqVecs)
			}
		}
	}

	return data, nil
}

// Delete marks a node as deleted in the index.
func (h *ArrowHNSW) Delete(id uint32) error {
	h.deleted.Set(int(id))
	return nil
}

// Grow ensures the graph has capacity for the given size.
// If validDims > 0 is provided, it forces the allocation of vector storage (Vectors, SQ8, PQ)
// even if the global h.dims hasn't been updated yet. This supports race-free initialization.
func (h *ArrowHNSW) Grow(minCap int, validDims int) {
	h.growMu.Lock()
	defer h.growMu.Unlock()

	metrics.HNSWResizesTotal.WithLabelValues("default").Inc()

	// Ensure location store capacity
	h.locationStore.EnsureCapacity(VectorID(minCap - 1))

	// Ensure bitset capacity
	// h.deleted (roaring) handles capacity dynamically

	// Determine dimensions to use for allocation
	// Prefer the explicit argument if valid (>0), otherwise fallback to global
	targetDims := validDims
	if targetDims <= 0 {
		targetDims = int(h.dims.Load())
	}

	// Ensure graph data chunks
	dataPtr := h.data.Load()
	if dataPtr == nil {
		h.data.Store(NewGraphData(minCap, targetDims, h.config.SQ8Enabled, h.config.PQEnabled))
		return
	}

	// Check if growth OR structure upgrade needed
	// Capacity is numChunks * ChunkSize
	// Check if growth OR structure upgrade needed
	// Capacity is numChunks * ChunkSize

	// Use targetDims for the check: if targetDims > 0 and we lack Vectors, upgrades needed
	dimsChanged := targetDims > 0 && dataPtr.Vectors == nil
	sq8Changed := h.config.SQ8Enabled && dataPtr.VectorsSQ8 == nil
	pqChanged := h.config.PQEnabled && dataPtr.VectorsPQ == nil

	if minCap <= dataPtr.Capacity && !dimsChanged && !sq8Changed && !pqChanged {
		return
	}

	// Clone and grow
	newData := dataPtr.Clone(minCap, targetDims, h.config.SQ8Enabled, h.config.PQEnabled)
	h.data.Store(newData)
	h.backend.Store(newData)
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
						neighborsChunk[baseIdx+writeIdx] = neighborID
					}
					writeIdx++
				}
			}

			// Zero out remainder
			for k := writeIdx; k < count; k++ {
				neighborsChunk[baseIdx+k] = 0
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

// Clone creates a new GraphData with at least the specified capacity, copying existing data.
// This is used for "Stop-The-World" growth (rare).
func (gd *GraphData) Clone(minCap int, targetDims int, sq8Enabled bool, pqEnabled bool) *GraphData {
	dims := targetDims
	if dims == 0 && len(gd.Vectors) > 0 && gd.Vectors[0] != nil {
		dims = 1 // Just needs to be > 0 to trigger vector allocation
	}
	newGD := NewGraphData(minCap, dims, sq8Enabled, pqEnabled)

	// Copy Levels
	copy(newGD.Levels, gd.Levels)

	// Copy Vectors
	if len(gd.Vectors) > 0 {
		copy(newGD.Vectors, gd.Vectors)
	}
	if len(gd.VectorsSQ8) > 0 {
		copy(newGD.VectorsSQ8, gd.VectorsSQ8)
	}
	if len(gd.VectorsPQ) > 0 {
		copy(newGD.VectorsPQ, gd.VectorsPQ)
	}

	// Copy Layers
	for i := 0; i < ArrowMaxLayers; i++ {
		copy(newGD.Neighbors[i], gd.Neighbors[i])
		copy(newGD.Counts[i], gd.Counts[i])
		copy(newGD.Versions[i], gd.Versions[i])
	}

	return newGD
}

// Size returns the number of nodes in the index.
func (h *ArrowHNSW) Size() int {
	return int(h.nodeCount.Load())
}

// IsDeleted checks if the node has been marked as deleted.
func (h *ArrowHNSW) IsDeleted(id uint32) bool {
	return h.deleted.Contains(int(id))
}

// GetEntryPoint returns the current entry point ID.
func (h *ArrowHNSW) GetEntryPoint() uint32 {
	return h.entryPoint.Load()
}

// GetMaxLevel returns the maximum level in the graph.
func (h *ArrowHNSW) GetMaxLevel() int {
	return int(h.maxLevel.Load())
}

// GetPQEncoder returns the PQ encoder if one exists.
func (h *ArrowHNSW) GetPQEncoder() *PQEncoder {
	// No lock needed if pqEncoder is immutable after training?
	// It is set during TrainPQ and never changed.
	return h.pqEncoder
}

// Helper functions for chunk calculation
func chunkID(id uint32) uint32 {
	return id >> ChunkShift
}

func chunkOffset(id uint32) uint32 {
	return id & ChunkMask
}

// ArrowSearchContextPool manages reusable search contexts.
type ArrowSearchContextPool struct {
	pool sync.Pool
}

// NewArrowSearchContextPool creates a new search context pool.
func NewArrowSearchContextPool() *ArrowSearchContextPool {
	return &ArrowSearchContextPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &ArrowSearchContext{
					candidates: NewFixedHeap(8000),      // Larger for high EfConstruction
					visited:    NewArrowBitset(2000000), // Support up to 2M vectors
					results:    make([]SearchResult, 0, 100),
					resultSet:  NewMaxHeap(8000),
					// Scratch buffers (sized for EfConstruction up to 2000+ and M up to 256)
					scratchResults:       make([]Candidate, 0, 8000),
					scratchVecs:          make([][]float32, 8000),
					scratchDists:         make([]float32, 8000),
					scratchDiscarded:     make([]Candidate, 0, 8000),
					scratchSelected:      make([]Candidate, 0, 1000),
					scratchRemaining:     make([]Candidate, 8000),
					scratchRemainingVecs: make([][]float32, 8000),
					scratchSelectedVecs:  make([][]float32, 0, 1000),
					scratchVecsSQ8:       make([][]byte, 8000),
					scratchSelectedIdxs:  make([]int, 0, 1000),
					scratchNeighbors:     make([]uint32, 0, MaxNeighbors),
					// PQ Scratch
					scratchPQTable: make([]float32, 32768), // Increased for larger M
					scratchIDs:     make([]uint32, 0, 8000),
				}
			},
		},
	}
}

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
	ctx.scratchSelectedVecs = ctx.scratchSelectedVecs[:0]
	p.pool.Put(ctx)
}

// ArrowSearchContext holds temporary state for a single search operation.
type ArrowSearchContext struct {
	candidates *FixedHeap
	visited    *ArrowBitset
	results    []SearchResult
	resultSet  *MaxHeap

	// Scratch buffers to avoid allocations in selectNeighbors/pruneConnections
	scratchResults       []Candidate
	scratchVecs          [][]float32
	scratchDists         []float32
	scratchDiscarded     []Candidate
	scratchSelected      []Candidate
	scratchRemaining     []Candidate
	scratchRemainingVecs [][]float32
	scratchSelectedVecs  [][]float32
	scratchVecsSQ8       [][]byte
	scratchNeighbors     []uint32  // Raw neighbor list from backend
	scratchPQTable       []float32 // For ADC table
	scratchIDs           []uint32  // For batching unvisited neighbors
	querySQ8             []byte    // SQ8 encoded query

	// Selection heuristic scratch
	scratchSelectedIdxs []int // For tracking selected indices in selectNeighbors

	// Recursion depth for pruneConnections
	pruneDepth int
}

// Atomic Accessors for GraphData chunks to prevent data races during lazy allocation.

// LoadVectorChunk atomically loads the vector chunk for the given chunk ID.
func (g *GraphData) LoadVectorChunk(cID uint32) *[]float32 {
	if int(cID) >= len(g.Vectors) {
		return nil
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.Vectors[cID]))
	return (*[]float32)(atomic.LoadPointer(ptr))
}

// StoreVectorChunk atomically stores the vector chunk for the given chunk ID.
func (g *GraphData) StoreVectorChunk(cID uint32, chunk *[]float32) {
	if int(cID) >= len(g.Vectors) {
		return
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.Vectors[cID]))
	atomic.StorePointer(ptr, unsafe.Pointer(chunk))
}

// LoadSQ8Chunk atomically loads the SQ8 chunk for the given chunk ID.
func (g *GraphData) LoadSQ8Chunk(cID uint32) *[]byte {
	if g.VectorsSQ8 == nil || int(cID) >= len(g.VectorsSQ8) {
		return nil
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.VectorsSQ8[cID]))
	return (*[]byte)(atomic.LoadPointer(ptr))
}

// StoreSQ8Chunk atomically stores the SQ8 chunk for the given chunk ID.
func (g *GraphData) StoreSQ8Chunk(cID uint32, chunk *[]byte) {
	if g.VectorsSQ8 == nil || int(cID) >= len(g.VectorsSQ8) {
		return
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.VectorsSQ8[cID]))
	atomic.StorePointer(ptr, unsafe.Pointer(chunk))
}

// LoadPQChunk atomically loads the PQ chunk for the given chunk ID.
func (g *GraphData) LoadPQChunk(cID uint32) *[]byte {
	if g.VectorsPQ == nil || int(cID) >= len(g.VectorsPQ) {
		return nil
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.VectorsPQ[cID]))
	return (*[]byte)(atomic.LoadPointer(ptr))
}

// StorePQChunk atomically stores the PQ chunk for the given chunk ID.
func (g *GraphData) StorePQChunk(cID uint32, chunk *[]byte) {
	if g.VectorsPQ == nil || int(cID) >= len(g.VectorsPQ) {
		return
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.VectorsPQ[cID]))
	atomic.StorePointer(ptr, unsafe.Pointer(chunk))
}

// LoadLevelChunk atomically loads the level chunk for the given chunk ID.
func (g *GraphData) LoadLevelChunk(cID uint32) *[]uint8 {
	if int(cID) >= len(g.Levels) {
		return nil
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.Levels[cID]))
	return (*[]uint8)(atomic.LoadPointer(ptr))
}

// StoreLevelChunk atomically stores the level chunk for the given chunk ID.
func (g *GraphData) StoreLevelChunk(cID uint32, chunk *[]uint8) {
	if int(cID) >= len(g.Levels) {
		return
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.Levels[cID]))
	atomic.StorePointer(ptr, unsafe.Pointer(chunk))
}

// LoadNeighborsChunk atomically loads the neighbors chunk for the given layer and chunk ID.
func (g *GraphData) LoadNeighborsChunk(layer int, cID uint32) *[]uint32 {
	if layer >= len(g.Neighbors) || int(cID) >= len(g.Neighbors[layer]) {
		return nil
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.Neighbors[layer][cID]))
	return (*[]uint32)(atomic.LoadPointer(ptr))
}

// StoreNeighborsChunk atomically stores the neighbors chunk for the given layer and chunk ID.
func (g *GraphData) StoreNeighborsChunk(layer int, cID uint32, chunk *[]uint32) {
	if layer >= len(g.Neighbors) || int(cID) >= len(g.Neighbors[layer]) {
		return
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.Neighbors[layer][cID]))
	atomic.StorePointer(ptr, unsafe.Pointer(chunk))
}

// LoadCountsChunk atomically loads the counts chunk for the given layer and chunk ID.
func (g *GraphData) LoadCountsChunk(layer int, cID uint32) *[]int32 {
	if layer >= len(g.Counts) || int(cID) >= len(g.Counts[layer]) {
		return nil
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.Counts[layer][cID]))
	return (*[]int32)(atomic.LoadPointer(ptr))
}

// StoreCountsChunk atomically stores the counts chunk for the given layer and chunk ID.
func (g *GraphData) StoreCountsChunk(layer int, cID uint32, chunk *[]int32) {
	if layer >= len(g.Counts) || int(cID) >= len(g.Counts[layer]) {
		return
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.Counts[layer][cID]))
	atomic.StorePointer(ptr, unsafe.Pointer(chunk))
}

// LoadVersionsChunk atomically loads the versions chunk for the given layer and chunk ID.
func (g *GraphData) LoadVersionsChunk(layer int, cID uint32) *[]uint32 {
	if layer >= len(g.Versions) || int(cID) >= len(g.Versions[layer]) {
		return nil
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.Versions[layer][cID]))
	return (*[]uint32)(atomic.LoadPointer(ptr))
}

// StoreVersionsChunk atomically stores the versions chunk for the given layer and chunk ID.
func (g *GraphData) StoreVersionsChunk(layer int, cID uint32, chunk *[]uint32) {
	if layer >= len(g.Versions) || int(cID) >= len(g.Versions[layer]) {
		return
	}
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&g.Versions[layer][cID]))
	atomic.StorePointer(ptr, unsafe.Pointer(chunk))
}

// GetNeighbors implements GraphBackend.
// It resolves the chunk and offset to return the neighbor slice.
// GetNeighbors implements GraphBackend.
// It uses Seqlock logic to ensure a consistent view of the neighbors list.
// If 'buf' is provided and has sufficient capacity, it is used to avoid allocation.
func (gd *GraphData) GetNeighbors(layer int, nodeID uint32, buf []uint32) []uint32 {
	if layer >= ArrowMaxLayers || layer >= len(gd.Neighbors) {
		return nil
	}
	cID := chunkID(nodeID)
	cOff := chunkOffset(nodeID)

	if int(cID) >= len(gd.Neighbors[layer]) {
		return nil
	}

	// Seqlock Loop
	for {
		// 1. Read Version (Start)
		verChunk := gd.LoadVersionsChunk(layer, cID)
		if verChunk == nil {
			// If version chunk is missing, the node likely doesn't exist fully yet
			return nil
		}
		verAddr := &(*verChunk)[cOff]
		ver := atomic.LoadUint32(verAddr)

		// If dirty (odd), wait and retry
		if ver%2 != 0 {
			runtime.Gosched()
			continue
		}

		// 2. Read Data
		// We use atomic loads for chunk pointers to ensure we see initialized chunks
		countChunk := gd.LoadCountsChunk(layer, cID)
		neighborsChunk := gd.LoadNeighborsChunk(layer, cID)

		if countChunk == nil || neighborsChunk == nil {
			// Inconsistent state or missing chunks, retry or return nil
			// If we are reading a valid node, these should exist.
			// But if we are racing with creation?
			// Check version again?
			if atomic.LoadUint32(verAddr) != ver {
				continue
			}
			return nil
		}

		count := atomic.LoadInt32(&(*countChunk)[cOff])
		if count <= 0 {
			// No neighbors or empty
			// Check version to be sure
			if atomic.LoadUint32(verAddr) != ver {
				continue
			}
			return nil
		}

		if count > MaxNeighbors {
			count = MaxNeighbors // Safety cap
		}

		baseIdx := int(cOff) * MaxNeighbors

		// 3. Copy Data
		var res []uint32
		if cap(buf) >= int(count) {
			res = buf[:count]
		} else {
			res = make([]uint32, count)
		}

		copy(res, (*neighborsChunk)[baseIdx:baseIdx+int(count)])

		// 4. Read Version (End)
		if atomic.LoadUint32(verAddr) == ver {
			return res
		}

		// Failed, data was modified during read, retry
		runtime.Gosched()
	}
}

// GetLevel returns the level of the node.
// Currently GraphData stores levels in a separate chunked array.
func (gd *GraphData) GetLevel(nodeID uint32) int {
	cID := chunkID(nodeID)
	cOff := chunkOffset(nodeID)

	chunkPtr := gd.LoadLevelChunk(cID)
	if chunkPtr == nil {
		return -1
	}
	return int((*chunkPtr)[cOff])
}

func (gd *GraphData) GetVectorSQ8(nodeID uint32) []byte {
	cID := chunkID(nodeID)
	cOff := chunkOffset(nodeID)
	dims := gd.Dims

	if dims == 0 {
		return nil
	}

	vecChunk := gd.LoadSQ8Chunk(cID)
	// Check chunk existence and bounds
	if vecChunk != nil {
		start := int(cOff) * dims
		end := start + dims
		if end <= len(*vecChunk) {
			return (*vecChunk)[start:end]
		}
	}
	return nil
}

func (gd *GraphData) GetVectorPQ(nodeID uint32) []byte {
	return nil // Same issue
}

// Implement GraphBackend.Size (?)
// GraphData represents a snapshot of specific capacity. It doesn't track "Active Node Count".
// ArrowHNSW tracks node count.
// But Size() in backend context might mean Capacity or MaxNodeID?
// Let's implement Capacity() for GraphData.
func (gd *GraphData) GetCapacity() int {
	return gd.Capacity
}

// Size returns Capacity for GraphData as it's a fixed-size container effectively.
func (gd *GraphData) Size() int {
	return gd.Capacity
}

// Close is a no-op for memory backend
func (gd *GraphData) Close() error {
	return nil
}
