package store

import (
	"math"
	"sync"
	"sync/atomic"

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
	data atomic.Pointer[GraphData]

	// Locking strategy:
	// initMu protects the initialization of the first node (entry point) and global dimension changes.
	initMu sync.Mutex

	// growMu protects the GraphData pointer and Global growth operations.
	// Insert() does NOT take this lock (lock-free read). Grow() takes Lock (exclusive).
	growMu sync.Mutex

	// shardedLocks protects individual node neighbor lists during updates.
	// lockID = nodeID % 1024
	shardedLocks [1024]sync.Mutex

	// Graph state (atomic for lock-free reads)
	entryPoint atomic.Uint32
	// Maximum level in the graph
	maxLevel atomic.Int32

	// Total number of nodes in the graph
	nodeCount atomic.Uint32

	// Component dependencies
	quantizer *ScalarQuantizer
	pqEncoder *PQEncoder

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
		M:                   32,
		MMax:                96, // Layer 0: 3 * M (Target is 2 * M = 64)
		MMax0:               48, // Layer > 0: 1.5 * M
		EfConstruction:      400,
		Ml:                  1.0 / math.Log(32),
		Alpha:               1.1,
		SQ8Enabled:          false,
		RefinementFactor:    1.0,
		InitialCapacity:     1000,
		AdaptiveEf:          false, // Disabled by default (opt-in)
		AdaptiveEfMin:       0,     // Auto-calculate as EfConstruction/4
		AdaptiveEfThreshold: 0,     // Auto-calculate as InitialCapacity/2
		PQEnabled:           false,
		PQM:                 0, // Auto-calculate?
		PQK:                 256,
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
	levelsExist := data.Levels[cID] != nil
	vectorsExist := true
	if dims > 0 {
		vectorsExist = data.Vectors != nil && data.Vectors[cID] != nil
		if h.config.SQ8Enabled {
			vectorsExist = vectorsExist && data.VectorsSQ8[cID] != nil
		}
		if h.config.PQEnabled {
			vectorsExist = vectorsExist && data.VectorsPQ[cID] != nil
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
		levelsExist = data.Levels[cID] != nil
		vectorsExist = true
		if dims > 0 {
			vectorsExist = data.Vectors != nil && data.Vectors[cID] != nil
			if h.config.SQ8Enabled {
				vectorsExist = vectorsExist && data.VectorsSQ8[cID] != nil
			}
			if h.config.PQEnabled {
				vectorsExist = vectorsExist && data.VectorsPQ[cID] != nil
			}
		}
		if levelsExist && vectorsExist {
			return data, nil
		}
	}

	// Re-check under lock (double-checked locking)
	if data.Levels[cID] == nil {
		levels := make([]uint8, ChunkSize)
		data.Levels[cID] = &levels

		// Allocate layers only if levels was nil (new chunk completely)
		for i := 0; i < ArrowMaxLayers; i++ {
			neighbors := make([]uint32, ChunkSize*MaxNeighbors)
			data.Neighbors[i][cID] = &neighbors

			counts := make([]int32, ChunkSize)
			data.Counts[i][cID] = &counts

			versions := make([]uint32, ChunkSize)
			data.Versions[i][cID] = &versions
		}
	}

	if dims > 0 {
		if data.Vectors != nil && data.Vectors[cID] == nil {
			vecs := make([]float32, ChunkSize*dims)
			data.Vectors[cID] = &vecs
		}

		if h.config.SQ8Enabled && data.VectorsSQ8 != nil && data.VectorsSQ8[cID] == nil {
			sq8 := make([]byte, ChunkSize*dims)
			data.VectorsSQ8[cID] = &sq8
		}

		if h.config.PQEnabled && data.VectorsPQ != nil && data.VectorsPQ[cID] == nil {
			// PQ storage size: ChunkSize * M
			m := h.config.PQM
			if m == 0 && h.pqEncoder != nil {
				m = h.pqEncoder.config.NumSubVectors
			}
			if m > 0 {
				pqVecs := make([]byte, ChunkSize*m)
				data.VectorsPQ[cID] = &pqVecs
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
	scratchPQTable       []float32 // For ADC table
	scratchIDs           []uint32  // For batching unvisited neighbors
	querySQ8             []byte    // SQ8 encoded query

	// Selection heuristic scratch
	scratchSelectedIdxs []int // For tracking selected indices in selectNeighbors

	// Recursion depth for pruneConnections
	pruneDepth int
}
