package hnsw2
 
import (
	"math"
	"sync"
	"sync/atomic"
	"unsafe"
	
	"github.com/23skdu/longbow/internal/store"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// GraphData holds the SoA (Struct of Arrays) representation of the graph.
// This structure is designed for lock-free snapshot access.
type GraphData struct {
	Capacity     int
	// SoA Slices
	Levels     []uint8          // [Capacity]
	// VectorPtrs holds unsafe pointers to the vectors in the Arrow buffers.
	// This avoids holding references to Arrow arrays preventing GC,
	// but requires careful management of lifetime.
	VectorPtrs []unsafe.Pointer // [Capacity]
	
	// VectorsSQ8 holds dense packed uint8 vectors for Scalar Quantization.
	// Layout: [vec0_byte0, vec0_byte1... vec1_byte0...]
	// Size: Capacity * Dims
	VectorsSQ8 []byte
	
	// Neighbors flattened per layer: Neighbors[layer][nodeID*MaxNeighbors + index]
	Neighbors [MaxLayers][]uint32
	// Counts per layer and node: Counts[layer][nodeID]
	// Accessed atomically
	Counts [MaxLayers][]int32
	
	// Versions per layer and node: Versions[layer][nodeID]
	// Used for optimistic locking (Seqlock) during neighbor updates
	Versions [MaxLayers][]uint32
}

// NewGraphData creates a new GraphData with the specified capacity.
func NewGraphData(capacity int) *GraphData {
	gd := &GraphData{
		Capacity:   capacity,
		Levels:     make([]uint8, capacity),
		VectorPtrs: make([]unsafe.Pointer, capacity),
		// VectorsSQ8 initialized if quantization is enabled.
	}
	for i := 0; i < MaxLayers; i++ {
		gd.Neighbors[i] = make([]uint32, capacity*MaxNeighbors)
		gd.Counts[i] = make([]int32, capacity)
		gd.Versions[i] = make([]uint32, capacity)
	}
	return gd
}

const (
	MaxLayers    = 10 // Maximum number of layers in the graph
	MaxNeighbors = 448 // Maximum neighbors per node per layer (sufficient for M up to 128, MMax up to 256)
)

// ArrowHNSW is the main HNSW index structure with Arrow integration.
// It uses atomic pointers for lock-free search and a mutex for serialized writes.
type ArrowHNSW struct {
	// Graph data snapshot (lock-free access)
	data    atomic.Pointer[GraphData]
	
	// Locking strategy:
	// initMu protects the initialization of the first node (entry point).
	initMu sync.Mutex

	// resizeMu protects the GraphData pointer and global changes (like Grow).
	// Insert() takes RLock (shared), Grow() takes Lock (exclusive).
	resizeMu sync.RWMutex
	
	// shardedLocks protects individual node neighbor lists during updates.
	// lockID = nodeID % 1024
	shardedLocks [1024]sync.Mutex

	// Graph state (atomic for lock-free reads)
	entryPoint atomic.Uint32
	// Maximum level in the graph
	maxLevel      atomic.Int32
	
	// Total number of nodes in the graph
	nodeCount     atomic.Uint32
	
	// Component dependencies
	quantizer     *ScalarQuantizer
	
	// Caches
	vectorColIdx  int // Index of the vector column in the Arrow schema

	// Arrow integration - reference to parent dataset for vector access
	dataset *store.Dataset
	dims    int // Vector dimensions, cached for unsafe operations

	// HNSW parameters
	config         Config  // Store full config
	m              int     // Number of neighbors per layer
	mMax           int     // Maximum neighbors for layer 0
	mMax0          int     // Maximum neighbors for higher layers
	efConstruction int     // Size of dynamic candidate list during construction
	ml             float64 // Level multiplier for exponential decay

	// Pooled resources
	searchPool    *SearchContextPool
	batchComputer *BatchDistanceComputer // Vectorized distance computation
	
	// Location tracking for VectorIndex implementation
	locationStore *store.ChunkedLocationStore
}

// Config holds HNSW configuration parameters.
type Config struct {
	M              int     // Max neighbors for layer > 0
	MMax           int     // Max neighbors for layer 0
	MMax0          int     // Max connected components for layer 0 (actually MMax0 is layer > 0 max neighbors during construction?)
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
}

// DefaultConfig returns sensible default HNSW parameters.
// Tuned for high recall (99.5%+) on 10K+ vector datasets.
func DefaultConfig() Config {
	return Config{
		M:              32,
		MMax:           96, // Layer 0: 3 * M (Target is 2 * M = 64)
		MMax0:          48, // Layer > 0: 1.5 * M
		EfConstruction: 400,
		Ml:             1.0 / math.Log(32),
		Alpha:          1.1,
		SQ8Enabled:     false,
		RefinementFactor: 1.0,
		InitialCapacity: 1000,
	}
}

// NewArrowHNSW creates a new Arrow-native HNSW index.
func NewArrowHNSW(dataset *store.Dataset, config Config) *ArrowHNSW {
	// Initialize HNSW
	h := &ArrowHNSW{
		config:        config,
		dataset:       dataset,
		entryPoint:    atomic.Uint32{},
		maxLevel:      atomic.Int32{},
		searchPool:    NewSearchContextPool(),
		resizeMu:      sync.RWMutex{},
		locationStore: store.NewChunkedLocationStore(),
		vectorColIdx:  -1,
	}
	h.entryPoint.Store(0)
	h.maxLevel.Store(-1)

	// Determine vector column index
	if dataset != nil && dataset.Schema != nil {
		for i, field := range dataset.Schema.Fields() {
			if field.Name == "vector" {
				h.vectorColIdx = i
				break
			}
		}
		// Fallback: Use 1 if available (common convention), else 0 if list
		if h.vectorColIdx == -1 {
			if len(dataset.Schema.Fields()) > 1 {
				h.vectorColIdx = 1
			} else if len(dataset.Schema.Fields()) > 0 {
				h.vectorColIdx = 0
			}
		}
	}

	// Set HNSW parameters from config
	h.m = config.M
	h.mMax = config.MMax
	h.mMax0 = config.MMax0
	h.efConstruction = config.EfConstruction
	h.ml = config.Ml

	// Initialize batch distance computer for vectorized operations
	// Dimensions will be set when first vector is inserted
	if dataset != nil && dataset.Schema != nil {
		// Try to get dimensions from schema
		for _, field := range dataset.Schema.Fields() {
			if field.Name == "vector" {
				if fsl, ok := field.Type.(*arrow.FixedSizeListType); ok {
					h.dims = int(fsl.Len())
					// Use Go allocator for batch computer
					h.batchComputer = NewBatchDistanceComputer(memory.NewGoAllocator(), h.dims)
					
					// Initialize Quantizer if enabled
					if config.SQ8Enabled {
						h.quantizer = NewScalarQuantizer(h.dims)
					}
					break
				}
			}
		}
	}

	// Initialize empty graph with initial capacity
	initialCap := config.InitialCapacity
	if initialCap < 1000 {
		initialCap = 1000
	}
	initData := NewGraphData(initialCap)
	
	// Pre-allocate SQ8 storage if enabled
	if config.SQ8Enabled && h.dims > 0 {
		initData.VectorsSQ8 = make([]byte, initialCap*h.dims)
	}
	
	h.data.Store(initData)
	h.entryPoint.Store(0)
	h.maxLevel.Store(-1)
	h.nodeCount.Store(0)

	return h
}

// Grow ensures the graph has enough capacity for the requested ID.
// It uses a Copy-On-Resize strategy: a new GraphData is allocated, data copied, and atomically swapped.
func (h *ArrowHNSW) Grow(minCap int) {
	// Acquire exclusive lock for resizing
	h.resizeMu.Lock()
	defer h.resizeMu.Unlock()
	
	// Double-check capacity under lock
	oldData := h.data.Load()
	
	// Check if we need to grow for Capacity OR for missing SQ8 storage
	sq8Missing := h.config.SQ8Enabled && len(oldData.VectorsSQ8) == 0 && h.dims > 0
	
	if oldData.Capacity > minCap && !sq8Missing {
		return
	}

	// Calculate new capacity
	newCap := oldData.Capacity * 2
	if newCap < minCap {
		newCap = minCap
	}
	if newCap < 1000 {
		newCap = 1000
	}

	// Allocate new data
	newData := NewGraphData(newCap)

	// Copy existing data
	// 1. Levels
	copy(newData.Levels, oldData.Levels)
	
	// 2. VectorPtrs
	copy(newData.VectorPtrs, oldData.VectorPtrs)
	
	// 2b. VectorsSQ8 (SQ8 storage)
	// Check if we need to migrate/create SQ8 storage
	if len(oldData.VectorsSQ8) > 0 {
		// Existing storage, calc dim from ratio
		dim := len(oldData.VectorsSQ8) / oldData.Capacity
		newData.VectorsSQ8 = make([]byte, newCap*dim)
		copy(newData.VectorsSQ8, oldData.VectorsSQ8)
	} else if h.config.SQ8Enabled && h.dims > 0 {
		// New storage (was missing)
		newData.VectorsSQ8 = make([]byte, newCap*h.dims)
	}
	
	// 3. Neighbors and Counts for each layer
	for i := 0; i < MaxLayers; i++ {
		copy(newData.Neighbors[i], oldData.Neighbors[i])
		copy(newData.Counts[i], oldData.Counts[i])
	}
	
	// Atomically switch to new data
	h.data.Store(newData)
}

// Size returns the number of nodes in the index.
func (h *ArrowHNSW) Size() int {
	return int(h.nodeCount.Load())
}

// GetEntryPoint returns the current entry point ID.
func (h *ArrowHNSW) GetEntryPoint() uint32 {
	return h.entryPoint.Load()
}

// GetMaxLevel returns the maximum level in the graph.
func (h *ArrowHNSW) GetMaxLevel() int {
	return int(h.maxLevel.Load())
}

// SearchContextPool manages reusable search contexts.
type SearchContextPool struct {
	pool sync.Pool
}

// NewSearchContextPool creates a new search context pool.
func NewSearchContextPool() *SearchContextPool {
	return &SearchContextPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &SearchContext{
					candidates: NewFixedHeap(8000), // Larger for high EfConstruction
					visited:    NewBitset(2000000), // Support up to 2M vectors
					results:    make([]store.SearchResult, 0, 100),
					resultSet:  NewMaxHeap(8000), 
					// Scratch buffers (sized for EfConstruction up to 2000+ and M up to 256)
					scratchResults:   make([]Candidate, 0, 8000),
					scratchVecs:      make([][]float32, 8000),
					scratchDists:     make([]float32, 8000),
					scratchDiscarded: make([]Candidate, 0, 8000),
					scratchSelected:  make([]Candidate, 0, 1000),
					scratchRemaining: make([]Candidate, 8000),
					scratchRemainingVecs: make([][]float32, 8000),
					scratchSelectedVecs:  make([][]float32, 0, 1000),
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
func (p *SearchContextPool) Get() *SearchContext {
	return p.pool.Get().(*SearchContext)
}

// Put returns a search context to the pool.
func (p *SearchContextPool) Put(ctx *SearchContext) {
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

// SearchContext holds temporary state for a single search operation.
type SearchContext struct {
	candidates *FixedHeap
	visited    *Bitset
	results    []store.SearchResult
	resultSet  *MaxHeap
	
	// Scratch buffers to avoid allocations in selectNeighbors/pruneConnections
	scratchResults   []Candidate
	scratchVecs      [][]float32
	scratchDists     []float32
	scratchDiscarded []Candidate
	scratchSelected  []Candidate
	scratchRemaining []Candidate
	scratchRemainingVecs [][]float32
	scratchSelectedVecs  [][]float32
	scratchPQTable   []float32 // For ADC table
	scratchIDs       []uint32  // For batching unvisited neighbors
	querySQ8         []byte    // SQ8 encoded query
	
	// Selection heuristic scratch
	scratchSelectedIdxs []int     // For tracking selected indices in selectNeighbors
	
	// Recursion depth for pruneConnections
	pruneDepth int
}
