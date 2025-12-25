package hnsw2
 
import (
	"math"
	"sync"
	"sync/atomic"
	"unsafe"
	
	"github.com/23skdu/longbow/internal/store"
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
	
	// QuantizedVectors holds dense packed PQ codes for all vectors.
	// Layout: [vec0_code0, vec0_code1... vec1_code0...]
	// Size: Capacity * PQ_M
	QuantizedVectors []byte
	// Neighbors flattened per layer: Neighbors[layer][nodeID*MaxNeighbors + index]
	Neighbors [MaxLayers][]uint32
	// Counts per layer and node: Counts[layer][nodeID]
	// Accessed atomically
	Counts [MaxLayers][]int32
}

// NewGraphData creates a new GraphData with the specified capacity.
func NewGraphData(capacity int) *GraphData {
	gd := &GraphData{
		Capacity:   capacity,
		Levels:     make([]uint8, capacity),
		VectorPtrs: make([]unsafe.Pointer, capacity),
		// QuantizedVectors initialized lazily or if config present?
		// For now just empty.
	}
	for i := 0; i < MaxLayers; i++ {
		gd.Neighbors[i] = make([]uint32, capacity*MaxNeighbors)
		gd.Counts[i] = make([]int32, capacity)
	}
	return gd
}

const (
	MaxLayers    = 16 // Maximum number of layers in the graph
	MaxNeighbors = 192 // Maximum neighbors per node per layer (must be > MMax to allow Add-then-Prune)
)

// ArrowHNSW is the main HNSW index structure with Arrow integration.
// It uses atomic pointers for lock-free search and a mutex for serialized writes.
type ArrowHNSW struct {
	// Graph data snapshot (lock-free access)
	// Graph data snapshot (lock-free access)
	data    atomic.Pointer[GraphData]
	
	// Locking strategy:
	// resizeMu protects the GraphData pointer and global changes (like Grow).
	// Insert() takes RLock (shared), Grow() takes Lock (exclusive).
	resizeMu sync.RWMutex
	
	// shardedLocks protects individual node neighbor lists during updates.
	// lockID = nodeID % 1024
	shardedLocks [1024]sync.Mutex

	// Graph state (atomic for lock-free reads)
	entryPoint atomic.Uint32
	maxLevel   atomic.Int32
	nodeCount  atomic.Uint32
	
	pqEncoder *PQEncoder

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
	searchPool *SearchContextPool
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

	// PQ specifies the Product Quantization configuration.
	PQ PQConfig
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
	}
}

// NewArrowHNSW creates a new Arrow-native HNSW index.
func NewArrowHNSW(dataset *store.Dataset, config Config) *ArrowHNSW {
	h := &ArrowHNSW{
		dataset:        dataset,
		config:         config, // Store full config
		m:              config.M,
		mMax:           config.MMax,
		mMax0:          config.MMax0,
		efConstruction: config.EfConstruction,
		ml:             config.Ml,
		searchPool:     NewSearchContextPool(),
	}

	// Initialize empty graph with initial capacity
	initialCap := 1000
	h.data.Store(NewGraphData(initialCap))
	h.entryPoint.Store(0)
	h.maxLevel.Store(-1)
	h.nodeCount.Store(0)

	return h
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
					candidates: NewFixedHeap(2000), // Increase to 2000 for larger ef
					visited:    NewBitset(100000),
					results:    make([]store.SearchResult, 0, 100),
					resultSet:  NewMaxHeap(2000), // Pooled result set heap
					// Scratch buffers
					scratchVecs:      make([][]float32, 2000), // Max capacity > efConstruction
					scratchDists:     make([]float32, 2000),
					scratchDiscarded: make([]Candidate, 0, 2000),
					// PQ Scratch
					// M*256 floats. Max M=64 -> 16384 floats. Safe.
					scratchPQTable: make([]float32, 16384),
					scratchIDs:     make([]uint32, 0, 2000),
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
	p.pool.Put(ctx)
}

// SearchContext holds temporary state for a single search operation.
type SearchContext struct {
	candidates *FixedHeap
	visited    *Bitset
	results    []store.SearchResult
	resultSet  *MaxHeap
	
	// Scratch buffers to avoid allocations in selectNeighbors/pruneConnections
	scratchVecs      [][]float32
	scratchDists     []float32
	scratchDiscarded []Candidate
	scratchPQTable   []float32 // For ADC table
	scratchIDs       []uint32  // For batching unvisited neighbors
}
