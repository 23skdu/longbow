package hnsw2

import (
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
	VectorPtrs []unsafe.Pointer // [Capacity]
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
	}
	for i := 0; i < MaxLayers; i++ {
		gd.Neighbors[i] = make([]uint32, capacity*MaxNeighbors)
		gd.Counts[i] = make([]int32, capacity)
	}
	return gd
}

const (
	MaxLayers    = 16 // Maximum number of layers in the graph
	MaxNeighbors = 64 // Maximum neighbors per node per layer (Mmax)
)

// ArrowHNSW is the main HNSW index structure with Arrow integration.
// It uses atomic pointers for lock-free search and a mutex for serialized writes.
type ArrowHNSW struct {
	// Graph data snapshot (lock-free access)
	data    atomic.Pointer[GraphData]
	writeMu sync.Mutex

	// Graph state (atomic for lock-free reads)
	entryPoint atomic.Uint32
	maxLevel   atomic.Int32
	nodeCount  atomic.Uint32

	// Arrow integration - reference to parent dataset for vector access
	dataset *store.Dataset
	dims    int // Vector dimensions, cached for unsafe operations

	// HNSW parameters
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
	M              int     // Number of neighbors (default: 16)
	MMax           int     // Max neighbors layer 0 (default: M * 2)
	MMax0          int     // Max neighbors higher layers (default: M)
	EfConstruction int     // Construction search depth (default: 200)
	Ml             float64 // Level multiplier (default: 1/ln(2))
}

// DefaultConfig returns sensible default HNSW parameters.
func DefaultConfig() Config {
	return Config{
		M:              16,
		MMax:           32,
		MMax0:          16,
		EfConstruction: 200,
		Ml:             1.44269504089, // 1/ln(2)
	}
}

// NewArrowHNSW creates a new Arrow-native HNSW index.
func NewArrowHNSW(dataset *store.Dataset, config Config) *ArrowHNSW {
	h := &ArrowHNSW{
		dataset:        dataset,
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
}
