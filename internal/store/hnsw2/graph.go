package hnsw2

import (
	"sync"
	
	"github.com/23skdu/longbow/internal/store"
)

// GraphNode represents a single node in the HNSW graph.
// It uses fixed-size arrays to avoid allocations during neighbor updates.
type GraphNode struct {
	ID    uint32  // VectorID
	Level uint8   // Maximum layer this node participates in
	
	// Neighbors at each layer (layer 0 to Level)
	// Each layer has up to M neighbors (Mmax for layer 0)
	// We use a fixed-size array to avoid allocations
	Neighbors [MaxLayers][MaxNeighbors]uint32
	NeighborCounts [MaxLayers]uint8
}

const (
	MaxLayers = 16      // Maximum number of layers in the graph
	MaxNeighbors = 32   // Maximum neighbors per node per layer (Mmax)
)

// ArrowHNSW is the main HNSW index structure with Arrow integration.
type ArrowHNSW struct {
	// Graph structure
	nodes      []GraphNode
	nodeMu     sync.RWMutex
	entryPoint uint32
	maxLevel   int
	
	// Arrow integration - reference to parent dataset for vector access
	dataset *store.Dataset
	
	// HNSW parameters
	m      int     // Number of neighbors per layer
	mMax   int     // Maximum neighbors for layer 0
	mMax0  int     // Maximum neighbors for higher layers
	efConstruction int // Size of dynamic candidate list during construction
	ml     float64 // Level multiplier for exponential decay
	
	// Pooled resources
	searchPool *SearchContextPool
	
	// Metrics
	nodeCount uint32
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
	return &ArrowHNSW{
		nodes:          make([]GraphNode, 0, 1000),
		dataset:        dataset,
		m:              config.M,
		mMax:           config.MMax,
		mMax0:          config.MMax0,
		efConstruction: config.EfConstruction,
		ml:             config.Ml,
		searchPool:     NewSearchContextPool(),
		entryPoint:     0,
		maxLevel:       -1,
	}
}

// Size returns the number of nodes in the index.
func (h *ArrowHNSW) Size() int {
	h.nodeMu.RLock()
	defer h.nodeMu.RUnlock()
	return len(h.nodes)
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
					candidates: NewFixedHeap(200),
					visited:    NewBitset(100000),
					results:    make([]store.SearchResult, 0, 100),
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
	p.pool.Put(ctx)
}

// SearchContext holds temporary state for a single search operation.
type SearchContext struct {
	candidates *FixedHeap
	visited    *Bitset
	results    []store.SearchResult
}
