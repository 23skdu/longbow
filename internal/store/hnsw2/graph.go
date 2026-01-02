package hnsw2
 
import (
	"math"
	"sync"
	"sync/atomic"

	
	"github.com/23skdu/longbow/internal/metrics"	
	"github.com/23skdu/longbow/internal/store"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	// ChunkShift determines the size of each chunk (1 << 16 = 65536).
	ChunkShift = 16
	ChunkSize  = 1 << ChunkShift
	ChunkMask  = ChunkSize - 1
)

// GraphData holds the SoA (Struct of Arrays) representation of the graph using a Chunked/Arena layout.
// This structure is designed for lock-free snapshot access and O(1) growth.
type GraphData struct {
	Capacity     int
	// SoA Slices of Chunks
	// Access: Levels[id >> ChunkShift][id & ChunkMask]
	Levels     [][]uint8
	// Vectors holds dense packed float32 vectors.
	// Layout: Vectors[chunk][offset * Dims ... (offset+1)*Dims]
	Vectors [][]float32 
	
	// VectorsSQ8 holds dense packed uint8 vectors for Scalar Quantization.
	// Layout: VectorsSQ8[chunk][offset * Dims ... (offset+1)*Dims]
	VectorsSQ8 [][]byte
	
	// Neighbors flattened per layer and chunk: Neighbors[layer][chunk][offset*MaxNeighbors + index]
	Neighbors [MaxLayers][][]uint32
	// Counts per layer and chunk: Counts[layer][chunk][offset]
	Counts [MaxLayers][][]int32
	
	// Versions per layer and chunk
	Versions [MaxLayers][][]uint32
}

// NewGraphData creates a new GraphData with the specified capacity.
// It allocates enough chunks to hold 'capacity' elements.
func NewGraphData(capacity, dims int) *GraphData {
	// Calculate number of chunks needed
	numChunks := (capacity + ChunkSize - 1) / ChunkSize
	if numChunks < 1 {
		numChunks = 1
	}

	gd := &GraphData{
		Capacity:   numChunks * ChunkSize,
		Levels:     make([][]uint8, numChunks),
		// Vectors initialized only if dims > 0
	}
	
	// Initialize chunks
	for i := 0; i < numChunks; i++ {
		gd.Levels[i] = make([]uint8, ChunkSize)
	}
	
	if dims > 0 {
		gd.Vectors = make([][]float32, numChunks)
		for i := 0; i < numChunks; i++ {
			gd.Vectors[i] = make([]float32, ChunkSize*dims)
		}
	}
	
	for i := 0; i < MaxLayers; i++ {
		gd.Neighbors[i] = make([][]uint32, numChunks)
		gd.Counts[i] = make([][]int32, numChunks)
		gd.Versions[i] = make([][]uint32, numChunks)
		
		for j := 0; j < numChunks; j++ {
			gd.Neighbors[i][j] = make([]uint32, ChunkSize*MaxNeighbors)
			gd.Counts[i][j] = make([]int32, ChunkSize)
			gd.Versions[i][j] = make([]uint32, ChunkSize)
		}
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

	// growMu protects the GraphData pointer and Global growth operations.
	// Insert() does NOT take this lock (lock-free read). Grow() takes Lock (exclusive).
	growMu sync.Mutex
	
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

	// Deleted nodes tracking
	deleted *Bitset
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
		AdaptiveEf:      false, // Disabled by default (opt-in)
		AdaptiveEfMin:   0,     // Auto-calculate as EfConstruction/4
		AdaptiveEfThreshold: 0, // Auto-calculate as InitialCapacity/2
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
		growMu:        sync.Mutex{},
		locationStore: store.NewChunkedLocationStore(),
		deleted:       NewBitset(config.InitialCapacity),
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
	initData := NewGraphData(initialCap, h.dims)
	
	// Pre-allocate SQ8 storage if enabled
	if config.SQ8Enabled && h.dims > 0 {
		numChunks := len(initData.Levels)
		for i := 0; i < numChunks; i++ {
			// Each chunk for SQ8 needs ChunkSize * Dims
			initData.VectorsSQ8 = append(initData.VectorsSQ8, make([]byte, ChunkSize*h.dims))
		}
	}
	
	h.data.Store(initData)
	h.entryPoint.Store(0)
	h.maxLevel.Store(-1)
	h.nodeCount.Store(0)

	return h
}

// Grow ensures the graph has enough capacity for the requested ID.
// It uses a Chunked Storage strategy: new chunks are appended, making growth O(1).
func (h *ArrowHNSW) Grow(minCap int) {
	// Acquire exclusive lock for serializing Growth operations only
	h.growMu.Lock()
	defer h.growMu.Unlock()
	
	metrics.HNSWResizesTotal.WithLabelValues("default").Inc()
	
	// Double-check capacity under lock
	// Note: We modify data *in place* because we are just appending slices.
	// Readers might see old slice length, but that's fine as they won't access IDs >= old Cap.
	// Actually, concurrent *append* to slice is not safe if reassignment happens.
	// But `h.data.Load()` returns a pointer to GraphData.
	// GraphData.Levels is a slice. Append reallocates the slice header.
	// So we DO need to update the pointer or protect access.
    // However, existing readers hold a pointer to `data`. If we update `data.Levels` in place, 
    // concurrent readers accessing `data.Levels[i]` are safe as long as `i` < old length.
    // But append might reallocate the backing array of the `Levels` slice-of-slices.
    // To be perfectly safe and lock-free for readers, we should copy the *slice headers* (which is small)
    // and swap the GraphData pointer.
	
	oldData := h.data.Load()
	
	// Calculate required chunks
    // We treat minCap as "index to be valid", so we need minCap+1 capacity.
	// But usually minCap is "total count". Let's assume minCap is "index + 1".
	
	reqChunks := (minCap + ChunkSize - 1) / ChunkSize
	currChunks := len(oldData.Levels)
	
	// Also check SQ8 initialization and Dense Vectors initialization
	sq8Missing := h.config.SQ8Enabled && len(oldData.VectorsSQ8) == 0 && h.dims > 0
	vectorsMissing := len(oldData.Vectors) == 0 && h.dims > 0

	if reqChunks <= currChunks && !sq8Missing && !vectorsMissing {
		return
	}

	// Create new GraphData (Shallow copy of slice headers)
	newData := &GraphData{
		Capacity:   reqChunks * ChunkSize,
		Levels:     make([][]uint8, len(oldData.Levels), reqChunks),
		Vectors:    make([][]float32, len(oldData.Vectors), reqChunks),
		VectorsSQ8: make([][]byte, len(oldData.VectorsSQ8), reqChunks),
	}
    
    // Copy headers
    copy(newData.Levels, oldData.Levels)
    copy(newData.Vectors, oldData.Vectors)
    copy(newData.VectorsSQ8, oldData.VectorsSQ8)
    
    // Grow Slices
    added := reqChunks - currChunks
    if added < 0 { added = 0 } // Just SQ8/Vectors missing case
    
    for i := 0; i < added; i++ {
        newData.Levels = append(newData.Levels, make([]uint8, ChunkSize))
    }

    // Grow Vectors (Dense)
    if h.dims > 0 {
		if len(newData.Vectors) == 0 {
			// Initialize completely if missing
			for i := 0; i < reqChunks; i++ {
				newData.Vectors = append(newData.Vectors, make([]float32, ChunkSize*h.dims))
			}
		} else {
			// Append new chunks
			for i := 0; i < added; i++ {
				newData.Vectors = append(newData.Vectors, make([]float32, ChunkSize*h.dims))
			}
		}
    }
    
    // Grow SQ8
    if h.config.SQ8Enabled && h.dims > 0 {
		// If fully missing, we need to alloc for ALL chunks
		if len(newData.VectorsSQ8) == 0 {
			for i := 0; i < reqChunks; i++ {
				newData.VectorsSQ8 = append(newData.VectorsSQ8, make([]byte, ChunkSize*h.dims))
			}
		} else {
             // Just append new chunks
             for i := 0; i < added; i++ {
                 newData.VectorsSQ8 = append(newData.VectorsSQ8, make([]byte, ChunkSize*h.dims))
             }
		}
    }
    
    // Grow Neighbors/Counts/Versions
    for i := 0; i < MaxLayers; i++ {
        // Copy existing headers
        newData.Neighbors[i] = make([][]uint32, len(oldData.Neighbors[i]), reqChunks)
        copy(newData.Neighbors[i], oldData.Neighbors[i])
        
        newData.Counts[i] = make([][]int32, len(oldData.Counts[i]), reqChunks)
        copy(newData.Counts[i], oldData.Counts[i])
        
        newData.Versions[i] = make([][]uint32, len(oldData.Versions[i]), reqChunks)
        copy(newData.Versions[i], oldData.Versions[i])
        
        // Append new
        for j := 0; j < added; j++ {
             newData.Neighbors[i] = append(newData.Neighbors[i], make([]uint32, ChunkSize*MaxNeighbors))
             newData.Counts[i] = append(newData.Counts[i], make([]int32, ChunkSize))
             newData.Versions[i] = append(newData.Versions[i], make([]uint32, ChunkSize))
        }
    }

	// Grow Deleted bitset
	h.deleted.Grow(newData.Capacity)

	// Atomically switch to new data
	h.data.Store(newData)
}

// Delete marks a node as deleted in the index.
func (h *ArrowHNSW) Delete(id uint32) error {
	h.deleted.Set(id)
	return nil
}

// IsDeleted returns true if the node is marked as deleted.
func (h *ArrowHNSW) IsDeleted(id uint32) bool {
	return h.deleted.IsSet(id)
}

// Size returns the number of nodes in the index.
func (h *ArrowHNSW) Size() int {
	return int(h.nodeCount.Load())
}

// GetEntryPoint returns the current entry point ID.
func (h *ArrowHNSW) GetEntryPoint() uint32 {
	return h.entryPoint.Load()
}

// Helpers for Chunked Access
func chunkID(id uint32) uint32 {
	return id >> ChunkShift
}

func chunkOffset(id uint32) uint32 {
	return id & ChunkMask
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
