package store

import (
	"fmt"
	"sort"
	"sync"

	"github.com/23skdu/longbow/internal/simd"
)

// =============================================================================
// DiskANN Index Implementation
// =============================================================================

// DiskANNIndex implements PluggableVectorIndex for DiskANN algorithm
// DiskANN uses a Vamana graph for efficient approximate nearest neighbor search
// optimized for disk-based storage.
type DiskANNIndex struct {
	mu        sync.RWMutex
	dimension int
	vectors   map[uint64][]float32
	graph     map[uint64][]uint64 // Vamana graph: node -> neighbors
	config    *DiskANNConfig
	built     bool
}

// NewDiskANNIndex creates a new DiskANN index
func NewDiskANNIndex(cfg IndexConfig) (*DiskANNIndex, error) {
	if cfg.Dimension <= 0 {
		return nil, fmt.Errorf("invalid dimension: %d", cfg.Dimension)
	}

	if cfg.DiskANNConfig == nil {
		cfg.DiskANNConfig = &DiskANNConfig{
			MaxDegree:    64,  // Maximum degree of graph nodes
			BeamWidth:    100, // Beam width for search
			BuildThreads: 4,   // Number of threads for building
		}
	}

	return &DiskANNIndex{
		dimension: cfg.Dimension,
		vectors:   make(map[uint64][]float32),
		graph:     make(map[uint64][]uint64),
		config:    cfg.DiskANNConfig,
		built:     false,
	}, nil
}

func (d *DiskANNIndex) Type() IndexType {
	return IndexTypeDiskANN
}

func (d *DiskANNIndex) Dimension() int {
	return d.dimension
}

func (d *DiskANNIndex) Size() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.vectors)
}

func (d *DiskANNIndex) NeedsBuild() bool {
	return true // DiskANN requires graph construction
}

func (d *DiskANNIndex) Add(id uint64, vector []float32) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(vector) != d.dimension {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", d.dimension, len(vector))
	}

	d.vectors[id] = vector

	// If already built, insert into graph
	if d.built {
		d.insertIntoGraph(id, vector)
	}

	return nil
}

func (d *DiskANNIndex) AddBatch(ids []uint64, vectors [][]float32) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(ids) != len(vectors) {
		return fmt.Errorf("ids and vectors length mismatch: %d vs %d", len(ids), len(vectors))
	}

	for i, id := range ids {
		vector := vectors[i]
		if len(vector) != d.dimension {
			return fmt.Errorf("vector dimension mismatch: expected %d, got %d", d.dimension, len(vector))
		}

		d.vectors[id] = vector

		// If already built, insert into graph
		if d.built {
			d.insertIntoGraph(id, vector)
		}
	}

	return nil
}

// Build constructs the Vamana graph from the current vectors
func (d *DiskANNIndex) Build() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.vectors) == 0 {
		return fmt.Errorf("no vectors to build index")
	}

	// Initialize graph with empty neighbor lists
	d.graph = make(map[uint64][]uint64)
	for id := range d.vectors {
		d.graph[id] = make([]uint64, 0)
	}

	// Build Vamana graph using greedy search with robust pruning
	// Simplified implementation: for each node, find nearest neighbors
	ids := make([]uint64, 0, len(d.vectors))
	for id := range d.vectors {
		ids = append(ids, id)
	}

	// For each node, build its neighbor list
	for _, id := range ids {
		d.buildNodeNeighbors(id)
	}

	d.built = true
	return nil
}

// buildNodeNeighbors builds the neighbor list for a single node
func (d *DiskANNIndex) buildNodeNeighbors(nodeID uint64) {
	vector := d.vectors[nodeID]
	candidates := make([]struct {
		id       uint64
		distance float32
	}, 0, len(d.vectors)-1)

	// Find all other nodes as candidates
	for otherID, otherVec := range d.vectors {
		if otherID == nodeID {
			continue
		}
		dist, _ := simd.L2Squared(vector, otherVec)
		candidates = append(candidates, struct {
			id       uint64
			distance float32
		}{id: otherID, distance: dist})
	}

	// Sort by distance
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].distance < candidates[j].distance
	})

	// Select top neighbors up to MaxDegree
	maxNeighbors := d.config.MaxDegree
	if len(candidates) < maxNeighbors {
		maxNeighbors = len(candidates)
	}

	nodeNeighbors := make([]uint64, 0, maxNeighbors)
	for i := 0; i < maxNeighbors; i++ {
		nodeNeighbors = append(nodeNeighbors, candidates[i].id)
	}

	d.graph[nodeID] = nodeNeighbors
}

// insertIntoGraph inserts a new node into the existing graph
func (d *DiskANNIndex) insertIntoGraph(newID uint64, vector []float32) {
	// Find nearest neighbors for the new node
	type candidate struct {
		id       uint64
		distance float32
	}

	candidates := make([]candidate, 0, len(d.vectors)-1)
	for id, vec := range d.vectors {
		if id == newID {
			continue
		}
		dist, _ := simd.L2Squared(vector, vec)
		candidates = append(candidates, candidate{id: id, distance: dist})
	}

	// Sort by distance
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].distance < candidates[j].distance
	})

	// Select top neighbors
	maxNeighbors := d.config.MaxDegree
	if len(candidates) < maxNeighbors {
		maxNeighbors = len(candidates)
	}

	newNeighbors := make([]uint64, 0, maxNeighbors)
	for i := 0; i < maxNeighbors; i++ {
		newNeighbors = append(newNeighbors, candidates[i].id)
	}

	d.graph[newID] = newNeighbors

	// Update neighbors of existing nodes (simplified: just add to top candidates)
	// In a full implementation, this would use robust pruning
	for i := 0; i < min(maxNeighbors, len(candidates)); i++ {
		neighborID := candidates[i].id
		// Add new node to neighbor's list if not already present
		found := false
		for _, existingID := range d.graph[neighborID] {
			if existingID == newID {
				found = true
				break
			}
		}
		if !found && len(d.graph[neighborID]) < d.config.MaxDegree {
			d.graph[neighborID] = append(d.graph[neighborID], newID)
		}
	}
}

// Search finds k nearest neighbors using greedy graph search
func (d *DiskANNIndex) Search(query []float32, k int) ([]IndexSearchResult, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if !d.built {
		return nil, fmt.Errorf("index not built. Call Build() first")
	}

	if len(d.vectors) == 0 {
		return []IndexSearchResult{}, nil
	}

	// Start from a random node
	var startNode uint64
	for id := range d.graph {
		startNode = id
		break
	}

	// Greedy graph search
	type visited struct {
		id       uint64
		distance float32
	}

	visitedSet := make(map[uint64]bool)
	startDist, _ := simd.L2Squared(query, d.vectors[startNode])
	candidates := []visited{
		{id: startNode, distance: startDist},
	}
	visitedSet[startNode] = true

	results := make([]IndexSearchResult, 0, k*2)

	// Beam search with pruning
	beamWidth := d.config.BeamWidth
	if beamWidth > len(d.vectors) {
		beamWidth = len(d.vectors)
	}

	for len(candidates) > 0 && len(results) < k*2 {
		// Get the best candidate
		best := candidates[0]
		candidates = candidates[1:]

		// Add to results
		results = append(results, IndexSearchResult{
			ID:       best.id,
			Distance: best.distance,
		})

		// Explore neighbors
		neighbors := d.graph[best.id]
		for _, neighborID := range neighbors {
			if visitedSet[neighborID] {
				continue
			}
			visitedSet[neighborID] = true
			dist, _ := simd.L2Squared(query, d.vectors[neighborID])
			candidates = append(candidates, visited{id: neighborID, distance: dist})
		}

		// Sort candidates by distance and keep only beam width
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].distance < candidates[j].distance
		})
		if len(candidates) > beamWidth {
			candidates = candidates[:beamWidth]
		}
	}

	// Sort results and return top k
	sort.Slice(results, func(i, j int) bool {
		return results[i].Distance < results[j].Distance
	})

	if k > len(results) {
		k = len(results)
	}

	return results[:k], nil
}

func (d *DiskANNIndex) SearchBatch(queries [][]float32, k int) ([][]IndexSearchResult, error) {
	results := make([][]IndexSearchResult, len(queries))
	for i, query := range queries {
		r, err := d.Search(query, k)
		if err != nil {
			return nil, err
		}
		results[i] = r
	}
	return results, nil
}

func (d *DiskANNIndex) Save(path string) error {
	// TODO: Implement proper persistence
	return nil
}

func (d *DiskANNIndex) Load(path string) error {
	// TODO: Implement proper loading
	return nil
}

func (d *DiskANNIndex) Close() error {
	return nil
}

func (d *DiskANNIndex) AddByLocation(batchIdx, rowIdx int) error {
	return nil
}

func (d *DiskANNIndex) SearchVectors(query []float32, k int, options SearchOptions) []SearchResult {
	results, _ := d.Search(query, k)
	searchResults := make([]SearchResult, len(results))
	for i, r := range results {
		id := r.ID
		if id > 4294967295 {
			id = 4294967295
		}
		searchResults[i] = SearchResult{ID: VectorID(id), Score: r.Distance}
	}
	return searchResults
}

func (d *DiskANNIndex) Len() int {
	return d.Size()
}
