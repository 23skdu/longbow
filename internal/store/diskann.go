package store

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"

	"github.com/23skdu/longbow/internal/simd"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
)

const (
	diskannMagic   = 0x44414B41 // "DAKA" in hex
	diskannVersion = 1
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

func (d *DiskANNIndex) GetNeighbors(ctx context.Context, id lbtypes.VectorID, k int) ([]lbtypes.SearchResult, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	neighbors, ok := d.graph[uint64(id)]
	if !ok {
		return nil, fmt.Errorf("vector id %d not found in DiskANN index", id)
	}

	results := make([]lbtypes.SearchResult, 0, min(k, len(neighbors)))
	for i := 0; i < len(neighbors) && i < k; i++ {
		neighborID := neighbors[i]
		results = append(results, lbtypes.SearchResult{
			ID: lbtypes.VectorID(neighborID),
		})
	}

	return results, nil
}

func (d *DiskANNIndex) Save(path string) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	// Write magic number and version
	if err := binary.Write(f, binary.LittleEndian, uint32(diskannMagic)); err != nil {
		return fmt.Errorf("failed to write magic: %w", err)
	}
	if err := binary.Write(f, binary.LittleEndian, uint32(diskannVersion)); err != nil {
		return fmt.Errorf("failed to write version: %w", err)
	}

	// Write dimension
	if err := binary.Write(f, binary.LittleEndian, uint32(d.dimension)); err != nil {
		return fmt.Errorf("failed to write dimension: %w", err)
	}

	// Write config as JSON
	configData, err := json.Marshal(d.config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}
	if err := binary.Write(f, binary.LittleEndian, uint32(len(configData))); err != nil {
		return fmt.Errorf("failed to write config length: %w", err)
	}
	if _, err := f.Write(configData); err != nil {
		return fmt.Errorf("failed to write config: %w", err)
	}

	// Write vector count
	vecCount := uint32(len(d.vectors))
	if err := binary.Write(f, binary.LittleEndian, vecCount); err != nil {
		return fmt.Errorf("failed to write vector count: %w", err)
	}

	// Sort IDs for deterministic ordering
	ids := make([]uint64, 0, len(d.vectors))
	for id := range d.vectors {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// Write vectors: for each id, write (id, vector)
	for _, id := range ids {
		vector := d.vectors[id]
		if err := binary.Write(f, binary.LittleEndian, id); err != nil {
			return fmt.Errorf("failed to write vector id: %w", err)
		}
		vecBytes := make([]byte, len(vector)*4)
		for i, v := range vector {
			binary.LittleEndian.PutUint32(vecBytes[i*4:], math.Float32bits(v))
		}
		if _, err := f.Write(vecBytes); err != nil {
			return fmt.Errorf("failed to write vector: %w", err)
		}
	}

	// Write graph count
	graphCount := uint32(len(d.graph))
	if err := binary.Write(f, binary.LittleEndian, graphCount); err != nil {
		return fmt.Errorf("failed to write graph count: %w", err)
	}

	// Write graph: for each id, write (id, neighbor_count, neighbors)
	for _, id := range ids {
		neighbors := d.graph[id]
		if err := binary.Write(f, binary.LittleEndian, id); err != nil {
			return fmt.Errorf("failed to write graph id: %w", err)
		}
		if err := binary.Write(f, binary.LittleEndian, uint32(len(neighbors))); err != nil {
			return fmt.Errorf("failed to write neighbor count: %w", err)
		}
		if len(neighbors) > 0 {
			neighborBytes := make([]byte, len(neighbors)*8)
			for i, n := range neighbors {
				binary.LittleEndian.PutUint64(neighborBytes[i*8:], n)
			}
			if _, err := f.Write(neighborBytes); err != nil {
				return fmt.Errorf("failed to write neighbors: %w", err)
			}
		}
	}

	// Write built flag
	builtFlag := uint8(0)
	if d.built {
		builtFlag = 1
	}
	if err := binary.Write(f, binary.LittleEndian, builtFlag); err != nil {
		return fmt.Errorf("failed to write built flag: %w", err)
	}

	return nil
}

func (d *DiskANNIndex) Load(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	// Read and verify magic number
	var magic uint32
	if err := binary.Read(f, binary.LittleEndian, &magic); err != nil {
		return fmt.Errorf("failed to read magic: %w", err)
	}
	if magic != diskannMagic {
		return fmt.Errorf("invalid magic number: got %x, expected %x", magic, diskannMagic)
	}

	// Read version
	var version uint32
	if err := binary.Read(f, binary.LittleEndian, &version); err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}
	if version > diskannVersion {
		return fmt.Errorf("unsupported version: %d", version)
	}

	// Read dimension
	var dimension uint32
	if err := binary.Read(f, binary.LittleEndian, &dimension); err != nil {
		return fmt.Errorf("failed to read dimension: %w", err)
	}
	d.dimension = int(dimension)

	// Read config
	var configLen uint32
	if err := binary.Read(f, binary.LittleEndian, &configLen); err != nil {
		return fmt.Errorf("failed to read config length: %w", err)
	}
	configData := make([]byte, configLen)
	if _, err := f.Read(configData); err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}
	d.config = &DiskANNConfig{}
	if err := json.Unmarshal(configData, d.config); err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Read vectors
	var vecCount uint32
	if err := binary.Read(f, binary.LittleEndian, &vecCount); err != nil {
		return fmt.Errorf("failed to read vector count: %w", err)
	}
	d.vectors = make(map[uint64][]float32, vecCount)
	for i := uint32(0); i < vecCount; i++ {
		var id uint64
		if err := binary.Read(f, binary.LittleEndian, &id); err != nil {
			return fmt.Errorf("failed to read vector id: %w", err)
		}
		vec := make([]float32, d.dimension)
		vecBytes := make([]byte, d.dimension*4)
		if _, err := f.Read(vecBytes); err != nil {
			return fmt.Errorf("failed to read vector: %w", err)
		}
		for j := 0; j < d.dimension; j++ {
			vec[j] = math.Float32frombits(binary.LittleEndian.Uint32(vecBytes[j*4:]))
		}
		d.vectors[id] = vec
	}

	// Read graph
	var graphCount uint32
	if err := binary.Read(f, binary.LittleEndian, &graphCount); err != nil {
		return fmt.Errorf("failed to read graph count: %w", err)
	}
	d.graph = make(map[uint64][]uint64, graphCount)
	for i := uint32(0); i < graphCount; i++ {
		var id uint64
		if err := binary.Read(f, binary.LittleEndian, &id); err != nil {
			return fmt.Errorf("failed to read graph id: %w", err)
		}
		var neighborCount uint32
		if err := binary.Read(f, binary.LittleEndian, &neighborCount); err != nil {
			return fmt.Errorf("failed to read neighbor count: %w", err)
		}
		neighbors := make([]uint64, neighborCount)
		if neighborCount > 0 {
			neighborBytes := make([]byte, neighborCount*8)
			if _, err := f.Read(neighborBytes); err != nil {
				return fmt.Errorf("failed to read neighbors: %w", err)
			}
			for j := uint32(0); j < neighborCount; j++ {
				neighbors[j] = binary.LittleEndian.Uint64(neighborBytes[j*8:])
			}
		}
		d.graph[id] = neighbors
	}

	// Read built flag
	var builtFlag uint8
	if err := binary.Read(f, binary.LittleEndian, &builtFlag); err != nil {
		return fmt.Errorf("failed to read built flag: %w", err)
	}
	d.built = builtFlag == 1

	return nil
}

func (d *DiskANNIndex) Close() error {
	return nil
}

func (d *DiskANNIndex) AddByLocation(batchIdx, rowIdx int) error {
	return nil
}

func (d *DiskANNIndex) GetVectorID(loc Location) (uint64, bool) {
	// Not supported for DiskANN adapter
	return 0, false
}

func (d *DiskANNIndex) SearchVectors(query []float32, k int, options SearchOptions) []lbtypes.SearchResult {
	results, _ := d.Search(query, k)
	searchResults := make([]lbtypes.SearchResult, len(results))
	for i, r := range results {
		id := r.ID
		if id > 4294967295 {
			id = 4294967295
		}
		searchResults[i] = lbtypes.SearchResult{
			ID:       lbtypes.VectorID(id),
			Distance: r.Distance,
			Score:    1.0 / (1.0 + r.Distance),
		}
	}
	return searchResults
}

func (d *DiskANNIndex) Len() int {
	return d.Size()
}
