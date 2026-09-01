package index

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
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
	distFunc  simd.DistanceKernel[float32]
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

// Type returns the index type identifier (DiskANN).
func (idx *DiskANNIndex) Type() IndexType {
	return IndexTypeDiskANN
}

// Dimension returns the vector dimension supported by the index.
func (idx *DiskANNIndex) Dimension() int {
	return idx.dimension
}

// Size returns the total number of vectors stored in the index.
func (idx *DiskANNIndex) Size() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.vectors)
}

// NeedsBuild returns true because DiskANN requires explicit graph construction.
func (idx *DiskANNIndex) NeedsBuild() bool {
	return true // DiskANN requires graph construction
}

// Add inserts a single vector into the index.
func (idx *DiskANNIndex) Add(id uint64, vector []float32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if len(vector) != idx.dimension {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", idx.dimension, len(vector))
	}

	idx.vectors[id] = vector

	// If already built, insert into graph
	if idx.built {
		idx.insertIntoGraph(id, vector)
	}

	return nil
}

// AddBatchRaw inserts multiple vectors into the index efficiently.
func (idx *DiskANNIndex) AddBatchRaw(ids []uint64, vectors [][]float32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if len(ids) != len(vectors) {
		return fmt.Errorf("ids and vectors length mismatch: %d vs %d", len(ids), len(vectors))
	}

	for i, id := range ids {
		vector := vectors[i]
		if len(vector) != idx.dimension {
			return fmt.Errorf("vector dimension mismatch: expected %d, got %d", idx.dimension, len(vector))
		}

		idx.vectors[id] = vector

		// If already built, insert into graph
		if idx.built {
			idx.insertIntoGraph(id, vector)
		}
	}

	return nil
}

// Build constructs the Vamana graph from the current vectors
func (idx *DiskANNIndex) Build() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if len(idx.vectors) == 0 {
		return fmt.Errorf("no vectors to build index")
	}

	// Initialize graph with empty neighbor lists
	idx.graph = make(map[uint64][]uint64)
	for id := range idx.vectors {
		idx.graph[id] = make([]uint64, 0)
	}

	// Build Vamana graph using greedy search with robust pruning
	// Simplified implementation: for each node, find nearest neighbors
	ids := make([]uint64, 0, len(idx.vectors))
	for id := range idx.vectors {
		ids = append(ids, id)
	}

	// For each node, build its neighbor list
	for _, id := range ids {
		idx.buildNodeNeighbors(id)
	}

	idx.built = true
	idx.ensureKernel()
	return nil
}

// buildNodeNeighbors builds the neighbor list for a single node
func (idx *DiskANNIndex) buildNodeNeighbors(nodeID uint64) {
	vector := idx.vectors[nodeID]
	candidates := make([]struct {
		id       uint64
		distance float32
	}, 0, len(idx.vectors)-1)

	// Find all other nodes as candidates
	for otherID, otherVec := range idx.vectors {
		if otherID == nodeID {
			continue
		}
		dist, _ := idx.getDistFunc()(vector, otherVec)
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
	maxNeighbors := idx.config.MaxDegree
	if len(candidates) < maxNeighbors {
		maxNeighbors = len(candidates)
	}

	nodeNeighbors := make([]uint64, 0, maxNeighbors)
	for i := 0; i < maxNeighbors; i++ {
		nodeNeighbors = append(nodeNeighbors, candidates[i].id)
	}

	idx.graph[nodeID] = nodeNeighbors
}

// insertIntoGraph inserts a new node into the existing graph
func (idx *DiskANNIndex) insertIntoGraph(newID uint64, vector []float32) {
	// Find nearest neighbors for the new node
	type candidate struct {
		id       uint64
		distance float32
	}

	candidates := make([]candidate, 0, len(idx.vectors)-1)
	for id, vec := range idx.vectors {
		if id == newID {
			continue
		}
		dist, _ := idx.getDistFunc()(vector, vec)
		candidates = append(candidates, candidate{id: id, distance: dist})
	}

	// Sort by distance
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].distance < candidates[j].distance
	})

	// Select top neighbors
	maxNeighbors := idx.config.MaxDegree
	if len(candidates) < maxNeighbors {
		maxNeighbors = len(candidates)
	}

	newNeighbors := make([]uint64, 0, maxNeighbors)
	for i := 0; i < maxNeighbors; i++ {
		newNeighbors = append(newNeighbors, candidates[i].id)
	}

	idx.graph[newID] = newNeighbors

	// Update neighbors of existing nodes and prune to MaxDegree
	for i := 0; i < min(maxNeighbors, len(candidates)); i++ {
		neighborID := candidates[i].id
		// Add new node to neighbor's list if not already present
		found := false
		for _, existingID := range idx.graph[neighborID] {
			if existingID == newID {
				found = true
				break
			}
		}
		if !found {
			idx.graph[neighborID] = append(idx.graph[neighborID], newID)
			// Prune: keep only the closest MaxDegree neighbors
			if len(idx.graph[neighborID]) > idx.config.MaxDegree {
				neighborVec := idx.vectors[neighborID]
				type nb struct {
					id   uint64
					dist float32
				}
				nbs := make([]nb, 0, len(idx.graph[neighborID]))
				for _, nid := range idx.graph[neighborID] {
					d, _ := idx.getDistFunc()(neighborVec, idx.vectors[nid])
					nbs = append(nbs, nb{id: nid, dist: d})
				}
				sort.Slice(nbs, func(i, j int) bool { return nbs[i].dist < nbs[j].dist })
				pruned := make([]uint64, idx.config.MaxDegree)
				for j := 0; j < idx.config.MaxDegree; j++ {
					pruned[j] = nbs[j].id
				}
				idx.graph[neighborID] = pruned
			}
		}
	}
}

// Search finds k nearest neighbors using greedy graph search
// Search finds the top-k nearest neighbors for a query vector using greedy graph traversal.
func (idx *DiskANNIndex) Search(query []float32, k int) ([]IndexSearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if !idx.built {
		return nil, fmt.Errorf("index not built. Call Build() first")
	}

	if len(idx.vectors) == 0 {
		return []IndexSearchResult{}, nil
	}

	// Start from a deterministic node (smallest ID for reproducibility)
	var startNode uint64
	first := true
	for id := range idx.graph {
		if first || id < startNode {
			startNode = id
			first = false
		}
	}

	// Greedy graph search
	type visited struct {
		id       uint64
		distance float32
	}

	visitedSet := make(map[uint64]bool)
	startDist, _ := idx.getDistFunc()(query, idx.vectors[startNode])
	candidates := []visited{
		{id: startNode, distance: startDist},
	}
	visitedSet[startNode] = true

	results := make([]IndexSearchResult, 0, k*2)

	// Beam search with pruning
	beamWidth := idx.config.BeamWidth
	if beamWidth > len(idx.vectors) {
		beamWidth = len(idx.vectors)
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
		neighbors := idx.graph[best.id]
		for _, neighborID := range neighbors {
			if visitedSet[neighborID] {
				continue
			}
			visitedSet[neighborID] = true
			dist, _ := idx.getDistFunc()(query, idx.vectors[neighborID])
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

// SearchBatch performs multiple vector searches in parallel.
func (idx *DiskANNIndex) SearchBatch(queries [][]float32, k int) ([][]IndexSearchResult, error) {
	results := make([][]IndexSearchResult, len(queries))
	for i, query := range queries {
		r, err := idx.Search(query, k)
		if err != nil {
			return nil, err
		}
		results[i] = r
	}
	return results, nil
}

// GetNeighbors returns the nearest neighbors for an existing vector ID.
func (idx *DiskANNIndex) GetNeighbors(ctx context.Context, id lbtypes.VectorID, k int) ([]lbtypes.SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	neighbors, ok := idx.graph[uint64(id)]
	if !ok {
		return nil, fmt.Errorf("vector id %d not found in DiskANN index", id)
	}

	results := make([]lbtypes.SearchResult, 0, min(k, len(neighbors)))
	for i := 0; i < len(neighbors) && i < k; i++ {
		neighborID := neighbors[i]
		results = append(results, lbtypes.SearchResult{
			ID: lbtypes.VectorID(neighborID), // #nosec G115
		})
	}

	return results, nil
}

// Save serializes the DiskANN index and its graph to the specified disk path.
func (idx *DiskANNIndex) Save(path string) error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	path = filepath.Clean(path)
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
	if err := binary.Write(f, binary.LittleEndian, uint32(idx.dimension)); err != nil { // #nosec G115
		return fmt.Errorf("failed to write dimension: %w", err)
	}

	// Write config as JSON
	configData, err := json.Marshal(idx.config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}
	if err := binary.Write(f, binary.LittleEndian, uint32(len(configData))); err != nil { // #nosec G115
		return fmt.Errorf("failed to write config length: %w", err)
	}
	if _, err := f.Write(configData); err != nil {
		return fmt.Errorf("failed to write config: %w", err)
	}

	// Write vector count
	vecCount := uint32(len(idx.vectors)) // #nosec G115
	if err := binary.Write(f, binary.LittleEndian, vecCount); err != nil {
		return fmt.Errorf("failed to write vector count: %w", err)
	}

	// Sort IDs for deterministic ordering
	ids := make([]uint64, 0, len(idx.vectors))
	for id := range idx.vectors {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// Write vectors: for each id, write (id, vector)
	for _, id := range ids {
		vector := idx.vectors[id]
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
	graphCount := uint32(len(idx.graph)) // #nosec G115
	if err := binary.Write(f, binary.LittleEndian, graphCount); err != nil {
		return fmt.Errorf("failed to write graph count: %w", err)
	}

	// Write graph: for each id, write (id, neighbor_count, neighbors)
	for _, id := range ids {
		neighbors := idx.graph[id]
		if err := binary.Write(f, binary.LittleEndian, id); err != nil {
			return fmt.Errorf("failed to write graph id: %w", err)
		}
		if err := binary.Write(f, binary.LittleEndian, uint32(len(neighbors))); err != nil { // #nosec G115
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
	if idx.built {
		builtFlag = 1
	}
	if err := binary.Write(f, binary.LittleEndian, builtFlag); err != nil {
		return fmt.Errorf("failed to write built flag: %w", err)
	}

	return nil
}

// Load restores the DiskANN index and its graph from the specified disk path.
func (idx *DiskANNIndex) Load(path string) error {
	path = filepath.Clean(path)
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
	idx.dimension = int(dimension)

	// Read config
	var configLen uint32
	if err := binary.Read(f, binary.LittleEndian, &configLen); err != nil {
		return fmt.Errorf("failed to read config length: %w", err)
	}
	configData := make([]byte, configLen)
	if _, err := io.ReadFull(f, configData); err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}
	idx.config = &DiskANNConfig{}
	if err := json.Unmarshal(configData, idx.config); err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Read vectors
	var vecCount uint32
	if err := binary.Read(f, binary.LittleEndian, &vecCount); err != nil {
		return fmt.Errorf("failed to read vector count: %w", err)
	}
	idx.vectors = make(map[uint64][]float32, vecCount)
	for i := uint32(0); i < vecCount; i++ {
		var id uint64
		if err := binary.Read(f, binary.LittleEndian, &id); err != nil {
			return fmt.Errorf("failed to read vector id: %w", err)
		}
		vec := make([]float32, idx.dimension)
		vecBytes := make([]byte, idx.dimension*4)
		if _, err := io.ReadFull(f, vecBytes); err != nil {
			return fmt.Errorf("failed to read vector: %w", err)
		}
		for j := 0; j < idx.dimension; j++ {
			vec[j] = math.Float32frombits(binary.LittleEndian.Uint32(vecBytes[j*4:]))
		}
		idx.vectors[id] = vec
	}

	// Read graph
	var graphCount uint32
	if err := binary.Read(f, binary.LittleEndian, &graphCount); err != nil {
		return fmt.Errorf("failed to read graph count: %w", err)
	}
	idx.graph = make(map[uint64][]uint64, graphCount)
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
			if _, err := io.ReadFull(f, neighborBytes); err != nil {
				return fmt.Errorf("failed to read neighbors: %w", err)
			}
			for j := uint32(0); j < neighborCount; j++ {
				neighbors[j] = binary.LittleEndian.Uint64(neighborBytes[j*8:])
			}
		}
		idx.graph[id] = neighbors
	}

	// Read built flag
	var builtFlag uint8
	if err := binary.Read(f, binary.LittleEndian, &builtFlag); err != nil {
		return fmt.Errorf("failed to read built flag: %w", err)
	}
	idx.built = builtFlag == 1

	return nil
}

// Close releases all resources associated with the DiskANN index.
func (idx *DiskANNIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	// Release graph and vector maps to allow GC
	idx.graph = nil
	idx.vectors = nil
	idx.built = false
	return nil
}

// ExportState returns the serialized state of the index as a byte slice.
func (idx *DiskANNIndex) ExportState() ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var buf bytes.Buffer
	w := &buf

	// Write magic number and version
	if err := binary.Write(w, binary.LittleEndian, uint32(diskannMagic)); err != nil {
		return nil, fmt.Errorf("failed to write magic: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(diskannVersion)); err != nil {
		return nil, fmt.Errorf("failed to write version: %w", err)
	}

	// Write dimension
	if err := binary.Write(w, binary.LittleEndian, uint32(idx.dimension)); err != nil { // #nosec G115
		return nil, fmt.Errorf("failed to write dimension: %w", err)
	}

	// Write config as JSON
	configData, err := json.Marshal(idx.config)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal config: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(configData))); err != nil { // #nosec G115
		return nil, fmt.Errorf("failed to write config length: %w", err)
	}
	if _, err := w.Write(configData); err != nil {
		return nil, fmt.Errorf("failed to write config: %w", err)
	}

	// Write vector count
	vecCount := uint32(len(idx.vectors)) // #nosec G115
	if err := binary.Write(w, binary.LittleEndian, vecCount); err != nil {
		return nil, fmt.Errorf("failed to write vector count: %w", err)
	}

	// Sort IDs for deterministic ordering
	ids := make([]uint64, 0, len(idx.vectors))
	for id := range idx.vectors {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	for _, id := range ids {
		vector := idx.vectors[id]
		if err := binary.Write(w, binary.LittleEndian, id); err != nil {
			return nil, fmt.Errorf("failed to write vector id: %w", err)
		}
		vecBytes := make([]byte, len(vector)*4)
		for i, v := range vector {
			binary.LittleEndian.PutUint32(vecBytes[i*4:], math.Float32bits(v))
		}
		if _, err := w.Write(vecBytes); err != nil {
			return nil, fmt.Errorf("failed to write vector: %w", err)
		}
	}

	// Write graph count
	graphCount := uint32(len(idx.graph)) // #nosec G115
	if err := binary.Write(w, binary.LittleEndian, graphCount); err != nil {
		return nil, fmt.Errorf("failed to write graph count: %w", err)
	}

	for _, id := range ids {
		neighbors := idx.graph[id]
		if err := binary.Write(w, binary.LittleEndian, id); err != nil {
			return nil, fmt.Errorf("failed to write graph id: %w", err)
		}
		if err := binary.Write(w, binary.LittleEndian, uint32(len(neighbors))); err != nil { // #nosec G115
			return nil, fmt.Errorf("failed to write neighbor count: %w", err)
		}
		if len(neighbors) > 0 {
			neighborBytes := make([]byte, len(neighbors)*8)
			for i, n := range neighbors {
				binary.LittleEndian.PutUint64(neighborBytes[i*8:], n)
			}
			if _, err := w.Write(neighborBytes); err != nil {
				return nil, fmt.Errorf("failed to write neighbors: %w", err)
			}
		}
	}

	// Write built flag
	builtFlag := uint8(0)
	if idx.built {
		builtFlag = 1
	}
	if err := binary.Write(w, binary.LittleEndian, builtFlag); err != nil {
		return nil, fmt.Errorf("failed to write built flag: %w", err)
	}

	return buf.Bytes(), nil
}

// ImportState restores the index state from a byte slice.
func (idx *DiskANNIndex) ImportState(data []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if len(data) == 0 {
		return fmt.Errorf("empty state data")
	}

	r := bytes.NewReader(data)

	var magic uint32
	if err := binary.Read(r, binary.LittleEndian, &magic); err != nil {
		return fmt.Errorf("failed to read magic: %w", err)
	}
	if magic != diskannMagic {
		return fmt.Errorf("invalid magic number: got %x, expected %x", magic, diskannMagic)
	}

	var version uint32
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}
	if version != diskannVersion {
		return fmt.Errorf("unsupported version: %d", version)
	}

	var dim uint32
	if err := binary.Read(r, binary.LittleEndian, &dim); err != nil {
		return fmt.Errorf("failed to read dimension: %w", err)
	}
	idx.dimension = int(dim)

	// Read config
	var configLen uint32
	if err := binary.Read(r, binary.LittleEndian, &configLen); err != nil {
		return fmt.Errorf("failed to read config length: %w", err)
	}
	configBytes := make([]byte, configLen)
	if _, err := io.ReadFull(r, configBytes); err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}
	cfg := &DiskANNConfig{}
	if err := json.Unmarshal(configBytes, cfg); err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}
	idx.config = cfg

	// Read vectors
	var vecCount uint32
	if err := binary.Read(r, binary.LittleEndian, &vecCount); err != nil {
		return fmt.Errorf("failed to read vector count: %w", err)
	}
	idx.vectors = make(map[uint64][]float32, vecCount)
	for i := uint32(0); i < vecCount; i++ {
		var id uint64
		if err := binary.Read(r, binary.LittleEndian, &id); err != nil {
			return fmt.Errorf("failed to read vector id: %w", err)
		}
		vecBytes := make([]byte, idx.dimension*4)
		if _, err := io.ReadFull(r, vecBytes); err != nil {
			return fmt.Errorf("failed to read vector: %w", err)
		}
		vec := make([]float32, idx.dimension)
		for j := range vec {
			vec[j] = math.Float32frombits(binary.LittleEndian.Uint32(vecBytes[j*4:]))
		}
		idx.vectors[id] = vec
	}

	// Read graph
	var graphCount uint32
	if err := binary.Read(r, binary.LittleEndian, &graphCount); err != nil {
		return fmt.Errorf("failed to read graph count: %w", err)
	}
	idx.graph = make(map[uint64][]uint64, graphCount)
	for i := uint32(0); i < graphCount; i++ {
		var id uint64
		if err := binary.Read(r, binary.LittleEndian, &id); err != nil {
			return fmt.Errorf("failed to read graph id: %w", err)
		}
		var neighborCount uint32
		if err := binary.Read(r, binary.LittleEndian, &neighborCount); err != nil {
			return fmt.Errorf("failed to read neighbor count: %w", err)
		}
		neighbors := make([]uint64, neighborCount)
		for j := range neighbors {
			if err := binary.Read(r, binary.LittleEndian, &neighbors[j]); err != nil {
				return fmt.Errorf("failed to read neighbor: %w", err)
			}
		}
		idx.graph[id] = neighbors
	}

	// Read built flag
	var builtFlag uint8
	if err := binary.Read(r, binary.LittleEndian, &builtFlag); err != nil {
		return fmt.Errorf("failed to read built flag: %w", err)
	}
	idx.built = builtFlag == 1

	return nil
}

// AddByLocation is a legacy adapter for location-based vector insertion.
// DiskANN does not support location-based insertion; use Add instead.
func (idx *DiskANNIndex) AddByLocation(batchIdx, rowIdx int) error {
	return fmt.Errorf("DiskANN does not support location-based insertion: use Add instead")
}

// GetVectorID resolves a storage location to its internal vector ID.
// DiskANN uses uint64 vector IDs directly and does not maintain a
// location-to-ID mapping; this always returns false.
func (idx *DiskANNIndex) GetVectorID(loc Location) (uint64, bool) {
	return 0, false
}

// SearchVectors is a legacy adapter for vector search with options.
func (idx *DiskANNIndex) SearchVectors(query []float32, k int, options types.SearchOptions) []lbtypes.SearchResult {
	results, _ := idx.Search(query, k)
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

// Len returns the number of vectors in the index.
func (idx *DiskANNIndex) Len() int {
	return idx.Size()
}

// GetIndexType returns the string identifier for the index type.
func (idx *DiskANNIndex) GetIndexType() string {
	return string(idx.Type())
}

func (idx *DiskANNIndex) ensureKernel() {
	if idx.distFunc != nil {
		return
	}
	idx.distFunc = simd.GetKernel[float32](simd.MetricEuclidean, idx.dimension)
	if idx.distFunc == nil {
		idx.distFunc = simd.L2Squared
	}
}

func (idx *DiskANNIndex) getDistFunc() simd.DistanceKernel[float32] {
	idx.ensureKernel()
	return idx.distFunc
}
