package store

import (
	"context"
	"errors"
	"math"
	"sort"
	"sync"

	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/query"
)

// IVFPQConfig holds configuration for IVF-PQ index
type IVFPQConfig struct {
	// Coarse quantizer: number of clusters (IVF)
	Nlist int // Default 1024

	// Product Quantization: number of subvectors and bits per subspace
	M int // Number of subvectors (subspaces) - typically 8-16
	K int // Number of centroids per subspace - typically 256 (8 bits)

	// Search parameters
	Nprobe int // Number of clusters to search - typically 8-64
}

// DefaultIVFPQConfig returns sensible defaults
func DefaultIVFPQConfig() IVFPQConfig {
	return IVFPQConfig{
		Nlist:  1024,
		M:      8,
		K:      256,
		Nprobe: 8,
	}
}

// IVFIndexEntry holds a single vector entry in the inverted index
type IVFIndexEntry struct {
	VectorID uint32
	PQCode   []byte
}

// IVFCluster holds all vectors belonging to one cluster
type IVFCluster struct {
	mu       sync.RWMutex
	entries  []IVFIndexEntry
	centroid []float32
}

// IVFPQIndex implements a standalone IVF-PQ index
type IVFPQIndex struct {
	config IVFPQConfig

	// Coarse quantizer (k-means)
	coarseCentroids []float32 // k * dim

	// Product Quantization encoder
	pqEncoder *pq.PQEncoder

	// Inverted index: clusterID -> vectors
	clusters []IVFCluster

	// Vector storage for search candidates
	vectorStore map[uint32][]float32

	// Metadata
	dim    int
	nextID uint32
	mu     sync.RWMutex
}

// NewIVFPQIndex creates a new IVF-PQ index
func NewIVFPQIndex(dim int, config IVFPQConfig) (*IVFPQIndex, error) {
	if dim <= 0 {
		return nil, errors.New("invalid dimension")
	}
	if config.Nlist <= 0 {
		config.Nlist = 1024
	}
	if config.M <= 0 {
		config.M = 8
	}
	if config.K <= 0 {
		config.K = 256
	}
	if config.Nprobe <= 0 {
		config.Nprobe = 8
	}

	pqEncoder, err := pq.NewPQEncoder(dim, config.M, config.K)
	if err != nil {
		return nil, err
	}

	idx := &IVFPQIndex{
		config:      config,
		pqEncoder:   pqEncoder,
		clusters:    make([]IVFCluster, config.Nlist),
		vectorStore: make(map[uint32][]float32),
		dim:         dim,
	}

	return idx, nil
}

// Train builds the coarse quantizer using k-means
func (idx *IVFPQIndex) Train(vectors [][]float32) error {
	if len(vectors) == 0 {
		return errors.New("empty training data")
	}

	// Flatten vectors for k-means
	n := len(vectors)
	flatData := make([]float32, n*idx.dim)
	for i, v := range vectors {
		copy(flatData[i*idx.dim:(i+1)*idx.dim], v)
	}

	// Train coarse quantizer (IVF)
	centroids, err := pq.TrainKMeans(flatData, n, idx.dim, idx.config.Nlist, 20)
	if err != nil {
		return err
	}
	idx.coarseCentroids = centroids

	// Train PQ encoder on sample data
	if err := idx.pqEncoder.Train(vectors); err != nil {
		return err
	}

	return nil
}

// Add adds vectors to the index
func (idx *IVFPQIndex) Add(ctx context.Context, vectors [][]float32) error {
	if len(vectors) == 0 {
		return nil
	}

	if idx.coarseCentroids == nil {
		return errors.New("index not trained")
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	for _, vec := range vectors {
		if len(vec) != idx.dim {
			return errors.New("vector dimension mismatch")
		}

		// 1. Find coarse cluster (using coarse centroids)
		clusterID := idx.assignToCluster(vec)

		// 2. Encode with PQ
		pqCode, err := idx.pqEncoder.Encode(vec)
		if err != nil {
			return err
		}

		// 3. Add to inverted index
		entry := IVFIndexEntry{
			VectorID: idx.nextID,
			PQCode:   pqCode,
		}

		idx.clusters[clusterID].mu.Lock()
		idx.clusters[clusterID].entries = append(idx.clusters[clusterID].entries, entry)
		idx.clusters[clusterID].mu.Unlock()

		// 4. Store vector for scoring
		vecCopy := make([]float32, idx.dim)
		copy(vecCopy, vec)
		idx.vectorStore[idx.nextID] = vecCopy

		idx.nextID++
	}

	return nil
}

// assignToCluster finds the nearest coarse centroid
func (idx *IVFPQIndex) assignToCluster(vec []float32) int {
	bestDist := float32(math.MaxFloat32)
	bestCluster := 0

	for c := 0; c < idx.config.Nlist; c++ {
		cent := idx.coarseCentroids[c*idx.dim : (c+1)*idx.dim]
		dist := idx.l2Squared(vec, cent)
		if dist < bestDist {
			bestDist = dist
			bestCluster = c
		}
	}

	return bestCluster
}

// l2Squared computes L2 squared distance between two vectors
func (idx *IVFPQIndex) l2Squared(a, b []float32) float32 {
	var sum float32
	for i := 0; i < len(a); i++ {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return sum
}

// SearchResult holds a search result
type IVFPQSearchResult struct {
	ID       uint32
	Distance float32
}

// Search searches for k nearest neighbors using IVF-PQ
func (idx *IVFPQIndex) Search(ctx context.Context, queryVec []float32, k int, _ []query.Filter, _ SearchOptions) ([]SearchResult, error) {
	if len(queryVec) != idx.dim {
		return nil, errors.New("query dimension mismatch")
	}
	if k <= 0 {
		return nil, errors.New("k must be positive")
	}

	// 1. Build ADC distance table for query
	adt, err := idx.pqEncoder.BuildADCTable(queryVec)
	if err != nil {
		return nil, err
	}

	// 2. Find candidate clusters
	candidateClusters := idx.findNearestClusters(queryVec)

	// 3. Search candidates in each cluster
	results := make([]IVFPQSearchResult, 0, k*len(candidateClusters))

	for _, clusterID := range candidateClusters {
		cluster := &idx.clusters[clusterID]

		cluster.mu.RLock()
		entries := cluster.entries
		cluster.mu.RUnlock()

		for _, entry := range entries {
			// Compute ADC distance: sum of precomputed distances
			dist := idx.computeADCDistance(entry.PQCode, adt)

			results = append(results, IVFPQSearchResult{
				ID:       entry.VectorID,
				Distance: dist,
			})
		}
	}

	// 4. Sort by distance and take top k
	sort.Slice(results, func(i, j int) bool {
		return results[i].Distance < results[j].Distance
	})

	if len(results) > k {
		results = results[:k]
	}

	// 5. Convert to SearchResult format
	searchResults := make([]SearchResult, len(results))
	for i, r := range results {
		searchResults[i] = SearchResult{
			ID:       VectorID(r.ID),
			Distance: r.Distance,
			Score:    1.0 / (1.0 + r.Distance),
		}
	}

	return searchResults, nil
}

// findNearestClusters finds the nprobe nearest clusters to the query
func (idx *IVFPQIndex) findNearestClusters(queryVec []float32) []int {
	type clusterDist struct {
		id   int
		dist float32
	}

	dists := make([]clusterDist, idx.config.Nlist)
	for c := 0; c < idx.config.Nlist; c++ {
		cent := idx.coarseCentroids[c*idx.dim : (c+1)*idx.dim]
		dists[c] = clusterDist{id: c, dist: idx.l2Squared(queryVec, cent)}
	}

	// Sort by distance
	sort.Slice(dists, func(i, j int) bool {
		return dists[i].dist < dists[j].dist
	})

	nprobe := idx.config.Nprobe
	if nprobe > idx.config.Nlist {
		nprobe = idx.config.Nlist
	}

	result := make([]int, nprobe)
	for i := 0; i < nprobe; i++ {
		result[i] = dists[i].id
	}

	return result
}

// computeADCDistance computes the sum of distances using precomputed ADC table
func (idx *IVFPQIndex) computeADCDistance(pqCode []byte, adt []float32) float32 {
	var dist float32
	for m := 0; m < idx.config.M; m++ {
		code := pqCode[m]
		dist += adt[m*idx.config.K+int(code)]
	}
	return dist
}

// Size returns the number of vectors in the index
func (idx *IVFPQIndex) Size() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return int(idx.nextID)
}

// GetDimension returns the vector dimension
func (idx *IVFPQIndex) GetDimension() uint32 {
	return uint32(idx.dim)
}

// GetMemoryUsage returns approximate memory usage in bytes
func (idx *IVFPQIndex) GetMemoryUsage() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var bytes int64

	// Coarse centroids
	bytes += int64(len(idx.coarseCentroids) * 4)

	// PQ codebooks
	bytes += int64(idx.config.M * idx.config.K * idx.dim / idx.config.M * 4)

	// Vector store
	for _, vec := range idx.vectorStore {
		bytes += int64(len(vec) * 4)
	}

	// PQ codes (M bytes per vector)
	bytes += int64(idx.nextID * uint32(idx.config.M))

	return bytes
}

// VectorIndex interface implementation for compatibility
func (idx *IVFPQIndex) SearchVectorsWithBitmap(ctx context.Context, q any, k int, filter any, options any) ([]SearchResult, error) {
	queryVec, ok := q.([]float32)
	if !ok {
		return nil, errors.New("unsupported query type")
	}
	opts, _ := options.(SearchOptions)
	return idx.Search(ctx, queryVec, k, nil, opts)
}

func (idx *IVFPQIndex) SearchVectors(ctx context.Context, q any, k int, filters []query.Filter, options any) ([]SearchResult, error) {
	return idx.SearchVectorsWithBitmap(ctx, q, k, nil, options)
}

func (idx *IVFPQIndex) IsSharded() bool               { return false }
func (idx *IVFPQIndex) GetShardedIndex() *ShardedHNSW { return nil }

// IndexLen returns the number of indexed vectors
func (idx *IVFPQIndex) IndexLen() int {
	return idx.Size()
}

// Warmup returns the number of vectors for warmup
func (idx *IVFPQIndex) Warmup() int {
	return idx.Size()
}
