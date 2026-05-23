package store

import (
	"context"
	"errors"
	"math"
	"sort"
	"sync"

	"bytes"
	"encoding/gob"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"
	"io"
)

// IVFPQConfig holds configuration for the IVF-PQ index.
type IVFPQConfig struct {
	// Coarse quantizer: number of clusters (IVF)
	Nlist int // Default 1024

	// Product Quantization: number of subvectors and bits per subspace
	M int // Number of subvectors (subspaces) - typically 8-16
	K int // Number of centroids per subspace - typically 256 (8 bits)

	// Search parameters
	Nprobe int // Number of clusters to search - typically 8-64
}

// DefaultIVFPQConfig returns sensible defaults for the IVF-PQ index.
func DefaultIVFPQConfig() IVFPQConfig {
	return IVFPQConfig{
		Nlist:  1024,
		M:      8,
		K:      256,
		Nprobe: 8,
	}
}

// IVFIndexEntry holds a single vector entry in the inverted index.
type IVFIndexEntry struct {
	VectorID uint32
	PQCode   []byte
}

// IVFCluster holds all vectors belonging to one cluster.
type IVFCluster struct {
	mu       sync.RWMutex
	Entries  []IVFIndexEntry
	centroid []float32
}

// IVFPQIndex implements a standalone IVF-PQ index for efficient approximate nearest neighbor search.
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

	// Location mapping
	locationToID map[uint64]uint32
	idToLocation map[uint32]uint64

	// Metadata
	dim    int
	nextID uint32
	mu     sync.RWMutex

	distFunc simd.DistanceKernel[float32]
}

// NewIVFPQIndex creates a new IVF-PQ index with the specified dimensions and configuration.
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
		config:       config,
		pqEncoder:    pqEncoder,
		clusters:     make([]IVFCluster, config.Nlist),
		vectorStore:  make(map[uint32][]float32),
		locationToID: make(map[uint64]uint32),
		idToLocation: make(map[uint32]uint64),
		dim:          dim,
	}

	return idx, nil
}

// Train builds the coarse quantizer using k-means and trains the product quantizer.
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

	// Resolve kernel
	idx.distFunc = simd.GetKernel[float32](simd.MetricEuclidean, idx.dim)
	if idx.distFunc == nil {
		idx.distFunc = simd.L2Squared
	}

	return nil
}

// Add inserts vectors into the index.
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
		idx.clusters[clusterID].Entries = append(idx.clusters[clusterID].Entries, entry)
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
		dist, _ := idx.getDistFunc()(vec, cent)
		if dist < bestDist {
			bestDist = dist
			bestCluster = c
		}
	}

	return bestCluster
}

func (idx *IVFPQIndex) getDistFunc() simd.DistanceKernel[float32] {
	if idx.distFunc != nil {
		return idx.distFunc
	}
	idx.distFunc = simd.GetKernel[float32](simd.MetricEuclidean, idx.dim)
	if idx.distFunc == nil {
		idx.distFunc = simd.L2Squared
	}
	return idx.distFunc
}

// IVFPQSearchResult holds a single search result with its ID and distance.
type IVFPQSearchResult struct {
	ID       uint32
	Distance float32
}

// SearchInternal searches for k nearest neighbors using IVF-PQ with optional bitmap filtering.
func (idx *IVFPQIndex) SearchInternal(ctx context.Context, queryVec []float32, k int, filter *roaring.Bitmap, _ SearchOptions) ([]types.SearchResult, error) {
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
		entries := cluster.Entries
		cluster.mu.RUnlock()

		for _, entry := range entries {
			// Apply filter pushdown
			if filter != nil && !filter.Contains(entry.VectorID) {
				continue
			}

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
	searchResults := make([]types.SearchResult, len(results))
	for i, r := range results {
		searchResults[i] = types.SearchResult{
			ID:       types.VectorID(r.ID),
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
		dist, _ := idx.getDistFunc()(queryVec, cent)
		dists[c] = clusterDist{id: c, dist: dist}
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

// AddByLocation inserts a vector from the specified physical location.
func (idx *IVFPQIndex) AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	// For IVF-PQ, we need the actual vector data to encode it.
	// This usually requires access to the dataset, which this index doesn't have directly.
	// However, if the vector is already in the record (passed via AddByRecord), we use that.
	return 0, errors.New("AddByLocation requires vector data for IVF-PQ encoding")
}

// AddByRecord inserts a vector from an Arrow record.
func (idx *IVFPQIndex) AddByRecord(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	vec, err := ExtractVectorFromArrow(rec, rowIdx, -1)
	if err != nil {
		return 0, err
	}
	if err := idx.Add(ctx, [][]float32{vec}); err != nil {
		return 0, err
	}

	id := idx.nextID - 1
	idx.SetLocation(id, types.Location{BatchIdx: batchIdx, RowIdx: rowIdx})

	return id, nil
}

// Search executes a vector search query.
func (idx *IVFPQIndex) Search(ctx context.Context, query any, k int, filter any) ([]types.Candidate, error) {
	results, err := idx.SearchVectorsWithBitmap(ctx, query, k, nil, nil)
	if err != nil {
		return nil, err
	}
	candidates := make([]types.Candidate, len(results))
	for i, r := range results {
		candidates[i] = types.Candidate{ID: uint32(r.ID), Dist: r.Distance}
	}
	return candidates, nil
}

// Size returns the total number of vectors in the index.
func (idx *IVFPQIndex) Size() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return int(idx.nextID)
}

// Len returns the number of vectors in the index.
func (idx *IVFPQIndex) Len() int {
	return idx.Size()
}

// GetEntryPoint returns 0 as IVF has no single traversal entry point.
func (idx *IVFPQIndex) GetEntryPoint() uint32 {
	return 0
}

// GetLocation returns the physical location for a given vector ID.
func (idx *IVFPQIndex) GetLocation(id uint32) (any, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	loc, ok := idx.idToLocation[id]
	if !ok {
		return nil, false
	}
	return types.UnpackLocation(loc), true
}

// GetVectorID returns the vector ID for a given physical location.
func (idx *IVFPQIndex) GetVectorID(loc Location) (uint64, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	id, ok := idx.locationToID[types.PackLocation(loc)]
	return uint64(id), ok
}

// SetLocation registers a mapping between a vector ID and its physical location.
func (idx *IVFPQIndex) SetLocation(id uint32, loc any) {
	l, ok := loc.(types.Location)
	if !ok {
		return
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	packed := types.PackLocation(l)
	idx.locationToID[packed] = id
	idx.idToLocation[id] = packed
}

// GetDimension returns the dimensionality of the vectors.
func (idx *IVFPQIndex) GetDimension() uint32 {
	return uint32(idx.dim) // #nosec G115
}

// SetIndexedColumns is a no-op for this index type.
func (idx *IVFPQIndex) SetIndexedColumns(cols []string) {}

// GetIndexType returns the index type identifier (IVF-PQ).
func (idx *IVFPQIndex) GetIndexType() string {
	return "ivf_pq"
}

// GetRawNeighbors is not supported for IVFPQIndex.
func (idx *IVFPQIndex) GetRawNeighbors(id uint32) ([]uint32, error) {
	return nil, errors.New("GetRawNeighbors not supported for IVFPQIndex")
}

// GetNeighbors is not supported for IVFPQIndex.
func (idx *IVFPQIndex) GetNeighbors(ctx context.Context, id uint32, k int) ([]types.SearchResult, error) {
	return nil, errors.New("GetNeighbors not supported for IVFPQIndex")
}

// PreWarm is a no-op for this index type.
func (idx *IVFPQIndex) PreWarm(targetSize int) {}

// Warmup returns the current size of the index.
func (idx *IVFPQIndex) Warmup() int {
	return idx.Size()
}

// EstimateMemory returns the estimated memory usage of the index.
func (idx *IVFPQIndex) EstimateMemory() int64 {
	return idx.GetMemoryUsage()
}

// GetPQEncoder returns the underlying PQ encoder.
func (idx *IVFPQIndex) GetPQEncoder() *pq.PQEncoder {
	return idx.pqEncoder
}

// Close releases all resources held by the index.
func (idx *IVFPQIndex) Close() error {
	return nil
}

// AddBatch inserts a batch of vectors.
func (idx *IVFPQIndex) AddBatch(ctx context.Context, recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	if len(recs) == 0 {
		return nil, nil
	}
	ids := make([]uint32, 0, len(recs))
	for _, rec := range recs {
		n := int(rec.NumRows())
		for row := 0; row < n; row++ {
			vec, err := ExtractVectorFromArrow(rec, row, -1)
			if err != nil {
				return nil, err
			}
			id := idx.nextID
			if err := idx.Add(ctx, [][]float32{vec}); err != nil {
				return nil, err
			}
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// DeleteBatch is not supported for this index type.
func (idx *IVFPQIndex) DeleteBatch(ctx context.Context, ids []uint32) error {
	return errors.New("DeleteBatch not supported for IVFPQIndex")
}

// SearchVectorsWithBitmap performs a search with bitset filtering.
func (idx *IVFPQIndex) SearchVectorsWithBitmap(ctx context.Context, q any, k int, filter *roaring.Bitmap, options any) ([]types.SearchResult, error) {
	queryVec, ok := q.([]float32)
	if !ok {
		return nil, errors.New("unsupported query type")
	}
	opts, _ := options.(SearchOptions)
	return idx.SearchInternal(ctx, queryVec, k, filter, opts)
}

// SearchVectors performs a search and returns standard SearchResult types.
func (idx *IVFPQIndex) SearchVectors(ctx context.Context, q any, k int, filters []query.Filter, options any) ([]types.SearchResult, error) {
	return idx.SearchVectorsWithBitmap(ctx, q, k, nil, options)
}

// SearchVectorsInRange returns results within a distance threshold.
func (idx *IVFPQIndex) SearchVectorsInRange(ctx context.Context, q any, threshold float32, filters []query.Filter, options any) ([]SearchResult, error) {
	// For IVF-PQ, range search uses high k to get candidates then filters by threshold
	qF32, ok := q.([]float32)
	if !ok {
		return nil, errors.New("IVFPQIndex only supports []float32 queries")
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.nextID == 0 {
		return nil, nil
	}

	// Use max possible candidates (all vectors)
	allResults, err := idx.SearchInternal(ctx, qF32, int(idx.nextID), nil, SearchOptions{})
	if err != nil {
		return nil, err
	}

	var results []SearchResult
	for _, r := range allResults {
		if r.Distance <= threshold {
			results = append(results, SearchResult{
				ID:       VectorID(r.ID),
				Distance: r.Distance,
				Score:    r.Score,
			})
		}
	}

	return results, nil
}

// TrainPQ delegates to the Train method.
func (idx *IVFPQIndex) TrainPQ(vectors [][]float32) error {
	return idx.Train(vectors)
}

// ExportState exports the internal state of the index.
func (idx *IVFPQIndex) ExportState() ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	// We wrap fields in a serializable struct
	state := struct {
		Config          IVFPQConfig
		CoarseCentroids []float32
		PQEncoder       *pq.PQEncoder
		NextID          uint32
		Dim             int
		VectorStore     map[uint32][]float32
		LocationToID    map[uint64]uint32
		IDToLocation    map[uint32]uint64
	}{
		Config:          idx.config,
		CoarseCentroids: idx.coarseCentroids,
		PQEncoder:       idx.pqEncoder,
		NextID:          idx.nextID,
		Dim:             idx.dim,
		VectorStore:     idx.vectorStore,
		LocationToID:    idx.locationToID,
		IDToLocation:    idx.idToLocation,
	}

	if err := enc.Encode(state); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// ImportState restores the index state from a byte slice.
func (idx *IVFPQIndex) ImportState(data []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	var state struct {
		Config          IVFPQConfig
		CoarseCentroids []float32
		PQEncoder       *pq.PQEncoder
		NextID          uint32
		Dim             int
		VectorStore     map[uint32][]float32
		LocationToID    map[uint64]uint32
		IDToLocation    map[uint32]uint64
	}

	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&state); err != nil {
		return err
	}

	idx.config = state.Config
	idx.coarseCentroids = state.CoarseCentroids
	idx.pqEncoder = state.PQEncoder
	idx.nextID = state.NextID
	idx.dim = state.Dim
	idx.vectorStore = state.VectorStore
	idx.locationToID = state.LocationToID
	idx.idToLocation = state.IDToLocation

	// Rebuild clusters structure (entries must be migrated manually or saved)
	// For IVFPQ, the 'clusters' field contains live entries. We should include them in state.
	return nil
}

// ExportGraph exports the graph structure of the index (inverted file entries).
func (idx *IVFPQIndex) ExportGraph(w io.Writer) error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	enc := gob.NewEncoder(w)
	// Serialize cluster entries
	clusterEntries := make([][]IVFIndexEntry, len(idx.clusters))
	for i := range idx.clusters {
		idx.clusters[i].mu.RLock()
		clusterEntries[i] = idx.clusters[i].Entries
		idx.clusters[i].mu.RUnlock()
	}

	return enc.Encode(clusterEntries)
}

// ImportGraph imports the graph structure of the index.
func (idx *IVFPQIndex) ImportGraph(r io.Reader) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	dec := gob.NewDecoder(r)
	var clusterEntries [][]IVFIndexEntry
	if err := dec.Decode(&clusterEntries); err != nil {
		return err
	}

	if len(clusterEntries) != len(idx.clusters) {
		idx.clusters = make([]IVFCluster, len(clusterEntries))
	}

	for i, entries := range clusterEntries {
		idx.clusters[i].Entries = entries
	}

	return nil
}

// ExportDelta is a no-op for this index type.
func (idx *IVFPQIndex) ExportDelta(fromV uint64) (*types.DeltaSync, error) { return nil, nil }

// ApplyDelta is a no-op for this index type.
func (idx *IVFPQIndex) ApplyDelta(delta *types.DeltaSync) error { return nil }

// SetParallelSearchConfig is a no-op for this index type.
func (idx *IVFPQIndex) SetParallelSearchConfig(cfg types.ParallelSearchConfig) {}

// GetParallelSearchConfig returns an empty config.
func (idx *IVFPQIndex) GetParallelSearchConfig() types.ParallelSearchConfig {
	return types.ParallelSearchConfig{}
}

// RemapLocations is a no-op for this index type.
func (idx *IVFPQIndex) RemapLocations(ctx context.Context, m map[uint32]any) error { return nil }

// GetMemoryUsage returns the estimated memory usage of the index.
func (idx *IVFPQIndex) GetMemoryUsage() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	var bytes int64
	bytes += int64(len(idx.coarseCentroids) * 4)
	bytes += int64(idx.config.M * idx.config.K * idx.dim / idx.config.M * 4)
	for _, vec := range idx.vectorStore {
		bytes += int64(len(vec) * 4)
	}
	bytes += int64(idx.nextID * uint32(idx.config.M)) // #nosec G115
	return bytes
}

// IsSharded returns false for this index type.
func (idx *IVFPQIndex) IsSharded() bool { return false }

// GetShardedIndex returns nil as it is not a sharded index.
func (idx *IVFPQIndex) GetShardedIndex() *ShardedHNSW { return nil }
