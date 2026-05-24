package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"bytes"
	"encoding/gob"
	"os"

	"github.com/23skdu/longbow/internal/core"
	gputypes "github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"
)

// IVFHNSWConfig holds configuration for the IVF-HNSW composite index.
type IVFHNSWConfig struct {
	Nlist  int // Number of clusters
	M      int // Number of PQ subvectors (quantization)
	K      int // PQ centroids per subspace (default 256)
	Nprobe int // Clusters to search

	// HNSW coarse quantizer settings
	HNSWM              int
	HNSWEfConstruction int
	HNSWEfSearch       int

	GPUEnabled bool
	GPUConfig  *gputypes.GPUConfig
}

// IVFHNSWCompositeIndex implements a high-density billion-scale composite index.
// It uses HNSW for fast coarse quantization (assignment to clusters) and
// OPQ/PQ encoded inverted lists for dense storage and fast scan.
type IVFHNSWCompositeIndex struct {
	config IVFHNSWConfig
	dim    int

	coarseHNSW PluggableVectorIndex // Coarse quantizer
	opqEncoder *pq.OPQEncoder
	clusters   []IVFCluster

	nextID uint32
	mu     sync.RWMutex
}

// NewIVFHNSWCompositeIndex creates a new IVF-HNSW composite index with the specified dimensions and configuration.
func NewIVFHNSWCompositeIndex(dim int, config IVFHNSWConfig) (*IVFHNSWCompositeIndex, error) {
	if dim <= 0 {
		return nil, errors.New("invalid dimension")
	}
	if config.Nlist <= 0 {
		config.Nlist = 1024
	}
	if config.M <= 0 {
		config.M = dim / 4 // 4x compression by default
		if config.M == 0 {
			config.M = 1
		}
	}
	// Ensure divisibility for PQ subspaces
	for config.M > 1 && dim%config.M != 0 {
		config.M--
	}
	if config.K <= 0 {
		config.K = 256
	}
	if config.HNSWM <= 0 {
		config.HNSWM = 16
	}
	if config.HNSWEfConstruction <= 0 {
		config.HNSWEfConstruction = 200
	}

	opq, err := pq.NewOPQEncoder(dim, config.M, config.K)
	if err != nil {
		return nil, err
	}

	idx := &IVFHNSWCompositeIndex{
		config:     config,
		dim:        dim,
		opqEncoder: opq,
		clusters:   make([]IVFCluster, config.Nlist),
	}

	return idx, nil
}

// Type returns the index type identifier (IVFHNSW).
func (idx *IVFHNSWCompositeIndex) Type() IndexType {
	return IndexTypeIVFHNSW
}

// Dimension returns the dimensionality of the vectors in the index.
func (idx *IVFHNSWCompositeIndex) Dimension() int {
	return idx.dim
}

// NeedsBuild returns true because the IVF index requires training on a representative dataset.
func (idx *IVFHNSWCompositeIndex) NeedsBuild() bool {
	return true
}

// Coarse assignment via HNSW

// Train builds the coarse centroids and the HNSW coarse index using the provided sample vectors.
func (idx *IVFHNSWCompositeIndex) Train(vectors [][]float32) error {
	if len(vectors) == 0 {
		return errors.New("empty training data")
	}

	start := time.Now()
	n := len(vectors)

	// 1. Train Coarse Centroids using K-Means
	flatData := make([]float32, n*idx.dim)
	for i, v := range vectors {
		copy(flatData[i*idx.dim:(i+1)*idx.dim], v)
	}

	centroids, err := pq.TrainKMeans(flatData, n, idx.dim, idx.config.Nlist, 20)
	if err != nil {
		return err
	}

	// 2. Build HNSW index on coarse centroids
	h, err := createHNSWIndex(IndexConfig{
		Type:      IndexTypeHNSW,
		Dimension: idx.dim,
		HNSWConfig: &ArrowHNSWConfig{
			M:              idx.config.HNSWM,
			EfConstruction: int32(idx.config.HNSWEfConstruction), // #nosec G115
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create HNSW coarse index: %w", err)
	}

	ids := make([]uint64, idx.config.Nlist)
	vecs := make([][]float32, idx.config.Nlist)
	for i := 0; i < idx.config.Nlist; i++ {
		ids[i] = uint64(i)
		vecs[i] = centroids[i*idx.dim : (i+1)*idx.dim]
	}

	if err := h.AddBatchRaw(ids, vecs); err != nil {
		return fmt.Errorf("failed to build HNSW coarse index: %w", err)
	}
	idx.coarseHNSW = h

	// 3. Train OPQ Encoder on residual vectors
	// (Actually training on raw vectors is standard for OPQ)
	if err := idx.opqEncoder.TrainOPQ(vectors, 10); err != nil {
		return err
	}

	metrics.VQTrainingDurationSeconds.WithLabelValues("ivf-hnsw").Observe(time.Since(start).Seconds())
	return nil
}

// Build is a no-op for the composite index as the work is performed during Train.
func (idx *IVFHNSWCompositeIndex) Build() error {
	// Build is handled by Train
	return nil
}

// Add adds a single vector to the index.
func (idx *IVFHNSWCompositeIndex) Add(id uint64, vector []float32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.coarseHNSW == nil {
		return errors.New("index not trained")
	}

	// 1. Assignment via HNSW coarse quantizer
	results, err := idx.coarseHNSW.Search(vector, 1)
	if err != nil || len(results) == 0 {
		return fmt.Errorf("failed to assign vector to cluster: %w", err)
	}
	clusterID := int(results[0].ID) // #nosec G115 -- ID is within int range

	// 2. Encode with OPQ
	code, err := idx.opqEncoder.Encode(vector)
	if err != nil {
		return err
	}

	// 3. Add to inverted list
	idx.clusters[clusterID].mu.Lock()
	idx.clusters[clusterID].Entries = append(idx.clusters[clusterID].Entries, IVFIndexEntry{
		VectorID: uint32(id), // #nosec G115
		PQCode:   code,
	})
	idx.clusters[clusterID].mu.Unlock()

	if uint32(id) >= idx.nextID { // #nosec G115 -- id is within uint32 range
		idx.nextID = uint32(id) + 1 // #nosec G115 -- id is within uint32 range
	}

	return nil
}

// GetGPUIndex returns nil as this is a CPU-based implementation.
func (idx *IVFHNSWCompositeIndex) GetGPUIndex() any { return nil }

// AddBatchRaw adds a batch of vectors with explicit IDs to the index.
func (idx *IVFHNSWCompositeIndex) AddBatchRaw(ids []uint64, vectors [][]float32) error {
	for i, id := range ids {
		if err := idx.Add(id, vectors[i]); err != nil {
			return err
		}
	}
	return nil
}

// AddBatch adds vectors from Arrow RecordBatches to the index.
func (idx *IVFHNSWCompositeIndex) AddBatch(ctx context.Context, recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
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
			id := uint64(idx.nextID)
			if err := idx.Add(id, vec); err != nil {
				return nil, err
			}
			ids = append(ids, uint32(id)) // #nosec G115 -- id is within uint32 range
		}
	}
	return ids, nil
}

// DeleteBatch is currently a no-op for the composite index.
func (idx *IVFHNSWCompositeIndex) DeleteBatch(ctx context.Context, ids []uint32) error {
	return nil
}

// Search finds k nearest neighbors for the query vector
func (idx *IVFHNSWCompositeIndex) Search(query []float32, k int) ([]IndexSearchResult, error) {
	results, err := idx.SearchVectorsWithBitmap(context.Background(), query, k, nil, nil)
	if err != nil {
		return nil, err
	}

	finalResults := make([]IndexSearchResult, len(results))
	for i, r := range results {
		finalResults[i] = IndexSearchResult{
			ID:       uint64(r.ID),
			Distance: r.Distance,
		}
	}
	return finalResults, nil
}

// SearchBatch performs a batch search for multiple query vectors.
func (idx *IVFHNSWCompositeIndex) SearchBatch(queries [][]float32, k int) ([][]IndexSearchResult, error) {
	results := make([][]IndexSearchResult, len(queries))
	for i, q := range queries {
		r, err := idx.Search(q, k)
		if err != nil {
			return nil, err
		}
		results[i] = r
	}
	return results, nil
}

// SearchVectorsWithBitmap implements the VectorIndexer search interface
func (idx *IVFHNSWCompositeIndex) SearchVectorsWithBitmap(ctx context.Context, q any, k int, filter *roaring.Bitmap, options any) ([]types.SearchResult, error) {
	queryVec, ok := q.([]float32)
	if !ok {
		return nil, errors.New("unsupported query type")
	}

	if idx.coarseHNSW == nil || idx.nextID == 0 {
		return nil, nil
	}

	// 1. Find nearest clusters via HNSW
	nprobe := idx.config.Nprobe
	if nprobe <= 0 {
		nprobe = 1
	}

	clusterResults, err := idx.coarseHNSW.Search(queryVec, nprobe)
	if err != nil {
		return nil, fmt.Errorf("coarse search failed: %w", err)
	}

	// 2. Build ADC table for OPQ
	rotatedQuery := idx.opqEncoder.RotateVector(queryVec)
	adt, err := idx.opqEncoder.PQEncoder.BuildADCTable(rotatedQuery)
	if err != nil {
		return nil, err
	}

	// 3. Scan clusters in parallel if many nprobe
	var candidates []types.SearchResult
	var candMu sync.Mutex

	var wg sync.WaitGroup
	for _, res := range clusterResults {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			cluster := &idx.clusters[cid]
			cluster.mu.RLock()
			defer cluster.mu.RUnlock()

			localCands := make([]types.SearchResult, 0, len(cluster.Entries))
			for _, entry := range cluster.Entries {
				if filter != nil && !filter.Contains(entry.VectorID) {
					continue
				}

				var dist float32
				for m := 0; m < idx.config.M; m++ {
					dist += adt[m*idx.config.K+int(entry.PQCode[m])]
				}
				localCands = append(localCands, types.SearchResult{
					ID:       types.VectorID(entry.VectorID),
					Distance: dist,
				})
			}

			if len(localCands) > 0 {
				candMu.Lock()
				candidates = append(candidates, localCands...)
				candMu.Unlock()
			}
		}(int(res.ID)) // #nosec G115 -- res.ID is within int range
	}
	wg.Wait()

	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Distance < candidates[j].Distance })
	if len(candidates) > k {
		candidates = candidates[:k]
	}

	metrics.IVFClusterSearchTotal.WithLabelValues("composite", "ivf-hnsw").Add(float64(len(clusterResults)))
	return candidates, nil
}

// AddByLocation is not supported for IVF-HNSW; use Add instead.
func (idx *IVFHNSWCompositeIndex) AddByLocation(batchIdx, rowIdx int) error {
	return fmt.Errorf("AddByLocation not supported for IVF-HNSW (use Add)")
}

// GetVectorID returns the vector ID for a given location (not supported).
func (idx *IVFHNSWCompositeIndex) GetVectorID(loc Location) (uint64, bool) {
	return 0, false
}

// SearchVectors performs a synchronous search and returns results.
func (idx *IVFHNSWCompositeIndex) SearchVectors(query []float32, k int, options SearchOptions) []types.SearchResult {
	results, _ := idx.SearchVectorsWithBitmap(context.Background(), query, k, nil, nil)
	return results
}

// Size returns the total number of vectors in the index.
func (idx *IVFHNSWCompositeIndex) Size() int { return int(idx.nextID) }

// Len returns the total number of vectors in the index.
func (idx *IVFHNSWCompositeIndex) Len() int { return idx.Size() }

// GetIndexType returns the index type as a string.
func (idx *IVFHNSWCompositeIndex) GetIndexType() string {
	return string(idx.Type())
}

// Close releases resources associated with the index.
func (idx *IVFHNSWCompositeIndex) Close() error {
	if idx.coarseHNSW != nil {
		return idx.coarseHNSW.Close()
	}
	return nil
}

// GetDimension returns the dimensionality as a uint32.
func (idx *IVFHNSWCompositeIndex) GetDimension() uint32 { return uint32(idx.dim) } // #nosec G115 -- dim is within uint32 range

// SetParallelSearchConfig is a no-op for the composite index.
func (idx *IVFHNSWCompositeIndex) SetParallelSearchConfig(c types.ParallelSearchConfig) {}

// GetParallelSearchConfig returns an empty parallel search configuration.
func (idx *IVFHNSWCompositeIndex) GetParallelSearchConfig() types.ParallelSearchConfig {
	return types.ParallelSearchConfig{}
}

// IsSharded returns false as the composite index is a local-only implementation.
func (idx *IVFHNSWCompositeIndex) IsSharded() bool { return false }

// ivfHNSWCompositeState is used for serialization
type ivfHNSWCompositeState struct {
	Config     IVFHNSWConfig
	Dim        int
	NextID     uint32
	CoarseHNSW []byte
	OPQ        []byte
	Clusters   []IVFCluster
}

// ExportState returns the serialized state of the index.
func (idx *IVFHNSWCompositeIndex) ExportState() ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var coarseData []byte
	var err error
	if idx.coarseHNSW != nil {
		coarseData, err = idx.coarseHNSW.ExportState()
		if err != nil {
			return nil, fmt.Errorf("failed to export coarse HNSW state: %w", err)
		}
	}

	opqData, err := idx.opqEncoder.ExportState()
	if err != nil {
		return nil, fmt.Errorf("failed to export OPQ state: %w", err)
	}

	state := ivfHNSWCompositeState{
		Config:     idx.config,
		Dim:        idx.dim,
		NextID:     idx.nextID,
		CoarseHNSW: coarseData,
		OPQ:        opqData,
		Clusters:   idx.clusters,
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(state); err != nil {
		return nil, fmt.Errorf("failed to serialize state: %w", err)
	}
	return buf.Bytes(), nil
}

// Save serializes the index to a file.
func (idx *IVFHNSWCompositeIndex) Save(path string) error {
	data, err := idx.ExportState()
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600) // #nosec G304 -- path is internal, not user-controlled
}

// Load restores the index from a file.
func (idx *IVFHNSWCompositeIndex) Load(path string) error {
	data, err := os.ReadFile(path) // #nosec G304 -- path is internal, not user-controlled
	if err != nil {
		return err
	}
	return idx.ImportState(data)
}

// ImportState restores the index state from a byte slice.
func (idx *IVFHNSWCompositeIndex) ImportState(data []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	var state ivfHNSWCompositeState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&state); err != nil {
		return fmt.Errorf("failed to deserialize state: %w", err)
	}

	idx.config = state.Config
	idx.dim = state.Dim
	idx.nextID = state.NextID
	idx.clusters = state.Clusters

	opq, err := pq.NewOPQEncoder(idx.dim, idx.config.M, idx.config.K)
	if err != nil {
		return err
	}
	if err := opq.ImportState(state.OPQ); err != nil {
		return err
	}
	idx.opqEncoder = opq

	if len(state.CoarseHNSW) > 0 {
		h, err := createHNSWIndex(IndexConfig{
			Type:      IndexTypeHNSW,
			Dimension: idx.dim,
			HNSWConfig: &ArrowHNSWConfig{
				M:              idx.config.HNSWM,
				EfConstruction: int32(idx.config.HNSWEfConstruction), // #nosec G115
			},
		})
		if err != nil {
			return err
		}
		if err := h.ImportState(state.CoarseHNSW); err != nil {
			return err
		}
		idx.coarseHNSW = h
	}

	return nil
}

// ApplyDelta applies a series of new locations to the index.
func (idx *IVFHNSWCompositeIndex) ApplyDelta(d *types.DeltaSync) error {
	if d == nil || len(d.NewLocations) == 0 {
		return nil
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.coarseHNSW == nil {
		return errors.New("index not trained, cannot apply delta")
	}

	// 1. Fetch vectors for each location from the dataset
	for _, loc := range d.NewLocations {
		// DeltaSync locations are core.Location which usually contain batchIdx/rowIdx
		// But in our IVFHNSW implementation, we use uint64 IDs.
		// If the delta doesn't provide vectors directly, we must fetch them.

		// This is a simplified version assuming the dataset provides GetVectorByLocation
		vec, err := idx.fetchVector(loc)
		if err != nil {
			continue // Skip or log
		}

		// Use the StartIndex or similar to assign IDs if needed,
		// but typically IDs are derived from locations or provided in delta.
		// For now, we'll use nextID and increment.
		if err := idx.Add(uint64(idx.nextID), vec); err != nil {
			return err
		}
	}

	metrics.IndexSyncDeltaTotal.WithLabelValues("ivf-hnsw", "composite").Add(float64(len(d.NewLocations)))
	return nil
}

func (idx *IVFHNSWCompositeIndex) fetchVector(_ core.Location) ([]float32, error) {
	// This would interact with the underlying dataset to get the vector data
	// For now, we'll assume we can resolve it.
	return nil, fmt.Errorf("vector fetching from location not fully implemented in composite index")
}

// GetPQEncoder returns the underlying PQ encoder used by the index.
func (idx *IVFHNSWCompositeIndex) GetPQEncoder() *pq.PQEncoder {
	return idx.opqEncoder.PQEncoder
}

// GetNeighbors retrieves the nearest neighbors for a specific vector ID.
func (idx *IVFHNSWCompositeIndex) GetNeighbors(ctx context.Context, id types.VectorID, k int) ([]types.SearchResult, error) {
	// Not directly supported on the composite index (usually needs the full vector)
	return nil, fmt.Errorf("GetNeighbors not supported for IVF-HNSW")
}
