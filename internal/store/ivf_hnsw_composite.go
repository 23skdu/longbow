package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	gputypes "github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"bytes"
	"encoding/gob"
	"os"
)

// IVFHNSWConfig holds configuration for the IVF-HNSW composite index
type IVFHNSWConfig struct {
	Nlist         int // Number of clusters
	M             int // Number of PQ subvectors (quantization)
	K             int // PQ centroids per subspace (default 256)
	Nprobe        int // Clusters to search
	
	// HNSW coarse quantizer settings
	HNSWM             int
	HNSWEfConstruction int
	HNSWEfSearch       int
	
	GPUEnabled    bool
	GPUConfig     *gputypes.GPUConfig
}

// IVFHNSWCompositeIndex implements a high-density billion-scale composite index
// It uses HNSW for fast coarse quantization (assignment to clusters) and
// OPQ/PQ encoded inverted lists for dense storage and fast scan.
type IVFHNSWCompositeIndex struct {
	config IVFHNSWConfig
	dim    int

	coarseHNSW      PluggableVectorIndex // Coarse quantizer
	opqEncoder      *pq.OPQEncoder
	clusters        []IVFCluster
	
	nextID uint32
	mu     sync.RWMutex
}

// NewIVFHNSWCompositeIndex creates a new IVF-HNSW composite index
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

func (idx *IVFHNSWCompositeIndex) Type() IndexType {
	return IndexTypeIVFHNSW
}

func (idx *IVFHNSWCompositeIndex) Dimension() int {
	return idx.dim
}

func (idx *IVFHNSWCompositeIndex) NeedsBuild() bool {
	return true
}

// Train builds the coarse centroids and the HNSW coarse index
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

	if err := h.AddBatch(ids, vecs); err != nil {
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

func (idx *IVFHNSWCompositeIndex) Build() error {
	// Build is handled by Train
	return nil
}

// Add adds a single vector to the index
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
	clusterID := int(results[0].ID)

	// 2. Encode with OPQ
	code, err := idx.opqEncoder.Encode(vector)
	if err != nil {
		return err
	}

	// 3. Add to inverted list
	idx.clusters[clusterID].mu.Lock()
	idx.clusters[clusterID].entries = append(idx.clusters[clusterID].entries, IVFIndexEntry{
		VectorID: uint32(id), // #nosec G115
		PQCode:   code,
	})
	idx.clusters[clusterID].mu.Unlock()
	
	if uint32(id) >= idx.nextID {
		idx.nextID = uint32(id) + 1
	}

	return nil
}

// AddBatch adds multiple vectors to the index
func (idx *IVFHNSWCompositeIndex) AddBatch(ids []uint64, vectors [][]float32) error {
	for i, id := range ids {
		if err := idx.Add(id, vectors[i]); err != nil {
			return err
		}
	}
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
			
			localCands := make([]types.SearchResult, 0, len(cluster.entries))
			for _, entry := range cluster.entries {
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
		}(int(res.ID))
	}
	wg.Wait()

	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Distance < candidates[j].Distance })
	if len(candidates) > k {
		candidates = candidates[:k]
	}

	metrics.IVFClusterSearchTotal.WithLabelValues("composite", "ivf-hnsw").Add(float64(len(clusterResults)))
	return candidates, nil
}

// Interface compliance stubs
func (idx *IVFHNSWCompositeIndex) AddByLocation(batchIdx, rowIdx int) error {
	return fmt.Errorf("AddByLocation not supported for IVF-HNSW (use Add)")
}

func (idx *IVFHNSWCompositeIndex) GetVectorID(loc Location) (uint64, bool) {
	return 0, false
}

func (idx *IVFHNSWCompositeIndex) SearchVectors(query []float32, k int, options SearchOptions) []types.SearchResult {
	results, _ := idx.SearchVectorsWithBitmap(context.Background(), query, k, nil, nil)
	return results
}
func (idx *IVFHNSWCompositeIndex) Size() int { return int(idx.nextID) }
func (idx *IVFHNSWCompositeIndex) Len() int { return idx.Size() }
func (idx *IVFHNSWCompositeIndex) Close() error { 
	if idx.coarseHNSW != nil {
		return idx.coarseHNSW.Close()
	}
	return nil 
}
func (idx *IVFHNSWCompositeIndex) GetDimension() uint32 { return uint32(idx.dim) }
func (idx *IVFHNSWCompositeIndex) SetParallelSearchConfig(c types.ParallelSearchConfig) {}
func (idx *IVFHNSWCompositeIndex) GetParallelSearchConfig() types.ParallelSearchConfig { return types.ParallelSearchConfig{} }
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

func (idx *IVFHNSWCompositeIndex) Save(path string) error {
	data, err := idx.ExportState()
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func (idx *IVFHNSWCompositeIndex) Load(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return idx.ImportState(data)
}

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

func (idx *IVFHNSWCompositeIndex) ApplyDelta(d *types.DeltaSync) error {
	// For 0.1.9, we'll implement a basic version that adds vectors from delta
	// This is a placeholder for the full anti-entropy logic
	return nil
}

func (idx *IVFHNSWCompositeIndex) GetPQEncoder() *pq.PQEncoder {
	return idx.opqEncoder.PQEncoder
}

func (idx *IVFHNSWCompositeIndex) GetNeighbors(ctx context.Context, id types.VectorID, k int) ([]types.SearchResult, error) {
	// Not directly supported on the composite index (usually needs the full vector)
	return nil, fmt.Errorf("GetNeighbors not supported for IVF-HNSW")
}
