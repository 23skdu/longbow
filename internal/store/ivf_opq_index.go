package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	gputypes "github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"
	"io"
)

// IVFOPQConfig holds configuration for IVF-OPQ index
type IVFOPQConfig struct {
	Nlist         int // Number of clusters
	M             int // Number of PQ subvectors
	K             int // PQ centroids per subspace (default 256)
	Nprobe        int // Clusters to search
	OPQIterations int // Number of OPQ training iterations

	// Coarse quantizer options
	UseHNSWCoarse bool
	HNSWConfig    *ArrowHNSWConfig
	GPUEnabled    bool
	GPUConfig     *gputypes.GPUConfig
}

// IVFOPQIndex implements a billion-scale optimized IVF-OPQ index
type IVFOPQIndex struct {
	config IVFOPQConfig
	dim    int

	coarseCentroids []float32
	coarseHNSW      PluggableVectorIndex // Optional HNSW coarse quantizer
	gpuIndex        gputypes.Index       // Optional GPU acceleration
	opqEncoder      *pq.OPQEncoder
	clusters        []IVFCluster

	nextID uint32
	mu     sync.RWMutex
}

// NewIVFOPQIndex creates a new IVF-OPQ index
func NewIVFOPQIndex(dim int, config IVFOPQConfig) (*IVFOPQIndex, error) {
	if dim <= 0 {
		return nil, errors.New("invalid dimension")
	}
	if config.Nlist <= 0 {
		config.Nlist = 1024
	}
	if config.M <= 0 {
		config.M = dim / 2
		if config.M == 0 {
			config.M = 1
		}
	}
	for dim%config.M != 0 {
		config.M--
	} // Ensure divisibility
	if config.K <= 0 {
		config.K = 256
	}
	if config.OPQIterations <= 0 {
		config.OPQIterations = 10
	}

	opq, err := pq.NewOPQEncoder(dim, config.M, config.K)
	if err != nil {
		return nil, err
	}

	idx := &IVFOPQIndex{
		config:     config,
		dim:        dim,
		opqEncoder: opq,
		clusters:   make([]IVFCluster, config.Nlist),
	}

	return idx, nil
}

// Train builds the coarse quantizer and trains the OPQ encoder
func (idx *IVFOPQIndex) Train(vectors [][]float32) error {
	if len(vectors) == 0 {
		return errors.New("empty training data")
	}

	start := time.Now()
	n := len(vectors)

	// 1. Initialize GPU if enabled
	if idx.config.GPUEnabled && idx.config.GPUConfig != nil {
		// Try to initialize GPU backend
		backend := idx.config.GPUConfig.Backend
		if backend == gputypes.BackendCPU {
			backend = gputypes.DetectGPUBackend()
		}
		
		// In a real scenario, we'd use a factory to get the backend
		// For now, we'll assume Metal implementation is available if on Mac
		// (Normally this would be handled by VectorStore.InitGPUBackend)
		// We'll skip initialization here and assume gpuIndex is already set if available
	}

	// 2. Train Coarse Quantizer (IVF) using K-Means (GPU-accelerated if possible)
	flatData := make([]float32, n*idx.dim)
	for i, v := range vectors {
		copy(flatData[i*idx.dim:(i+1)*idx.dim], v)
	}

	kmeansOpts := pq.KMeansOptions{
		MaxIter: 20,
	}
	if idx.gpuIndex != nil {
		kmeansOpts.GPUAssigner = func(data []float32, centroids []float32) ([]uint32, error) {
			return idx.gpuIndex.AssignToClusters(data, centroids)
		}
	}

	centroids, err := pq.TrainKMeansWithOptions(flatData, n, idx.dim, idx.config.Nlist, kmeansOpts)
	if err != nil {
		return err
	}
	idx.coarseCentroids = centroids

	// 3. Optional: Build HNSW index on coarse centroids for faster assignment
	if idx.config.UseHNSWCoarse {
		hnswCfg := idx.config.HNSWConfig
		if hnswCfg == nil {
			hnswCfg = &ArrowHNSWConfig{M: 16, EfConstruction: 200}
		}
		
		h, err := createHNSWIndex(IndexConfig{
			Type:       IndexTypeHNSW,
			Dimension:  idx.dim,
			HNSWConfig: hnswCfg,
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
	}

	// 4. Train OPQ Encoder
	if err := idx.opqEncoder.TrainOPQ(vectors, idx.config.OPQIterations); err != nil {
		return err
	}

	metrics.VQTrainingDurationSeconds.WithLabelValues("ivf-opq").Observe(time.Since(start).Seconds())
	return nil
}

// Add adds vectors to the index
func (idx *IVFOPQIndex) Add(ctx context.Context, vectors [][]float32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for _, vec := range vectors {
		// 1. Assign to cluster
		bestCluster := 0
		if idx.coarseHNSW != nil {
			results, err := idx.coarseHNSW.Search(vec, 1)
			if err == nil && len(results) > 0 {
				bestCluster = int(results[0].ID) // #nosec G115
			}
		} else {
			minDist := float32(math.MaxFloat32)
			for c := 0; c < idx.config.Nlist; c++ {
				cent := idx.coarseCentroids[c*idx.dim : (c+1)*idx.dim]
				var dist float32
				for i := 0; i < idx.dim; i++ {
					diff := vec[i] - cent[i]
					dist += diff * diff
				}
				if dist < minDist {
					minDist = dist
					bestCluster = c
				}
			}
		}

		// 2. Encode with OPQ
		code, err := idx.opqEncoder.Encode(vec)
		if err != nil {
			return err
		}

		// 3. Add to inverted list
		idx.clusters[bestCluster].mu.Lock()
		idx.clusters[bestCluster].entries = append(idx.clusters[bestCluster].entries, IVFIndexEntry{
			VectorID: idx.nextID,
			PQCode:   code,
		})
		idx.clusters[bestCluster].mu.Unlock()
		idx.nextID++
	}

	idx.updateLoadBalanceMetric()
	return nil
}

func (idx *IVFOPQIndex) updateLoadBalanceMetric() {
	var maxCount int
	var totalCount int
	for i := range idx.clusters {
		count := len(idx.clusters[i].entries)
		if count > maxCount {
			maxCount = count
		}
		totalCount += count
	}
	if totalCount > 0 {
		avg := float64(totalCount) / float64(len(idx.clusters))
		metrics.IVFLoadBalanceRatio.WithLabelValues("default").Set(float64(maxCount) / avg)
	}
}

// Search implements the VectorIndexer search interface
func (idx *IVFOPQIndex) SearchVectorsWithBitmap(ctx context.Context, q any, k int, filter *roaring.Bitmap, options any) ([]types.SearchResult, error) {
	queryVec, ok := q.([]float32)
	if !ok {
		return nil, errors.New("unsupported query type")
	}

	if idx.coarseCentroids == nil || idx.nextID == 0 {
		return nil, nil
	}

	// 1. Find nearest clusters
	type clusterDist struct {
		id   int
		dist float32
	}
	var dists []clusterDist
	
	if idx.coarseHNSW != nil {
		results, err := idx.coarseHNSW.Search(queryVec, idx.config.Nprobe)
		if err == nil {
			dists = make([]clusterDist, len(results))
			for i, res := range results {
				dists[i] = clusterDist{id: int(res.ID), dist: res.Distance} // #nosec G115
			}
		}
	} 
	
	if dists == nil {
		dists = make([]clusterDist, idx.config.Nlist)
		for c := 0; c < idx.config.Nlist; c++ {
			cent := idx.coarseCentroids[c*idx.dim : (c+1)*idx.dim]
			var d float32
			for i := 0; i < idx.dim; i++ {
				diff := queryVec[i] - cent[i]
				d += diff * diff
			}
			dists[c] = clusterDist{id: c, dist: d}
		}
		sort.Slice(dists, func(i, j int) bool { return dists[i].dist < dists[j].dist })
	}

	nprobe := idx.config.Nprobe
	if nprobe > len(dists) {
		nprobe = len(dists)
	}
	
	// 2. Build ADC table for OPQ (note: query must be rotated by OPQ first)
	// (Actually, the OPQ rotation is part of the Encode process, 
	// but for ADC we need the query in the rotated space)
	rotatedQuery := idx.opqEncoder.RotateVector(queryVec)
	adt, err := idx.opqEncoder.PQEncoder.BuildADCTable(rotatedQuery)
	if err != nil {
		return nil, err
	}

	// 3. Scan clusters
	var candidates []types.SearchResult
	for i := 0; i < nprobe; i++ {
		clusterID := dists[i].id
		cluster := &idx.clusters[clusterID]
		cluster.mu.RLock()
		for _, entry := range cluster.entries {
			if filter != nil && !filter.Contains(entry.VectorID) {
				continue
			}
			// Compute ADC distance
			var dist float32
			for m := 0; m < idx.config.M; m++ {
				dist += adt[m*idx.config.K+int(entry.PQCode[m])]
			}
			candidates = append(candidates, types.SearchResult{
				ID:       types.VectorID(entry.VectorID),
				Distance: dist,
			})
		}
		cluster.mu.RUnlock()
	}

	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Distance < candidates[j].Distance })
	if len(candidates) > k {
		candidates = candidates[:k]
	}

	metrics.IVFClusterSearchTotal.WithLabelValues("default", "opq").Add(float64(nprobe))
	return candidates, nil
}

// Implement required interfaces (stubs)
func (idx *IVFOPQIndex) AddByLocation(ctx context.Context, b, r int) (uint32, error) { return 0, nil }
func (idx *IVFOPQIndex) AddByRecord(ctx context.Context, rec arrow.RecordBatch, r, b int) (uint32, error) { return 0, nil }
func (idx *IVFOPQIndex) Search(ctx context.Context, q any, k int, f any) ([]types.Candidate, error) { return nil, nil }
func (idx *IVFOPQIndex) SearchVectors(ctx context.Context, q any, k int, f []query.Filter, o any) ([]types.SearchResult, error) {
	return idx.SearchVectorsWithBitmap(ctx, q, k, nil, o)
}
func (idx *IVFOPQIndex) Size() int { return int(idx.nextID) }
func (idx *IVFOPQIndex) Len() int { return idx.Size() }
func (idx *IVFOPQIndex) GetEntryPoint() uint32 { return 0 }
func (idx *IVFOPQIndex) GetLocation(id uint32) (any, bool) { return nil, false }
func (idx *IVFOPQIndex) GetVectorID(loc any) (uint32, bool) { return 0, false }
func (idx *IVFOPQIndex) GetDimension() uint32 { return uint32(idx.dim) } // #nosec G115
func (idx *IVFOPQIndex) SetIndexedColumns(cols []string) {}
func (idx *IVFOPQIndex) GetRawNeighbors(id uint32) ([]uint32, error) { return nil, nil }
func (idx *IVFOPQIndex) GetNeighbors(ctx context.Context, id uint32, k int) ([]types.SearchResult, error) { return nil, nil }
func (idx *IVFOPQIndex) PreWarm(s int) {}
func (idx *IVFOPQIndex) Warmup() int { return idx.Size() }
func (idx *IVFOPQIndex) EstimateMemory() int64 { return 0 }
func (idx *IVFOPQIndex) GetPQEncoder() *pq.PQEncoder { return idx.opqEncoder.PQEncoder }
func (idx *IVFOPQIndex) Close() error { return nil }
func (idx *IVFOPQIndex) AddBatch(ctx context.Context, recs []arrow.RecordBatch, rs, bs []int) ([]uint32, error) { return nil, nil }
func (idx *IVFOPQIndex) DeleteBatch(ctx context.Context, ids []uint32) error { return nil }
func (idx *IVFOPQIndex) ExportState() ([]byte, error) { return nil, nil }
func (idx *IVFOPQIndex) ImportState(d []byte) error { return nil }
func (idx *IVFOPQIndex) ExportGraph(w io.Writer) error { return nil }
func (idx *IVFOPQIndex) ImportGraph(r io.Reader) error { return nil }
func (idx *IVFOPQIndex) ExportDelta(v uint64) (*types.DeltaSync, error) { return nil, nil }
func (idx *IVFOPQIndex) ApplyDelta(d *types.DeltaSync) error { return nil }
func (idx *IVFOPQIndex) SetParallelSearchConfig(c types.ParallelSearchConfig) {}
func (idx *IVFOPQIndex) GetParallelSearchConfig() types.ParallelSearchConfig { return types.ParallelSearchConfig{} }
func (idx *IVFOPQIndex) RemapLocations(ctx context.Context, m map[uint32]any) error { return nil }
func (idx *IVFOPQIndex) SearchVectorsInRange(ctx context.Context, q any, t float32, f []query.Filter, o any) ([]types.SearchResult, error) { return nil, nil }
func (idx *IVFOPQIndex) IsSharded() bool { return false }
func (idx *IVFOPQIndex) TrainPQ(v [][]float32) error { return idx.Train(v) }
