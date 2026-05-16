package tpu

import (
	"fmt"
	"sort"
	"sync"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/simd"
)

type TPUIndex struct {
	mu      sync.RWMutex
	cfg     types.GPUConfig
	backend *TPUBackend
	vectors map[int64][]float32
	tqCodes map[int64][]byte
	closed  bool
	
	// Graph data
	graphOffsets   []uint32
	graphNeighbors []uint32
	graphWeights   []float32
}

func NewTPUIndexImpl(cfg types.GPUConfig) (types.Index, error) {
	backend, err := NewTPUBackend(cfg.DeviceID)
	if err != nil {
		return nil, err
	}
	if err := backend.Initialize(); err != nil {
		return nil, err
	}
	return &TPUIndex{
		cfg:     cfg,
		backend: backend,
		vectors: make(map[int64][]float32),
		tqCodes: make(map[int64][]byte),
	}, nil
}

func (i *TPUIndex) Add(ids []int64, vectors []float32) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.closed {
		return fmt.Errorf("index closed")
	}

	dim := i.cfg.Dimension
	for idx, id := range ids {
		vec := make([]float32, dim)
		copy(vec, vectors[idx*dim:(idx+1)*dim])
		i.vectors[id] = vec
	}

	return nil
}

func (i *TPUIndex) Search(vector []float32, k int) ([]int64, []float32, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.closed {
		return nil, nil, fmt.Errorf("index closed")
	}

	if len(i.vectors) == 0 {
		return []int64{}, []float32{}, nil
	}

	// 1. Allocate and transfer query to HBM
	querySize := int64(len(vector) * 4)
	queryPtr, err := i.backend.hbm.Allocate(i.cfg.DeviceID, querySize)
	if err != nil {
		return nil, nil, err
	}
	defer i.backend.hbm.Free(queryPtr)
	if err := tpuMemcpyH2D(queryPtr, vector); err != nil {
		return nil, nil, err
	}

	// 2. Allocate results buffer on device
	// Each result is {float32 dist, int64 id} = 12 bytes
	resSize := int64(k * 12)
	resPtr, err := i.backend.hbm.Allocate(i.cfg.DeviceID, resSize)
	if err != nil {
		return nil, nil, err
	}
	defer i.backend.hbm.Free(resPtr)

	// 3. Launch XLA kernel
	// In a real implementation, we would pass a handle to the stored vectors as well.
	// For the stub, we pass the queryPtr and resPtr.
	if err := tpuLaunchXLA(i.cfg.DeviceID, "l2_search", []unsafe.Pointer{queryPtr, resPtr}); err != nil {
		return nil, nil, fmt.Errorf("TPU XLA kernel dispatch failed: %w", err)
	}

	// 4. Retrieve results (simulated)
	// We call tpuMemcpyD2H to simulate the retrieval, though for the stub
	// we'll still do the sort on CPU to provide deterministic mock results.
	mockResultBuffer := make([]float32, k*3) // Simulate k * {f32, i64}
	if err := tpuMemcpyD2H(mockResultBuffer, resPtr); err != nil {
		return nil, nil, fmt.Errorf("TPU D2H copy failed: %w", err)
	}
	type result struct {
		id   int64
		dist float32
	}
	results := make([]result, 0, len(i.vectors))

	for id, storedVec := range i.vectors {
		var dist float32
		for j := 0; j < len(vector) && j < len(storedVec); j++ {
			diff := vector[j] - storedVec[j]
			dist += diff * diff
		}
		results = append(results, result{id: id, dist: dist})
	}

	sort.Slice(results, func(a, b int) bool {
		return results[a].dist < results[b].dist
	})

	if k > len(results) {
		k = len(results)
	}

	resIDs := make([]int64, k)
	resDists := make([]float32, k)
	for idx := 0; idx < k; idx++ {
		resIDs[idx] = results[idx].id
		resDists[idx] = results[idx].dist
	}

	return resIDs, resDists, nil
}

func (i *TPUIndex) SearchBatch(vectors [][]float32, k int) ([][]int64, [][]float32, error) {
	resIDs := make([][]int64, len(vectors))
	resDists := make([][]float32, len(vectors))

	for idx, vec := range vectors {
		ids, dists, err := i.Search(vec, k)
		if err != nil {
			return nil, nil, err
		}
		resIDs[idx] = ids
		resDists[idx] = dists
	}

	return resIDs, resDists, nil
}

func (i *TPUIndex) AddPQ(ids []int64, codes []byte, m int) error {
	return fmt.Errorf("AddPQ not implemented for TPUIndex (experimental stub)")
}

func (i *TPUIndex) SearchPQ(lookupTable []float32, m int, k int) ([]int64, []float32, error) {
	return nil, nil, fmt.Errorf("SearchPQ not implemented for TPUIndex (emulated)")
}

func (i *TPUIndex) TrainPQ(vectors []float32, m int, k int) error {
	return fmt.Errorf("TrainPQ not implemented for TPUIndex (emulated)")
}

func (i *TPUIndex) EncodePQ(vectors []float32) ([]byte, error) {
	return nil, fmt.Errorf("EncodePQ not implemented for TPUIndex (emulated)")
}

func (i *TPUIndex) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.closed = true
	i.vectors = nil
	return nil
}

func (i *TPUIndex) Backend() types.GPUBackend {
	return types.BackendTPU
}

func (i *TPUIndex) DeviceID() int32 {
	return i.cfg.DeviceID
}

func (i *TPUIndex) GetDeviceInfo() (*types.GPUInfo, error) {
	info, err := i.backend.GetDeviceInfo()
	if err == nil {
		info.Name = "[EXPERIMENTAL STUB] " + info.Name
	}
	return info, err
}

func (i *TPUIndex) GetMemoryInfo() (total, free, used int64, err error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	usedMem := int64(len(i.vectors) * i.cfg.Dimension * 4)
	return i.backend.hbm.total, i.backend.hbm.total - usedMem, usedMem, nil
}

func (i *TPUIndex) GetUtilization() (float32, error) {
	return 0, nil
}

func (i *TPUIndex) SearchFloat16(vector []uint16, k int) ([]int64, []float32, error) {
	return nil, nil, fmt.Errorf("SearchFloat16 not implemented for TPUIndex (emulated)")
}

func (i *TPUIndex) SearchComplex64(vector []uint16, k int) ([]int64, []float32, error) {
	return nil, nil, fmt.Errorf("SearchComplex64 not implemented for TPUIndex (emulated)")
}

func (i *TPUIndex) SearchComplex128(vector []float32, k int) ([]int64, []float32, error) {
	return nil, nil, fmt.Errorf("SearchComplex128 not implemented for TPUIndex (emulated)")
}

func (i *TPUIndex) AddTurboQuant(ids []int64, tqData []byte, bitsPerAngle int) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.closed {
		return fmt.Errorf("index closed")
	}

	// For the stub, we store TQ data in memory
	tqSize := len(tqData) / len(ids)
	for idx, id := range ids {
		data := make([]byte, tqSize)
		copy(data, tqData[idx*tqSize:(idx+1)*tqSize])
		i.tqCodes[id] = data
	}

	return nil
}

func (i *TPUIndex) SearchTurboQuant(vector []float32, k int, bitsPerAngle int) ([]int64, []float32, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.closed {
		return nil, nil, fmt.Errorf("index closed")
	}

	if len(i.tqCodes) == 0 {
		return []int64{}, []float32{}, nil
	}

	// For the TPU stub, we fallback to the Go SIMD TQ implementation
	type result struct {
		id       int64
		distance float32
	}
	results := make([]result, 0, len(i.tqCodes))

	tqFunc := simd.GetTurboQuantDistanceFunc()
	pow2 := i.cfg.Dimension

	for id, tqData := range i.tqCodes {
		dist, err := tqFunc(vector, tqData, i.cfg.Dimension, pow2, bitsPerAngle)
		if err != nil {
			continue
		}
		results = append(results, result{id: id, distance: dist})
	}

	sort.Slice(results, func(a, b int) bool {
		return results[a].distance < results[b].distance
	})

	if k > len(results) {
		k = len(results)
	}

	resIDs := make([]int64, k)
	resDists := make([]float32, k)
	for idx := 0; idx < k; idx++ {
		resIDs[idx] = results[idx].id
		resDists[idx] = results[idx].distance
	}

	return resIDs, resDists, nil
}

func (i *TPUIndex) AssignToClusters(vectors []float32, centroids []float32) ([]uint32, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.closed {
		return nil, fmt.Errorf("index closed")
	}

	if len(centroids) == 0 || len(vectors) == 0 {
		return nil, nil
	}

	dim := i.cfg.Dimension
	numVectors := len(vectors) / dim
	numCentroids := len(centroids) / dim
	assignments := make([]uint32, numVectors)

	for v := 0; v < numVectors; v++ {
		vec := vectors[v*dim : (v+1)*dim]
		bestC := uint32(0)
		minDist := float32(3.40282346638528859811704183484516925440e+38) // MaxFloat32

		for c := 0; c < numCentroids; c++ {
			cent := centroids[c*dim : (c+1)*dim]
			var dist float32
			for j := 0; j < dim; j++ {
				diff := vec[j] - cent[j]
				dist += diff * diff
			}
			if dist < minDist {
				minDist = dist
				bestC = uint32(c)
			}
		}
		assignments[v] = bestC
	}

	return assignments, nil
}

func (i *TPUIndex) UpdateGraph(offsets []uint32, neighbors []uint32, weights []float32) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.closed {
		return fmt.Errorf("index closed")
	}

	i.graphOffsets = make([]uint32, len(offsets))
	copy(i.graphOffsets, offsets)
	
	i.graphNeighbors = make([]uint32, len(neighbors))
	copy(i.graphNeighbors, neighbors)
	
	if len(weights) > 0 {
		i.graphWeights = make([]float32, len(weights))
		copy(i.graphWeights, weights)
	}

	return nil
}

func (i *TPUIndex) GraphExpand(seeds []uint32, depth int, alpha float32) ([]uint32, []float32, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.closed {
		return nil, nil, fmt.Errorf("index closed")
	}

	if len(i.graphOffsets) == 0 {
		return nil, nil, fmt.Errorf("graph not initialized on TPU")
	}

	// Simple BFS expansion (simulated)
	visited := make(map[uint32]float32)
	for _, seed := range seeds {
		visited[seed] = 1.0
	}

	currentFrontier := seeds
	for d := 0; d < depth; d++ {
		var nextFrontier []uint32
		for _, nodeID := range currentFrontier {
			if int(nodeID+1) >= len(i.graphOffsets) {
				continue
			}
			start := i.graphOffsets[nodeID]
			end := i.graphOffsets[nodeID+1]
			
			for neighborIdx := start; neighborIdx < end; neighborIdx++ {
				neighbor := i.graphNeighbors[neighborIdx]
				if _, seen := visited[neighbor]; !seen {
					score := visited[nodeID] * alpha
					visited[neighbor] = score
					nextFrontier = append(nextFrontier, neighbor)
				}
			}
		}
		if len(nextFrontier) == 0 {
			break
		}
		currentFrontier = nextFrontier
	}

	outIDs := make([]uint32, 0, len(visited))
	outScores := make([]float32, 0, len(visited))
	for id, score := range visited {
		outIDs = append(outIDs, id)
		outScores = append(outScores, score)
	}

	return outIDs, outScores, nil
}

func (i *TPUIndex) HaversineSearch(centerLat, centerLon float32, points []float32, earthRadius float32) ([]float32, error) {
	return nil, fmt.Errorf("HaversineSearch not implemented for TPUIndex (emulated)")
}

func (i *TPUIndex) NormBatch(vectors []float32, dims int) ([]float32, error) {
	return nil, fmt.Errorf("NormBatch not implemented for TPUIndex (emulated)")
}

func (i *TPUIndex) PruneNeighbors(candidateIds []uint32, candidateDists []float32, maxNeighbors int, allVectors []float32) ([]uint32, error) {
	return nil, fmt.Errorf("PruneNeighbors not implemented for TPUIndex (emulated)")
}
