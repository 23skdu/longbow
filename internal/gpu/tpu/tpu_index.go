package tpu

import (
	"fmt"
	"sort"
	"sync"

	"github.com/23skdu/longbow/internal/gpu/types"
)

type TPUIndex struct {
	mu      sync.RWMutex
	cfg     types.GPUConfig
	backend *TPUBackend
	vectors map[int64][]float32
	closed  bool
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

func (i *TPUIndex) DeviceID() int {
	return i.cfg.DeviceID
}

func (i *TPUIndex) GetDeviceInfo() (*types.GPUInfo, error) {
	info, err := i.backend.GetDeviceInfo()
	if err == nil {
		info.Name = "[EMULATED] " + info.Name
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
	return fmt.Errorf("AddTurboQuant not implemented for TPUIndex (emulated)")
}

func (i *TPUIndex) SearchTurboQuant(vector []float32, k int, bitsPerAngle int) ([]int64, []float32, error) {
	return nil, nil, fmt.Errorf("SearchTurboQuant not implemented for TPUIndex (emulated)")
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
