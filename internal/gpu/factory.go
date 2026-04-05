package gpu

import (
	"fmt"
	"sort"
)

// NewIndexWithBackend creates a GPU index with specified backend (delegates to implementation)
func NewIndexWithBackend(cfg GPUConfig, backend GPUBackend) (Index, error) {
	switch backend {
	case BackendCPU, BackendOpenCL:
		return NewCPUIndex(cfg)
	default:
		return newGPUIndexImpl(cfg, backend)
	}
}

// NewIndex creates a GPU index with auto-detected backend (delegates to implementation)
func NewIndex(cfg GPUConfig) (Index, error) {
	if cfg.Backend == BackendCPU || !cfg.Enabled {
		return NewCPUIndex(cfg)
	}

	preferredBackend := DetectGPUBackend()
	return NewIndexWithBackend(cfg, preferredBackend)
}

// CPUIndex implements a CPU-only fallback index using linear scan
type CPUIndex struct {
	vectors   map[int64][]float32
	dimension int
	deviceID  int
}

func NewCPUIndex(cfg GPUConfig) (Index, error) {
	return &CPUIndex{
		vectors:   make(map[int64][]float32),
		dimension: cfg.Dimension,
		deviceID:  cfg.DeviceID,
	}, nil
}

func (i *CPUIndex) Add(ids []int64, vectors []float32) error {
	if len(ids) == 0 {
		return nil
	}

	vectorsPerID := len(vectors) / len(ids)

	for idx, id := range ids {
		start := idx * vectorsPerID
		end := start + vectorsPerID
		if end > len(vectors) {
			end = len(vectors)
		}
		i.vectors[id] = vectors[start:end]
	}

	return nil
}

func (i *CPUIndex) Search(vector []float32, k int) (ids []int64, distances []float32, err error) {
	if len(i.vectors) == 0 {
		return []int64{}, []float32{}, nil
	}

	if k > len(i.vectors) {
		k = len(i.vectors)
	}

	type result struct {
		id       int64
		distance float32
	}

	results := make([]result, 0, len(i.vectors))

	for id, storedVec := range i.vectors {
		dist := euclideanDistance(vector, storedVec)
		results = append(results, result{id: id, distance: dist})
	}

	// Sort by distance (ascending)
	sort.Slice(results, func(a, b int) bool {
		return results[a].distance < results[b].distance
	})

	// Return top k results
	ids = make([]int64, k)
	distances = make([]float32, k)
	for idx := 0; idx < k && idx < len(results); idx++ {
		ids[idx] = results[idx].id
		distances[idx] = results[idx].distance
	}

	return ids, distances, nil
}

// euclideanDistance computes the squared Euclidean distance between two vectors
func euclideanDistance(a, b []float32) float32 {
	var sum float32
	for i := 0; i < len(a) && i < len(b); i++ {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return sum
}

func (i *CPUIndex) Close() error {
	i.vectors = nil
	return nil
}

func (i *CPUIndex) Backend() GPUBackend {
	return BackendCPU
}

func (i *CPUIndex) DeviceID() int {
	return i.deviceID
}

func (i *CPUIndex) GetDeviceInfo() (*GPUInfo, error) {
	return &GPUInfo{
		Backend:  BackendCPU,
		Name:     "CPU",
		MemoryMB: 0,
	}, nil
}

func (i *CPUIndex) GetMemoryInfo() (total, free, used int64, err error) {
	var totalMem int64
	for _, vec := range i.vectors {
		totalMem += int64(len(vec) * 4)
	}
	return totalMem, 0, totalMem, nil
}

func (i *CPUIndex) GetUtilization() (float32, error) {
	return 0, nil
}

func (i *CPUIndex) SearchBatch(vectors [][]float32, k int) ([][]int64, [][]float32, error) {
	if len(vectors) == 0 {
		return nil, nil, nil
	}

	results := make([][]int64, len(vectors))
	distances := make([][]float32, len(vectors))

	for idx, vec := range vectors {
		ids, dist, err := i.Search(vec, k)
		if err != nil {
			return nil, nil, fmt.Errorf("batch search[%d]: %w", idx, err)
		}
		results[idx] = ids
		distances[idx] = dist
	}

	return results, distances, nil
}
