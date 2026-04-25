package gpu

import (
	"fmt"
	"sort"
	"unsafe"
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

func (i *CPUIndex) AddPQ(ids []int64, codes []byte, m int) error {
	return fmt.Errorf("AddPQ not supported on CPUIndex")
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


func (i *CPUIndex) SearchPQ(lookupTable []float32, m int, k int) (ids []int64, distances []float32, err error) {
	// Fallback implementation for CPU
	return nil, nil, fmt.Errorf("SearchPQ not implemented for CPUIndex (interface only)")
}

func (i *CPUIndex) TrainPQ(vectors []float32, m int, k int) error {
	return fmt.Errorf("TrainPQ not implemented for CPUIndex")
}

func (i *CPUIndex) EncodePQ(vectors []float32) ([]byte, error) {
	return nil, fmt.Errorf("EncodePQ not implemented for CPUIndex")
}

func (i *CPUIndex) SearchFloat16(vector []uint16, k int) ([]int64, []float32, error) {
	// Convert float16 (uint16) to float32
	f32 := make([]float32, len(vector))
	for idx, v := range vector {
		f32[idx] = float16ToFloat32(v)
	}
	return i.Search(f32, k)
}

func (i *CPUIndex) SearchComplex64(vector []uint16, k int) ([]int64, []float32, error) {
	// complex64 is 2 x float32, stored as uint16 pairs (for GPU compatibility)
	// Convert to float32 vector (2 * len)
	f32 := make([]float32, len(vector))
	for idx, v := range vector {
		f32[idx] = float16ToFloat32(v)
	}
	return i.Search(f32, k)
}

func (i *CPUIndex) SearchComplex128(vector []float32, k int) ([]int64, []float32, error) {
	// complex128 is 2 x float64, but stored as float32 pairs in our format
	// Search using the float32 representation directly
	return i.Search(vector, k)
}

func (i *CPUIndex) AddTurboQuant(ids []int64, tqData []byte, bitsPerAngle int) error {
	return fmt.Errorf("AddTurboQuant not implemented for CPUIndex")
}

func (i *CPUIndex) SearchTurboQuant(vector []float32, k int, bitsPerAngle int) ([]int64, []float32, error) {
	return nil, nil, fmt.Errorf("SearchTurboQuant not implemented for CPUIndex")
}

func (i *CPUIndex) AssignToClusters(vectors []float32, centroids []float32) ([]uint32, error) {
	if len(centroids) == 0 || len(vectors) == 0 {
		return nil, nil
	}

	dim := i.dimension
	numVectors := len(vectors) / dim
	numCentroids := len(centroids) / dim
	assignments := make([]uint32, numVectors)

	for v := 0; v < numVectors; v++ {
		vec := vectors[v*dim : (v+1)*dim]
		bestC := uint32(0)
		minDist := float32(3.40282346638528859811704183484516925440e+38) // MaxFloat32

		for c := 0; c < numCentroids; c++ {
			cent := centroids[c*dim : (c+1)*dim]
			dist := euclideanDistance(vec, cent)
			if dist < minDist {
				minDist = dist
				bestC = uint32(c)
			}
		}
		assignments[v] = bestC
	}

	return assignments, nil
}

func (i *CPUIndex) UpdateGraph(offsets []uint32, neighbors []uint32, weights []float32) error {
	return fmt.Errorf("UpdateGraph not supported on CPUIndex")
}

func (i *CPUIndex) GraphExpand(seeds []uint32, depth int, alpha float32) ([]uint32, []float32, error) {
	return nil, nil, fmt.Errorf("GraphExpand not supported on CPUIndex")
}

// float16ToFloat32 ...
func float16ToFloat32(v uint16) float32 {
	// Extract float16 components
	sign := uint32(v >> 15)
	exp := uint32((v >> 10) & 0x1F)
	mant := uint32(v & 0x3FF)

	// Handle special cases
	if exp == 0 {
		if mant == 0 {
			// Zero
			return float32FromBits(sign << 31)
		}
		// Subnormal
		return float32FromBits((sign << 31) | (mant << 13))
	}
	if exp == 31 {
		// Infinity or NaN
		return float32FromBits((sign << 31) | (0xFF << 23) | (mant << 13))
	}

	// Normalized: convert exponent from 5-bit bias-15 to 8-bit bias-127
	newExp := (exp - 15 + 127) << 23
	return float32FromBits((sign << 31) | newExp | (mant << 13))
}

// float32FromBits converts uint32 bits to float32
func float32FromBits(bits uint32) float32 {
	return *(*float32)(unsafe.Pointer(&bits))
}
