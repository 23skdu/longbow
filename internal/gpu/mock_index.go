package gpu

import (
	"fmt"
	"sort"
	"sync"
	"unsafe"
)

// MockIndex implements the Index interface for testing purposes.
// It uses a simple in-memory map to store vectors and performs linear search.
type MockIndex struct {
	mu        sync.RWMutex
	vectors   map[int64][]float32
	dimension int
	deviceID  int
	backend   GPUBackend
	closed    bool
}

func NewMockIndex(cfg GPUConfig, backend GPUBackend) Index {
	return &MockIndex{
		vectors:   make(map[int64][]float32),
		dimension: cfg.Dimension,
		deviceID:  cfg.DeviceID,
		backend:   backend,
	}
}

func (m *MockIndex) Add(ids []int64, vectors []float32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return fmt.Errorf("index closed")
	}

	if len(ids) == 0 {
		return nil
	}

	vectorsPerID := len(vectors) / len(ids)
	if vectorsPerID != m.dimension {
		return fmt.Errorf("dimension mismatch: expected %d, got %d", m.dimension, vectorsPerID)
	}

	for idx, id := range ids {
		start := idx * vectorsPerID
		end := start + vectorsPerID
		vec := make([]float32, vectorsPerID)
		copy(vec, vectors[start:end])
		m.vectors[id] = vec
	}

	return nil
}

func (m *MockIndex) AddPQ(ids []int64, codes []byte, m_val int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return fmt.Errorf("index closed")
	}
	return nil
}

func (m *MockIndex) Search(vector []float32, k int) ([]int64, []float32, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, nil, fmt.Errorf("index closed")
	}

	if len(m.vectors) == 0 {
		return []int64{}, []float32{}, nil
	}

	type result struct {
		id   int64
		dist float32
	}

	results := make([]result, 0, len(m.vectors))
	for id, storedVec := range m.vectors {
		dist := m.euclideanDistance(vector, storedVec)
		results = append(results, result{id: id, dist: dist})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].dist < results[j].dist
	})

	if k > len(results) {
		k = len(results)
	}

	ids := make([]int64, k)
	distances := make([]float32, k)
	for i := 0; i < k; i++ {
		ids[i] = results[i].id
		distances[i] = results[i].dist
	}

	return ids, distances, nil
}

func (m *MockIndex) SearchBatch(vectors [][]float32, k int) ([][]int64, [][]float32, error) {
	batchIDs := make([][]int64, len(vectors))
	batchDists := make([][]float32, len(vectors))

	for i, vec := range vectors {
		ids, dists, err := m.Search(vec, k)
		if err != nil {
			return nil, nil, err
		}
		batchIDs[i] = ids
		batchDists[i] = dists
	}

	return batchIDs, batchDists, nil
}

func (m *MockIndex) SearchPQ(lookupTable []float32, m_val int, k int) ([]int64, []float32, error) {
	// Simple fallback for mock
	return nil, nil, fmt.Errorf("SearchPQ not implemented in MockIndex")
}

func (m *MockIndex) TrainPQ(vectors []float32, m_val int, k int) error {
	return nil
}

func (m *MockIndex) EncodePQ(vectors []float32) ([]byte, error) {
	return nil, nil
}

func (m *MockIndex) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.vectors = nil
	return nil
}

func (m *MockIndex) Backend() GPUBackend {
	return m.backend
}

func (m *MockIndex) DeviceID() int {
	return m.deviceID
}

func (m *MockIndex) GetDeviceInfo() (*GPUInfo, error) {
	return &GPUInfo{
		Backend:  m.backend,
		Name:     "Mock GPU",
		MemoryMB: 16384,
		DeviceID: m.deviceID,
	}, nil
}

func (m *MockIndex) GetMemoryInfo() (total, free, used int64, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	usedMem := int64(len(m.vectors) * m.dimension * 4)
	return 16384 * 1024 * 1024, 16384*1024*1024 - usedMem, usedMem, nil
}

func (m *MockIndex) GetUtilization() (float32, error) {
	return 0.1, nil
}

func (m *MockIndex) euclideanDistance(a, b []float32) float32 {
	var sum float32
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return sum
}

func (m *MockIndex) SearchFloat16(vector []uint16, k int) ([]int64, []float32, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, nil, fmt.Errorf("index closed")
	}

	// Convert float16 (uint16) to float32
	f32 := make([]float32, len(vector))
	for idx, v := range vector {
		f32[idx] = float16ToFloat32Mock(v)
	}
	return m.Search(f32, k)
}

func (m *MockIndex) SearchComplex64(vector []uint16, k int) ([]int64, []float32, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, nil, fmt.Errorf("index closed")
	}

	// complex64 is 2 x float32, stored as uint16 pairs - convert to float32
	f32 := make([]float32, len(vector))
	for idx, v := range vector {
		f32[idx] = float16ToFloat32Mock(v)
	}
	return m.Search(f32, k)
}

func (m *MockIndex) SearchComplex128(vector []float32, k int) ([]int64, []float32, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, nil, fmt.Errorf("index closed")
	}

	// complex128 is 2 x float64, stored as float32 pairs - search directly
	return m.Search(vector, k)
}

func (m *MockIndex) AddTurboQuant(ids []int64, tqData []byte, bitsPerAngle int) error {
	return nil
}

func (m *MockIndex) SearchTurboQuant(vector []float32, k int, bitsPerAngle int) ([]int64, []float32, error) {
	return nil, nil, nil
}

func (m *MockIndex) AssignToClusters(vectors []float32, centroids []float32) ([]uint32, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, fmt.Errorf("index closed")
	}

	if len(centroids) == 0 || len(vectors) == 0 {
		return nil, nil
	}

	dim := m.dimension
	numVectors := len(vectors) / dim
	numCentroids := len(centroids) / dim
	assignments := make([]uint32, numVectors)

	for v := 0; v < numVectors; v++ {
		vec := vectors[v*dim : (v+1)*dim]
		bestC := uint32(0)
		minDist := float32(3.40282346638528859811704183484516925440e+38) // MaxFloat32

		for c := 0; c < numCentroids; c++ {
			cent := centroids[c*dim : (c+1)*dim]
			dist := m.euclideanDistance(vec, cent)
			if dist < minDist {
				minDist = dist
				bestC = uint32(c)
			}
		}
		assignments[v] = bestC
	}

	return assignments, nil
}

func (m *MockIndex) UpdateGraph(offsets []uint32, neighbors []uint32, weights []float32) error {
	return nil // Mock success
}

func (m *MockIndex) GraphExpand(seeds []uint32, depth int, alpha float32) ([]uint32, []float32, error) {
	// Simple mock return: the seeds themselves
	return seeds, make([]float32, len(seeds)), nil
}

// float16ToFloat32Mock converts a uint16 float16 value to float32
func float16ToFloat32Mock(v uint16) float32 {
	sign := uint32(v >> 15)
	exp := uint32((v >> 10) & 0x1F)
	mant := uint32(v & 0x3FF)

	if exp == 0 {
		if mant == 0 {
			return float32FromBitsMock(sign << 31)
		}
		return float32FromBitsMock((sign << 31) | (mant << 13))
	}
	if exp == 31 {
		return float32FromBitsMock((sign << 31) | (0xFF << 23) | (mant << 13))
	}

	newExp := (exp - 15 + 127) << 23
	return float32FromBitsMock((sign << 31) | newExp | (mant << 13))
}

// float32FromBitsMock converts uint32 bits to float32
func float32FromBitsMock(bits uint32) float32 {
	return *(*float32)(unsafe.Pointer(&bits))
}
