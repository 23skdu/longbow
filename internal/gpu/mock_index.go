package gpu

import (
	"fmt"
	"sort"
	"sync"
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
