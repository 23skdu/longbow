package store

import (
	"testing"

	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
)

// MockIndex is a simple mock implementation of the Index interface for testing.
type MockIndex struct {
	Vectors     map[VectorID]Location
	AddCalls    int
	SearchCalls int
	CloseCalls  int
}

func NewMockIndex() *MockIndex {
	return &MockIndex{
		Vectors: make(map[VectorID]Location),
	}
}

func (m *MockIndex) AddByLocation(batchIdx, rowIdx int) (uint32, error) {
	m.AddCalls++
	id := VectorID(len(m.Vectors))
	m.Vectors[id] = Location{BatchIdx: batchIdx, RowIdx: rowIdx}
	return uint32(id), nil
}

func (m *MockIndex) AddByRecord(rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	m.AddCalls++
	id := VectorID(len(m.Vectors))
	m.Vectors[id] = Location{BatchIdx: batchIdx, RowIdx: rowIdx}
	return uint32(id), nil
}

func (m *MockIndex) AddBatch(recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	ids := make([]uint32, len(recs))
	for i := range recs {
		id, _ := m.AddByRecord(recs[i], rowIdxs[i], batchIdxs[i])
		ids[i] = id
	}
	return ids, nil
}

func (m *MockIndex) SearchByID(id VectorID, k int) []VectorID {
	m.SearchCalls++
	// specialized mock behavior: return self and up to k-1 other vectors
	var results []VectorID
	results = append(results, id)
	for otherID := range m.Vectors {
		if otherID != id && len(results) < k {
			results = append(results, otherID)
		}
	}
	return results
}

func (m *MockIndex) Search(q []float32, k int) []VectorID {
	m.SearchCalls++
	// Mock: return empty results or dummy
	return []VectorID{}
}

func (m *MockIndex) SearchVectors(q []float32, k int, filters []query.Filter) ([]SearchResult, error) {
	m.SearchCalls++
	results := make([]SearchResult, 0, k)
	for i := 0; i < k; i++ {
		results = append(results, SearchResult{ID: VectorID(i), Score: 1.0 - float32(i)*0.01})
	}
	return results, nil
}

func (m *MockIndex) SearchVectorsWithBitmap(q []float32, k int, filter *query.Bitset) []SearchResult {
	m.SearchCalls++
	return []SearchResult{}
}

func (m *MockIndex) SetIndexedColumns(cols []string) {
	// Stub for interface satisfaction
}

func (m *MockIndex) Warmup() int {
	return 0
}

func (m *MockIndex) Len() int {
	return len(m.Vectors)
}

func (m *MockIndex) GetDimension() uint32 {
	return 128 // Mock dimension
}

func (m *MockIndex) GetLocation(id VectorID) (Location, bool) {
	loc, ok := m.Vectors[id]
	return loc, ok
}

func (m *MockIndex) GetNeighbors(id VectorID) ([]VectorID, error) {
	// Mock: return SearchByID results (self + up to 15 others)
	neighbors := m.SearchByID(id, 16)
	// Filter out self
	res := make([]VectorID, 0, len(neighbors))
	for _, n := range neighbors {
		if n != id {
			res = append(res, n)
		}
	}
	return res, nil
}

func (m *MockIndex) Close() error {
	m.CloseCalls++
	return nil
}

func (m *MockIndex) TrainPQ(vectors [][]float32) error {
	return nil
}

func (m *MockIndex) GetPQEncoder() *pq.PQEncoder {
	return nil
}

func (m *MockIndex) EstimateMemory() int64 {
	return 0
}

// TestPluggableIndex verifies that Dataset can work with any implementation of Index.
func TestPluggableIndex(t *testing.T) {
	// 1. Setup Mock Index
	mockIdx := NewMockIndex()

	// 2. Setup Dataset with Mock Index
	// NOTE: This will fail until Dataset.Index type is changed from *HNSWIndex to Index interface
	ds := &Dataset{
		Name:  "test-dataset",
		Index: mockIdx, // This assignment is the goal
	}

	// Verify we can access the index via Dataset
	if ds.Index.Len() != 0 {
		t.Error("expected 0 len")
	}

	// Temporarily comment out assignment to avoid compilation error during TDD setup
	// In a real TDD cycle, we'd uncomment this and let it fail, then fix.
	// For this flow, I'll simulate the "red" phase by checking type compatibility manually or
	// by trying to assign it via a wrapper.

	// We want to verify: ds.Index = mockIdx
	// Since ds.Index is currently VectorIndex, we can assign it.
	// Let's verify our MockIndex satisfies the interface we just defined.
	var _ VectorIndex = mockIdx

	t.Log("MockIndex successfully implements Index interface")
}
