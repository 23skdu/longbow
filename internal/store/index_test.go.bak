package store

import (
	"testing"

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

func (m *MockIndex) Add(batchIdx, rowIdx int) error {
	m.AddCalls++
	id := VectorID(len(m.Vectors))
	m.Vectors[id] = Location{BatchIdx: batchIdx, RowIdx: rowIdx}
	return nil
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

func (m *MockIndex) Search(query []float32, k int) []VectorID {
	m.SearchCalls++
	// Mock: return empty results or dummy
	return []VectorID{}
}

func (m *MockIndex) SearchVectors(query []float32, k int) []SearchResult {
	m.SearchCalls++
	return []SearchResult{}
}

func (m *MockIndex) AddByLocation(batchIdx, rowIdx int) error {
	return m.Add(batchIdx, rowIdx)
}

func (m *MockIndex) AddByRecord(rec arrow.RecordBatch, rowIdx, batchIdx int) error {
	return m.Add(batchIdx, rowIdx)
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

func (m *MockIndex) Close() error {
	m.CloseCalls++
	return nil
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
	// Since ds.Index is currently *HNSWIndex, we can't assign it yet.
	// Let's verify our MockIndex satisfies the interface we just defined.
	var _ Index = mockIdx

	t.Log("MockIndex successfully implements Index interface")
}
