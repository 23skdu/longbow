package core

import (
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/23skdu/longbow/internal/query"
)

// MockDataset implements types.IndexDataProvider for testing
type MockDataset struct {
	Records   []arrow.RecordBatch
	Name      string
	Schema    *arrow.Schema
	PQEncoder *pq.PQEncoder
	Index     any
	BM25Index any
	Graph     any
}

func (m *MockDataset) Close() {
	if m.Index != nil {
		if idx, ok := m.Index.(interface{ Close() error }); ok {
			_ = idx.Close()
		}
		m.Index = nil
	}
	m.BM25Index = nil
	m.Graph = nil
}

func (m *MockDataset) GetName() string { return m.Name }
func (m *MockDataset) GetRecords() []arrow.RecordBatch { return m.Records }
func (m *MockDataset) GetSchema() *arrow.Schema { return m.Schema }
func (m *MockDataset) GetTombstones() map[int]*types.Bitset { return nil }
func (m *MockDataset) GetPQEncoder() *pq.PQEncoder { return m.PQEncoder }
func (m *MockDataset) RLockData() {}
func (m *MockDataset) RUnlockData() {}
func (m *MockDataset) GenerateFilterBitset(filters []query.Filter, expr types.FilterExpr) (*types.Bitset, error) {
	return nil, nil
}
func (m *MockDataset) ResetTombstones() {}
func (m *MockDataset) GetIndex() any { return nil }

func NewMockDataset(name string, schema *arrow.Schema) *MockDataset {
	return &MockDataset{
		Name: name,
		Records: make([]arrow.RecordBatch, 0),
		Schema: schema,
	}
}
