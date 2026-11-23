package hnsw

import (
	"strconv"
	"sync"

	topcore "github.com/23skdu/longbow/internal/core"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/rs/zerolog"
)

// MockDataset implements types.IndexDataProvider for testing
type MockDataset struct {
	mu         sync.RWMutex
	Records    []arrow.RecordBatch
	Name       string
	Schema     *arrow.Schema
	PQEncoder  *pq.PQEncoder
	Index      any
	BM25Index  any
	Graph      any
	Tombstones map[int]*types.Bitset
}

// Close runs the close operation.
func (m *MockDataset) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Index != nil {
		if idx, ok := m.Index.(interface{ Close() error }); ok {
			_ = idx.Close()
		}
		m.Index = nil
	}
	m.BM25Index = nil
	m.Graph = nil
}

// GetName returns the name.
func (m *MockDataset) GetName() string { return m.Name }

// GetRecords returns the records.
func (m *MockDataset) GetRecords() []arrow.RecordBatch {
	m.mu.RLock()
	defer m.mu.RUnlock()
	recs := make([]arrow.RecordBatch, len(m.Records))
	copy(recs, m.Records)
	return recs
}

// GetSchema returns the schema.
func (m *MockDataset) GetSchema() *arrow.Schema { return m.Schema }

// GetTombstones returns the tombstones.
func (m *MockDataset) GetTombstones() map[int]*types.Bitset { return m.Tombstones }

// GetPQEncoder returns the pq encoder.
func (m *MockDataset) GetPQEncoder() *pq.PQEncoder { return m.PQEncoder }

// RLockData runs the r lock data operation.
func (m *MockDataset) RLockData() { m.mu.RLock() }

// RUnlockData runs the r unlock data operation.
func (m *MockDataset) RUnlockData() { m.mu.RUnlock() }

// Lock runs the lock operation.
func (m *MockDataset) Lock() { m.mu.Lock() }

// Unlock runs the unlock operation.
func (m *MockDataset) Unlock() { m.mu.Unlock() }

// GenerateFilterBitset runs the generate filter bitset operation.
func (m *MockDataset) GenerateFilterBitset(filters []query.Filter, expr types.FilterExpr) (*types.Bitset, error) {
	if len(filters) > 0 && len(m.Records) > 0 {
		for _, f := range filters {
			if f.Field == "id_col" {
				bs := types.NewBitset()
				colIdx := m.Schema.FieldIndices("id_col")
				if len(colIdx) > 0 {
					col := m.Records[0].Column(colIdx[0]).(*array.Int64)
					targetVal, _ := strconv.ParseInt(f.Value, 10, 64)
					for i := 0; i < int(m.Records[0].NumRows()); i++ {
						if col.Value(i) == targetVal {
							bs.Set(i)
						}
					}
					return bs, nil
				}
			}
		}
	}
	return nil, nil
}

// ResetTombstones runs the reset tombstones operation.
func (m *MockDataset) ResetTombstones() {}

// GetIndex returns the index.
func (m *MockDataset) GetIndex() any { return nil }

// NewMockDataset constructs a new MockDataset.
func NewMockDataset(name string, schema *arrow.Schema) *MockDataset {
	return &MockDataset{
		Name:    name,
		Records: make([]arrow.RecordBatch, 0),
		Schema:  schema,
	}
}

// GetMetric returns the metric.
func (m *MockDataset) GetMetric() topcore.DistanceMetric {
	return topcore.MetricEuclidean
}

// GetLogger returns the logger.
func (m *MockDataset) GetLogger() zerolog.Logger {
	return zerolog.Nop()
}

// GetTopo returns the topo.
func (m *MockDataset) GetTopo() *memory.NUMATopology {
	return nil
}

// TurboQuantBits runs the turbo quant bits operation.
func (m *MockDataset) TurboQuantBits() int { return 8 }

// GetEvictionManager returns the eviction manager.
func (m *MockDataset) GetEvictionManager() any { return nil }

// GetDiskStore returns the disk store.
func (m *MockDataset) GetDiskStore() any { return nil }
