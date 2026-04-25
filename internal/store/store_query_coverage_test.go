package store

import (
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockListFlightsStream struct {
	flight.FlightService_ListFlightsServer
	results []*flight.FlightInfo
}

func (m *mockListFlightsStream) Send(info *flight.FlightInfo) error {
	m.results = append(m.results, info)
	return nil
}

func (m *mockListFlightsStream) Context() context.Context {
	return context.Background()
}

func TestVectorStore_ListFlights(t *testing.T) {
	logger := zerolog.Nop()
	mem := memory.NewGoAllocator()
	s := NewVectorStore(mem, logger, 1024*1024, 0, 0)
	defer s.Close()

	// Create some datasets
	s.getOrCreateDataset("test-1", func() *Dataset {
		return NewDataset("test-1", arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Uint32}}, nil))
	})
	s.getOrCreateDataset("other-2", func() *Dataset {
		return NewDataset("other-2", arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Uint32}}, nil))
	})

	t.Run("NoFilter", func(t *testing.T) {
		stream := &mockListFlightsStream{}
		err := s.ListFlights(nil, stream)
		require.NoError(t, err)
		assert.Len(t, stream.results, 2)
	})

	t.Run("NameFilter_Contains", func(t *testing.T) {
		stream := &mockListFlightsStream{}
		criteria := &flight.Criteria{
			Expression: []byte(`{"filters": [{"field": "name", "operator": "contains", "value": "test"}]}`),
		}
		err := s.ListFlights(criteria, stream)
		require.NoError(t, err)
		assert.Len(t, stream.results, 1)
		assert.Equal(t, "test-1", stream.results[0].FlightDescriptor.Path[0])
	})
}

func TestVectorStore_GetFlightInfo(t *testing.T) {
	logger := zerolog.Nop()
	mem := memory.NewGoAllocator()
	s := NewVectorStore(mem, logger, 1024*1024, 0, 0)
	defer s.Close()

	s.getOrCreateDataset("test-1", func() *Dataset {
		ds := NewDataset("test-1", arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Uint32}}, nil))
		ds.SizeBytes.Store(100)
		return ds
	})

	t.Run("Success", func(t *testing.T) {
		desc := &flight.FlightDescriptor{Path: []string{"test-1"}}
		info, err := s.GetFlightInfo(context.Background(), desc)
		require.NoError(t, err)
		assert.Equal(t, int64(100), info.TotalBytes)
	})

	t.Run("NotFound", func(t *testing.T) {
		desc := &flight.FlightDescriptor{Path: []string{"non-existent"}}
		_, err := s.GetFlightInfo(context.Background(), desc)
		assert.Error(t, err)
	})
}

func TestVectorStore_MapInternalToUserIDs(t *testing.T) {
	logger := zerolog.Nop()
	mem := memory.NewGoAllocator()
	s := NewVectorStore(mem, logger, 1024*1024, 0, 0)
	defer s.Close()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "metadata", Type: arrow.BinaryTypes.Binary},
		},
		nil,
	)

	ds := NewDataset("test-id-map", schema)
	
	// Create mock index
	mockIdx := &mockVectorIndex{
		locations: map[uint32]Location{
			1: {BatchIdx: 0, RowIdx: 0},
			2: {BatchIdx: 0, RowIdx: 1},
		},
	}
	ds.Index = mockIdx

	// Create data
	builder := array.NewRecordBuilder(mem, schema)
	builder.Field(0).(*array.Uint32Builder).AppendValues([]uint32{1001, 1002}, nil)
	builder.Field(1).(*array.BinaryBuilder).AppendValues([][]byte{[]byte("meta1"), []byte("meta2")}, nil)
	rec := builder.NewRecordBatch()
	ds.Records = []arrow.RecordBatch{rec}

	results := []types.SearchResult{
		{ID: 1, Distance: 0.1},
		{ID: 2, Distance: 0.2},
	}

	mapped := s.MapInternalToUserIDs(ds, results)
	require.Len(t, mapped, 2)
	assert.Equal(t, types.VectorID(1001), mapped[0].ID)
	assert.Equal(t, []byte("meta1"), mapped[0].Metadata)
	assert.Equal(t, types.VectorID(1002), mapped[1].ID)
	assert.Equal(t, []byte("meta2"), mapped[1].Metadata)
}

// Minimal mock for VectorIndex
type mockVectorIndex struct {
	types.VectorIndexer
	locations map[uint32]Location
}

func (m *mockVectorIndex) GetLocation(id uint32) (interface{}, bool) {
	loc, ok := m.locations[id]
	return loc, ok
}

func (m *mockVectorIndex) Type() IndexType { return "mock" }
func (m *mockVectorIndex) SearchVectors(ctx context.Context, query any, k int, filters []core.Filter, options any) ([]types.SearchResult, error) {
	return nil, nil
}
func (m *mockVectorIndex) AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	return 0, nil
}
func (m *mockVectorIndex) Warmup() int { return 0 }
