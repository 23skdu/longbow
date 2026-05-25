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
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
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
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.Nop()
	mem := memory.NewGoAllocator()
	s := NewVectorStore(mem, logger, 1024*1024, 0, 0)
	defer s.Close()

	s.getOrCreateDataset("test-1", func() *Dataset {
		ds := NewDataset("test-1", arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Uint32}}, nil))
		ds.SizeBytes.Store(100)
		ds.IsReady.Store(true)
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
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
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
	ds.Records = NewLockFreeSliceFrom([]arrow.RecordBatch{rec})

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
func (m *mockVectorIndex) Len() int { return 0 }
func (m *mockVectorIndex) Size() int { return 0 }
func (m *mockVectorIndex) Close() error { return nil }
func (m *mockVectorIndex) Warmup() int { return 0 }

func TestDoGetSchemaAllocations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
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

	ds := NewDataset("test-alloc-map", schema)

	// Populate locations
	locations := make(map[uint32]Location)
	results := make([]SearchResult, 100)
	for i := uint32(0); i < 100; i++ {
		locations[i+1] = Location{BatchIdx: 0, RowIdx: int(i)}
		results[i] = SearchResult{ID: VectorID(i + 1), Distance: float32(i) * 0.01}
	}

	ds.Index = &mockVectorIndex{
		locations: locations,
	}

	// Append record batch
	builder := array.NewRecordBuilder(mem, schema)
	ids := make([]uint32, 100)
	metas := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		ids[i] = uint32(1000 + i)
		metas[i] = []byte("meta")
	}
	builder.Field(0).(*array.Uint32Builder).AppendValues(ids, nil)
	builder.Field(1).(*array.BinaryBuilder).AppendValues(metas, nil)
	rec := builder.NewRecordBatch()
	ds.Records = NewLockFreeSliceFrom([]arrow.RecordBatch{rec})

	// Measure allocations per run
	allocs := testing.AllocsPerRun(10, func() {
		mapped := s.MapInternalToUserIDs(ds, results)
		assert.Len(t, mapped, 100)
	})

	// With the caching optimization, the allocations per run are limited to:
	// 1. One allocation for the output slice mappedResults.
	// 2. 100 allocations for boxing Location (struct to any/interface{} conversion in GetLocation).
	// 3. 100 allocations for deep-copying metadata.
	// So total allocations is 201 (plus minor runtime overhead, <= 205).
	// Before caching the column indexes outside the loop, rec.Schema().Fields() was called
	// twice per loop, producing an extra 200 allocations (total 401+ allocations).
	// This confirms the schema field allocation overhead is completely eliminated!
	assert.LessOrEqual(t, allocs, float64(205), "MapInternalToUserIDs is allocating too much memory")
}
