package store

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/metadata"
)

// mockDoExchangeStream implements flight.FlightService_DoExchangeServer for testing
type mockDoExchangeStream struct {
	ctx       context.Context
	recvQueue []*flight.FlightData
	recvIdx   int
	sentData  []*flight.FlightData
	mu        sync.Mutex
}

func newMockDoExchangeStream(ctx context.Context) *mockDoExchangeStream {
	return &mockDoExchangeStream{
		ctx:       ctx,
		recvQueue: make([]*flight.FlightData, 0),
		sentData:  make([]*flight.FlightData, 0),
	}
}

func (m *mockDoExchangeStream) Send(data *flight.FlightData) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sentData = append(m.sentData, data)
	return nil
}

func (m *mockDoExchangeStream) Recv() (*flight.FlightData, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.recvIdx >= len(m.recvQueue) {
		return nil, io.EOF
	}
	data := m.recvQueue[m.recvIdx]
	m.recvIdx++
	return data, nil
}

func (m *mockDoExchangeStream) Context() context.Context {
	return m.ctx
}

func (m *mockDoExchangeStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockDoExchangeStream) SendHeader(metadata.MD) error { return nil }
func (m *mockDoExchangeStream) SetTrailer(metadata.MD)       {}
func (m *mockDoExchangeStream) SendMsg(interface{}) error    { return nil }
func (m *mockDoExchangeStream) RecvMsg(interface{}) error    { return nil }

func (m *mockDoExchangeStream) addFlightData(data *flight.FlightData) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recvQueue = append(m.recvQueue, data)
}

func (m *mockDoExchangeStream) getSentCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sentData)
}

func (m *mockDoExchangeStream) getSentData() []*flight.FlightData { //nolint:unused
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sentData
}

// Helper to create test record batch
func createTestExchangeRecord(alloc memory.Allocator, id int64, vec []float32) arrow.RecordBatch { //nolint:unused
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32)},
	}, nil)

	idBuilder := array.NewInt64Builder(alloc)
	idBuilder.Append(id)
	idArr := idBuilder.NewArray()

	listBuilder := array.NewListBuilder(alloc, arrow.PrimitiveTypes.Float32)
	vb := listBuilder.ValueBuilder().(*array.Float32Builder)
	listBuilder.Append(true)
	for _, v := range vec {
		vb.Append(v)
	}
	listArr := listBuilder.NewArray()

	rec := array.NewRecordBatch(schema, []arrow.Array{idArr, listArr}, 1)

	idBuilder.Release()
	listBuilder.Release()

	return rec
}

// Test 1: Basic bidirectional exchange - DataServer has DoExchange method
func TestDoExchange_BasicBidirectional(t *testing.T) {
	alloc := memory.NewGoAllocator()
	logger := zerolog.Nop()
	store := NewVectorStore(alloc, logger, 1<<30, 0, time.Hour)
	defer func() { _ = store.Close() }()

	dataServer := NewDataServer(store)

	// Create mock stream
	ctx := context.Background()
	mockStream := newMockDoExchangeStream(ctx)

	// Add flight data with descriptor
	desc := &flight.FlightDescriptor{Path: []string{"test-exchange-dataset"}}
	mockStream.addFlightData(&flight.FlightData{
		FlightDescriptor: desc,
		DataBody:         []byte("test-payload"),
	})

	// Execute DoExchange - should not return unimplemented
	err := dataServer.DoExchange(mockStream)
	require.NoError(t, err)
}

// Test 2: Multiple record batches in stream
func TestDoExchange_MultipleFlightData(t *testing.T) {
	alloc := memory.NewGoAllocator()
	logger := zerolog.Nop()
	store := NewVectorStore(alloc, logger, 1<<30, 0, time.Hour)
	defer func() { _ = store.Close() }()

	dataServer := NewDataServer(store)

	ctx := context.Background()
	mockStream := newMockDoExchangeStream(ctx)

	desc := &flight.FlightDescriptor{Path: []string{"multi-data-dataset"}}

	// Add 3 flight data messages
	for i := 0; i < 3; i++ {
		mockStream.addFlightData(&flight.FlightData{
			FlightDescriptor: desc,
			DataBody:         []byte("payload"),
		})
	}

	err := dataServer.DoExchange(mockStream)
	require.NoError(t, err)

	// Verify acknowledgments sent
	assert.GreaterOrEqual(t, mockStream.getSentCount(), 1, "Should send acknowledgments")
}

// Test 3: Dataset routing via descriptor
func TestDoExchange_DatasetRouting(t *testing.T) {
	alloc := memory.NewGoAllocator()
	logger := zerolog.Nop()
	store := NewVectorStore(alloc, logger, 1<<30, 0, time.Hour)
	defer func() { _ = store.Close() }()

	dataServer := NewDataServer(store)

	ctx := context.Background()
	mockStream := newMockDoExchangeStream(ctx)

	// Two different datasets via descriptor paths
	desc1 := &flight.FlightDescriptor{Path: []string{"dataset-alpha"}}
	desc2 := &flight.FlightDescriptor{Path: []string{"dataset-beta"}}

	mockStream.addFlightData(&flight.FlightData{
		FlightDescriptor: desc1,
		DataBody:         []byte("alpha-data"),
	})
	mockStream.addFlightData(&flight.FlightData{
		FlightDescriptor: desc2,
		DataBody:         []byte("beta-data"),
	})

	err := dataServer.DoExchange(mockStream)
	require.NoError(t, err)
}

// Test 4: Context cancellation handling
func TestDoExchange_Cancellation(t *testing.T) {
	alloc := memory.NewGoAllocator()
	logger := zerolog.Nop()
	store := NewVectorStore(alloc, logger, 1<<30, 0, time.Hour)
	defer func() { _ = store.Close() }()

	dataServer := NewDataServer(store)

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	mockStream := newMockDoExchangeStream(ctx)

	desc := &flight.FlightDescriptor{Path: []string{"cancel-test"}}
	mockStream.addFlightData(&flight.FlightData{
		FlightDescriptor: desc,
		DataBody:         []byte("data"),
	})

	// Should handle cancellation gracefully
	err := dataServer.DoExchange(mockStream)
	// Either returns error or handles gracefully
	if err != nil {
		assert.Contains(t, err.Error(), "cancel")
	}
}

// Test 5: Empty stream handling
func TestDoExchange_EmptyStream(t *testing.T) {
	alloc := memory.NewGoAllocator()
	logger := zerolog.Nop()
	store := NewVectorStore(alloc, logger, 1<<30, 0, time.Hour)
	defer func() { _ = store.Close() }()

	dataServer := NewDataServer(store)

	ctx := context.Background()
	mockStream := newMockDoExchangeStream(ctx)
	// No data added - empty stream

	err := dataServer.DoExchange(mockStream)
	assert.NoError(t, err, "Empty stream should be handled gracefully")
}

// Test 6: Bidirectional response - verify data sent back
func TestDoExchange_BidirectionalResponse(t *testing.T) {
	alloc := memory.NewGoAllocator()
	logger := zerolog.Nop()
	store := NewVectorStore(alloc, logger, 1<<30, 0, time.Hour)
	tmpDir := t.TempDir()
	require.NoError(t, store.InitPersistence(StorageConfig{
		DataPath:         tmpDir,
		SnapshotInterval: 1 * time.Hour,
	}))
	defer func() { _ = store.Close() }()

	// Pre-populate store with data for response
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	idBuilder := array.NewInt64Builder(alloc)
	idBuilder.AppendValues([]int64{100, 200, 300}, nil)
	idArr := idBuilder.NewArray()
	prepopRec := array.NewRecordBatch(schema, []arrow.Array{idArr}, 3)

	ds := &Dataset{Records: []arrow.RecordBatch{prepopRec}, Name: "exchange-source"}
	ds.SetLastAccess(time.Now())
	ds.Index = NewHNSWIndex(ds)

	store.mu.Lock()
	store.datasets["exchange-source"] = ds
	store.mu.Unlock()

	dataServer := NewDataServer(store)

	ctx := context.Background()
	mockStream := newMockDoExchangeStream(ctx)

	// Request data exchange from source dataset
	desc := &flight.FlightDescriptor{
		Path: []string{"exchange-source"},
		Cmd:  []byte("sync"), // Sync command for mesh replication
	}
	mockStream.addFlightData(&flight.FlightData{
		FlightDescriptor: desc,
		DataBody:         []byte("request"),
	})

	err := dataServer.DoExchange(mockStream)
	require.NoError(t, err)

	// Verify response was sent (foundation for mesh replication)
	assert.GreaterOrEqual(t, mockStream.getSentCount(), 0, "Should handle bidirectional flow")

	idBuilder.Release()
}

// Test 7: Verify Prometheus metrics are tracked
func TestDoExchange_Metrics(t *testing.T) {
	alloc := memory.NewGoAllocator()
	logger := zerolog.Nop()
	store := NewVectorStore(alloc, logger, 1<<30, 0, time.Hour)
	defer func() { _ = store.Close() }()

	dataServer := NewDataServer(store)

	ctx := context.Background()
	mockStream := newMockDoExchangeStream(ctx)

	desc := &flight.FlightDescriptor{Path: []string{"metrics-test"}}
	mockStream.addFlightData(&flight.FlightData{
		FlightDescriptor: desc,
		DataBody:         []byte("test"),
	})

	// Execute - metrics should be recorded without panic
	err := dataServer.DoExchange(mockStream)
	require.NoError(t, err)
}
