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
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

// reuse mockDoExchangeStream from do_exchange_test.go if package level,
// otherwise redefine locally for isolation.

type mockSearchStream struct {
	ctx       context.Context
	recvQueue []*flight.FlightData
	recvIdx   int
	sentData  []*flight.FlightData
	mu        sync.Mutex
}

func newMockSearchStream(ctx context.Context) *mockSearchStream {
	return &mockSearchStream{
		ctx:       ctx,
		recvQueue: make([]*flight.FlightData, 0),
		sentData:  make([]*flight.FlightData, 0),
	}
}

func (m *mockSearchStream) Send(data *flight.FlightData) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sentData = append(m.sentData, data)
	return nil
}

func (m *mockSearchStream) Recv() (*flight.FlightData, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.recvIdx >= len(m.recvQueue) {
		return nil, io.EOF
	}
	data := m.recvQueue[m.recvIdx]
	m.recvIdx++
	return data, nil
}

func (m *mockSearchStream) Context() context.Context     { return m.ctx }
func (m *mockSearchStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockSearchStream) SendHeader(metadata.MD) error { return nil }
func (m *mockSearchStream) SetTrailer(metadata.MD)       {}
func (m *mockSearchStream) SendMsg(interface{}) error    { return nil }
func (m *mockSearchStream) RecvMsg(interface{}) error    { return nil }

func (m *mockSearchStream) addFlightData(data *flight.FlightData) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Deep copy executed by caller (clientAdapter) to be safe or here?
	// To be extra safe, mock stores what it gets.
	// But clientAdapter should deep copy if modifying.
	m.recvQueue = append(m.recvQueue, data)
}

func TestDoExchange_VectorSearch(t *testing.T) {
	pool := memory.NewGoAllocator()
	logger := zerolog.Nop()
	store := NewVectorStore(pool, logger, 1<<30, 0, time.Hour)
	defer func() { _ = store.Close() }()

	// 1. Setup Dataset with Index
	setupTestDataset(t, store, "test-data")

	// 2. Prepare Search Request (Arrow RecordBatch)
	// Schema: query_vector (List<F32>), k (Int32), ef (Int32), dataset (String)
	reqSchema := arrow.NewSchema([]arrow.Field{
		{Name: "query_vector", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)},
		{Name: "k", Type: arrow.PrimitiveTypes.Int32},
		{Name: "ef", Type: arrow.PrimitiveTypes.Int32},
		{Name: "dataset", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(pool, reqSchema)
	defer b.Release()

	// Add 1 Row
	// Query Vector: [1.0, 1.0]
	vecBuilder := b.Field(0).(*array.FixedSizeListBuilder)
	vecValBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)
	vecBuilder.Append(true)
	vecValBuilder.AppendValues([]float32{1.0, 1.0}, nil)

	b.Field(1).(*array.Int32Builder).Append(5)  // k=5
	b.Field(2).(*array.Int32Builder).Append(10) // ef=10
	b.Field(3).(*array.StringBuilder).Append("test-data")

	reqRec := b.NewRecordBatch()
	defer reqRec.Release()

	// 3. Serialise Request to FlightData (Mocking Client)
	mockStream := newMockSearchStream(context.Background())

	clientAdapter := &clientStreamAdapter{stream: mockStream}
	writer := flight.NewRecordWriter(clientAdapter, ipc.WithSchema(reqSchema))

	require.NoError(t, writer.Write(reqRec))
	_ = writer.Close() // Flushes

	// 4. Exec DoExchange Handler Directly
	// Simulate DoExchange logic reading first message
	firstMsg, err := mockStream.Recv()
	require.NoError(t, err, "Failed to read first message")
	require.NotNil(t, firstMsg)
	require.NotNil(t, firstMsg.FlightDescriptor, "First message should have descriptor")
	require.Equal(t, "VectorSearch", string(firstMsg.FlightDescriptor.Cmd))

	// DataServer wraps VectorStore
	// We need to call handleVectorSearchExchange on VectorStore
	err = store.handleVectorSearchExchange(mockStream, firstMsg)
	require.NoError(t, err)

	// 5. Verify Response (MockStream.sentData)
	require.Greater(t, len(mockStream.sentData), 0, "Handler should send response data")

	// Verify we got flight data with content
	totalBytes := 0
	for _, fd := range mockStream.sentData {
		totalBytes += len(fd.DataHeader) + len(fd.DataBody)
	}
	require.Greater(t, totalBytes, 0, "Response should contain data")
}

// Helpers

func setupTestDataset(t *testing.T, s *VectorStore, name string) {
	// Simple setup with 1 vector
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)},
	}, nil)

	alloc := memory.NewGoAllocator()
	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).Append(123)
	vb := b.Field(1).(*array.FixedSizeListBuilder)
	vb.Append(true)
	vb.ValueBuilder().(*array.Float32Builder).AppendValues([]float32{1.0, 1.0}, nil)

	rec := b.NewRecordBatch()
	// Don't release - dataset owns this record

	ds := &Dataset{
		Name:    name,
		Records: []arrow.RecordBatch{rec},
	}
	ds.SetLastAccess(time.Now())
	// Init Index
	idx := NewHNSWIndex(ds)
	idx.dims = 2
	ds.Index = idx

	// Pre-populate hack
	s.updateDatasets(func(m map[string]*Dataset) {
		m[name] = ds
	})

	// Insert vector to ensure results
	// Using AddByRecord or AddByLocation
	// AddByRecord requires batch tracking which we don't have fully setup.
	// But AddByLocation(batchIdx, rowIdx) is low level.
	// batchIdx=0, rowIdx=0.
	_, err := idx.AddByLocation(0, 0)
	require.NoError(t, err)
}

type clientStreamAdapter struct {
	stream *mockSearchStream
	first  bool
}

func (c *clientStreamAdapter) Send(data *flight.FlightData) error {
	// Deep copy FIRST so we don't modify the writer's internal struct
	cp := &flight.FlightData{
		DataHeader:       make([]byte, len(data.DataHeader)),
		DataBody:         make([]byte, len(data.DataBody)),
		FlightDescriptor: data.FlightDescriptor,
	}
	copy(cp.DataHeader, data.DataHeader)
	copy(cp.DataBody, data.DataBody)

	if !c.first {
		c.first = true
		// Attach descriptor to the first message (Schema)
		cp.FlightDescriptor = &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  []byte("VectorSearch"),
		}
	}
	// Add the COPY to the stream
	c.stream.addFlightData(cp)
	return nil
}
