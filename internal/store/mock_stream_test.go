package store

import (
	"context"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"google.golang.org/grpc/metadata"
)

// MockDoGetStream implements flight.FlightService_DoGetServer for testing
type MockDoGetStream struct {
	ctx     context.Context
	records []arrow.Record //nolint:staticcheck
	mu      sync.Mutex
	header  metadata.MD
	trailer metadata.MD
}

// NewMockDoGetStream creates a new mock stream for DoGet testing
func NewMockDoGetStream(ctx context.Context) *MockDoGetStream {
	return &MockDoGetStream{
		ctx:     ctx,
		records: make([]arrow.Record, 0), //nolint:staticcheck
	}
}

// Context returns the stream context
func (m *MockDoGetStream) Context() context.Context {
	return m.ctx
}

// Send captures flight data - implements the RecordWriter interface indirectly
func (m *MockDoGetStream) Send(data *flight.FlightData) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// FlightData contains IPC serialized record batches
	// For testing, we track that Send was called
	return nil
}

// SetHeader sets response headers
func (m *MockDoGetStream) SetHeader(md metadata.MD) error {
	m.header = md
	return nil
}

// SendHeader sends response headers
func (m *MockDoGetStream) SendHeader(md metadata.MD) error {
	m.header = md
	return nil
}

// SetTrailer sets response trailers
func (m *MockDoGetStream) SetTrailer(md metadata.MD) {
	m.trailer = md
}

// SendMsg implements grpc.ServerStream
func (m *MockDoGetStream) SendMsg(msg interface{}) error {
	return nil
}

// RecvMsg implements grpc.ServerStream
func (m *MockDoGetStream) RecvMsg(msg interface{}) error {
	return nil
}

// GetRecords returns captured records (for verification)
func (m *MockDoGetStream) GetRecords() []arrow.Record { //nolint:staticcheck
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.records
}

// AddRecord allows tests to manually track records written via the writer
func (m *MockDoGetStream) AddRecord(rec arrow.Record) { //nolint:staticcheck
	m.mu.Lock()
	defer m.mu.Unlock()
	rec.Retain()
	m.records = append(m.records, rec)
}

// MockRecordWriter wraps MockDoGetStream to capture records
type MockRecordWriter struct {
	*ipc.Writer
}
