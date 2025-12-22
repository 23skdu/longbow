package store

import (
	"context"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/metadata"
)

// MockDoGetStream implements flight.FlightService_DoGetServer for testing
type MockDoGetStream struct {
	ctx     context.Context
	records []arrow.RecordBatch
	mu      sync.Mutex
	header  metadata.MD
	trailer metadata.MD
}

// NewMockDoGetStream creates a new mock stream for DoGet testing
func NewMockDoGetStream(ctx context.Context) *MockDoGetStream {
	return &MockDoGetStream{
		ctx:     ctx,
		records: make([]arrow.RecordBatch, 0),
	}
}

// Context returns the stream context
func (m *MockDoGetStream) Context() context.Context {
	return m.ctx
}

// Send captures flight data
func (m *MockDoGetStream) Send(data *flight.FlightData) error {
	m.mu.Lock()
	defer m.mu.Unlock()
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

// GetRecords returns captured records
func (m *MockDoGetStream) GetRecords() []arrow.RecordBatch {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.records
}

// AddRecord adds a record
func (m *MockDoGetStream) AddRecord(rec arrow.RecordBatch) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec.Retain()
	m.records = append(m.records, rec)
}
