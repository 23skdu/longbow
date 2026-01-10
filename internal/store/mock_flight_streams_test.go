package store

import (
	"io"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
)

// MockAllocator tracks bytes allocated
type MockAllocator struct {
	memory.Allocator
	Allocated int64
	Freed     int64
}

func (m *MockAllocator) Allocate(size int) []byte {
	atomic.AddInt64(&m.Allocated, int64(size))
	return m.Allocator.Allocate(size)
}

func (m *MockAllocator) Reallocate(size int, b []byte) []byte {
	atomic.AddInt64(&m.Allocated, int64(size))
	// We don't subtract old size as we don't know it easily, but usually Realloc is net growth or similar.
	// For baseline this is acceptable approximation.
	return m.Allocator.Reallocate(size, b)
}

func (m *MockAllocator) Free(b []byte) {
	atomic.AddInt64(&m.Freed, int64(len(b)))
	m.Allocator.Free(b)
}

// mockPutStream implements flight.FlightService_DoPutServer for testing
type mockPutStream struct {
	grpc.ServerStream
	chunks []*flight.FlightData
	idx    int
}

func (m *mockPutStream) Recv() (*flight.FlightData, error) {
	if m.idx >= len(m.chunks) {
		return nil, io.EOF
	}
	d := m.chunks[m.idx]
	m.idx++
	return d, nil
}

func (m *mockPutStream) Send(*flight.PutResult) error {
	return nil
}

// mockClientStream implements flight.FlightService_DoPutClient for testing
type mockClientStream struct {
	grpc.ClientStream
	recvChunks chan *flight.FlightData
	closed     atomic.Bool
}

func (m *mockClientStream) Send(d *flight.FlightData) error {
	if m.recvChunks == nil {
		// Panic or handle, but test setup should init
		return io.ErrClosedPipe
	}
	// Check if closed to avoid panic on send?
	if m.closed.Load() {
		return io.ErrClosedPipe
	}
	// Deep copy since Writer reuse FlightData object.
	// We cannot copy the struct directly due to internal locks (protobuf state).
	cp := &flight.FlightData{
		DataHeader: make([]byte, len(d.DataHeader)),
		DataBody:   make([]byte, len(d.DataBody)),
		// Descriptor and Metadata might need copy if used, but usually nil or static for simple streams.
		FlightDescriptor: d.FlightDescriptor,
		AppMetadata:      d.AppMetadata,
	}
	copy(cp.DataHeader, d.DataHeader)
	copy(cp.DataBody, d.DataBody)

	m.recvChunks <- cp
	return nil
}

func (m *mockClientStream) CloseSend() error {
	if m.closed.Swap(true) {
		return nil
	}
	if m.recvChunks != nil {
		close(m.recvChunks)
	}
	return nil
}

func (m *mockClientStream) Recv() (*flight.PutResult, error) {
	return &flight.PutResult{}, nil
}

// Mocks extracted for sharing
