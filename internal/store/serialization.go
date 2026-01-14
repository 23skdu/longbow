package store

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// bufferPool recycles buffers to avoid allocation and copy
var bufferPool = sync.Pool{
	New: func() interface{} {
		// Pre-allocate ~2.5MB (5000 rows * 512 bytes)
		// capacity 3MB
		return bytes.NewBuffer(make([]byte, 0, 3*1024*1024))
	},
}

type SerializedBatch struct {
	FDs []*flight.FlightData
	Buf *bytes.Buffer
}

func (s *SerializedBatch) Release() {
	if s.Buf != nil {
		s.Buf.Reset()
		bufferPool.Put(s.Buf)
		s.Buf = nil
	}
	// FlightData pointers become invalid here
}

// StatefulSerializer reuses an IPC writer for efficient serialization
type StatefulSerializer struct {
	buf    *bytes.Buffer
	w      *ipc.Writer
	schema *arrow.Schema // needed to recreate writer if we swap buffer?
	// ipc.Writer wraps an io.Writer. If we swap underlying buffer, we might need to reset writer.
	// ipc.NewWriter takes w io.Writer.
	// If we change s.buf pointer, `s.w` still points to OLD buffer struct?
	// bytes.Buffer is a struct. internal slice changes.
	// If we assume `s.w` holds pointer to `s.buf`, we cannot just swap `s.buf`.

	// Issue: ipc.Writer holds reference to the `io.Writer` interface.
	// If we pass `s.buf` (pointer), it holds that pointer.
	// If we want to swap the buffer, we need to creating a NEW ipc.Writer?
	// Creating ipc.Writer writes Schema!

	// Workaround:
	// We need a "SwappableWriter" wrapper?
	// type SwappableWriter struct { target io.Writer }

}

type swappableBuffer struct {
	target *bytes.Buffer
}

func (s *swappableBuffer) Write(p []byte) (n int, err error) {
	return s.target.Write(p)
}

// capturingStream implements flight.DataStreamWriter to capture serialized data
type capturingStream struct {
	chunks []*flight.FlightData
}

func (c *capturingStream) Send(d *flight.FlightData) error {
	// We must copy the data because the writer reuses buffers.
	// Deep copy header and body.
	dCopy := &flight.FlightData{}
	if len(d.DataHeader) > 0 {
		dCopy.DataHeader = make([]byte, len(d.DataHeader))
		copy(dCopy.DataHeader, d.DataHeader)
	}
	if len(d.DataBody) > 0 {
		dCopy.DataBody = make([]byte, len(d.DataBody))
		copy(dCopy.DataBody, d.DataBody)
	}
	// Copy metadata if needed (app_metadata) - usually empty for RecordWriter
	if len(d.AppMetadata) > 0 {
		dCopy.AppMetadata = make([]byte, len(d.AppMetadata))
		copy(dCopy.AppMetadata, d.AppMetadata)
	}

	c.chunks = append(c.chunks, dCopy)
	return nil
}

type StatefulSerializerV2 struct {
	schema *arrow.Schema
}

func NewStatefulSerializer(schema *arrow.Schema) (*StatefulSerializerV2, error) {
	return &StatefulSerializerV2{
		schema: schema,
	}, nil
}

func (s *StatefulSerializerV2) Serialize(rec arrow.RecordBatch) (*SerializedBatch, error) {
	// Use flight.NewRecordWriter to ensure correct serialization
	cs := &capturingStream{chunks: make([]*flight.FlightData, 0, 2)}

	// Create writer. This immediately Sends Schema.
	w := flight.NewRecordWriter(cs, ipc.WithSchema(s.schema))
	// We don't control when Schema is sent, usually first.

	// Write Record
	if err := w.Write(rec); err != nil {
		w.Close()
		return nil, err
	}
	w.Close()

	if len(cs.chunks) == 0 {
		return nil, fmt.Errorf("no flight data produced")
	}

	// Return ALL chunks (typically [Schema, Batch]).
	// This ensures the stream is self-contained and valid.

	return &SerializedBatch{
		FDs: cs.chunks,
	}, nil
}

func (s *StatefulSerializerV2) Close() error {
	// No resources to close for this implementation
	return nil
}

// flightDataRecorder stubs (keep for compatibility if referenced elsewhere)
type flightDataRecorder struct{}

func (f *flightDataRecorder) Send(fd *flight.FlightData) error             { return nil }
func (f *flightDataRecorder) SetHeader(metadata *flight.FlightData) error  { return nil }
func (f *flightDataRecorder) SetTrailer(metadata *flight.FlightData) error { return nil }
func (f *flightDataRecorder) Context() context.Context                     { return context.Background() }
func (f *flightDataRecorder) RecvMsg(m interface{}) error                  { return nil }
func (f *flightDataRecorder) SendMsg(m interface{}) error                  { return nil }
