package store

import (
	"bytes"
	"context"
	"encoding/binary"
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
	FD  *flight.FlightData
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

type StatefulSerializerV2 struct {
	sw      *swappableBuffer
	w       *ipc.Writer
	isFirst bool
}

func NewStatefulSerializer(schema *arrow.Schema) (*StatefulSerializerV2, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	sw := &swappableBuffer{target: buf}

	// Create writer writing to our swappable wrapper
	w := ipc.NewWriter(sw, ipc.WithSchema(schema))

	// Discard schema from buf
	buf.Reset()

	return &StatefulSerializerV2{
		sw:      sw,
		w:       w,
		isFirst: true,
	}, nil
}

func (s *StatefulSerializerV2) Serialize(rec arrow.RecordBatch) (*SerializedBatch, error) {
	// 1. Reset current buffer (it should be empty if we swapped? No, we reset after discard schema)
	// We assume buffer is ready to write.

	if err := s.w.Write(rec); err != nil {
		return nil, err
	}

	// Buffer now contains the message.
	// We want to SHIP this buffer.
	fullBuf := s.sw.target

	// Create NEW buffer for next time
	newBuf := bufferPool.Get().(*bytes.Buffer)
	s.sw.target = newBuf

	// Process fullBuf to extract FlightData
	data := fullBuf.Bytes()

	// If this is the first write, the buffer contains [Schema] [Batch].
	// We want to Strip [Schema].
	// Assumption: Schema has no body (BodyLen=0).
	if s.isFirst {
		if len(data) >= 4 {
			schemaLen := int32(binary.LittleEndian.Uint32(data[0:4]))
			offset := 4
			if schemaLen == -1 {
				offset = 8
				if len(data) >= 8 {
					schemaLen = int32(binary.LittleEndian.Uint32(data[4:8]))
				}
			}
			// Skip schema (header + metadata). Body is 0.
			messageEnd := offset + int(schemaLen)
			if len(data) > messageEnd {
				// We have more data! Slice it.
				data = data[messageEnd:]
			}
		}
		s.isFirst = false
	}

	// Decode IPC length manually (same logic)
	// [int32 len] [meta] [body]
	if len(data) < 4 {
		// Should release fullBuf
		fullBuf.Reset()
		bufferPool.Put(fullBuf)
		return nil, fmt.Errorf("invalid ipc message length")
	}

	msgLen := int32(binary.LittleEndian.Uint32(data[0:4]))
	offset := 4
	if msgLen == -1 {
		if len(data) < 8 {
			fullBuf.Reset()
			bufferPool.Put(fullBuf)
			return nil, fmt.Errorf("invalid ipc continuation")
		}
		msgLen = int32(binary.LittleEndian.Uint32(data[4:8]))
		offset = 8
	}

	if int(msgLen) > len(data)-offset {
		fullBuf.Reset()
		bufferPool.Put(fullBuf)
		return nil, fmt.Errorf("buffer too short")
	}

	metaBytes := data[offset : offset+int(msgLen)]
	bodyBytes := data[offset+int(msgLen):]

	// Zero-Copy: metaBytes and bodyBytes are slices of fullBuf.Bytes()

	fd := &flight.FlightData{
		DataHeader: metaBytes,
		DataBody:   bodyBytes,
	}

	return &SerializedBatch{
		FD:  fd,
		Buf: fullBuf, // Main thread will Release this
	}, nil
}

func (s *StatefulSerializerV2) Close() error {
	s.sw.target.Reset()
	bufferPool.Put(s.sw.target)
	return s.w.Close()
}

// flightDataRecorder stubs (keep for compatibility if referenced elsewhere)
type flightDataRecorder struct{}

func (f *flightDataRecorder) Send(fd *flight.FlightData) error             { return nil }
func (f *flightDataRecorder) SetHeader(metadata *flight.FlightData) error  { return nil }
func (f *flightDataRecorder) SetTrailer(metadata *flight.FlightData) error { return nil }
func (f *flightDataRecorder) Context() context.Context                     { return context.Background() }
func (f *flightDataRecorder) RecvMsg(m interface{}) error                  { return nil }
func (f *flightDataRecorder) SendMsg(m interface{}) error                  { return nil }
