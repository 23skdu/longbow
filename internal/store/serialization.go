package store

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// bufferPool recycles buffers to avoid allocation and copy
var bufferPool = sync.Pool{
	New: func() any {
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

// Clear returns existing chunks and resets internal slice
func (c *capturingStream) Clear() []*flight.FlightData {
	ret := c.chunks
	c.chunks = make([]*flight.FlightData, 0, 2)
	return ret
}

type StatefulSerializerV2 struct {
	schema  *arrow.Schema
	cs      *capturingStream
	w       *flight.Writer
	isFirst bool
}

func NewStatefulSerializer(schema *arrow.Schema) (*StatefulSerializerV2, error) {
	cs := &capturingStream{chunks: make([]*flight.FlightData, 0, 2)}
	w := flight.NewRecordWriter(cs, ipc.WithSchema(schema))

	// flight.RecordWriter is lazy; it writes Schema on first Write.
	// We cannot drain here. We will strip it in Serialize.

	return &StatefulSerializerV2{
		schema:  schema,
		cs:      cs,
		w:       w,
		isFirst: true,
	}, nil
}

func (s *StatefulSerializerV2) Serialize(rec arrow.RecordBatch) (*SerializedBatch, error) {
	// Reuse existing writer.
	if err := s.w.Write(rec); err != nil {
		return nil, err
	}

	// Capture chunks
	chunks := s.cs.Clear()

	if s.isFirst {
		s.isFirst = false
		// The first write includes the Schema as the first chunk.
		// We expect the caller (DoGet) to have already sent the Schema.
		// So we strip it here to return ONLY the RecordBatch (and potentially Dictionaries).
		if len(chunks) > 0 {
			chunks = chunks[1:]
		}
	}

	if len(chunks) == 0 {
		return nil, fmt.Errorf("no flight data produced (or filtered out)")
	}

	return &SerializedBatch{
		FDs: chunks,
	}, nil
}

func (s *StatefulSerializerV2) Close() error {
	if s.w != nil {
		err := s.w.Close()
		s.w = nil
		return err
	}
	return nil
}
