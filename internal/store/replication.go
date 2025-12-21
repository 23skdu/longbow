package store

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ZeroCopyPayload represents a serialized RecordBatch that references raw buffers.
// This is used for high-speed P2P replication (Item 7).
type ZeroCopyPayload struct {
	DatasetName string
	Buffers     [][]byte
	Schema      []byte // Encoded schema
}

// MarshalZeroCopy creates a payload that references the underlying Arrow buffers.
// WARNING: The resulting payload is only valid as long as the RecordBatch is retained.
func MarshalZeroCopy(name string, rec arrow.RecordBatch) (*ZeroCopyPayload, error) {
	payload := &ZeroCopyPayload{
		DatasetName: name,
		Buffers:     make([][]byte, 0),
	}

	// 1. Encode Schema using IPC
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
	// We only need the schema header, but writing the first record and closing
	// is the easiest way to ensure a valid IPC stream header is captured.
	// In a more optimized version, we'd use low-level IPC message encoding.
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("serialize schema: %w", err)
	}
	payload.Schema = buf.Bytes()

	// 2. Collect all buffers from all columns
	for i := 0; i < int(rec.NumCols()); i++ {
		col := rec.Column(i)
		collectBuffers(col.Data(), &payload.Buffers)
	}

	return payload, nil
}

func collectBuffers(data arrow.ArrayData, list *[][]byte) {
	for _, buf := range data.Buffers() {
		if buf == nil {
			continue
		}
		// Directly reference the underlying slice returned by Bytes()
		*list = append(*list, buf.Bytes())
	}
	for _, child := range data.Children() {
		collectBuffers(child, list)
	}
}

// UnmarshalZeroCopy reconstructs a RecordBatch from a ZeroCopyPayload.
// This is currently a simplified implementation that uses the provided buffers
// to rebuild ArrayData objects.
func UnmarshalZeroCopy(payload *ZeroCopyPayload, mem memory.Allocator) (arrow.RecordBatch, error) {
	// 1. Read Schema from payload
	reader, err := ipc.NewReader(bytes.NewReader(payload.Schema), ipc.WithAllocator(mem))
	if err != nil {
		return nil, fmt.Errorf("read schema: %w", err)
	}
	defer reader.Release()
	schema := reader.Schema()
	_ = schema // Use it later to rebuild record

	// 2. Reconstruct ArrayData from buffers
	// This requires knowing the exact sequence of buffers collected by collectBuffers.
	// For a robust implementation, we'd need to store the buffer counts per column.

	// Implementation note: This simplified version is for demonstration of the
	// zero-copy principle. A production version would use the full IPC
	// message format for blood-free reconstruction.

	return nil, fmt.Errorf("UnmarshalZeroCopy: full buffer reconstruction not yet implemented")
}
