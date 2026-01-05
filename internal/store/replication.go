package store

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ArrayStructure captures the metadata needed to rebuild an ArrayData hierarchy.
type ArrayStructure struct {
	Length    int
	NullCount int
	Offset    int
	Buffers   int              // Number of buffers used by this node specifically
	Children  []ArrayStructure // Recursive children
}

// ZeroCopyPayload represents a serialized RecordBatch that references raw buffers.
// This is used for high-speed P2P replication (Item 7).
type ZeroCopyPayload struct {
	DatasetName string
	Schema      []byte
	RowCount    int64
	Meta        []ArrayStructure // One per column
	Buffers     [][]byte
}

// MarshalZeroCopy creates a payload that references the underlying Arrow buffers.
// WARNING: The resulting payload is only valid as long as the RecordBatch is retained.
func MarshalZeroCopy(name string, rec arrow.RecordBatch) (*ZeroCopyPayload, error) {
	payload := &ZeroCopyPayload{
		DatasetName: name,
		RowCount:    rec.NumRows(),
		Meta:        make([]ArrayStructure, rec.NumCols()),
		Buffers:     make([][]byte, 0),
	}

	// 1. Encode Schema using IPC
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("serialize schema: %w", err)
	}
	payload.Schema = buf.Bytes()

	// 2. Traversal
	for i := 0; i < int(rec.NumCols()); i++ {
		col := rec.Column(i)
		meta := captureStructure(col.Data(), &payload.Buffers)
		payload.Meta[i] = meta
	}

	return payload, nil
}

func captureStructure(data arrow.ArrayData, buffers *[][]byte) ArrayStructure {
	meta := ArrayStructure{
		Length:    data.Len(),
		NullCount: data.NullN(),
		Offset:    data.Offset(),
		Buffers:   len(data.Buffers()),
		Children:  make([]ArrayStructure, len(data.Children())),
	}

	// Capture buffers for this node
	for _, buf := range data.Buffers() {
		if buf == nil {
			*buffers = append(*buffers, nil) // Keep placeholder for nil buffers (validity bitmap etc)
		} else {
			*buffers = append(*buffers, buf.Bytes())
		}
	}

	// Recurse children
	for i, child := range data.Children() {
		meta.Children[i] = captureStructure(child, buffers)
	}

	return meta
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

	if len(payload.Meta) != len(schema.Fields()) {
		return nil, fmt.Errorf("metadata count %d does not match fields %d", len(payload.Meta), len(schema.Fields()))
	}

	// 2. Reconstruct Columns
	cols := make([]arrow.Array, len(schema.Fields()))
	defer func() {
		for _, col := range cols {
			if col != nil {
				col.Release()
			}
		}
	}()

	bufferIdx := 0
	for i, field := range schema.Fields() {
		data, n, err := rebuildArray(field.Type, payload.Meta[i], payload.Buffers, bufferIdx)
		if err != nil {
			return nil, fmt.Errorf("rebuild column %s: %w", field.Name, err)
		}
		bufferIdx += n
		cols[i] = array.MakeFromData(data)
		data.Release() // MakeFromData holds a reference, but we must release ours
	}

	// Validate buffer consumption
	if bufferIdx != len(payload.Buffers) {
		return nil, fmt.Errorf("buffer mismatch: used %d, available %d", bufferIdx, len(payload.Buffers))
	}

	return array.NewRecordBatch(schema, cols, payload.RowCount), nil
}

func rebuildArray(dt arrow.DataType, meta ArrayStructure, allBuffers [][]byte, startIdx int) (arrow.ArrayData, int, error) {
	// Collect buffers for this node
	nodeBuffers := make([]*memory.Buffer, meta.Buffers)
	used := 0
	for i := 0; i < meta.Buffers; i++ {
		if startIdx+i >= len(allBuffers) {
			return nil, 0, fmt.Errorf("buffer overrun")
		}
		raw := allBuffers[startIdx+i]
		if raw != nil {
			// Wrap bytes without copy
			nodeBuffers[i] = memory.NewBufferBytes(raw)
		}
		// else nil buffer remains nil
		used++
	}

	// Recurse children
	children := make([]arrow.ArrayData, len(meta.Children))
	for i, childMeta := range meta.Children {
		// Child type? We need to know specific child type from parent type.
		// For List, child is ElementType. For Struct, child is field type.
		// Constructing fully correct type is hard without traversing Schema type too.
		// Actually, ArrayData doesn't STRICTLY need to check type against children immediately if we trust meta?
		// But MakeFromData needs valid types.
		// We passed `dt` (DataType).
		// We can inspect `dt` to find child types.

		var childType arrow.DataType
		switch t := dt.(type) {
		case *arrow.ListType:
			childType = t.Elem()
		case *arrow.FixedSizeListType:
			childType = t.Elem()
		case *arrow.MapType:
			childType = t.Elem()
		case *arrow.StructType:
			childType = t.Field(i).Type
		case arrow.UnionType:
			childType = t.Fields()[i].Type
		default:
			return nil, 0, fmt.Errorf("unsupported nested type: %T", dt)
		}

		childData, n, err := rebuildArray(childType, childMeta, allBuffers, startIdx+used)
		if err != nil {
			return nil, 0, err
		}
		children[i] = childData
		used += n
	}

	data := array.NewData(dt, meta.Length, nodeBuffers, children, meta.NullCount, meta.Offset)
	return data, used, nil
}
