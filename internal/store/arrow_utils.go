package store

import (
	"fmt"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// ExtractVectorFromArrow extracts a vector from an Arrow record batch at the given row index.
// This is a zero-copy operation that returns a slice pointing directly to Arrow's memory.
func ExtractVectorFromArrow(rec arrow.RecordBatch, rowIdx, colIdx int) ([]float32, error) {
	if rec == nil {
		return nil, fmt.Errorf("record is nil")
	}

	var vecCol arrow.Array
	cols := rec.Columns()

	// Use provided index if valid
	if colIdx >= 0 && colIdx < len(cols) {
		vecCol = cols[colIdx]
	} else {
		// Fallback search (legacy flow for tests or uninit)
		// Fast path: check common names at index 1
		if len(cols) > 1 && (rec.ColumnName(1) == "vector" || rec.ColumnName(1) == "embedding") {
			vecCol = cols[1]
		} else {
			// Fallback search
			for i, field := range rec.Schema().Fields() {
				if field.Name == "vector" || field.Name == "embedding" {
					vecCol = cols[i]
					break
				}
			}
		}
	}

	if vecCol == nil {
		return nil, fmt.Errorf("vector column not found")
	}

	// Cast to FixedSizeList
	listArr, ok := vecCol.(*array.FixedSizeList)
	if !ok {
		return nil, fmt.Errorf("vector column is not FixedSizeList")
	}

	// Calculate offset and width directly
	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
	start := rowIdx * width

	// Get the underlying array data
	values := listArr.Data().Children()[0]

	// Check Element Type
	elemType := listArr.DataType().(*arrow.FixedSizeListType).Elem()

	if elemType.ID() == arrow.FLOAT16 {
		// FP16 -> FP32 Conversion (Allocates)
		// We cannot return zero-copy slice because types differ.
		buf := values.Buffers()[1]
		if buf == nil {
			return nil, nil // Or error?
		}

		// Access raw bytes
		rawBytes := buf.Bytes()
		// Calculate byte offset (2 bytes per float16)
		byteOffset := start * 2

		if byteOffset+width*2 > len(rawBytes) {
			return nil, fmt.Errorf("index out of bounds")
		}

		// Convert
		res := make([]float32, width)
		// Use unsafe to view as Float16, then convert
		ptr := unsafe.Pointer(&rawBytes[byteOffset])
		f16s := unsafe.Slice((*float16.Num)(ptr), width)

		for i := 0; i < width; i++ {
			res[i] = f16s[i].Float32()
		}

		metrics.VectorCastF16ToF32Total.Inc()
		return res, nil
	}

	// Float32 Case (Zero Copy)
	return unsafeVectorSlice(values, start, width), nil
}

// ExtractVectorF16FromArrow extracts a vector as Float16 (Zero-Copy).
func ExtractVectorF16FromArrow(rec arrow.RecordBatch, rowIdx, colIdx int) ([]float16.Num, error) {
	if rec == nil {
		return nil, fmt.Errorf("record is nil")
	}

	var vecCol arrow.Array
	cols := rec.Columns()

	if colIdx >= 0 && colIdx < len(cols) {
		vecCol = cols[colIdx]
	} else {
		// Fallback search
		if len(cols) > 1 && (rec.ColumnName(1) == "vector" || rec.ColumnName(1) == "embedding") {
			vecCol = cols[1]
		} else {
			for i, field := range rec.Schema().Fields() {
				if field.Name == "vector" || field.Name == "embedding" {
					vecCol = cols[i]
					break
				}
			}
		}
	}

	if vecCol == nil {
		return nil, fmt.Errorf("vector column not found")
	}

	listArr, ok := vecCol.(*array.FixedSizeList)
	if !ok {
		return nil, fmt.Errorf("vector column is not FixedSizeList")
	}

	elemType := listArr.DataType().(*arrow.FixedSizeListType).Elem()
	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
	start := rowIdx * width
	values := listArr.Data().Children()[0]

	if elemType.ID() == arrow.FLOAT32 {
		// F32 -> F16 Conversion (Allocates)
		f32s := unsafeVectorSlice(values, start, width)
		if f32s == nil {
			return nil, nil
		}
		res := make([]float16.Num, width)
		for i, v := range f32s {
			res[i] = float16.New(v)
		}
		metrics.VectorCastF32ToF16Total.Inc()
		return res, nil
	} else if elemType.ID() == arrow.FLOAT16 {
		// Zero-Copy Return
		buf := values.Buffers()[1]
		if buf == nil {
			return nil, nil
		}
		rawBytes := buf.Bytes()
		byteOffset := start * 2
		if byteOffset+width*2 > len(rawBytes) {
			return nil, fmt.Errorf("index out of bounds")
		}
		ptr := unsafe.Pointer(&rawBytes[byteOffset])
		return unsafe.Slice((*float16.Num)(ptr), width), nil
	}

	return nil, fmt.Errorf("unsupported vector element type: %s", elemType)
}

// extractVectorCopy extracts a vector and returns a copy (for when vector needs to be stored).
func extractVectorCopy(rec arrow.RecordBatch, rowIdx, colIdx int) ([]float32, error) {
	// Get zero-copy slice first
	vec, err := ExtractVectorFromArrow(rec, rowIdx, colIdx)
	if err != nil {
		return nil, err
	}

	// Make a copy
	result := make([]float32, len(vec))
	copy(result, vec)
	return result, nil
}

// unsafeVectorSlice creates a zero-copy slice from Arrow data.
// UNSAFE: The returned slice is only valid while the Arrow array is retained.
func unsafeVectorSlice(data arrow.ArrayData, offset, length int) []float32 {
	if data == nil || data.Len() == 0 {
		return nil
	}

	// Get the raw buffer
	buf := data.Buffers()[1] // Values buffer (buffer 0 is validity bitmap)
	if buf == nil {
		return nil
	}

	// Cast to float32 slice
	ptr := unsafe.Pointer(&buf.Bytes()[offset*4])
	return unsafe.Slice((*float32)(ptr), length)
}
