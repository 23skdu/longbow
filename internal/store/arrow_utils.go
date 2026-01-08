package store

import (
	"fmt"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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

	// Get the underlying float32 array
	values := listArr.Data().Children()[0]

	// Calculate offset and width directly
	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
	start := rowIdx * width

	return unsafeVectorSlice(values, start, width), nil
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
