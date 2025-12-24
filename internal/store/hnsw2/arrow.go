package hnsw2

import (
	"fmt"
	"unsafe"
	
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// getVectorZeroCopy retrieves a vector from Arrow storage using zero-copy access.
// This is the Arrow-native implementation that avoids allocations.
func (h *ArrowHNSW) getVectorZeroCopy(id uint32) ([]float32, error) {
	if int(id) >= int(h.nodeCount.Load()) {
		return nil, fmt.Errorf("vector ID %d out of bounds", id)
	}
	
	// TODO: For now, we need a location store to map VectorID -> (BatchIdx, RowIdx)
	// This will be implemented when we add Insert functionality
	// For Phase 2, we'll implement the Arrow access logic assuming we have the location
	
	// Placeholder: In a real implementation, we would:
	// 1. Get location from locationStore: loc := h.locationStore.Get(id)
	// 2. Access the Arrow record batch: rec := h.dataset.Records[loc.BatchIdx]
	// 3. Extract vector column and access the specific row
	
	return nil, fmt.Errorf("location store not yet integrated")
}

// extractVectorFromArrow extracts a vector from an Arrow record batch at the given row index.
// This is a zero-copy operation that returns a slice pointing directly to Arrow's memory.
func extractVectorFromArrow(rec arrow.Record, rowIdx int) ([]float32, error) {
	if rec == nil {
		return nil, fmt.Errorf("record is nil")
	}
	
	// Fast path: check for "vector" column at index 1 (common case)
	// TODO: Store vector column index in ArrowHNSW to avoid lookup
	var vecCol arrow.Array
	cols := rec.Columns()
	
	// Heuristic: usually it's the second column (id, vector)
	if len(cols) > 1 && rec.ColumnName(1) == "vector" {
		vecCol = cols[1]
	} else {
		// Fallback search
		for i, field := range rec.Schema().Fields() {
			if field.Name == "vector" {
				vecCol = cols[i]
				break
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
func extractVectorCopy(rec arrow.Record, rowIdx int) ([]float32, error) {
	// Get zero-copy slice first
	vec, err := extractVectorFromArrow(rec, rowIdx)
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
