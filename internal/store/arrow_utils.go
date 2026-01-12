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
// ExtractVectorAny extracts a vector and returns it as a slice of the appropriate type.
func ExtractVectorAny(rec arrow.RecordBatch, rowIdx, colIdx int) (any, error) {
	if rec == nil {
		return nil, fmt.Errorf("record is nil")
	}

	var vecCol arrow.Array
	cols := rec.Columns()

	if colIdx >= 0 && colIdx < len(cols) {
		vecCol = cols[colIdx]
	} else {
		for i, field := range rec.Schema().Fields() {
			if field.Name == "vector" || field.Name == "embedding" {
				vecCol = cols[i]
				break
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
	switch elemType.ID() {
	case arrow.INT8:
		return ExtractVectorGeneric[int8](rec, rowIdx, colIdx)
	case arrow.UINT8:
		return ExtractVectorGeneric[uint8](rec, rowIdx, colIdx)
	case arrow.INT16:
		return ExtractVectorGeneric[int16](rec, rowIdx, colIdx)
	case arrow.UINT16:
		return ExtractVectorGeneric[uint16](rec, rowIdx, colIdx)
	case arrow.INT32:
		return ExtractVectorGeneric[int32](rec, rowIdx, colIdx)
	case arrow.UINT32:
		return ExtractVectorGeneric[uint32](rec, rowIdx, colIdx)
	case arrow.INT64:
		return ExtractVectorGeneric[int64](rec, rowIdx, colIdx)
	case arrow.UINT64:
		return ExtractVectorGeneric[uint64](rec, rowIdx, colIdx)
	case arrow.FLOAT32:
		return ExtractVectorGeneric[float32](rec, rowIdx, colIdx)
	case arrow.FLOAT64:
		return ExtractVectorGeneric[float64](rec, rowIdx, colIdx)
	case arrow.FLOAT16:
		return ExtractVectorGeneric[float16.Num](rec, rowIdx, colIdx)
	}

	return nil, fmt.Errorf("unsupported vector element type: %s", elemType)
}

// ExtractVectorF16FromArrow extracts a vector as Float16 (Zero-Copy).
func ExtractVectorF16FromArrow(rec arrow.RecordBatch, rowIdx, colIdx int) ([]float16.Num, error) {
	anyVec, err := ExtractVectorAny(rec, rowIdx, colIdx)
	if err != nil {
		return nil, err
	}

	switch v := anyVec.(type) {
	case []float16.Num:
		return v, nil
	case []float32:
		// F32 -> F16 Conversion (Allocates)
		res := make([]float16.Num, len(v))
		for i, val := range v {
			res[i] = float16.New(val)
		}
		metrics.VectorCastF32ToF16Total.Inc()
		return res, nil
	}

	return nil, fmt.Errorf("cannot convert %T to []float16.Num", anyVec)
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

// ExtractVectorGeneric extracts a vector of the requested type from an Arrow record batch.
func ExtractVectorGeneric[T any](rec arrow.RecordBatch, rowIdx, colIdx int) ([]T, error) {
	if rec == nil {
		return nil, fmt.Errorf("record is nil")
	}

	var vecCol arrow.Array
	cols := rec.Columns()

	if colIdx >= 0 && colIdx < len(cols) {
		vecCol = cols[colIdx]
	} else {
		// Fallback search
		for i, field := range rec.Schema().Fields() {
			if field.Name == "vector" || field.Name == "embedding" {
				vecCol = cols[i]
				break
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

	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
	listOffset := listArr.Data().Offset()
	start := (listOffset + rowIdx) * width
	values := listArr.Data().Children()[0]

	// Zero-copy extraction
	return unsafeVectorSliceGeneric[T](values, start, width), nil
}

// unsafeVectorSliceGeneric creates a zero-copy slice from Arrow data of type T.
func unsafeVectorSliceGeneric[T any](data arrow.ArrayData, offset, length int) []T {
	if data == nil || data.Len() == 0 {
		return nil
	}

	buf := data.Buffers()[1]
	if buf == nil {
		return nil
	}

	var zero T
	elementSize := int(unsafe.Sizeof(zero))
	ptr := unsafe.Pointer(&buf.Bytes()[offset*elementSize])
	return unsafe.Slice((*T)(ptr), length)
}

// ExtractVectorFromArrow (Compatibility helper - Returns Float32, casting if necessary)
func ExtractVectorFromArrow(rec arrow.RecordBatch, rowIdx, colIdx int) ([]float32, error) {
	anyVec, err := ExtractVectorAny(rec, rowIdx, colIdx)
	if err != nil {
		return nil, err
	}

	switch v := anyVec.(type) {
	case []float32:
		return v, nil
	case []float16.Num:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = val.Float32()
		}
		metrics.VectorCastF16ToF32Total.Inc()
		return res, nil
	case []float64:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []int8:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []uint8:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	default:
		return nil, fmt.Errorf("ExtractVectorFromArrow: casting from %T to []float32 not implemented", anyVec)
	}
}
