package index

import (
	"strconv"
	"strings"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	arrowarray "github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

func (h *ArrowHNSW) extractFromDataset(batchIdx, rowIdx int) any {
	if h.dataset == nil {
		return nil
	}
	recs := h.dataset.GetRecords()
	if batchIdx < 0 || batchIdx >= len(recs) {
		return nil
	}
	rec := recs[batchIdx]
	if rec == nil {
		return nil
	}
	vecColIdx := h.getVectorColumnIndex(rec)
	if vecColIdx == -1 {
		return nil
	}
	return h.extractVector(rec, vecColIdx, rowIdx)
}

func (h *ArrowHNSW) extractVector(rec arrow.RecordBatch, colIdx, rowIdx int) any {
	col := rec.Column(colIdx)

	var values arrow.Array
	var start, end int

	if list, ok := col.(*arrowarray.FixedSizeList); ok {
		size := int(list.DataType().(*arrow.FixedSizeListType).Len())
		values = list.ListValues()
		start = (list.Offset() + rowIdx) * size
		end = start + size
	} else if list, ok := col.(*arrowarray.List); ok {
		offsets := list.Offsets()
		start = int(offsets[rowIdx])
		end = int(offsets[rowIdx+1])
		values = list.ListValues()
	} else {
		return nil
	}

	// Guard against out-of-bounds (should not happen with valid Arrow data)
	if end > values.Len() {
		end = values.Len()
	}
	if start > end {
		start = end
	}

	switch arr := values.(type) {
	case *arrowarray.Float32:
		floats := arr.Float32Values()[start:end]
		if h.config.DataType == types.VectorTypeComplex64 {
			if len(floats) < 2 {
				return nil
			}
			complexes := make([]complex64, len(floats)/2)
			for i := 0; i < len(complexes); i++ {
				complexes[i] = complex(floats[2*i], floats[2*i+1])
			}
			return complexes
		}
		// Zero-copy: return the underlying slice directly
		return floats

	case *arrowarray.Float64:
		floats := arr.Float64Values()[start:end]
		if h.config.DataType == types.VectorTypeComplex128 {
			if len(floats) < 2 {
				return nil
			}
			complexes := make([]complex128, len(floats)/2)
			for i := 0; i < len(complexes); i++ {
				complexes[i] = complex(floats[2*i], floats[2*i+1])
			}
			return complexes
		}
		// Zero-copy: return the underlying slice directly
		return floats

	default:
		// Generic fallback using Value(i)
		return h.extractValuesGeneric(values, start, end)
	}
}

func (h *ArrowHNSW) extractValuesGeneric(values arrow.Array, start, end int) any {
	size := end - start
	if size <= 0 {
		return nil
	}

	switch arr := values.(type) {
	case *arrowarray.Int8:
		res := make([]int8, size)
		for i := 0; i < size; i++ {
			res[i] = arr.Value(start + i)
		}
		return res
	case *arrowarray.Uint8:
		res := make([]uint8, size)
		for i := 0; i < size; i++ {
			res[i] = arr.Value(start + i)
		}
		return res
	case *arrowarray.Uint32:
		res := make([]uint32, size)
		for i := 0; i < size; i++ {
			res[i] = arr.Value(start + i)
		}
		return res
	case *arrowarray.Int32:
		res := make([]int32, size)
		for i := 0; i < size; i++ {
			res[i] = arr.Value(start + i)
		}
		return res
	case *arrowarray.Int64:
		res := make([]int64, size)
		for i := 0; i < size; i++ {
			res[i] = arr.Value(start + i)
		}
		return res
	case *arrowarray.Uint64:
		res := make([]uint64, size)
		for i := 0; i < size; i++ {
			res[i] = arr.Value(start + i)
		}
		return res
	case *arrowarray.Int16:
		res := make([]int16, size)
		for i := 0; i < size; i++ {
			res[i] = arr.Value(start + i)
		}
		return res
	case *arrowarray.Uint16:
		res := make([]uint16, size)
		for i := 0; i < size; i++ {
			res[i] = arr.Value(start + i)
		}
		return res
	case *arrowarray.Float16:
		res := make([]float16.Num, size)
		for i := 0; i < size; i++ {
			res[i] = arr.Value(start + i)
		}
		return res
	}
	return nil
}

func (h *ArrowHNSW) getColumnIdx(rec arrow.RecordBatch, name string) int {
	if rec == nil {
		return -1
	}

	// 1. Try cache first
	if val, ok := h.metadata.fieldMap.Load(name); ok {
		return val.(int)
	}

	// 2. Linear search if not in cache
	idx := -1
	numCols := int(rec.NumCols())
	for i := 0; i < numCols; i++ {
		if strings.EqualFold(rec.ColumnName(i), name) {
			idx = i
			break
		}
	}

	if idx != -1 {
		h.metadata.fieldMap.Store(name, idx)
	}
	return idx
}

func (h *ArrowHNSW) getVectorColumnIndex(rec arrow.RecordBatch) int {
	if rec == nil {
		return -1
	}
	cached := h.metadata.vecColIdx.Load()
	if cached != -1 {
		return int(cached)
	}

	// Schema lookups are expensive in the hot path. Cache the result.
	vecColIdx := h.getColumnIdx(rec, "vector")
	if vecColIdx == -1 {
		vecColIdx = h.getColumnIdx(rec, "embedding")
	}
	if vecColIdx == -1 {
		vecColIdx = h.getColumnIdx(rec, "vec")
	}

	if vecColIdx != -1 && vecColIdx <= 2147483647 {
		h.metadata.vecColIdx.Store(int32(vecColIdx)) // #nosec G115
	}

	// Pre-cache other metadata while we have the schema
	h.precacheMetadata(rec.Schema())

	return vecColIdx
}

func (h *ArrowHNSW) precacheMetadata(schema *arrow.Schema) {
	if schema == nil || h.metadata.cached.Load() {
		return
	}

	md := schema.Metadata()

	// Pre-cache TurboQuant bits
	if val, ok := md.GetValue("longbow.turboquant_bits"); ok {
		if bits, err := strconv.ParseInt(val, 10, 32); err == nil {
			h.metadata.tqBits.Store(int32(bits))
		}
	}

	// Pre-cache vector type
	if val, ok := md.GetValue("longbow.vector_type"); ok {
		vt := parseVectorType(val)
		h.metadata.vecType.Store(int32(vt)) // #nosec G115
	}

	// Pre-cache complex flag
	if val, ok := md.GetValue("longbow.complex"); ok && val == "true" {
		h.metadata.isComplex.Store(true)
	}

	// Pre-cache metric
	if val, ok := md.GetValue("longbow.metric"); ok {
		var m int32 = -1
		switch strings.ToLower(val) {
		case "euclidean", "l2":
			m = 0
		case "cosine":
			m = 1
		case "dot_product":
			m = 2
		}
		if m != -1 {
			h.metadata.metric.Store(m)
		}
	}

	h.metadata.cached.Store(true)
}

func parseVectorType(val string) types.VectorDataType {
	switch val {
	case "complex64":
		return types.VectorTypeComplex64
	case "complex128":
		return types.VectorTypeComplex128
	case "float16":
		return types.VectorTypeFloat16
	case "float32":
		return types.VectorTypeFloat32
	case "float64":
		return types.VectorTypeFloat64
	case "int8":
		return types.VectorTypeInt8
	case "uint8":
		return types.VectorTypeUint8
	case "int16":
		return types.VectorTypeInt16
	case "uint16":
		return types.VectorTypeUint16
	case "int32":
		return types.VectorTypeInt32
	case "uint32":
		return types.VectorTypeUint32
	case "int64":
		return types.VectorTypeInt64
	case "uint64":
		return types.VectorTypeUint64
	case "turboquant", "tq":
		return types.VectorTypeTQ
	default:
		return types.VectorTypeFloat32
	}
}
