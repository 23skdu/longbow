package index

import (
	"fmt"
	"unsafe"

	types "github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// GetParallelSearchConfig implements VectorIndex.
func (h *ArrowHNSW) GetParallelSearchConfig() types.ParallelSearchConfig {
	return h.parallelConfig
}

// SetParallelSearchConfig implements VectorIndex.
func (h *ArrowHNSW) SetParallelSearchConfig(cfg types.ParallelSearchConfig) {
	h.parallelConfig = cfg
}

// GetLocationForParallel retrieves a vector location for parallel processing.
func (h *ArrowHNSW) GetLocationForParallel(id uint32) (types.Location, bool) {
	return h.locationStore.Get(types.VectorID(id))
}

// ExtractVectorForParallel extracts a vector for parallel search refinement.
func (h *ArrowHNSW) ExtractVectorForParallel(rec arrow.RecordBatch, rowIdx int) ([]float32, error) {
	if rec == nil {
		return nil, fmt.Errorf("record is nil")
	}
	vecColIdx := h.getVectorColumnIndex(rec)
	if vecColIdx == -1 {
		if rec.NumCols() == 1 {
			vecColIdx = 0
		} else {
			return nil, fmt.Errorf("vector column not found in record")
		}
	}

	vec, err := ExtractVectorRaw(rec, rowIdx, vecColIdx)
	if err != nil {
		return nil, err
	}

	// Complex128 stored in Arrow as FixedSizeList<float64>. Convert to []float32 for the SIMD path.
	if h.config.DataType == types.VectorTypeComplex128 {
		vf64, ok := vec.([]float64)
		if !ok {
			return nil, fmt.Errorf("expected []float64 for Complex128, got %T", vec)
		}
		f32 := make([]float32, len(vf64))
		for i, v := range vf64 {
			f32[i] = float32(v)
		}
		return f32, nil
	}

	// Convert other types to float32 for parallel refinement
	switch v := vec.(type) {
	case []float32:
		return v, nil
	case []float64:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []float16.Num:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = val.Float32()
		}
		return res, nil
	case []int32:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []uint32:
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
	}

	return nil, fmt.Errorf("unsupported vector type %T for parallel search refinement", vec)
}

// ExtractVectorByIDForParallel extracts a vector by ID as a float32 slice.
func (h *ArrowHNSW) ExtractVectorByIDForParallel(id uint32) ([]float32, error) {
	vecAny, err := h.GetVector(id)
	if err != nil {
		return nil, err
	}
	if v, ok := vecAny.([]float32); ok {
		return v, nil
	}

	// Handle Complex128/64 and other types by flattening to float32
	switch v := vecAny.(type) {
	case []complex128:
		res := make([]float32, len(v)*2)
		for i, val := range v {
			res[i*2] = float32(real(val))
			res[i*2+1] = float32(imag(val))
		}
		return res, nil
	case []complex64:
		if len(v) == 0 {
			return nil, nil
		}
		res := unsafe.Slice((*float32)(unsafe.Pointer(&v[0])), len(v)*2) // #nosec G103
		resCopy := make([]float32, len(res))
		copy(resCopy, res)
		return resCopy, nil
	case []float64:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []float16.Num:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = val.Float32()
		}
		return res, nil
	case []int8:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []uint8:
		if h.quantizer != nil && h.sq8Ready.Load() {
			return h.quantizer.Decode(v), nil
		}
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []int32:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	case []uint32:
		res := make([]float32, len(v))
		for i, val := range v {
			res[i] = float32(val)
		}
		return res, nil
	}

	return nil, fmt.Errorf("unsupported vector type %T for parallel search", vecAny)
}
