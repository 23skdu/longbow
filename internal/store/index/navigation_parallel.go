package index

import (
	"context"
	"fmt" // // // // //
	"unsafe"

	basecore "github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

type parallelSearchHostF32 struct{ h *ArrowHNSW }

func (p parallelSearchHostF32) GetDataset() types.IndexDataProvider { return p.h.dataset }

func (p parallelSearchHostF32) GetLocationForParallel(id uint32) (types.Location, bool) {
	return p.h.locationStore.Get(types.VectorID(id))
}

func (p parallelSearchHostF32) GetParallelSearchConfig() types.ParallelSearchConfig {
	return p.h.parallelConfig
}

func (p parallelSearchHostF32) GetDistanceFuncForParallel() func(a, b []float32) float32 {
	return func(a, b []float32) float32 {
		d, _ := p.h.distFunc(a, b)
		return d
	}
}

func (p parallelSearchHostF32) GetDistanceMetric() basecore.DistanceMetric { return p.h.config.Metric }

func (p parallelSearchHostF32) IsDeleted(id uint32) bool { return p.h.IsDeleted(id) }

func (p parallelSearchHostF32) GetNUMAConfig() (*memory.NUMATopology, int) {
	return p.h.topo, p.h.config.NUMANode
}

type parallelSearchHostF64 struct{ h *ArrowHNSW }

func (p parallelSearchHostF64) GetDataset() types.IndexDataProvider { return p.h.dataset }

func (p parallelSearchHostF64) GetLocationForParallel(id uint32) (types.Location, bool) {
	return p.h.locationStore.Get(types.VectorID(id))
}

func (p parallelSearchHostF64) ExtractVectorToBufferForParallel(rec arrow.RecordBatch, rowIdx int, dst []float64) error {
	return p.h.ExtractVectorF64ToBufferForParallel(rec, rowIdx, dst)
}

func (p parallelSearchHostF64) GetParallelSearchConfig() types.ParallelSearchConfig {
	return p.h.parallelConfig
}

func (p parallelSearchHostF64) GetDistanceFuncForParallel() func(a, b []float64) float32 {
	return func(a, b []float64) float32 {
		d, _ := p.h.distFuncF64(a, b)
		return d
	}
}

func (p parallelSearchHostF64) ExtractVectorByIDToBufferForParallel(id uint32, dst []float64) error {
	return p.h.ExtractVectorF64ByIDToBufferForParallel(id, dst)
}

func (p parallelSearchHostF64) GetDistanceMetric() basecore.DistanceMetric { return p.h.config.Metric }

func (p parallelSearchHostF64) IsDeleted(id uint32) bool { return p.h.IsDeleted(id) }

func (p parallelSearchHostF64) GetNUMAConfig() (*memory.NUMATopology, int) {
	return p.h.topo, p.h.config.NUMANode
}

func (h *ArrowHNSW) SearchForParallel(queryVec []float32, k int) []types.Candidate {
	// Use the existing Search implementation which handles bitmask and conversion
	res, err := h.Search(context.Background(), queryVec, k, nil)
	if err != nil {
		return nil
	}
	return res
}

func (h *ArrowHNSW) ExtractVectorToBufferForParallel(rec arrow.RecordBatch, rowIdx int, dst []float32) error {
	vecColIdx := h.getVectorColumnIndex(rec)

	if vecColIdx == -1 {
		return fmt.Errorf("vector column not found in record")
	}

	vec, err := ExtractVectorRaw(rec, rowIdx, vecColIdx)
	if err != nil {
		return err
	}

	// Optimized buffer-based conversion
	switch v := vec.(type) {
	case []float32:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		copy(dst, v)
		return nil
	case []float64:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	case []complex128:
		if len(dst) != len(v)*2 {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v)*2)
		}
		for i, val := range v {
			dst[i*2] = float32(real(val))
			dst[i*2+1] = float32(imag(val))
		}
		return nil
	case []complex64:
		if len(dst) != len(v)*2 {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v)*2)
		}
		// Complex64 is 2x float32 in memory
		if len(v) == 0 {
			return nil
		}
		raw := unsafe.Slice((*float32)(unsafe.Pointer(&v[0])), len(v)*2) // #nosec G103
		copy(dst, raw)
		return nil
	case []float16.Num:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = val.Float32()
		}
		return nil
	case []int8:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	case []uint8:
		if h.quantizer != nil && h.sq8Ready.Load() {
			decoded := h.quantizer.Decode(v)
			if len(dst) != len(decoded) {
				return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(decoded))
			}
			copy(dst, decoded)
			return nil
		}
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	case []int32:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	}

	return fmt.Errorf("unsupported vector type %T for buffer-based extraction", vec)
}

func (h *ArrowHNSW) ExtractVectorByIDToBufferForParallel(id uint32, dst []float32) error {
	vecAny, err := h.GetVector(id)
	if err != nil {
		return err
	}

	switch v := vecAny.(type) {
	case []float32:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		copy(dst, v)
		return nil
	case []complex128:
		if len(dst) != len(v)*2 {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v)*2)
		}
		for i, val := range v {
			dst[i*2] = float32(real(val))
			dst[i*2+1] = float32(imag(val))
		}
		return nil
	case []complex64:
		if len(dst) != len(v)*2 {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v)*2)
		}
		if len(v) == 0 {
			return nil
		}
		raw := unsafe.Slice((*float32)(unsafe.Pointer(&v[0])), len(v)*2) // #nosec G103
		copy(dst, raw)
		return nil
	case []float64:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	case []float16.Num:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = val.Float32()
		}
		return nil
	case []int8:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	case []uint8:
		if h.quantizer != nil && h.sq8Ready.Load() {
			decoded := h.quantizer.Decode(v)
			if len(dst) != len(decoded) {
				return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(decoded))
			}
			copy(dst, decoded)
			return nil
		}
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	case []int32:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	case []uint32:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float32(val)
		}
		return nil
	}

	return fmt.Errorf("unsupported vector type %T for buffer-based extraction", vecAny)
}
