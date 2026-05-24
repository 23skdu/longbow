package index

import (
	"fmt"
	"math"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
)

func (p parallelSearchHostF32) ExtractVectorToBufferForParallel(rec arrow.RecordBatch, rowIdx int, dst []float32) error {
	return p.h.ExtractVectorToBufferForParallel(rec, rowIdx, dst)
}

func (h *ArrowHNSW) ExtractVectorF64ToBufferForParallel(rec arrow.RecordBatch, rowIdx int, dst []float64) error {
	vecColIdx := h.getVectorColumnIndex(rec)

	if vecColIdx == -1 {
		return fmt.Errorf("vector column not found in record")
	}

	vec, err := ExtractVectorRaw(rec, rowIdx, vecColIdx)
	if err != nil {
		return err
	}

	switch v := vec.(type) {
	case []float64:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		copy(dst, v)
		return nil
	case []complex128:
		if len(dst) != len(v)*2 {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v)*2)
		}
		if len(v) == 0 {
			return nil
		}
		raw := unsafe.Slice((*float64)(unsafe.Pointer(&v[0])), len(v)*2) // #nosec G103
		copy(dst, raw)
		return nil
	case []float32:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float64(val)
		}
		return nil
	default:
		return fmt.Errorf("unsupported vector type for F64 extraction: %T", vec)
	}
}

func (p parallelSearchHostF32) ExtractVectorByIDToBufferForParallel(id uint32, dst []float32) error {
	return p.h.ExtractVectorByIDToBufferForParallel(id, dst)
}

func (h *ArrowHNSW) ExtractVectorF64ByIDToBufferForParallel(id uint32, dst []float64) error {
	vecAny, err := h.GetVector(id)
	if err != nil {
		return err
	}

	switch v := vecAny.(type) {
	case []float64:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		copy(dst, v)
		return nil
	case []complex128:
		if len(dst) != len(v)*2 {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v)*2)
		}
		if len(v) == 0 {
			return nil
		}
		raw := unsafe.Slice((*float64)(unsafe.Pointer(&v[0])), len(v)*2) // #nosec G103
		copy(dst, raw)
		return nil
	case []float32:
		if len(dst) != len(v) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(v))
		}
		for i, val := range v {
			dst[i] = float64(val)
		}
		return nil
	}

	return fmt.Errorf("unsupported vector type %T for F64 extraction", vecAny)
}

func (h *ArrowHNSW) GetVector(id uint32) (any, error) {
	data := h.data.Load()
	if data == nil {
		return nil, fmt.Errorf("index data not initialized")
	}

	// 1. Try raw vector from memory first (most accurate)
	if v, err := data.GetVector(id); v != nil || err != nil {
		return v, err
	}

	// Shared Vector Space Path for memory locality
	if h.sharedVectorSpace.Load() {
		loc, ok := h.locationStore.Get(types.VectorID(id))
		if ok {
			vec := h.extractFromDataset(loc.BatchIdx, loc.RowIdx)
			if vec != nil {
				return vec, nil
			}
		}
	}

	// 2. Fallback to DiskGraph in hybrid mode
	dg := h.diskGraph.Load()
	if dg != nil {
		if h.config.SQ8Enabled {
			if v := dg.GetVectorSQ8(id); v != nil {
				return v, nil
			}
		}
		if h.config.PQEnabled {
			if v := dg.GetVectorPQ(id); v != nil {
				return v, nil
			}
		}
		// Try raw from disk if available
		if v, _ := dg.GetVector(id); v != nil {
			return v, nil
		}
	}

	// 3. Last resort: internal compressed copies in types.GraphData (only if raw wasn't found)
	if h.config.SQ8Enabled {
		if v := data.GetVectorSQ8(id); v != nil {
			return v, nil
		}
	}
	if h.config.PQEnabled {
		if v := data.GetVectorPQ(id); v != nil {
			return v, nil
		}
	}
	if h.config.BQEnabled {
		if v, err := data.GetVectorBQ(id); err == nil && v != nil {
			return v, nil
		}
	}
	if h.tqEncoder != nil {
		chunk := data.GetVectorsTQChunkWithGen(int(types.ChunkID(id)), math.MaxUint64)
		if chunk != nil {
			// TQ stride calculation must match GraphData's layout
			paddedDims := data.GetPaddedDimsForType(types.VectorTypeTQ)
			stride := (paddedDims * data.TurboQuantBits) / 8
			start := int(types.ChunkOffset(id)) * stride // #nosec G115
			return h.tqEncoder.Decode(chunk[start : start+stride])
		}
	}

	return nil, nil
}

func (h *ArrowHNSW) GetVectorAny(id uint32) (any, error) {
	return h.GetVector(id)
}

func (h *ArrowHNSW) getVectorWithData(data *types.GraphData, id uint32) (any, error) {
	return h.getVectorWithCachedDisk(data, nil, id, math.MaxUint64)
}

func (h *ArrowHNSW) getVectorWithCachedDisk(data *types.GraphData, dg *DiskGraph, id uint32, maxGen uint64) (any, error) {
	v, _ := data.GetVectorWithGen(id, maxGen)
	if v != nil {
		return v, nil
	}

	// Shared Vector Space Path
	if h.sharedVectorSpace.Load() {
		loc, ok := h.locationStore.Get(types.VectorID(id))
		if ok {
			vec := h.extractFromDataset(loc.BatchIdx, loc.RowIdx)
			if vec != nil {
				return vec, nil
			}
		}
	}

	// Fallback to DiskGraph
	if dg == nil {
		dg = h.diskGraph.Load()
	}
	if dg != nil {
		if h.config.SQ8Enabled {
			return dg.GetVectorSQ8(id), nil
		}
		if h.config.PQEnabled {
			return dg.GetVectorPQ(id), nil
		}
	}

	// 4. If all else fails, return a sentinel vector of the correct dimensionality
	// This prevents panics in distance calculations during edge case lookups
	metrics.VectorSentinelHitTotal.Inc()
	if data != nil && data.Dims > 0 {
		return make([]float32, data.Dims), nil
	}
	if dims := h.GetDims(); dims > 0 {
		return make([]float32, dims), nil
	}
	return nil, fmt.Errorf("could not resolve dimensions for Sentinel vector")
}

func (h *ArrowHNSW) mustGetVectorFromData(data *types.GraphData, id uint32) any {
	vec, err := h.getVectorWithData(data, id)
	if err != nil || vec == nil {
		if id < 100 {
		}
		return nil
	}
	if id == 1 {
		if v, ok := vec.([]float32); ok && len(v) >= 4 {
		}
	}
	return vec
}
