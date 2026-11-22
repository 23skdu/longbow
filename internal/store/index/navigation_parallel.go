package index

import (
	"context"
	"fmt" // // // // //

	basecore "github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
)

type parallelSearchHostF32 struct{ h *ArrowHNSW }

// GetDataset returns the dataset.
func (p parallelSearchHostF32) GetDataset() types.IndexDataProvider { return p.h.dataset }

// GetLocationForParallel returns the location for parallel.
func (p parallelSearchHostF32) GetLocationForParallel(id uint32) (types.Location, bool) {
	return p.h.locationStore.Get(types.VectorID(id))
}

// GetParallelSearchConfig returns the parallel search config.
func (p parallelSearchHostF32) GetParallelSearchConfig() types.ParallelSearchConfig {
	return p.h.parallelConfig
}

// GetDistanceFuncForParallel returns the distance func for parallel.
func (p parallelSearchHostF32) GetDistanceFuncForParallel() func(a, b []float32) float32 {
	return func(a, b []float32) float32 {
		d, _ := p.h.distFunc(a, b)
		return d
	}
}

// GetDistanceMetric returns the distance metric.
func (p parallelSearchHostF32) GetDistanceMetric() basecore.DistanceMetric { return p.h.config.Metric }

// IsDeleted reports whether the condition holds.
func (p parallelSearchHostF32) IsDeleted(id uint32) bool { return p.h.IsDeleted(id) }

// GetNUMAConfig returns the numa config.
func (p parallelSearchHostF32) GetNUMAConfig() (*memory.NUMATopology, int) {
	return p.h.topo, p.h.config.NUMANode
}

type parallelSearchHostF64 struct{ h *ArrowHNSW }

// GetDataset returns the dataset.
func (p parallelSearchHostF64) GetDataset() types.IndexDataProvider { return p.h.dataset }

// GetLocationForParallel returns the location for parallel.
func (p parallelSearchHostF64) GetLocationForParallel(id uint32) (types.Location, bool) {
	return p.h.locationStore.Get(types.VectorID(id))
}

// ExtractVectorToBufferForParallel runs the extract vector to buffer for parallel operation.
func (p parallelSearchHostF64) ExtractVectorToBufferForParallel(rec arrow.RecordBatch, rowIdx int, dst []float64) error {
	return p.h.ExtractVectorF64ToBufferForParallel(rec, rowIdx, dst)
}

// GetParallelSearchConfig returns the parallel search config.
func (p parallelSearchHostF64) GetParallelSearchConfig() types.ParallelSearchConfig {
	return p.h.parallelConfig
}

// GetDistanceFuncForParallel returns the distance func for parallel.
func (p parallelSearchHostF64) GetDistanceFuncForParallel() func(a, b []float64) float32 {
	return func(a, b []float64) float32 {
		d, _ := p.h.distFuncF64(a, b)
		return d
	}
}

// ExtractVectorByIDToBufferForParallel runs the extract vector by id to buffer for parallel operation.
func (p parallelSearchHostF64) ExtractVectorByIDToBufferForParallel(id uint32, dst []float64) error {
	return p.h.ExtractVectorF64ByIDToBufferForParallel(id, dst)
}

// GetDistanceMetric returns the distance metric.
func (p parallelSearchHostF64) GetDistanceMetric() basecore.DistanceMetric { return p.h.config.Metric }

// IsDeleted reports whether the condition holds.
func (p parallelSearchHostF64) IsDeleted(id uint32) bool { return p.h.IsDeleted(id) }

// GetNUMAConfig returns the numa config.
func (p parallelSearchHostF64) GetNUMAConfig() (*memory.NUMATopology, int) {
	return p.h.topo, p.h.config.NUMANode
}

// SearchForParallel runs the search for parallel operation.
func (h *ArrowHNSW) SearchForParallel(queryVec []float32, k int) []types.Candidate {
	// Use the existing Search implementation which handles bitmask and conversion
	res, err := h.Search(context.Background(), queryVec, k, nil)
	if err != nil {
		return nil
	}
	return res
}

// ExtractVectorToBufferForParallel runs the extract vector to buffer for parallel operation.
func (h *ArrowHNSW) ExtractVectorToBufferForParallel(rec arrow.RecordBatch, rowIdx int, dst []float32) error {
	vecColIdx := h.getVectorColumnIndex(rec)

	if vecColIdx == -1 {
		return fmt.Errorf("vector column not found in record")
	}

	vec, err := ExtractVectorRaw(rec, rowIdx, vecColIdx)
	if err != nil {
		return err
	}

	// Handle SQ8 quantized uint8 specially (needs quantizer state from ArrowHNSW)
	if v8, ok := vec.([]uint8); ok && h.quantizer != nil && h.sq8Ready.Load() {
		decoded := h.quantizer.Decode(v8)
		if len(dst) != len(decoded) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(decoded))
		}
		copy(dst, decoded)
		return nil
	}
	return CopyVectorToFloat32(vec, dst)
}

// ExtractVectorByIDToBufferForParallel runs the extract vector by id to buffer for parallel operation.
func (h *ArrowHNSW) ExtractVectorByIDToBufferForParallel(id uint32, dst []float32) error {
	vecAny, err := h.GetVector(id)
	if err != nil {
		return err
	}

	// Handle SQ8 quantized uint8 specially (needs quantizer state from ArrowHNSW)
	if v8, ok := vecAny.([]uint8); ok && h.quantizer != nil && h.sq8Ready.Load() {
		decoded := h.quantizer.Decode(v8)
		if len(dst) != len(decoded) {
			return fmt.Errorf("dst length mismatch: got %d, expected %d", len(dst), len(decoded))
		}
		copy(dst, decoded)
		return nil
	}
	return CopyVectorToFloat32(vecAny, dst)
}
