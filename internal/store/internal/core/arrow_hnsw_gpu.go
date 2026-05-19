package core

import (
	"context"
	"fmt"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// GetGPUIndex returns the underlying GPU index if enabled.
func (h *ArrowHNSW) GetGPUIndex() any {
	h.gpuMu.RLock()
	defer h.gpuMu.RUnlock()
	return h.gpuIndex
}

// searchGPU performs automatic GPU dispatch based on data type
func (h *ArrowHNSW) searchGPU(_ context.Context, queryVec any, k int) ([]types.SearchResult, error) {
	if h.gpuIndex == nil {
		return nil, fmt.Errorf("GPU index not initialized")
	}

	// Check circuit breaker
	if h.gpuCircuitBreaker != nil && !h.gpuCircuitBreaker.Allow() {
		return nil, fmt.Errorf("circuit breaker open")
	}

	var ids []int64
	var distances []float32
	var err error

	switch h.config.DataType {
	case types.VectorTypeFloat32:
		if q, ok := queryVec.([]float32); ok {
			ids, distances, err = h.gpuIndex.Search(q, k)
		} else {
			return nil, fmt.Errorf("float32 search requires float32 query, got %T", queryVec)
		}

	case types.VectorTypeFloat16:
		if q, ok := queryVec.([]float32); ok {
			// Convert float32 to uint16 (fp16 encoding)
			fp16Query := make([]uint16, len(q))
			for i, v := range q {
				fp16Query[i] = float16.New(v).Uint16()
			}
			ids, distances, err = h.gpuIndex.SearchFloat16(fp16Query, k)
		} else if q16, ok := queryVec.([]float16.Num); ok {
			fp16Query := make([]uint16, len(q16))
			for i, v := range q16 {
				fp16Query[i] = v.Uint16()
			}
			ids, distances, err = h.gpuIndex.SearchFloat16(fp16Query, k)
		} else {
			return nil, fmt.Errorf("float16 search requires float32 or float16 query, got %T", queryVec)
		}

	case types.VectorTypeComplex64:
		if q, ok := queryVec.([]complex64); ok {
			// Convert complex64 to uint16 pairs (half precision)
			fp16Query := make([]uint16, len(q)*2)
			for i, v := range q {
				fp16Query[i*2] = float16.New(real(v)).Uint16()
				fp16Query[i*2+1] = float16.New(imag(v)).Uint16()
			}
			ids, distances, err = h.gpuIndex.SearchComplex64(fp16Query, k)
		} else {
			return nil, fmt.Errorf("complex64 search requires complex64 query, got %T", queryVec)
		}

	case types.VectorTypeComplex128:
		if q, ok := queryVec.([]complex128); ok {
			// Convert complex128 to float32 pairs (stored as float32 for this implementation)
			f32Query := make([]float32, len(q)*2)
			for i, v := range q {
				f32Query[i*2] = float32(real(v))
				f32Query[i*2+1] = float32(imag(v))
			}
			ids, distances, err = h.gpuIndex.SearchComplex128(f32Query, k)
		} else {
			return nil, fmt.Errorf("complex128 search requires complex128 query, got %T", queryVec)
		}

	case types.VectorTypeTQ:
		if q, ok := queryVec.([]float32); ok {
			ids, distances, err = h.gpuIndex.SearchTurboQuant(q, k, h.config.TurboQuantBits)
		} else {
			return nil, fmt.Errorf("TurboQuant search requires float32 query, got %T", queryVec)
		}

	default:
		return nil, fmt.Errorf("GPU search not supported for type %s", h.config.DataType)
	}

	if err != nil {
		if h.gpuCircuitBreaker != nil {
			h.gpuCircuitBreaker.RecordFailure()
		}
		return nil, err
	}

	if h.gpuCircuitBreaker != nil {
		h.gpuCircuitBreaker.RecordSuccess()
	}

	// Convert to SearchResult
	results := make([]types.SearchResult, len(ids))
	for i, id := range ids {
		results[i] = types.SearchResult{
			ID:       types.VectorID(id), // #nosec G115 -- id is uint32
			Distance: distances[i],
			Score:    1.0 / (1.0 + distances[i]),
		}
	}

	return results, nil
}
