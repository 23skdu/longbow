package core

import (
	"sync"

	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
)

var float32DecodePool = sync.Pool{
	New: func() interface{} {
		return new([]float32)
	},
}

// ScalarQuantizer handles SQ8 quantization for the HNSW index.
type ScalarQuantizer struct {
	minVal float32
	maxVal float32
	dim    int

	// Lock for updating bounds (if we support dynamic bounds updates)
	mu sync.RWMutex

	// If true, bounds are fixed and won't update
	frozen bool
}

// NewScalarQuantizer creates a new quantizer.
// Default bounds can be provided, or learned later.
func NewScalarQuantizer(dim int) *ScalarQuantizer {
	return &ScalarQuantizer{
		dim:    dim,
		minVal: -0.2, // Default tight bounds
		maxVal: 0.2,
		frozen: false, // Not trained yet
	}
}

// NewScalarQuantizerFromParams restores a trained quantizer.
func NewScalarQuantizerFromParams(dim int, minVal, maxVal float32) *ScalarQuantizer {
	return &ScalarQuantizer{
		dim:    dim,
		minVal: minVal,
		maxVal: maxVal,
		frozen: true, // Marked as trained
	}
}

// IsTrained returns true if the quantizer has been trained on data.
func (sq *ScalarQuantizer) IsTrained() bool {
	sq.mu.RLock()
	defer sq.mu.RUnlock()
	return sq.frozen
}

// Train (simple online version) updates bounds based on a batch of vectors.
// NOTE: Changing bounds invalidates previously quantized vectors!
// In a real system, we train on a sample, freeze, then index.
func (sq *ScalarQuantizer) Train(vectors [][]float32) {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	if sq.frozen {
		return
	}

	// Reset if first time?
	// For now, simple min/max across batch
	minV := sq.minVal
	maxV := sq.maxVal
	first := true

	for _, vec := range vectors {
		lMin, lMax := simd.ComputeBounds(vec)
		if first {
			minV, maxV = lMin, lMax
			first = false
		} else {
			if lMin < minV {
				minV = lMin
			}
			if lMax > maxV {
				maxV = lMax
			}
		}
	}

	sq.minVal = minV
	sq.maxVal = maxV
	sq.frozen = true
}

// Encode converts float vector to byte vector
func (sq *ScalarQuantizer) Encode(vec []float32, dst []byte) []byte {
	sq.mu.RLock()
	minV, maxV := sq.minVal, sq.maxVal
	sq.mu.RUnlock()

	if cap(dst) < len(vec) {
		dst = make([]byte, len(vec))
	}
	dst = dst[:len(vec)]
	simd.QuantizeSQ8(vec, dst, minV, maxV)
	return dst
}

// Distance returns squared L2 in quantized space
func (sq *ScalarQuantizer) Distance(a, b []byte) (int32, error) {
	return simd.EuclideanDistanceSQ8(a, b)
}

// Decode converts byte vector to float vector
func (sq *ScalarQuantizer) Decode(src []byte) []float32 {
	sq.mu.RLock()
	minV, maxV := sq.minVal, sq.maxVal
	sq.mu.RUnlock()

	// Ensure capacity for SIMD loads (align to 16 floats / 64 bytes)
	paddedLen := (len(src) + 15) & ^15
	if paddedLen < len(src) {
		paddedLen = len(src)
	}

	// Use pool to reduce allocations in hot path
	pooled := float32DecodePool.Get().(*[]float32)
	if cap(*pooled) < paddedLen {
		*pooled = make([]float32, paddedLen)
	} else {
		*pooled = (*pooled)[:paddedLen]
	}
	dst := (*pooled)[:len(src)]

	scale := (maxV - minV) / 255.0

	for i, b := range src {
		dst[i] = minV + float32(b)*scale
	}

	// Return a copy to the caller since we're returning the pooled buffer
	result := make([]float32, len(src))
	copy(result, dst)
	float32DecodePool.Put(pooled)

	return result
}

// L2Scale returns the scaling factor (scale^2) to convert SQ8 integer L2 to float32 L2.
func (sq *ScalarQuantizer) L2Scale() float32 {
	sq.mu.RLock()
	minV, maxV := sq.minVal, sq.maxVal
	sq.mu.RUnlock()

	scale := (maxV - minV) / 255.0
	return scale * scale
}

// Params returns the current min/max values.
func (sq *ScalarQuantizer) Params() (minVal, maxVal float32) {
	sq.mu.RLock()
	defer sq.mu.RUnlock()
	return sq.minVal, sq.maxVal
}

// GetQuantizer returns the scalar quantizer
func (h *ArrowHNSW) GetQuantizer() *ScalarQuantizer {
	return h.quantizer
}

// IsSQ8Ready returns whether scalar quantization is ready
func (h *ArrowHNSW) IsSQ8Ready() bool {
	return h.sq8Ready.Load()
}

// GetBQEncoder returns the BQ encoder
func (h *ArrowHNSW) GetBQEncoder() *types.BQEncoder {
	return h.bqEncoder
}

// SetBQEncoder sets the BQ encoder
func (h *ArrowHNSW) SetBQEncoder(encoder *types.BQEncoder) {
	h.bqEncoder = encoder
}

// GetOPQEncoder returns the OPQ encoder (or legacy PQ if OPQ not used)
func (h *ArrowHNSW) GetOPQEncoder() any {
	return h.oopqEncoder
}

// GetPQEncoder returns legacy PQ encoder (for VectorIndexer interface compliance)
// For new code, use GetOPQEncoder instead
func (h *ArrowHNSW) GetPQEncoder() *pq.PQEncoder {
	if h.oopqEncoder != nil {
		if enc, ok := h.oopqEncoder.(*pq.PQEncoder); ok {
			return enc
		}
		if enc, ok := h.oopqEncoder.(*pq.OPQEncoder); ok {
			return enc.PQEncoder
		}
	}
	return nil
}

// SetOPQEncoder sets the OPQ encoder (accepts both OPQ and legacy PQ for backward compatibility)
func (h *ArrowHNSW) SetOPQEncoder(encoder any) {
	var m, k int
	switch enc := encoder.(type) {
	case *pq.PQEncoder:
		h.oopqEncoder = encoder
		if encoder != nil {
			m = enc.M
			k = enc.K
			h.config.PQM = m
			h.config.PQK = k
			h.config.PQEnabled = true
		}
	case *pq.OPQEncoder:
		h.oopqEncoder = encoder
		if encoder != nil {
			m = enc.M
			k = enc.K
			h.config.PQM = m
			h.config.PQK = k
			h.config.PQEnabled = true
		}
	default:
		return
	}

	// Initialize data if not yet created
	if h.data.Load() == nil {
		if err := h.growInternal(1024, 0); err != nil {
			return
		}
	}

	data := h.data.Load()
	if data != nil && m > 0 {
		data.PQEnabled = true
		data.PQM = m

		// Force re-allocation if current offset is 0 (placeholder from dims=0 call)
		if len(data.VectorsPQ) > 0 && data.VectorsPQ[0] == 0 && data.Dims > 0 {
			// Re-allocate with proper dims
			data.VectorsPQ = nil // Reset to trigger real allocation
		}

		// Explicitly ensure PQ chunk 0 if capacity > 0
		if data.Capacity > 0 && data.Dims > 0 {
			if err := data.EnsureChunk(0, 0, data.Dims); err != nil {
				return
			}
		}
	}
}
