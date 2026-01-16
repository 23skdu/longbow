package store

import (
	"sync"

	"github.com/23skdu/longbow/internal/simd"
)

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
func (sq *ScalarQuantizer) Distance(a, b []byte) int32 {
	return simd.EuclideanDistanceSQ8(a, b)
}

// Decode converts byte vector to float vector
func (sq *ScalarQuantizer) Decode(src []byte) []float32 {
	sq.mu.RLock()
	minV, maxV := sq.minVal, sq.maxVal
	sq.mu.RUnlock()

	dst := make([]float32, len(src))
	scale := (maxV - minV) / 255.0

	for i, b := range src {
		dst[i] = minV + float32(b)*scale
	}
	return dst
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
