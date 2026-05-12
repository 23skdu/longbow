package simd

import (
	"errors"
)

// HadamardTransformer manages state for Fast Walsh-Hadamard Transforms.
// It can be reused to avoid allocations.
type HadamardTransformer struct {
	pow2 int
}

// NewHadamardTransformer creates a new transformer for the given power-of-2 size.
func NewHadamardTransformer(pow2 int) *HadamardTransformer {
	return &HadamardTransformer{pow2: pow2}
}

// Transform applies an in-place Fast Walsh-Hadamard Transform to the slice.
func (h *HadamardTransformer) Transform(a []float32) error {
	if len(a) != h.pow2 {
		return errors.New("simd: vector length mismatch for transformer")
	}
	return FastWalshHadamardTransform32(a)
}

// InverseTransform applies the inverse FWHT. Since FWHT is its own inverse
// (up to a scaling factor), this just calls Transform and optionally scales.
func (h *HadamardTransformer) InverseTransform(a []float32) error {
	if err := h.Transform(a); err != nil {
		return err
	}
	// For TurboQuant reconstruction, we often need the 1/N scaling
	factor := float32(1.0 / float64(h.pow2))
	for i := range a {
		a[i] *= factor
	}
	return nil
}
