package store

import (
	"math"

	"github.com/23skdu/longbow/internal/simd"
)

// BQEncoder compresses float vectors into binary codes (1 bit per dimension).
// It uses sign-based quantization: > 0 => 1, <= 0 => 0.
type BQEncoder struct {
	Dimensions int
}

// NewBQEncoder creates a new Binary Quantization encoder.
func NewBQEncoder(dims int) *BQEncoder {
	return &BQEncoder{
		Dimensions: dims,
	}
}

// Encode quantizes a float32 vector into a slice of uint64s.
// Returns (dims + 63) / 64 uint64s.
func (e *BQEncoder) Encode(vec []float32) []uint64 {
	if len(vec) != e.Dimensions {
		// Should we panic or return nil?
		// For performance critical paths, we assume callers check bounds or we verify once.
		// Let's protect against mismatched dimensions loosely.
		if len(vec) < e.Dimensions {
			return nil
		}
	}

	numWords := (e.Dimensions + 63) / 64
	codes := make([]uint64, numWords)

	for i, v := range vec {
		if i >= e.Dimensions {
			break
		}
		if v > 0 {
			wordIdx := i / 64
			bitIdx := uint(i % 64)
			codes[wordIdx] |= (1 << bitIdx)
		}
	}
	return codes
}

// HammingDistance computes the Hamming distance between two binary vectors.
func (e *BQEncoder) HammingDistance(a, b []uint64) int {
	return simd.HammingDistance(a, b)
}

// HammingDistanceBatch computes distances from a query to multiple candidates.
func (e *BQEncoder) HammingDistanceBatch(query []uint64, candidates [][]uint64, results []int) {
	for i, cand := range candidates {
		results[i] = simd.HammingDistance(query, cand)
	}
}

// CodeSize returns number of uint64s required for storage.
func (e *BQEncoder) CodeSize() int {
	return (e.Dimensions + 63) / 64
}

// ScoreToFloat32 converts Hamming distance to a similarity score (0..1).
// Similarity = 1 - (Hamming / Dimensions)
func (e *BQEncoder) ScoreToFloat32(hamming int) float32 {
	return 1.0 - (float32(hamming) / float32(e.Dimensions))
}

// Float32ToHamming converts a similarity score to max Hamming distance threshold.
func (e *BQEncoder) Float32ToHamming(score float32) int {
	return int(math.Floor(float64(e.Dimensions) * (1.0 - float64(score))))
}
