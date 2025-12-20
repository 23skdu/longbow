package store

import (
	"errors"
	"math/bits"
	"sort"

	"github.com/23skdu/longbow/internal/metrics"
)

// =============================================================================
// Binary Quantization - 32x Memory Reduction + POPCNT 40x Speedup
// =============================================================================
// Converts float32 vectors to single-bit representations.
// Each dimension becomes 1 bit: above threshold = 1, below = 0.
// Distance computed via Hamming distance using POPCNT instruction.
//
// Memory: 1536-dim float32 (6144 bytes) -> 1536 bits (192 bytes) = 32x reduction
// Speed: POPCNT enables ~40x faster distance calculation vs float operations

// BinaryQuantizer holds per-dimension thresholds for binary quantization.
type BinaryQuantizer struct {
	Threshold []float32 // Per-dimension threshold (typically median)
}

// Dims returns the number of dimensions this quantizer handles.
func (bq *BinaryQuantizer) Dims() int {
	return len(bq.Threshold)
}

// TrainBinaryQuantizer learns per-dimension thresholds from sample vectors.
// Uses median as threshold for balanced bit distribution.
func TrainBinaryQuantizer(vectors [][]float32) (*BinaryQuantizer, error) {
	if len(vectors) == 0 {
		return nil, errors.New("no vectors provided for training")
	}

	dims := len(vectors[0])
	if dims == 0 {
		return nil, errors.New("vectors have zero dimensions")
	}

	// Validate all vectors have same dimensions
	for _, vec := range vectors {
		if len(vec) != dims {
			return nil, errors.New("all vectors must have same dimensions")
		}
	}

	// Compute median for each dimension
	thresholds := make([]float32, dims)
	values := make([]float32, len(vectors))

	for d := 0; d < dims; d++ {
		// Collect values for this dimension
		for i, vec := range vectors {
			values[i] = vec[d]
		}

		// Sort to find median
		sort.Slice(values, func(i, j int) bool {
			return values[i] < values[j]
		})

		// Median (middle value or average of two middle values)
		mid := len(values) / 2
		if len(values)%2 == 0 {
			thresholds[d] = (values[mid-1] + values[mid]) / 2
		} else {
			thresholds[d] = values[mid]
		}
	}

	metrics.BinaryQuantizeOpsTotal.Inc()

	return &BinaryQuantizer{Threshold: thresholds}, nil
}

// Quantize converts a float32 vector to bit-packed uint64 representation.
// Each dimension becomes 1 bit: value >= threshold -> 1, else -> 0.
// Bits are packed LSB-first: dimension 0 is bit 0 of first uint64.
func (bq *BinaryQuantizer) Quantize(vec []float32) []uint64 {
	dims := len(vec)
	numUint64s := (dims + 63) / 64 // Ceiling division
	result := make([]uint64, numUint64s)

	for i, v := range vec {
		if v >= bq.Threshold[i] {
			wordIdx := i / 64
			bitIdx := i % 64
			result[wordIdx] |= 1 << bitIdx
		}
	}

	metrics.BinaryQuantizeOpsTotal.Inc()
	return result
}

// QuantizeInto encodes a vector into a pre-allocated destination slice.
// Zero-allocation hot path for bulk encoding.
func (bq *BinaryQuantizer) QuantizeInto(vec []float32, dst []uint64) {
	// Clear destination
	for i := range dst {
		dst[i] = 0
	}

	for i, v := range vec {
		if v >= bq.Threshold[i] {
			wordIdx := i / 64
			bitIdx := i % 64
			dst[wordIdx] |= 1 << bitIdx
		}
	}
}

// HammingDistancePOPCNT computes Hamming distance using POPCNT instruction.
// Returns the number of differing bits between two binary vectors.
// Uses math/bits.OnesCount64 which compiles to POPCNT on supported CPUs.
func HammingDistancePOPCNT(a, b []uint64) int {
	dist := 0
	for i := range a {
		// XOR gives 1s where bits differ, POPCNT counts them
		dist += bits.OnesCount64(a[i] ^ b[i])
	}
	metrics.POPCNTDistanceOpsTotal.Inc()
	return dist
}

// HammingDistanceBatch computes Hamming distances from query to multiple vectors.
// Optimized for batch processing with better cache utilization.
func HammingDistanceBatch(query []uint64, vectors [][]uint64, distances []int) {
	for i, vec := range vectors {
		dist := 0
		for j := range query {
			dist += bits.OnesCount64(query[j] ^ vec[j])
		}
		distances[i] = dist
	}
	metrics.POPCNTDistanceOpsTotal.Add(float64(len(vectors)))
}

// =============================================================================
// Asymmetric Distance Computation (ADC) for Binary Vectors
// =============================================================================
// For even faster search, we can compute asymmetric distance where the query
// remains in float32 but database vectors are binary. This gives better recall
// than symmetric binary comparison.

// AsymmetricBinaryDistance computes distance from float32 query to binary vector.
// This provides better recall than pure binary comparison.
func (bq *BinaryQuantizer) AsymmetricBinaryDistance(query []float32, quantized []uint64) float32 {
	var dist float32
	for i, q := range query {
		wordIdx := i / 64
		bitIdx := i % 64
		bit := (quantized[wordIdx] >> bitIdx) & 1

		// Distance based on whether query agrees with binary value
		if bit == 1 {
			if q < bq.Threshold[i] {
				// Binary says above threshold, but query is below
				dist += bq.Threshold[i] - q
			}
		} else {
			if q >= bq.Threshold[i] {
				// Binary says below threshold, but query is above
				dist += q - bq.Threshold[i]
			}
		}
	}
	return dist
}
