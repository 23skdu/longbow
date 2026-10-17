package simd

import (
	"math"
)

// DistanceDirectADC computes Euclidean distance between query and a single vector's PQ codes directly
// from precomputed distance table without allocating an intermediate distance slice.
func DistanceDirectADC(table []float32, codes []byte, m int) float32 {
	var sum float32
	for sub := 0; sub < m; sub++ {
		code := int(codes[sub])
		sum += table[sub*256+code]
	}
	return float32(math.Sqrt(float64(sum)))
}
