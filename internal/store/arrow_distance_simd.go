package store

import (
	"math"

	"github.com/23skdu/longbow/internal/simd"
)

// distanceSIMD computes L2 distance using SIMD optimizations.
// This replaces the simple l2Distance function for better performance.
func distanceSIMD(a, b []float32) float32 {
	d, err := simd.EuclideanDistance(a, b)
	if err != nil {
		return math.MaxFloat32
	}
	return d
}
