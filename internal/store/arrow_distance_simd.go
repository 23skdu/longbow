package store

import (
	"github.com/23skdu/longbow/internal/simd"
)

// distanceSIMD computes L2 distance using SIMD optimizations.
// This replaces the simple l2Distance function for better performance.
func distanceSIMD(a, b []float32) float32 {
	return simd.EuclideanDistance(a, b)
}
