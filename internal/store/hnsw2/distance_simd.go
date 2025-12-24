package hnsw2

import (
	"github.com/23skdu/longbow/internal/simd"
)

// distanceSIMD computes L2 distance using SIMD optimizations.
// This replaces the simple l2Distance function for better performance.
func distanceSIMD(a, b []float32) float32 {
	return simd.L2DistanceSIMD(a, b)
}

// Update search.go and insert.go to use distanceSIMD instead of l2Distance
// for production builds. Keep l2Distance for testing/fallback.
