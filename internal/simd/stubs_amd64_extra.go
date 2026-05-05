//go:build amd64

package simd

import (
	lbcore "github.com/23skdu/longbow/internal/core"
)

// Extra stubs for AMD64 to satisfy test references

func haversineBatchAVX2(centerLat, centerLon float64, points []lbcore.GeoPoint, earthRadius float64, results []float32) {
	haversineBatchGeneric(centerLat, centerLon, points, earthRadius, results)
}

func matMulAVX2Go(a, b []float32, m, n, k int, dst []float32) {
	matMulGeneric(a, b, m, n, k, dst)
}

func ManhattanDistanceFloat32AVX2(a, b []float32) (float32, error) {
	return ManhattanDistanceFloat32(a, b)
}

func ChebyshevDistanceFloat32AVX2(a, b []float32) (float32, error) {
	return ChebyshevDistanceFloat32(a, b)
}

func BrayCurtisDistanceFloat32AVX2(a, b []float32) (float32, error) {
	return BrayCurtisDistanceFloat32(a, b)
}

