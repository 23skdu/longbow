package simd

import (
	lbcore "github.com/23skdu/longbow/internal/core"
)

// SinFloat32 calculates the sine of each element in src.
func SinFloat32(src, dst []float32) {
	sinFloat32Impl(src, dst)
}

// CosFloat32 calculates the cosine of each element in src.
func CosFloat32(src, dst []float32) {
	cosFloat32Impl(src, dst)
}

// SincosFloat32 calculates the sine and cosine of each element in src.
func SincosFloat32(src, sinDst, cosDst []float32) {
	sincosFloat32Impl(src, sinDst, cosDst)
}

// SqrtFloat32 calculates the square root of each element in src.
func SqrtFloat32(src, dst []float32) {
	sqrtFloat32Impl(src, dst)
}

// Atan2Float32 calculates the arc tangent of y/x for each pair of elements.
func Atan2Float32(y, x, dst []float32) {
	atan2Float32Impl(y, x, dst)
}

// ManhattanDistance calculates the L1 distance between two vectors.
func ManhattanDistance(a, b []float32) (float32, error) {
	return manhattanDistanceImpl(a, b)
}

// ChebyshevDistance calculates the L-infinity distance between two vectors.
func ChebyshevDistance(a, b []float32) (float32, error) {
	return chebyshevDistanceImpl(a, b)
}

// BrayCurtisDistance calculates the Bray-Curtis distance between two vectors.
func BrayCurtisDistance(a, b []float32) (float32, error) {
	return brayCurtisDistanceImpl(a, b)
}

// MatMul performs matrix multiplication: dst = a * b
func MatMul(a, b []float32, m, n, k int, dst []float32) {
	matMulFloat32Impl(a, b, m, n, k, dst)
}

// HaversineBatch calculates the haversine distance between a center point and a batch of points.
func HaversineBatch(centerLat, centerLon float64, points []lbcore.GeoPoint, earthRadius float64, results []float32) {
	if haversineBatchImpl != nil {
		haversineBatchImpl(centerLat, centerLon, points, earthRadius, results)
	} else {
		haversineBatchGeneric(centerLat, centerLon, points, earthRadius, results)
	}
}
