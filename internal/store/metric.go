package store

// DistanceMetric and constants are now aliases from internal/core
// See internal/store/type_aliases.go

// DistanceFunc is the function signature for calculating distance between two vectors.
type DistanceFunc func(a, b []float32) float32

// BatchDistanceFunc is the function signature for calculating distances between one query and multiple vectors.
type BatchDistanceFunc func(query []float32, vectors [][]float32, results []float32)

// BatchDistanceSQ8Func is the function signature for SQ8 batch distance.
type BatchDistanceSQ8Func func(query []byte, vectors [][]byte, results []float32)
