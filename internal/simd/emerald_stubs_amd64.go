//go:build amd64 && !emerald
// +build amd64,!emerald

package simd

// Emerald stubs for systems without the emerald build tag.
// These fall back to generic implementations.

func euclideanAMX(a, b []float32) (float32, error) {
	return euclideanGeneric(a, b)
}

func dotAMX(a, b []float32) (float32, error) {
	return dotGeneric(a, b)
}

func l2SquaredAMX(a, b []float32) (float32, error) {
	return L2SquaredFloat32(a, b)
}

func matMulAMX(a, b []float32, m, n, k int, dst []float32) {
	matMulGeneric(a, b, m, n, k, dst)
}

func euclideanBatchAMX(query []float32, vectors [][]float32, results []float32) error {
	return euclideanBatchGeneric(query, vectors, results)
}

func dotBatchAMX(query []float32, vectors [][]float32, results []float32) error {
	return dotBatchGeneric(query, vectors, results)
}
