//go:build amd64 && emerald
// +build amd64,emerald

package simd

// Emerald Rapids AMX-accelerated kernel Go wrappers.
// These use Intel AMX (Advanced Matrix Extensions) tile operations
// for high-throughput INT8/BF16 matrix math.
//
// Real AMX assembly kernels (TDPBSSD, TDPBF16PS, etc.) should be
// implemented in emerald_amd64.s when AMX assembly is available.
// Until then, these delegate to generic implementations.
// At runtime, the "emerald" dispatch is selected only on CPUs with
// AMX hardware (which also have AVX512), so generic fallback is
// functionally correct though not yet optimal.

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
