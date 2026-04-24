//go:build !amd64
// +build !amd64

package simd

func dotInt4AVX512(a, b []byte) (float32, error) { return dotInt4Generic(a, b) }
func dotInt4AVX2(a, b []byte) (float32, error)   { return dotInt4Generic(a, b) }
func dotInt2AVX512(a, b []byte) (float32, error) { return dotInt2Generic(a, b) }
func dotInt2AVX2(a, b []byte) (float32, error)   { return dotInt2Generic(a, b) }

func euclideanAVX2(a, b []float32) (float32, error) { return euclideanUnrolled4x(a, b) }
func l2SquaredAVX2(a, b []float32) (float32, error) { return L2SquaredFloat32(a, b) }
func euclidean384AVX2(a, b []float32) (float32, error) { return euclideanUnrolled4x(a, b) }
func euclidean768AVX2(a, b []float32) (float32, error) { return euclideanUnrolled4x(a, b) }
func euclidean1536AVX2(a, b []float32) (float32, error) { return euclideanUnrolled4x(a, b) }
func cosineAVX2(a, b []float32) (float32, error) { return cosineUnrolled4x(a, b) }
func dotAVX2(a, b []float32) (float32, error) { return dotUnrolled4x(a, b) }
func euclideanBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	return euclideanBatchUnrolled4x(query, vectors, results)
}
func dotBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	return dotBatchUnrolled4x(query, vectors, results)
}
func cosineBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	return cosineBatchUnrolled4x(query, vectors, results)
}
func adcBatchAVX2(table []float32, flatCodes []byte, m int, results []float32) error {
	return adcBatchGeneric(table, flatCodes, m, results)
}
