//go:build !amd64
// +build !amd64

package simd

func dotInt4AVX512(a, b []byte) (float32, error) { return dotInt4Generic(a, b) }
func dotInt4AVX2(a, b []byte) (float32, error)   { return dotInt4Generic(a, b) }
func dotInt2AVX512(a, b []byte) (float32, error) { return dotInt2Generic(a, b) }
func dotInt2AVX2(a, b []byte) (float32, error)   { return dotInt2Generic(a, b) }
