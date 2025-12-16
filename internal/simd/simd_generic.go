//go:build !amd64 && !arm64

package simd

// Generic fallback for unsupported architectures
// All functions delegate to generic implementations

func euclideanAVX2(a, b []float32) float32   { return euclideanGeneric(a, b) }
func euclideanAVX512(a, b []float32) float32 { return euclideanGeneric(a, b) }
func euclideanNEON(a, b []float32) float32   { return euclideanGeneric(a, b) }
func cosineAVX2(a, b []float32) float32      { return cosineGeneric(a, b) }
func cosineAVX512(a, b []float32) float32    { return cosineGeneric(a, b) }
func cosineNEON(a, b []float32) float32      { return cosineGeneric(a, b) }
func dotAVX2(a, b []float32) float32         { return dotGeneric(a, b) }
func dotAVX512(a, b []float32) float32       { return dotGeneric(a, b) }
func dotNEON(a, b []float32) float32         { return dotGeneric(a, b) }
