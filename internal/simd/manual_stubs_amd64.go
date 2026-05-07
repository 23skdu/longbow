//go:build amd64
package simd

// Transcendental and Activation functions implemented in transcendental_amd64.s
//go:noescape
func expAVX512Kernel(src uintptr, dst uintptr, n int)
//go:noescape
func logAVX512Kernel(src uintptr, dst uintptr, n int)
//go:noescape
func softmaxAVX512Kernel(src uintptr, dst uintptr, n int)
//go:noescape
func sigmoidAVX512Kernel(src uintptr, dst uintptr, n int)

// Quantization kernels (if not in all_kernels_stubs_amd64.go)
//go:noescape
func dotInt4AVX512Kernel(a uintptr, b uintptr, n int) float32
