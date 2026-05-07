//go:build amd64
package simd

// Transcendental and Activation functions implemented in transcendental_amd64.s
func expAVX512Kernel(src uintptr, dst uintptr, n int)
func logAVX512Kernel(src uintptr, dst uintptr, n int)
func softmaxAVX512Kernel(src uintptr, dst uintptr, n int)
func sigmoidAVX512Kernel(src uintptr, dst uintptr, n int)

// Additional kernels
func dotInt4AVX512Kernel(a uintptr, b uintptr, n int) float32
func int8ToFloat32AVX512Kernel(src uintptr, dst uintptr, n int)
func uint8ToFloat32AVX512Kernel(src uintptr, dst uintptr, n int)
func int16ToFloat32AVX512Kernel(src uintptr, dst uintptr, n int)
func uint16ToFloat32AVX512Kernel(src uintptr, dst uintptr, n int)
func int32ToFloat32AVX512Kernel(src uintptr, dst uintptr, n int)
func uint32ToFloat32AVX512Kernel(src uintptr, dst uintptr, n int)
func float16ToFloat32AVX512Kernel(src uintptr, dst uintptr, n int)
