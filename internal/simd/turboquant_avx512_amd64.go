//go:build amd64

package simd

import "unsafe" // #nosec G103

// AVX-512 specialized implementations

// AVX-512 stubs
func UnpackTQ2AVX512(src []byte, dst []float32, scale, bias float32) {
	UnpackTQ2AVX2(src, dst, scale, bias)
}
func UnpackTQ4AVX512(src []byte, dst []float32, scale, bias float32) {
	UnpackTQ4AVX2(src, dst, scale, bias)
}
func UnpackTQ8AVX512(src []byte, dst []float32, scale, bias float32) {
	UnpackTQ8AVX2(src, dst, scale, bias)
}
func PackTQ2AVX512(src []float32, dst []byte) {
	if len(src) == 0 {
		return
	}
	if !features.HasAVX512 {
		PackTQ2AVX2(src, dst)
		return
	}
	packTQ2AVX512Kernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(src)) // #nosec G103
}

func PackTQ4AVX512(src []float32, dst []byte) {
	if len(src) == 0 {
		return
	}
	if !features.HasAVX512 {
		PackTQ4AVX2(src, dst)
		return
	}
	packTQ4AVX512Kernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(src)) // #nosec G103
}

func PackTQ8AVX512(src []float32, dst []byte) {
	if len(src) == 0 {
		return
	}
	if !features.HasAVX512 {
		PackTQ8AVX2(src, dst)
		return
	}
	packTQ8AVX512Kernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(src)) // #nosec G103
}

// UnpackTQ2AVX512VBMI decodes a 2-bit TurboQuant payload.
//
// Like PackTQ2AVX512VBMI, the VBMI kernel is not dispatched. It disagrees with
// UnpackTQ2Generic on bit order: byte 0x01 decodes to -3.1415927, which is code
// 0, where the generic unpacker decodes it to -1.0471976, which is code 1. The
// packer and the unpacker are two halves of one format, so leaving one of them
// correct and the other not would be worse than leaving both alone.
//
// The delegation is to UnpackTQ2 rather than to UnpackTQ2AVX512: the raw
// AVX-512 unpack kernel disagrees with UnpackTQ2Generic on bit order for 172 of
// 256 elements, and UnpackTQ2 - the entry point the index actually uses - agrees
// with generic, so it is the reference for what these kernels are supposed to
// mean. None of these three symbols is called from production code; they are
// reachable only from tests, which is why the disagreement went unnoticed.
//
// See the note on PackTQ2AVX512VBMI and docs/roadmap.md item 10.
func UnpackTQ2AVX512VBMI(src []byte, dst []float32, scale, bias float32) {
	if len(dst) == 0 {
		return
	}
	UnpackTQ2(src, dst, scale, bias)
}

// PackTQ2AVX512VBMI packs via VPMULTISHIFTQB.
//
// The VBMI kernel is not used. It disagrees with PackTQ2Generic - the reference
// definition of the 2-bit code - for every length the parity test covers, and
// nothing caught that because no test compared the two: the only test that
// touched this packer round-tripped it through UnpackTQ2AVX512VBMI with a
// tolerance of 2.0, while adjacent 2-bit codes are 2*PI/3 = 2.094 apart, so a
// packer that is one code off everywhere passes it.
//
// The defect was found by scripts/check_avx512_coverage.sh, which runs
// internal/simd under Intel SDE with an emulated Ice Lake / Sapphire Rapids
// (docs/roadmap.md item 5). Before that lane existed these kernels had never
// executed anywhere in CI, because every test that guards on HasVBMI skips on a
// standard runner.
//
// PackTQ2 is dispatched instead: it is the entry point the index actually uses
// and it has always been checked against the generic packer. Fixing the VBMI kernel is tracked as docs/roadmap.md item 10; when it
// is fixed this function goes back to dispatching the kernel and
// TestPackTQ2AVX512VBMIMatchesGeneric is what says so.
//
// TODO(roadmap-10): restore packTQ2AVX512VBMIKernel once it matches generic.
func PackTQ2AVX512VBMI(src []float32, dst []byte) {
	if len(src) == 0 {
		return
	}
	PackTQ2(src, dst)
}

// Assembly kernel stubs

//go:noescape
func unpackTQ2AVX512VBMIKernel(src, dst unsafe.Pointer, n int, scale, bias float32) // #nosec G103

//go:noescape
func packTQ2AVX512VBMIKernel(src, dst unsafe.Pointer, n int) // #nosec G103

var (
	_ = unpackTQ2AVX512VBMIKernel
	_ = packTQ2AVX512VBMIKernel
)

//go:noescape
func packTQ2AVX512Kernel(src, dst unsafe.Pointer, n int) // #nosec G103

//go:noescape
func packTQ4AVX512Kernel(src, dst unsafe.Pointer, n int) // #nosec G103

//go:noescape
func packTQ8AVX512Kernel(src, dst unsafe.Pointer, n int) // #nosec G103
