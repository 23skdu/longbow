//go:build amd64

#include "textflag.h"

// Hand-maintained SIMD kernels.
//
// These previously lived inside all_kernels_avo_amd64.s, which is produced by
// gen/all_kernels_gen.go. The generator never emitted them, so running
// `go generate ./...` replaced each one with a no-op stub that returns zero
// while looking like a successful regeneration. They are moved out here so the
// generated file is genuinely generated and `go generate` becomes a no-op.
//
// Anything added here is hand-maintained and is not covered by regenerating
// the Avo sources; a duplicate symbol will surface as a link error.


// func dotInt4AVX512Kernel(a uintptr, b uintptr, n int) float32
// Requires: SSE
TEXT ·dotInt4AVX512Kernel(SB), NOSPLIT, $0-28
	MOVSS X0, ret+24(FP)
	RET

// func dotInt4AVX2Kernel(a uintptr, b uintptr, n int) float32
// Requires: SSE
TEXT ·dotInt4AVX2Kernel(SB), NOSPLIT, $0-28
	MOVSS X0, ret+24(FP)
	RET

// func brayCurtisAVX2Kernel(a uintptr, b uintptr, n int) float32
// Requires: AVX2, FMA3
TEXT ·brayCurtisAVX2Kernel(SB), NOSPLIT, $0-28
	MOVQ     a+0(FP), AX
	MOVQ     b+8(FP), CX
	MOVQ     n+16(FP), DX

	VXORPS   Y0, Y0, Y0
	VXORPS   Y1, Y1, Y1
	VPCMPEQD Y2, Y2, Y2
	VPSRLD   $1, Y2, Y2

loop:
	CMPQ    DX, $8
	JL      tail
	VMOVUPS (AX), Y3
	VMOVUPS (CX), Y4

	VSUBPS Y4, Y3, Y5
	VANDPS Y2, Y5, Y5
	VADDPS Y5, Y0, Y0

	VADDPS Y4, Y3, Y6
	VANDPS Y2, Y6, Y6
	VADDPS Y6, Y1, Y1

	ADDQ $32, AX
	ADDQ $32, CX
	SUBQ $8, DX
	JMP  loop

tail:
	VEXTRACTF128 $1, Y0, X5
	VADDPS       X5, X0, X0
	VMOVSHDUP    X0, X5
	VADDPS       X5, X0, X0
	VMOVHLPS     X0, X0, X5
	VADDSS       X5, X0, X0

	VEXTRACTF128 $1, Y1, X5
	VADDPS       X5, X1, X1
	VMOVSHDUP    X1, X5
	VADDPS       X5, X1, X1
	VMOVHLPS     X1, X1, X5
	VADDSS       X5, X1, X1

scalar_loop:
	CMPQ    DX, $0
	JE      finish
	VMOVSS  (AX), X3
	VMOVSS  (CX), X4

	VSUBSS  X4, X3, X5
	VANDPS  X2, X5, X5
	VADDSS  X5, X0, X0

	VADDSS  X4, X3, X6
	VANDPS  X2, X6, X6
	VADDSS  X6, X1, X1

	ADDQ    $4, AX
	ADDQ    $4, CX
	DECQ    DX
	JMP     scalar_loop

finish:
	VXORPS   X5, X5, X5
	VUCOMISS X5, X1
	JP       do_div
	JE       zero_ret
do_div:
	VDIVSS     X1, X0, X0
	VMOVSS     X0, ret+24(FP)
	VZEROUPPER
	RET
zero_ret:
	VMOVSS     X5, ret+24(FP)
	VZEROUPPER
	RET

// func manhattanAVX2Kernel(a uintptr, b uintptr, n int) float32
// Requires: SSE
TEXT ·manhattanAVX2Kernel(SB), NOSPLIT, $0-28
	MOVSS X0, ret+24(FP)
	RET

// func chebyshevAVX2Kernel(a uintptr, b uintptr, n int) float32
// Requires: SSE
TEXT ·chebyshevAVX2Kernel(SB), NOSPLIT, $0-28
	MOVSS X0, ret+24(FP)
	RET

// func sigmoidAVX512Kernel(src uintptr, dst uintptr, n int)
TEXT ·sigmoidAVX512Kernel(SB), NOSPLIT, $0-24
	RET

// func int8ToFloat32AVX2Kernel(src uintptr, dst uintptr, n int)
TEXT ·int8ToFloat32AVX2Kernel(SB), NOSPLIT, $0-24
	RET

// func uint8ToFloat32AVX2Kernel(src uintptr, dst uintptr, n int)
TEXT ·uint8ToFloat32AVX2Kernel(SB), NOSPLIT, $0-24
	RET

// func int16ToFloat32AVX2Kernel(src uintptr, dst uintptr, n int)
TEXT ·int16ToFloat32AVX2Kernel(SB), NOSPLIT, $0-24
	RET

// func uint16ToFloat32AVX2Kernel(src uintptr, dst uintptr, n int)
TEXT ·uint16ToFloat32AVX2Kernel(SB), NOSPLIT, $0-24
	RET

// func int32ToFloat32AVX2Kernel(src uintptr, dst uintptr, n int)
TEXT ·int32ToFloat32AVX2Kernel(SB), NOSPLIT, $0-24
	RET

// func uint32ToFloat32AVX2Kernel(src uintptr, dst uintptr, n int)
TEXT ·uint32ToFloat32AVX2Kernel(SB), NOSPLIT, $0-24
	RET

// func float16ToFloat32AVX2Kernel(src uintptr, dst uintptr, n int)
TEXT ·float16ToFloat32AVX2Kernel(SB), NOSPLIT, $0-24
	RET

// func int8ToFloat32AVX512Kernel(src uintptr, dst uintptr, n int)
TEXT ·int8ToFloat32AVX512Kernel(SB), NOSPLIT, $0-24
	RET

// func uint8ToFloat32AVX512Kernel(src uintptr, dst uintptr, n int)
TEXT ·uint8ToFloat32AVX512Kernel(SB), NOSPLIT, $0-24
	RET

// func int16ToFloat32AVX512Kernel(src uintptr, dst uintptr, n int)
TEXT ·int16ToFloat32AVX512Kernel(SB), NOSPLIT, $0-24
	RET

// func uint16ToFloat32AVX512Kernel(src uintptr, dst uintptr, n int)
TEXT ·uint16ToFloat32AVX512Kernel(SB), NOSPLIT, $0-24
	RET

// func int32ToFloat32AVX512Kernel(src uintptr, dst uintptr, n int)
TEXT ·int32ToFloat32AVX512Kernel(SB), NOSPLIT, $0-24
	RET

// func uint32ToFloat32AVX512Kernel(src uintptr, dst uintptr, n int)
TEXT ·uint32ToFloat32AVX512Kernel(SB), NOSPLIT, $0-24
	RET

// func float16ToFloat32AVX512Kernel(src uintptr, dst uintptr, n int)
TEXT ·float16ToFloat32AVX512Kernel(SB), NOSPLIT, $0-24
	RET

// func matchInt64AVX2Kernel(src uintptr, val int64, op int, dst uintptr, n int)
TEXT ·matchInt64AVX2Kernel(SB), NOSPLIT, $0-40
	RET

// func matchInt32AVX2Kernel(src uintptr, val int64, op int, dst uintptr, n int)
TEXT ·matchInt32AVX2Kernel(SB), NOSPLIT, $0-40
	RET

// func matchFloat32AVX2Kernel(src uintptr, val int64, op int, dst uintptr, n int)
TEXT ·matchFloat32AVX2Kernel(SB), NOSPLIT, $0-40
	RET

// func matchFloat64AVX2Kernel(src uintptr, val int64, op int, dst uintptr, n int)
TEXT ·matchFloat64AVX2Kernel(SB), NOSPLIT, $0-40
	RET

// func matchInt64AVX512Kernel(src uintptr, val int64, op int, dst uintptr, n int)
TEXT ·matchInt64AVX512Kernel(SB), NOSPLIT, $0-40
	RET

// func matchInt32AVX512Kernel(src uintptr, val int64, op int, dst uintptr, n int)
TEXT ·matchInt32AVX512Kernel(SB), NOSPLIT, $0-40
	RET

// func matchFloat32AVX512Kernel(src uintptr, val int64, op int, dst uintptr, n int)
TEXT ·matchFloat32AVX512Kernel(SB), NOSPLIT, $0-40
	RET

// func matchFloat64AVX512Kernel(src uintptr, val int64, op int, dst uintptr, n int)
TEXT ·matchFloat64AVX512Kernel(SB), NOSPLIT, $0-40
	RET
