//go:build arm64

#include "textflag.h"

// Macros for instructions not standard in all Go arm64 assemblers
#define VFADD_V(m, n, d) WORD $(0x4e20d400 | ((m) << 16) | ((n) << 5) | (d))
#define VFSUB_V(m, n, d) WORD $(0x4ea0d400 | ((m) << 16) | ((n) << 5) | (d))
#define VFMLA_V(m, n, d) WORD $(0x4e20cc00 | ((m) << 16) | ((n) << 5) | (d))

// USHLL Vd.8H, Vn.8B, #0 (zero-extend 8 uint8 to 8 uint16)
#define VUXTL_8H(n, d)   WORD $(0x2f08a400 | ((n) << 5) | (d))
// USHLL Vd.4S, Vn.4H, #0 (zero-extend lower 4 uint16 to 4 uint32)
#define VUXTL_4S(n, d)   WORD $(0x2f10a400 | ((n) << 5) | (d))
// USHLL2 Vd.4S, Vn.8H, #0 (zero-extend upper 4 uint16 to 4 uint32)
#define VUXTL2_4S(n, d)  WORD $(0x6f10a400 | ((n) << 5) | (d))
// UCVTF Vd.4S, Vn.4S (convert 4 uint32 to 4 float32)
#define VUCVTF_4S(n, d)  WORD $(0x6e21d800 | ((n) << 5) | (d))

// func dequantL2Float32Uint8NEONKernel(q, v unsafe.Pointer, n int, minV, scale float32) float32
TEXT ·dequantL2Float32Uint8NEONKernel(SB), NOSPLIT, $0-36
	MOVD    q+0(FP), R0
	MOVD    v+8(FP), R1
	MOVD    n+16(FP), R2

	FMOVS   minV+24(FP), F4
	VDUP    V4.S[0], V4.S4
	FMOVS   scale+28(FP), F5
	VDUP    V5.S[0], V5.S4

	VEOR    V6.B16, V6.B16, V6.B16
	VEOR    V7.B16, V7.B16, V7.B16
	FMOVS   $0.0, F0

	CMP     $8, R2
	BLT     tail

loop8:
	// Load 8 uint8 values
	VLD1.P  8(R1), [V0.B8]

	// Zero-extend 8 uint8 -> 8 uint16
	VUXTL_8H(0, 1)

	// Zero-extend to uint32 (lower and upper 4)
	VUXTL_4S(1, 2)
	VUXTL2_4S(1, 3)

	// Convert uint32 to float32
	VUCVTF_4S(2, 2)
	VUCVTF_4S(3, 3)

	// deq = minV + v_float * scale
	VORR    V4.B16, V4.B16, V8.B16
	VORR    V4.B16, V4.B16, V9.B16
	VFMLA_V(5, 2, 8)
	VFMLA_V(5, 3, 9)

	// Load 8 float32 values of q
	VLD1.P  16(R0), [V10.S4]
	VLD1.P  16(R0), [V11.S4]

	// diff = q - deq
	VFSUB_V(8, 10, 10)
	VFSUB_V(9, 11, 11)

	// sum += diff * diff
	VFMLA_V(10, 10, 6)
	VFMLA_V(11, 11, 7)

	SUB     $8, R2
	CMP     $8, R2
	BGE     loop8

reduce:
	VFADD_V(7, 6, 6)

	// Horizontal sum of V6.4S into F0
	VMOV    V6.S[1], V1.S[0]
	VMOV    V6.S[2], V2.S[0]
	VMOV    V6.S[3], V3.S[0]
	FADDS   F6, F1, F0
	FADDS   F2, F0, F0
	FADDS   F3, F0, F0

tail:
	CBZ     R2, done

tail_loop:
	MOVBU.P 1(R1), R3
	SCVTFWS R3, F1
	FMULS   F5, F1, F1
	FADDS   F4, F1, F1
	FMOVS.P 4(R0), F2
	FSUBS   F1, F2, F3
	FMULS   F3, F3, F3
	FADDS   F3, F0, F0
	SUB     $1, R2
	CBNZ    R2, tail_loop

done:
	FMOVS   F0, ret+32(FP)
	RET
