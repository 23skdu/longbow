//go:build amd64

#include "textflag.h"

// func temporalLowerBoundAVX2Asm(ts []int64, x int64) int
//
// Returns the index of the first element of the ascending slice ts that is >=
// x, or len(ts) when every element is < x. This is the lower bound used by the
// columnar temporal index to turn a timestamp range into a group range.
//
// Each probe loads four consecutive int64 timestamps into one YMM register and
// compares them against the broadcast needle with VPCMPGTQ, so four candidate
// positions are eliminated per 32-byte load instead of four dependent scalar
// loads. The comparison result is flipped against an all-ones register and
// folded into a predicate with VPTEST, which reports whether every probed lane
// was strictly below the needle; when it does, the whole probe window is
// discarded in one step. The invariant maintained across iterations is that the
// answer lies in the half-open window [base, base+len], and it is written in
// bytes so the probe address is a single LEA.
//
// R8  = byte offset of the current window start
// R9  = length of the current window, in elements
// R10 = half = (len/2) rounded down to a multiple of 4
// R11 = scratch address
// R12 = scratch offset
// R13 = tail remaining
// R14 = scratch
TEXT ·temporalLowerBoundAVX2Asm(SB), NOSPLIT, $0-40
	MOVQ ts_base+0(FP), SI
	MOVQ ts_len+8(FP), R9
	MOVQ x+24(FP), AX

	MOVQ     AX, X0
	VPBROADCASTQ X0, Y0
	VPXOR      Y3, Y3, Y3
	VPCMPEQD   Y3, Y3, Y4

	XORQ R8, R8

loop:
	CMPQ R9, $8
	JL   tail

	MOVQ R9, R10
	SHRQ $1, R10
	ANDQ $-4, R10

	LEAQ (R8)(R10*8), R11
	ADDQ SI, R11
	SUBQ $32, R11
	VMOVDQU  (R11), Y1
	VPCMPGTQ Y1, Y0, Y2
	VPXOR    Y2, Y4, Y2
	VPTEST   Y2, Y2
	JEQ      all_less

	MOVQ R10, R9
	JMP  loop

all_less:
	LEAQ (R8)(R10*8), R8
	SUBQ R10, R9
	JMP  loop

tail:
	LEAQ (SI)(R8*1), R11
	MOVQ R9, R13

tail_loop:
	TESTQ R13, R13
	JEQ   not_found
	MOVQ  (R11), R14
	CMPQ  R14, AX
	JGE   found
	ADDQ  $8, R11
	DECQ  R13
	JMP   tail_loop

found:
	MOVQ R11, R14
	SUBQ SI, R14
	SHRQ $3, R14
	MOVQ R14, ret+32(FP)
	VZEROUPPER
	RET

not_found:
	LEAQ (R8)(R9*8), R14
	SHRQ    $3, R14
	MOVQ    R14, ret+32(FP)
	VZEROUPPER
	RET
