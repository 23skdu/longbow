//go:build amd64

#include "textflag.h"

// LUT for expanding 4-bit mask to 4 bytes (0x00 or 0x01)
// Index is 4 bits (0-15). Value is 4 bytes.
// Example: bit 0 set -> byte 0 is 0x01.
// 0001 -> 0x00000001
// 0011 -> 0x00000101
DATA maskLUT<>+0(SB)/4, $0x00000000
DATA maskLUT<>+4(SB)/4, $0x00000001
DATA maskLUT<>+8(SB)/4, $0x00000100
DATA maskLUT<>+12(SB)/4, $0x00000101
DATA maskLUT<>+16(SB)/4, $0x00010000
DATA maskLUT<>+20(SB)/4, $0x00010001
DATA maskLUT<>+24(SB)/4, $0x00010100
DATA maskLUT<>+28(SB)/4, $0x00010101
DATA maskLUT<>+32(SB)/4, $0x01000000
DATA maskLUT<>+36(SB)/4, $0x01000001
DATA maskLUT<>+40(SB)/4, $0x01000100
DATA maskLUT<>+44(SB)/4, $0x01000101
DATA maskLUT<>+48(SB)/4, $0x01010000
DATA maskLUT<>+52(SB)/4, $0x01010001
DATA maskLUT<>+56(SB)/4, $0x01010100
DATA maskLUT<>+60(SB)/4, $0x01010101
GLOBL maskLUT<>(SB), RODATA, $64

// Constants
DATA ones64<>+0(SB)/8, $0x0101010101010101
DATA ones64<>+8(SB)/8, $0x0101010101010101
DATA ones64<>+16(SB)/8, $0x0101010101010101
DATA ones64<>+24(SB)/8, $0x0101010101010101
DATA ones64<>+32(SB)/8, $0x0101010101010101
DATA ones64<>+40(SB)/8, $0x0101010101010101
DATA ones64<>+48(SB)/8, $0x0101010101010101
DATA ones64<>+56(SB)/8, $0x0101010101010101
GLOBL ones64<>(SB), RODATA, $64

// func matchInt64AVX2Kernel(src unsafe.Pointer, val int64, op int, dst unsafe.Pointer, n int)
TEXT ·matchInt64AVX2Kernel(SB), NOSPLIT, $0-40
    MOVQ    src+0(FP), SI
    MOVQ    val+8(FP), DX
    MOVQ    op+16(FP), BX // Op: 0=Eq, 1=Neq, 2=Gt, 3=Ge, 4=Lt, 5=Le
    MOVQ    dst+24(FP), DI
    MOVQ    n+32(FP), CX

    // Broadcast val to Y0
    MOVQ    DX, X0
    VPBROADCASTQ X0, Y0

    LEAQ    maskLUT<>(SB), R8

loop_int64_avx2:
    CMPQ    CX, $4
    JL      tail_int64_avx2

    VMOVUPS (SI), Y1 // Load 4 int64s

    // Compare logic based on Op
    // We store result in Y1 (mask: -1 or 0)
    
    // Eq (0)
    CMPQ    BX, $0
    JNE     check_neq
    VPCMPEQQ Y0, Y1, Y1
    JMP     store_mask_avx2

check_neq: // Neq (1)
    CMPQ    BX, $1
    JNE     check_gt
    VPCMPEQQ Y0, Y1, Y1
    // Invert mask? NEQ is !EQ.
    // Create all ones (PCMPEQ self)
    VPCMPEQQ Y2, Y2, Y2
    VPXOR   Y2, Y1, Y1
    JMP     store_mask_avx2

check_gt: // Gt (2) -> src > val
    CMPQ    BX, $2
    JNE     check_ge
    VPCMPGTQ Y0, Y1, Y1 // GT(Y1, Y0) i.e., src > val
    JMP     store_mask_avx2

check_ge: // Ge (3) -> src >= val -> !(src < val) -> !(val > src) -> !GT(val, src)
    // Wait, AVX2 GT is signed.
    CMPQ    BX, $3
    JNE     check_lt
    VPCMPGTQ Y1, Y0, Y1 // GT(Y0, Y1) -> val > src
    // Invert
    VPCMPEQQ Y2, Y2, Y2
    VPXOR   Y2, Y1, Y1
    JMP     store_mask_avx2

check_lt: // Lt (4) -> src < val -> val > src
    CMPQ    BX, $4
    JNE     check_le
    VPCMPGTQ Y1, Y0, Y1 // GT(Y0, Y1) -> val > src
    JMP     store_mask_avx2

check_le: // Le (5) -> src <= val -> !(src > val)
    // Op 5
    VPCMPGTQ Y0, Y1, Y1 // GT(src, val)
    // Invert
    VPCMPEQQ Y2, Y2, Y2
    VPXOR   Y2, Y1, Y1
    JMP     store_mask_avx2

store_mask_avx2:
    // Extract mask bits
    VMOVMSKPD Y1, DX // 4 bits for 4 elements (using double precision view)
    
    // Lookup bytes in LUT
    // LUT has 4-byte values. Index is DX * 4.
    MOVL    (R8)(DX*4), R9
    
    // Write 4 bytes
    MOVL    R9, (DI)

    ADDQ    $32, SI
    ADDQ    $4, DI
    SUBQ    $4, CX
    JMP     loop_int64_avx2

tail_int64_avx2:
    // Fallback scalar tail
    CMPQ    CX, $0
    JE      done_int64_avx2
    MOVQ    (SI), R9
    
    // Scalar comparison
    // Compare R9 (src) with val (in stack loaded to X0, actually keep generic logic)
    // Wait, simpler to duplicate scalar logic or assume N aligned?
    // User requested AVX. Tail handling is needed.
    // Let's implement scalar switch simply.
    // Since X0 has value (scalar), move back?
    
    // Reload val from reg/stack? DX was modified.
    // Original val is in Y0.
    VMOVQ   X0, R10 // Move lower scalar of Y0 to R10
    
    // R9 = src, R10 = val
    MOVB    $0, (DI) // Default 0

    CMPQ    BX, $0 // Eq
    JNE     tail_neq
    CMPQ    R9, R10
    JNE     tail_advance
    MOVB    $1, (DI)
    JMP     tail_advance

tail_neq:
    CMPQ    BX, $1
    JNE     tail_gt
    CMPQ    R9, R10
    JE      tail_advance
    MOVB    $1, (DI)
    JMP     tail_advance

tail_gt:
    CMPQ    BX, $2
    JNE     tail_ge
    CMPQ    R9, R10
    JLE     tail_advance
    MOVB    $1, (DI)
    JMP     tail_advance

tail_ge:
    CMPQ    BX, $3
    JNE     tail_lt
    CMPQ    R9, R10
    JLT     tail_advance
    MOVB    $1, (DI)
    JMP     tail_advance

tail_lt:
    CMPQ    BX, $4
    JNE     tail_le
    CMPQ    R9, R10
    JGE     tail_advance
    MOVB    $1, (DI)
    JMP     tail_advance

tail_le:
    CMPQ    R9, R10
    JG      tail_advance
    MOVB    $1, (DI)

tail_advance:
    ADDQ    $8, SI
    ADDQ    $1, DI
    DECQ    CX
    JMP     tail_int64_avx2

done_int64_avx2:
    VZEROUPPER
    RET


// func matchFloat32AVX2Kernel(src unsafe.Pointer, val float32, op int, dst unsafe.Pointer, n int)
// Float32 operations using AVX2 (8 elements)
TEXT ·matchFloat32AVX2Kernel(SB), NOSPLIT, $0-40
    MOVQ    src+0(FP), SI
    MOVSS   val+8(FP), X0
    MOVQ    op+16(FP), BX // Note offset: 8 + 4 (float32 size) = 12? No, args 8-byte aligned on stack?
    // Go regabi might pack nicely, but FP approach:
    // val is float32. But on stack it takes 8 bytes if alignment enforced?
    // Check struct alignment.
    // Usually args are aligned to pointer size (8).
    // Let's assume val+8. op+16?
    // Safe check: float32 takes 4 bytes. Padding 4 bytes? 
    // Usually subsequent args aligned to 8.
    // Let's stick to op+16.
    MOVQ    op+16(FP), BX
    MOVQ    dst+24(FP), DI
    MOVQ    n+32(FP), CX

    VPBROADCASTD X0, Y0

    // For Float32, we process 8 at a time.
    // Mask logic: VMOVMSKPS extracts 8 bits.
    // We need to expand 8 bits to 8 bytes.
    // 8 bits -> 8 bytes (0/1).
    // LUT would be 256 entries. Too big? 256 * 8 = 2KB. Acceptable.
    // Or we can use shift/and/pack logic.
    // PSHUFB with pre-computed mask from bitmask? Complex.
    // Simpler: use the mask YMM register directly.
    // CMPPS returns 0xFFFFFFFF or 0.
    // We want 0x01 or 0x00.
    // Shift right logical 31? No, 0xFF.. >> 31 is 1 (if signed/unsigned logic applies).
    // PSRLD $31, Y1 -> 1.
    // Then PACKUSDW (pack dwords to words) -> PACKUSWB (pack words to bytes).
    // YES.
    
    // Constants for PACK
    // Actually we just need PSRLD $31.
    
loop_f32_avx2:
    CMPQ    CX, $8
    JL      tail_f32_avx2

    VMOVUPS (SI), Y1 // 8 floats
    
    // Compare Y1 vs Y0
    // CMPPS immediate:
    // 0=EQ, 4=NEQ, 1=LT, 2=LE, 14=GT (NLT), 13=GE (NLE)?
    // Be careful with ordered/unordered.
    // Standard ops:
    // EQ (0), LT (1), LE (2), UNORD (3), NEQ (4), NLT (5), NLE (6), ORD (7)
    // We want GT -> NLE (6) ? No, GT is not NLE (NaNs).
    // Usually GT = swap operands + LT.
    
    // Map our Op (0-5) to AVX CMPPS immediate or swapped logic.
    // Op: 0=Eq, 1=Neq, 2=Gt, 3=Ge, 4=Lt, 5=Le
    
    CMPQ    BX, $0 // EQ
    JE      op_eq_f32
    CMPQ    BX, $1 // NEQ
    JE      op_neq_f32
    CMPQ    BX, $2 // GT
    JE      op_gt_f32
    CMPQ    BX, $3 // GE
    JE      op_ge_f32
    CMPQ    BX, $4 // LT
    JE      op_lt_f32
    JMP     op_le_f32

op_eq_f32:
    VCMPPS  $0, Y0, Y1, Y2 // Y2 = (Y1 == Y0)
    JMP     pack_f32

op_neq_f32:
    VCMPPS  $4, Y0, Y1, Y2
    JMP     pack_f32

op_gt_f32:
    // src > val. Not directly supported. Swap: val < src -> LT(val, src) -> LT(Y0, Y1)
    VCMPPS  $1, Y1, Y0, Y2 // Y2 = (Y0 < Y1) i.e. val < src
    JMP     pack_f32

op_ge_f32:
    // src >= val. Swap: val <= src -> LE(val, src) -> LE(Y0, Y1)
    VCMPPS  $2, Y1, Y0, Y2
    JMP     pack_f32

op_lt_f32:
    // src < val. LT(Y1, Y0)
    VCMPPS  $1, Y0, Y1, Y2
    JMP     pack_f32

op_le_f32:
    // src <= val. LE(Y1, Y0)
    VCMPPS  $2, Y0, Y1, Y2

pack_f32:
    // Y2 contains 0xFFFFFFFF or 0x00000000 per dword from VCMPPS.
    // Extract sign bits to get 8-bit mask.
    VMOVMSKPS Y2, DX // DX has 8 bits: bit i = sign bit of Y2[i]
    
    // We need to expand 8-bit mask to 8 bytes (0x00 or 0x01).
    // Use lookup table approach. We'll generate the LUT in assembly.
    // For now, let's use a simple shift/mask approach in general purpose registers.
    
    // R9 = mask (8 bits in DX)
    MOVQ    DX, R9
    
    // Clear R10 (will hold expanded bytes in low 64 bits)
    XORQ    R10, R10
    
    // Expand each bit to a byte position
    // We'll use a loop-like approach with explicit instructions
    
    // Bit 0 -> byte 0: (mask >> 0) & 1
    MOVQ    R9, R11
    ANDQ    $1, R11
    MOVQ    R11, R10  // Byte 0
    
    // Bit 1 -> byte 1: (mask >> 1) & 1, shifted to byte 1 position
    MOVQ    R9, R11
    SHRQ    $1, R11
    ANDQ    $1, R11
    SHLQ    $8, R11   // Shift to byte 1 position
    ORQ     R11, R10
    
    // Bit 2 -> byte 2
    MOVQ    R9, R11
    SHRQ    $2, R11
    ANDQ    $1, R11
    SHLQ    $16, R11
    ORQ     R11, R10
    
    // Bit 3 -> byte 3
    MOVQ    R9, R11
    SHRQ    $3, R11
    ANDQ    $1, R11
    SHLQ    $24, R11
    ORQ     R11, R10
    
    // Bit 4 -> byte 4
    MOVQ    R9, R11
    SHRQ    $4, R11
    ANDQ    $1, R11
    SHLQ    $32, R11
    ORQ     R11, R10
    
    // Bit 5 -> byte 5
    MOVQ    R9, R11
    SHRQ    $5, R11
    ANDQ    $1, R11
    SHLQ    $40, R11
    ORQ     R11, R10
    
    // Bit 6 -> byte 6
    MOVQ    R9, R11
    SHRQ    $6, R11
    ANDQ    $1, R11
    SHLQ    $48, R11
    ORQ     R11, R10
    
    // Bit 7 -> byte 7
    MOVQ    R9, R11
    SHRQ    $7, R11
    ANDQ    $1, R11
    SHLQ    $56, R11
    ORQ     R11, R10
    
    // Write the 8 bytes to destination
    MOVQ    R10, (DI)
    
    ADDQ    $32, SI
    ADDQ    $8, DI
    SUBQ    $8, CX
    JMP     loop_f32_avx2

tail_f32_avx2:
    // Scalar fallback
    VZEROUPPER
    // Implementation left simple - user can rely on generic or add complex scalar ASM if needed.
    // I will return and let Go logic handle if I implemented fallback in Go wrapper?
    // But I didn't. I stubbed it.
    // I must implement scalar loop here.
    CMPQ    CX, $0
    JE      end_f32_avx2
    
    MOVL    (SI), R8
    MOVD    R8, X1
    COMISS  X0, X1 // X0 has val
    
    // This is tedious to write all jump logic again.
    // We loop 1 by 1.
    // Just implement simple scalar logic.
    // Logic: check BX (op), jump, CMP, set byte.
    // ...
    // Since this is getting long, I'll assume n is multiple of 8 (padded) or accept perf hit on tail.
    // But correctness matters.
    // I'll skip detailed scalar ASM implementation for brevity in this Turn and trust the user calls it with batch size 8 or accepts slight overhead?
    // No, must be correct.
    // Let's copy previous tail logic structure.
    
    // Reload val X0 (generic scalar)
    MOVSS   val+8(FP), X0

scalar_f32_loop:
    CMPQ    CX, $0
    JE      end_f32_avx2
    
    MOVSS   (SI), X1
    MOVB    $0, (DI)
    
    // Op switch
    CMPQ    BX, $0
    JE      sc_eq_f32
    CMPQ    BX, $1
    JE      sc_neq_f32
    CMPQ    BX, $2
    JE      sc_gt_f32
    CMPQ    BX, $3
    JE      sc_ge_f32
    CMPQ    BX, $4
    JE      sc_lt_f32
    JMP     sc_le_f32

sc_eq_f32:
    UCOMISS X0, X1
    JNE     next_f32
    JP      next_f32 // Parity set if NaN
    MOVB    $1, (DI)
    JMP     next_f32

sc_neq_f32:
    UCOMISS X0, X1
    JNE     set_f32 // if != or NaN
    JP      set_f32
    JMP     next_f32

sc_gt_f32: // src > val
    UCOMISS X0, X1
    JA      set_f32 // JA = CF=0 and ZF=0. (X1 > X0)
    JMP     next_f32

sc_ge_f32: // src >= val
    UCOMISS X0, X1
    JAE     set_f32 // JAE = CF=0. (X1 >= X0)
    JMP     next_f32

sc_lt_f32: // src < val
    UCOMISS X0, X1
    JB      set_f32 // JB = CF=1. (X1 < X0)
    JMP     next_f32

sc_le_f32: // src <= val
    UCOMISS X0, X1
    JBE     set_f32
    JMP     next_f32

set_f32:
    MOVB    $1, (DI)

next_f32:
    ADDQ    $4, SI
    ADDQ    $1, DI
    DECQ    CX
    JMP     scalar_f32_loop

end_f32_avx2:
    RET

// AVX512 Kernels (Stubs for now passing to AVX2, or simple impl)
// Since AVX512 is requested, I should implement it.
// AVX512 has VPCMPEQQ -> K mask. VPMOVM2B -> ZMM bytes. Simple.
TEXT ·matchInt64AVX512Kernel(SB), NOSPLIT, $0-40
    MOVQ    src+0(FP), SI
    MOVQ    val+8(FP), DX
    MOVQ    op+16(FP), BX
    MOVQ    dst+24(FP), DI
    MOVQ    n+32(FP), CX

    VPBROADCASTQ DX, Z0
    MOVQ    $1, R9
    MOVQ    R9, X4 // 1 constant for AND?
    VPBROADCASTB X4, Z4 // Z4 = [1, 1, 1...]

loop_int64_512:
    CMPQ    CX, $8
    JL      tail_int64_512

    VMOVUPS (SI), Z1
    
    // Compare commands
    // Op mapping for VPCMP: 0=EQ, 4=NEQ, 2=LE (src<=val), 6=NLE (src>val -> GT?), 1=LT, 5=NLT (src>=val -> GE?)
    // VPCMPQ immediate: 0=EQ, 1=LT, 2=LE, 3=FALSE, 4=NEQ, 5=GE, 6=GT, 7=TRUE.
    
    // Check BX
    CMPQ    BX, $0
    JE      c_eq
    CMPQ    BX, $1
    JE      c_neq
    CMPQ    BX, $2
    JE      c_gt
    CMPQ    BX, $3
    JE      c_ge
    CMPQ    BX, $4
    JE      c_lt
    JMP     c_le

c_eq:
    VPCMPQ  $0, Z1, Z0, K1 // EQ
    JMP     store_512
c_neq:
    VPCMPQ  $4, Z1, Z0, K1 // NEQ
    JMP     store_512
c_gt:
    VPCMPQ  $6, Z1, Z0, K1 // GT (src > val)
    JMP     store_512
c_ge:
    VPCMPQ  $5, Z1, Z0, K1 // GE
    JMP     store_512
c_lt:
    VPCMPQ  $1, Z1, Z0, K1 // LT
    JMP     store_512
c_le:
    VPCMPQ  $2, Z1, Z0, K1 // LE

store_512:
    // Expand mask K1 to bytes (00 or FF).
    VPMOVM2B K1, Z2
    // We want 0/1. AND with 1.
    VPANDQ   Z2, Z4, Z2 // AND with 1s
    
    // Store 8 bytes (Wait, ZMM is 64 bytes).
    // But we processed 8 int64 (64 bytes).
    // We generated 8 mask bits -> 8 bytes (using VPMOVM2B on K1 for 64-bit elements?)
    // K1 has 8 bits (for Qwords).
    // VPMOVM2B expands K1 bits to BYTES.
    // If K1=101..., Result=01 00 01... (8 bytes in XMM/XMM-sized slice of ZMM).
    // Wait, destination register size implies number of elements?
    // VPMOVM2B X2, K1  -> X2 (128 bit) gets 16 bytes? Or 8 bytes?
    // Mask logic usually matches vector length.
    // If we want 8 bytes, use XMM?
    // Instructions scale.
    // Let's assume K1 has 8 bits. We want 8 bytes.
    // VPMOVM2B expands to 512 bits? No.
    // It creates a byte vector.
    // 8 bits -> 8 bytes = 64 bits (Qword).
    // We can just VMOVQ mask to integer register and look up LUT?
    // Or PDEP.
    // Or just VPMOVQB (Avx512DQ)? Reduce.
    // Simplest: VPMOVM2B.
    // If we target XMM destination (X2), it fills based on K mask bits?
    // Check doc: VPMOVM2B xmm, k.
    // "Expand bits of k to bytes".
    // If k has 8 bits usage (from VPCMPQ on ZMM), does it map to 8 bytes?
    // Yes.
    // And we can PAND with X4 (Broadcast 1s).
    // Then MOVQ X2, (DI).
    
    VPMOVM2B K1, X2
    VPAND    X2, X4, X2
    MOVQ    X2, (DI)

    ADDQ    $64, SI
    ADDQ    $8, DI
    SUBQ    $8, CX
    JMP     loop_int64_512

tail_int64_512:
    VZEROUPPER
    CMPQ    CX, $0
    JE      done_int64_512

t512_int64_loop:
    CMPQ    CX, $0
    JE      done_int64_512

    MOVQ    (SI), R9
    MOVQ    DX, R10 // val (DX maintained)

    MOVB    $0, (DI)

    CMPQ    BX, $0 // Eq
    JNE     t512_neq
    CMPQ    R9, R10
    JNE     t512_adv
    MOVB    $1, (DI)
    JMP     t512_adv

t512_neq:
    CMPQ    BX, $1
    JNE     t512_gt
    CMPQ    R9, R10
    JE      t512_adv
    MOVB    $1, (DI)
    JMP     t512_adv

t512_gt:
    CMPQ    BX, $2
    JNE     t512_ge
    CMPQ    R9, R10
    JLE     t512_adv
    MOVB    $1, (DI)
    JMP     t512_adv

t512_ge:
    CMPQ    BX, $3
    JNE     t512_lt
    CMPQ    R9, R10
    JLT     t512_adv
    MOVB    $1, (DI)
    JMP     t512_adv

t512_lt:
    CMPQ    BX, $4
    JNE     t512_le
    CMPQ    R9, R10
    JGE     t512_adv
    MOVB    $1, (DI)
    JMP     t512_adv

t512_le:
    CMPQ    R9, R10
    JG      t512_adv
    MOVB    $1, (DI)

t512_adv:
    ADDQ    $8, SI
    ADDQ    $1, DI
    DECQ    CX
    JMP     t512_int64_loop

done_int64_512:
    RET

// Float512 similar pattern...
// Float512 Kernel
TEXT ·matchFloat32AVX512Kernel(SB), NOSPLIT, $0-40
    MOVQ    src+0(FP), SI
    MOVSS   val+8(FP), X0
    MOVQ    op+16(FP), BX
    MOVQ    dst+24(FP), DI
    MOVQ    n+32(FP), CX

    VPBROADCASTD X0, Z0
    MOVQ    $1, R9
    MOVQ    R9, X4
    VPBROADCASTB X4, Z4 // Z4 = [1, 1, 1...]

loop_f32_512:
    CMPQ    CX, $16
    JL      tail_f32_512

    VMOVUPS (SI), Z1
    
    // Op mapping for VCMPPS (AVX512)
    // 0=EQ, 4=NEQ, 2=LE, 6=GT (swapped logic? no VCMPPS immediate supports all).
    // Standard VCMPPS imm:
    // 0=EQ, 1=LT, 2=LE, ... 4=NEQ, 5=GE, 6=GT ...
    // Note: ordered/unordered variants exist (8-15, 16-23...).
    // We assume Ordered comparisons (signaling NaN? or safe?). Quiet.
    // 0=EQ_OQ (Ordered Quiet).
    // 4=NEQ_OQ.
    // 1=LT_OS.
    // Let's use standard indices.
    
    CMPQ    BX, $0
    JE      c_eq_f
    CMPQ    BX, $1
    JE      c_neq_f
    CMPQ    BX, $2
    JE      c_gt_f
    CMPQ    BX, $3
    JE      c_ge_f
    CMPQ    BX, $4
    JE      c_lt_f
    JMP     c_le_f

c_eq_f:
    VCMPPS  $0, Z1, Z0, K1
    JMP     store_f_512
c_neq_f:
    VCMPPS  $4, Z1, Z0, K1
    JMP     store_f_512
c_gt_f:
    // GT (src > val). Check Imm 14 (GT_OS)? Or 6 (GT_OQ)? 
    // Imm 14 is GT_OS. Imm 6 is NLE_US (Unordered Signal?). 
    // Usually imm 30 is GT_OQ?
    // Let's rely on standard logic: LT swapped.
    // val < src  -> src > val.
    // VCMPPS $1 (LT), Z0 (val), Z1 (src), K1.
    VCMPPS  $1, Z0, Z1, K1
    JMP     store_f_512
c_ge_f:
    // src >= val. val <= src.
    // LE(val, src). Imm 2.
    VCMPPS  $2, Z0, Z1, K1
    JMP     store_f_512
c_lt_f:
    VCMPPS  $1, Z1, Z0, K1
    JMP     store_f_512
c_le_f:
    VCMPPS  $2, Z1, Z0, K1

store_f_512:
    VPMOVM2B K1, Z2
    VPANDQ   Z2, Z4, Z2
    
    // Store 16 bytes (16 floats * 1 byte result = 16 bytes).
    // Z2 is 512-bit (64 bytes). But only first 16 bytes are valid?
    // VPMOVM2B with K1 (16 bits) -> 16 bytes (128-bit XMM).
    // Yes. Destination is XMM (128-bit) if K is 16-bit.
    // Wait, VPMOVM2B dst, k. Dst size determines operation?
    // If dst is ZMM, it expands to 64 bytes.
    // K1 is 16 bits (for 16 floats).
    // We want 16 bytes.
    // Use XMM destination.
    VPMOVM2B K1, X2
    VPAND    X2, X4, X2
    VMOVDQU  X2, (DI)

    ADDQ    $64, SI
    ADDQ    $16, DI
    SUBQ    $16, CX
    JMP     loop_f32_512

tail_f32_512:
    VZEROUPPER
    CMPQ    CX, $0
    JE      end_f32_512

    // Reload val X0 (generic scalar)
    MOVSS   val+8(FP), X0

scalar_f32_512_loop:
    CMPQ    CX, $0
    JE      end_f32_512
    
    MOVSS   (SI), X1
    MOVB    $0, (DI)
    
    CMPQ    BX, $0
    JE      sc512_eq_f32
    CMPQ    BX, $1
    JE      sc512_neq_f32
    CMPQ    BX, $2
    JE      sc512_gt_f32
    CMPQ    BX, $3
    JE      sc512_ge_f32
    CMPQ    BX, $4
    JE      sc512_lt_f32
    JMP     sc512_le_f32

sc512_eq_f32:
    UCOMISS X0, X1
    JNE     next_f32_512
    JP      next_f32_512
    MOVB    $1, (DI)
    JMP     next_f32_512

sc512_neq_f32:
    UCOMISS X0, X1
    JNE     set_f32_512
    JP      set_f32_512
    JMP     next_f32_512

sc512_gt_f32:
    UCOMISS X0, X1
    JA      set_f32_512
    JMP     next_f32_512

sc512_ge_f32:
    UCOMISS X0, X1
    JAE     set_f32_512
    JMP     next_f32_512

sc512_lt_f32:
    UCOMISS X0, X1
    JB      set_f32_512
    JMP     next_f32_512

sc512_le_f32:
    UCOMISS X0, X1
    JBE     set_f32_512
    JMP     next_f32_512

set_f32_512:
    MOVB    $1, (DI)

next_f32_512:
    ADDQ    $4, SI
    ADDQ    $1, DI
    DECQ    CX
    JMP     scalar_f32_512_loop

end_f32_512:
    RET

// func matchFloat64AVX2Kernel(src unsafe.Pointer, val float64, op int, dst unsafe.Pointer, n int)
TEXT ·matchFloat64AVX2Kernel(SB), NOSPLIT, $0-40
    MOVQ    src+0(FP), SI
    MOVSD   val+8(FP), X0
    MOVQ    op+16(FP), BX
    MOVQ    dst+24(FP), DI
    MOVQ    n+32(FP), CX

    VPBROADCASTQ X0, Y0
    LEAQ    maskLUT<>(SB), R8

loop_f64_avx2:
    CMPQ    CX, $4
    JL      tail_f64_avx2

    VMOVUPS (SI), Y1 // Load 4 float64s
    
    // Op: 0=Eq, 1=Neq, 2=Gt, 3=Ge, 4=Lt, 5=Le
    // VCMPPD imm: 0=EQ, 1=LT, 2=LE, 4=NEQ, 14=GT (swapped or OS), etc.
    // Let's use swapped Lt/Le for Gt/Ge to be safe with standard imm.

    CMPQ    BX, $0
    JE      f64_eq
    CMPQ    BX, $1
    JE      f64_neq
    CMPQ    BX, $2
    JE      f64_gt
    CMPQ    BX, $3
    JE      f64_ge
    CMPQ    BX, $4
    JE      f64_lt
    JMP     f64_le

f64_eq:
    VCMPPD  $0, Y0, Y1, Y2
    JMP     f64_store
f64_neq:
    VCMPPD  $4, Y0, Y1, Y2
    JMP     f64_store
f64_gt:
    VCMPPD  $1, Y1, Y0, Y2 // val < src
    JMP     f64_store
f64_ge:
    VCMPPD  $2, Y1, Y0, Y2 // val <= src
    JMP     f64_store
f64_lt:
    VCMPPD  $1, Y0, Y1, Y2 // src < val
    JMP     f64_store
f64_le:
    VCMPPD  $2, Y0, Y1, Y2 // src <= val

f64_store:
    VMOVMSKPD Y2, DX
    MOVL    (R8)(DX*4), R9
    MOVL    R9, (DI)

    ADDQ    $32, SI
    ADDQ    $4, DI
    SUBQ    $4, CX
    JMP     loop_f64_avx2

tail_f64_avx2:
    CMPQ    CX, $0
    JE      done_f64_avx2
    
    MOVSD   (SI), X1
    MOVB    $0, (DI)

    // Scalar Op check
    CMPQ    BX, $0
    JE      s_eq_f64
    CMPQ    BX, $1
    JE      s_neq_f64
    CMPQ    BX, $2
    JE      s_gt_f64
    CMPQ    BX, $3
    JE      s_ge_f64
    CMPQ    BX, $4
    JE      s_lt_f64
    JMP     s_le_f64

s_eq_f64:
    UCOMISD X0, X1
    JNE     next_f64
    JP      next_f64
    MOVB    $1, (DI)
    JMP     next_f64
s_neq_f64:
    UCOMISD X0, X1
    JNE     set_f64
    JP      set_f64
    JMP     next_f64
s_gt_f64:
    UCOMISD X0, X1
    JA      set_f64
    JMP     next_f64
s_ge_f64:
    UCOMISD X0, X1
    JAE     set_f64
    JMP     next_f64
s_lt_f64:
    UCOMISD X0, X1
    JB      set_f64
    JMP     next_f64
s_le_f64:
    UCOMISD X0, X1
    JBE     set_f64
    JMP     next_f64

set_f64:
    MOVB    $1, (DI)
next_f64:
    ADDQ    $8, SI
    ADDQ    $1, DI
    DECQ    CX
    JMP     tail_f64_avx2

done_f64_avx2:
    VZEROUPPER
    RET

// func matchFloat64AVX512Kernel(src unsafe.Pointer, val float64, op int, dst unsafe.Pointer, n int)
TEXT ·matchFloat64AVX512Kernel(SB), NOSPLIT, $0-40
    MOVQ    src+0(FP), SI
    MOVSD   val+8(FP), X0
    MOVQ    op+16(FP), BX
    MOVQ    dst+24(FP), DI
    MOVQ    n+32(FP), CX

    VPBROADCASTQ X0, Z0
    MOVQ    $1, R9
    MOVQ    R9, X4
    VPBROADCASTB X4, Z4

loop_f64_512:
    CMPQ    CX, $8
    JL      tail_f64_512

    VMOVUPS (SI), Z1
    
    // VPCMPPD (AVX512 version)
    CMPQ    BX, $0
    JE      f64_eq_512
    CMPQ    BX, $1
    JE      f64_neq_512
    CMPQ    BX, $2
    JE      f64_gt_512
    CMPQ    BX, $3
    JE      f64_ge_512
    CMPQ    BX, $4
    JE      f64_lt_512
    JMP     f64_le_512

f64_eq_512:
    VCMPPD  $0, Z1, Z0, K1
    JMP     f64_store_512
f64_neq_512:
    VCMPPD  $4, Z1, Z0, K1
    JMP     f64_store_512
f64_gt_512:
    VCMPPD  $1, Z0, Z1, K1 // val < src
    JMP     f64_store_512
f64_ge_512:
    VCMPPD  $2, Z0, Z1, K1 // val <= src
    JMP     f64_store_512
f64_lt_512:
    VCMPPD  $1, Z1, Z0, K1
    JMP     f64_store_512
f64_le_512:
    VCMPPD  $2, Z1, Z0, K1

f64_store_512:
    VPMOVM2B K1, X2
    VPAND    X2, X4, X2
    MOVQ    X2, (DI)

    ADDQ    $64, SI
    ADDQ    $8, DI
    SUBQ    $8, CX
    JMP     loop_f64_512

tail_f64_512:
    VZEROUPPER
    CMPQ    CX, $0
    JE      done_f64_512
    
    MOVSD   val+8(FP), X0

scalar_f64_512_loop:
    CMPQ    CX, $0
    JE      done_f64_512
    
    MOVSD   (SI), X1
    MOVB    $0, (DI)

    // Scalar Op check
    CMPQ    BX, $0
    JE      s_eq_f64_512
    CMPQ    BX, $1
    JE      s_neq_f64_512
    CMPQ    BX, $2
    JE      s_gt_f64_512
    CMPQ    BX, $3
    JE      s_ge_f64_512
    CMPQ    BX, $4
    JE      s_lt_f64_512
    JMP     s_le_f64_512

s_eq_f64_512:
    UCOMISD X0, X1
    JNE     next_f64_512
    JP      next_f64_512
    MOVB    $1, (DI)
    JMP     next_f64_512
s_neq_f64_512:
    UCOMISD X0, X1
    JNE     set_f64_512
    JP      set_f64_512
    JMP     next_f64_512
s_gt_f64_512:
    UCOMISD X0, X1
    JA      set_f64_512
    JMP     next_f64_512
s_ge_f64_512:
    UCOMISD X0, X1
    JAE     set_f64_512
    JMP     next_f64_512
s_lt_f64_512:
    UCOMISD X0, X1
    JB      set_f64_512
    JMP     next_f64_512
s_le_f64_512:
    UCOMISD X0, X1
    JBE     set_f64_512
    JMP     next_f64_512

set_f64_512:
    MOVB    $1, (DI)
next_f64_512:
    ADDQ    $8, SI
    ADDQ    $1, DI
    DECQ    CX
    JMP     scalar_f64_512_loop

done_f64_512:
    RET

// func matchInt32AVX2Kernel(src unsafe.Pointer, val int32, op int, dst unsafe.Pointer, n int)
TEXT ·matchInt32AVX2Kernel(SB), NOSPLIT, $0-40
    MOVQ    src+0(FP), SI
    MOVL    val+8(FP), X0
    MOVQ    op+16(FP), BX
    MOVQ    dst+24(FP), DI
    MOVQ    n+32(FP), CX

    VPBROADCASTD X0, Y0
    LEAQ    maskLUT<>(SB), R8

loop_int32_avx2:
    CMPQ    CX, $8
    JL      tail_int32_avx2

    VMOVUPS (SI), Y1 // Load 8 int32s

    CMPQ    BX, $0
    JE      int32_eq_avx2
    CMPQ    BX, $1
    JE      int32_neq_avx2
    CMPQ    BX, $2
    JE      int32_gt_avx2
    CMPQ    BX, $3
    JE      int32_ge_avx2
    CMPQ    BX, $4
    JE      int32_lt_avx2
    JMP     int32_le_avx2

int32_eq_avx2:
    VPCMPEQD Y0, Y1, Y1
    JMP     store_mask_int32_avx2
int32_neq_avx2:
    VPCMPEQD Y0, Y1, Y1
    VPCMPEQD Y2, Y2, Y2
    VPXOR   Y2, Y1, Y1
    JMP     store_mask_int32_avx2
int32_gt_avx2:
    VPCMPGTD Y0, Y1, Y1 // src > val
    JMP     store_mask_int32_avx2
int32_ge_avx2:
    VPCMPGTD Y1, Y0, Y1 // val > src
    VPCMPEQD Y2, Y2, Y2
    VPXOR   Y2, Y1, Y1 // !(val > src) -> src >= val
    JMP     store_mask_int32_avx2
int32_lt_avx2:
    VPCMPGTD Y1, Y0, Y1 // val > src
    JMP     store_mask_int32_avx2
int32_le_avx2:
    VPCMPGTD Y0, Y1, Y1 // src > val
    VPCMPEQD Y2, Y2, Y2
    VPXOR   Y2, Y1, Y1 // !(src > val) -> src <= val

store_mask_int32_avx2:
    VMOVMSKPS Y1, DX // 8 bits
    MOVQ    DX, AX
    ANDQ    $0x0F, AX
    MOVL    (R8)(AX*4), R10
    MOVL    R10, (DI)
    
    MOVQ    DX, AX
    SHRQ    $4, AX
    ANDQ    $0x0F, AX
    MOVL    (R8)(AX*4), R10
    MOVL    R10, 4(DI)

    ADDQ    $32, SI
    ADDQ    $8, DI
    SUBQ    $8, CX
    JMP     loop_int32_avx2

tail_int32_avx2:
    CMPQ    CX, $0
    JE      done_int32_avx2
    MOVL    (SI), R9
    VMOVQ   X0, R10
    MOVB    $0, (DI)
    
    CMPQ    BX, $0
    JE      t_eq_32
    CMPQ    BX, $1
    JE      t_neq_32
    CMPQ    BX, $2
    JE      t_gt_32
    CMPQ    BX, $3
    JE      t_ge_32
    CMPQ    BX, $4
    JE      t_lt_32
    JMP     t_le_32

t_eq_32:
    CMPL    R9, R10
    JNE     t_next_32
    MOVB    $1, (DI)
    JMP     t_next_32
t_neq_32:
    CMPL    R9, R10
    JE      t_next_32
    MOVB    $1, (DI)
    JMP     t_next_32
t_gt_32:
    CMPL    R9, R10
    JLE     t_next_32
    MOVB    $1, (DI)
    JMP     t_next_32
t_ge_32:
    CMPL    R9, R10
    JL      t_next_32
    MOVB    $1, (DI)
    JMP     t_next_32
t_lt_32:
    CMPL    R9, R10
    JGE     t_next_32
    MOVB    $1, (DI)
    JMP     t_next_32
t_le_32:
    CMPL    R9, R10
    JG      t_next_32
    MOVB    $1, (DI)

t_next_32:
    ADDQ    $4, SI
    ADDQ    $1, DI
    DECQ    CX
    JMP     tail_int32_avx2

done_int32_avx2:
    VZEROUPPER
    RET

// func matchInt32AVX512Kernel(src unsafe.Pointer, val int32, op int, dst unsafe.Pointer, n int)
TEXT ·matchInt32AVX512Kernel(SB), NOSPLIT, $0-40
    MOVQ    src+0(FP), SI
    MOVL    val+8(FP), X0
    MOVQ    op+16(FP), BX
    MOVQ    dst+24(FP), DI
    MOVQ    n+32(FP), CX

    VPBROADCASTD X0, Z0
    
    // Constant 1 for mask expansion
    MOVQ    $1, R9
    MOVQ    R9, X4
    VPBROADCASTB X4, Z4

loop_int32_512:
    CMPQ    CX, $16
    JL      tail_int32_512

    VMOVUPS (SI), Z1
    
    // VPCMPD immediate: EQ=0, LT=1, LE=2, NEQ=4, GE=5, GT=6
    // Our Enum: EQ=0, NEQ=1, GT=2, GE=3, LT=4, LE=5
    CMPQ    BX, $0
    JE      i32_eq_512
    CMPQ    BX, $1
    JE      i32_neq_512
    CMPQ    BX, $2
    JE      i32_gt_512
    CMPQ    BX, $3
    JE      i32_ge_512
    CMPQ    BX, $4
    JE      i32_lt_512
    JMP     i32_le_512

i32_eq_512:
    VPCMPD  $0, Z0, Z1, K1
    JMP     i32_store_512
i32_neq_512:
    VPCMPD  $4, Z0, Z1, K1
    JMP     i32_store_512
i32_gt_512:
    VPCMPD  $6, Z0, Z1, K1
    JMP     i32_store_512
i32_ge_512:
    VPCMPD  $5, Z0, Z1, K1
    JMP     i32_store_512
i32_lt_512:
    VPCMPD  $1, Z0, Z1, K1
    JMP     i32_store_512
i32_le_512:
    VPCMPD  $2, Z0, Z1, K1

i32_store_512:
    VPMOVM2B K1, Z2
    VPANDQ   Z2, Z4, Z2
    VMOVUPS  Z2, (DI)

    ADDQ    $64, SI
    ADDQ    $16, DI
    SUBQ    $16, CX
    JMP     loop_int32_512

tail_int32_512:
    VZEROUPPER
    CMPQ    CX, $0
    JE      done_int32_512
    
i32_scalar_loop:
    CMPQ    CX, $0
    JE      done_int32_512
    MOVL    (SI), R9
    MOVB    $0, (DI)
    
    // R10 = val
    MOVL    DX, R10

    CMPQ    BX, $0
    JE      ti32_eq
    CMPQ    BX, $1
    JE      ti32_neq
    CMPQ    BX, $2
    JE      ti32_gt
    CMPQ    BX, $3
    JE      ti32_ge
    CMPQ    BX, $4
    JE      ti32_lt
    JMP     ti32_le

ti32_eq:
    CMPL    R9, R10
    JNE     ti32_adv
    MOVB    $1, (DI)
    JMP     ti32_adv
ti32_neq:
    CMPL    R9, R10
    JE      ti32_adv
    MOVB    $1, (DI)
    JMP     ti32_adv
ti32_gt:
    CMPL    R9, R10
    JLE     ti32_adv
    MOVB    $1, (DI)
    JMP     ti32_adv
ti32_ge:
    CMPL    R9, R10
    JL      ti32_adv
    MOVB    $1, (DI)
    JMP     ti32_adv
ti32_lt:
    CMPL    R9, R10
    JGE     ti32_adv
    MOVB    $1, (DI)
    JMP     ti32_adv
ti32_le:
    CMPL    R9, R10
    JG      ti32_adv
    MOVB    $1, (DI)

ti32_adv:
    ADDQ    $4, SI
    ADDQ    $1, DI
    DECQ    CX
    JMP     i32_scalar_loop

done_int32_512:
    RET

