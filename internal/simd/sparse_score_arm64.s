//go:build arm64
 
#include "textflag.h"
 
// func bm25ScoreBatchNEON(tfs, docLens unsafe.Pointer, n int, invAvgDL, idf, k1, b float32, results unsafe.Pointer)
TEXT ·bm25ScoreBatchNEON(SB), NOSPLIT, $0-64
    MOVD    tfs+0(FP), R0
    MOVD    docLens+8(FP), R1
    MOVD    n+16(FP), R2
    FMOVS   invAvgDL+24(FP), F0
    FMOVS   idf+28(FP), F1
    FMOVS   k1+32(FP), F2
    FMOVS   b+36(FP), F3
    MOVD    results+40(FP), R3
 
    CBZ     R2, done
 
    FMOVS   $1.0, F4
    FSUBS   F3, F4, F5          // F5 = 1.0 - b
    FADDS   F2, F4, F6          // F6 = k1 + 1.0
    FMULS   F1, F6, F7          // F7 = idf * (k1 + 1.0) (Numerator multiplier)
 
    // Prepare constants in registers by broadcasting from the low lane
    VDUP    V0.S[0], V0.S4      // V0 = [invAvgDL, invAvgDL, invAvgDL, invAvgDL]
    VDUP    V7.S[0], V1.S4      // V1 = [NumeratorMultiplier, ...]
    VDUP    V2.S[0], V2.S4      // V2 = [k1, ...]
    VDUP    V3.S[0], V3.S4      // V3 = [b, ...]
    VDUP    V5.S[0], V4.S4      // V4 = [1.0 - b, ...]
 
 loop_4x:
    CMP     $4, R2
    BLT     tail
 
    // Load 4 TFs and 4 DocLengths (ints)
    VLD1.P  16(R0), [V5.S4]     // V5 = TFs
    VLD1.P  16(R1), [V6.S4]     // V6 = DocLens
 
    // SCVTF V5.4S, V7.4S
    WORD    $0x4e21d8af         // SCVTF V7.4S, V5.4S
    // SCVTF V6.4S, V8.4S
    WORD    $0x4e21d8d0         // SCVTF V8.4S, V6.4S
 
    // FMUL V9.4S, V8.4S, V0.4S
    WORD    $0x4e20d909
    
    // FMUL V10.4S, V9.4S, V3.4S
    WORD    $0x4e23d92a
    
    // FADD V11.4S, V10.4S, V4.4S
    WORD    $0x4e24d54b
 
    // FMUL V12.4S, V11.4S, V2.4S
    WORD    $0x4e22d96c
    
    // FADD V13.4S, V12.4S, V7.4S
    WORD    $0x4e27d58d
 
    // FMUL V14.4S, V7.4S, V1.4S
    WORD    $0x4e21d8ee
 
    // FDIV V15.4S, V14.4S, V13.4S
    WORD    $0x4e3dd9cf
 
    // Store results
    VST1.P  [V15.S4], 16(R3)
 
    SUB     $4, R2
    B       loop_4x
 
 tail:
    CBZ     R2, done
    
    MOVW.P  4(R0), R4           // TF
    MOVW.P  4(R1), R5           // DocLen
    
    SCVTFS  R4, F8              // TF
    SCVTFS  R5, F9              // DocLen
    
    // lengthNorm = (1.0 - b) + b * (docLen * invAvgDL)
    FMULS   F0, F9, F10         // F10 = docLen * invAvgDL
    FMULS   F3, F10, F11        // F11 = b * ...
    FADDS   F5, F11, F12        // F12 = lengthNorm
    
    // denominator = tf + k1 * lengthNorm
    FMULS   F2, F12, F13        // F13 = k1 * lengthNorm
    FADDS   F8, F13, F14        // F14 = denominator
    
    // numerator = tf * idf * (k1 + 1.0)
    FMULS   F7, F8, F15         // F15 = numerator
    
    // score = numerator / denominator
    FDIVS   F14, F15, F16
    
    FMOVS.P F16, 4(R3)
    
    SUB     $1, R2
    B       tail
 
 done:
    RET
