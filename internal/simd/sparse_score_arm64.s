//go:build arm64
 
#include "textflag.h"

#define VSCVTF_V(n, d)   WORD $(0x4e21d800 | ((n) << 5) | (d))
#define VFADD_V(m, n, d) WORD $(0x4e20d400 | ((m) << 16) | ((n) << 5) | (d))
#define VFMUL_V(m, n, d) WORD $(0x6e20dc00 | ((m) << 16) | ((n) << 5) | (d))
#define VFDIV_V(m, n, d) WORD $(0x6e20fc00 | ((m) << 16) | ((n) << 5) | (d))

// func bm25ScoreBatchNEON(tfs, docLens unsafe.Pointer, n int, invAvgDL, idf, k1, b float32, results unsafe.Pointer)
TEXT ·bm25ScoreBatchNEON(SB), NOSPLIT, $0-48
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
    FMULS   F1, F6, F7          // F7 = idf * (k1 + 1.0)
 
    // Broadcast constants to vectors V16-V20
    VDUP    V0.S[0], V16.S4     // V16 = [invAvgDL, ...]
    VDUP    V7.S[0], V17.S4     // V17 = [NumeratorMultiplier, ...]
    VDUP    V2.S[0], V18.S4     // V18 = [k1, ...]
    VDUP    V3.S[0], V19.S4     // V19 = [b, ...]
    VDUP    V5.S[0], V20.S4     // V20 = [1.0 - b, ...]
 
 loop_4x:
    CMP     $4, R2
    BLT     tail
 
    // Load 4 TFs and 4 DocLengths (int32)
    VLD1.P  16(R0), [V0.S4]     // V0 = TFs
    VLD1.P  16(R1), [V1.S4]     // V1 = DocLens
 
    VSCVTF_V(0, 2)              // V2 = float(TFs)
    VSCVTF_V(1, 3)              // V3 = float(DocLens)
 
    // lengthNorm = (1.0 - b) + b * (docLen * invAvgDL)
    VFMUL_V(16, 3, 4)           // V4 = docLen * invAvgDL
    VFMUL_V(19, 4, 5)           // V5 = b * ...
    VFADD_V(20, 5, 6)           // V6 = lengthNorm
 
    // denominator = tf + k1 * lengthNorm
    VFMUL_V(18, 6, 7)           // V7 = k1 * lengthNorm
    VFADD_V(2, 7, 8)            // V8 = denominator
 
    // numerator = tf * NumeratorMultiplier
    VFMUL_V(17, 2, 9)           // V9 = numerator
 
    // score = numerator / denominator
    VFDIV_V(8, 9, 10)           // V10 = score
 
    // Store results
    VST1.P  [V10.S4], 16(R3)
 
    SUB     $4, R2
    B       loop_4x
 
 tail:
    CBZ     R2, done
    
    MOVW.P  4(R0), R4           // TF
    MOVW.P  4(R1), R5           // DocLen
    
    SCVTFS  R4, F8              // TF
    SCVTFS  R5, F9              // DocLen
    
    FMULS   F0, F9, F10         // F10 = docLen * invAvgDL
    FMULS   F3, F10, F11        // F11 = b * ...
    FADDS   F5, F11, F12        // F12 = lengthNorm
    
    FMULS   F2, F12, F13        // F13 = k1 * lengthNorm
    FADDS   F8, F13, F14        // F14 = denominator
    
    FMULS   F7, F8, F15         // F15 = numerator
    
    FDIVS   F14, F15, F16
    
    FMOVS.P F16, 4(R3)
    
    SUB     $1, R2
    B       tail
 
 done:
    RET
