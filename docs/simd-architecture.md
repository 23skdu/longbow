# SIMD Architecture

## Dispatch System

Function pointers are initialized at startup in `dispatch.go`. Higher-level packages call public wrappers (e.g., `simd.DotProduct`) which delegate to the resolved architecture-specific implementation.

Runtime detection checks CPUID/feature flags and selects from:
- **AVX-512**: Full SIMD-width kernels for x86_64
- **AMX (emerald/granite)**: Matrix multiply accelerator for large dot products
- **AVX2/FMA**: 256-bit SIMD fallback
- **NEON**: ARM64 SIMD
- **Generic**: Scalar Go fallbacks for all platforms

## Blocking & Tiling

For large vectors or batch operations, cache-aware blocking (`simd_blocked.go`) maximizes cache locality. Tiled batch variants process vectors in cache-friendly chunks.

## Assembly Kernels

Critical hot-paths use native assembly:
- **x86_64**: Avo-generated AVX2/AVX-512 kernels
- **ARM64**: Hand-written or Avo-generated NEON kernels

Assembly stubs are in `*_amd64.go` / `*_arm64.go` files. Fallbacks in `simd_baseline.go` ensure correctness on all platforms.

## Fallback Strategy

Every distance function has three layers:
1. **SIMD kernel** (assembly) — fastest, platform-specific
2. **Unrolled Go** — portable, 4x loop unrolling
3. **Generic Go** — simple loop, always correct

The dispatch table selects the best available at init time.
