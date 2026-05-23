// Package simd provides high-performance Single Instruction Multiple Data (SIMD)
// mathematical and logical operations for vector search and GraphRAG.
//
// The package uses a dynamic dispatch system that selects the best available
// implementation at runtime based on CPU capabilities (AVX2, AVX-512, NEON).
// It also provides a JIT (Just-In-Time) compilation path for dynamic kernel generation.
//
// Architecture Overview:
//
//  1. Dispatch System: Function pointers are initialized at startup in dispatch.go.
//     Higher-level packages call public wrappers (e.g., simd.DotProduct) which
//     delegate to the resolved architecture-specific implementation.
//
//  2. Blocking & Tiling: For very large vectors or batch operations, the package
//     implements cache-aware blocking and tiling (simd_blocked.go) to maximize
//     cache locality and prefetching efficiency.
//
// 3. Assembly Kernels: Critical hot-paths are implemented in native assembly:
//   - x86_64: Avo-generated AVX2 and AVX-512 kernels.
//   - ARM64: Hand-written or Avo-generated NEON kernels.
//
//  4. Fallbacks: Standard Go (scalar) unrolled loops are provided in
//     simd_baseline.go to ensure correctness across all platforms and
//     during early bootstrap phases.
package simd
