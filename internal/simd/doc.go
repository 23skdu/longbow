// Package simd provides SIMD-accelerated distance functions, batch operations,
// and utility kernels for vector search.
//
// Implementations are selected at runtime via dispatch tables based on CPU
// capabilities (AVX-512, AVX2, NEON). See docs/simd-architecture.md for
// detailed architecture documentation.
package simd
