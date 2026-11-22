//go:build arm64

package simd

import (
	"testing"
)

// BenchmarkNeonProfile_768_BaselineASM runs the benchmark neon profile_768_baseline asm operation.
func BenchmarkNeonProfile_768_BaselineASM(b *testing.B) {
	a := make([]float32, 768)
	c := make([]float32, 768)
	for i := 0; i < 768; i++ {
		a[i] = float32(i) * 0.1
		c[i] = float32(i) * 0.2
	}
	for b.Loop() {
		_ = euclideanNEONKernel(a, c)
	}
}

// BenchmarkNeonProfile_768_GenericUnrolled runs the benchmark neon profile_768_generic unrolled operation.
func BenchmarkNeonProfile_768_GenericUnrolled(b *testing.B) {
	a := make([]float32, 768)
	c := make([]float32, 768)
	for i := 0; i < 768; i++ {
		a[i] = float32(i) * 0.1
		c[i] = float32(i) * 0.2
	}
	for b.Loop() {
		_, _ = euclidean768Unrolled4x(a, c)
	}
}

// BenchmarkNeonProfile_768_Blocked runs the benchmark neon profile_768_blocked operation.
func BenchmarkNeonProfile_768_Blocked(b *testing.B) {
	a := make([]float32, 768)
	c := make([]float32, 768)
	for i := 0; i < 768; i++ {
		a[i] = float32(i) * 0.1
		c[i] = float32(i) * 0.2
	}
	for b.Loop() {
		_, _ = euclidean768NEON(a, c)
	}
}

// BenchmarkNeonProfile_768_Direct runs the benchmark neon profile_768_direct operation.
func BenchmarkNeonProfile_768_Direct(b *testing.B) {
	a := make([]float32, 768)
	c := make([]float32, 768)
	for i := 0; i < 768; i++ {
		a[i] = float32(i) * 0.1
		c[i] = float32(i) * 0.2
	}
	for b.Loop() {
		_, _ = euclideanNEON(a, c)
	}
}

// BenchmarkNeonProfile_768_UnrolledPrefetchedASM runs the benchmark neon profile_768_unrolled prefetched asm operation.
func BenchmarkNeonProfile_768_UnrolledPrefetchedASM(b *testing.B) {
	a := make([]float32, 768)
	c := make([]float32, 768)
	for i := 0; i < 768; i++ {
		a[i] = float32(i) * 0.1
		c[i] = float32(i) * 0.2
	}
	for b.Loop() {
		_ = euclideanHighDimNEONKernel(a, c)
	}
}
