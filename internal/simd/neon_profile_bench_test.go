//go:build arm64

package simd

import (
	"testing"
)

func BenchmarkNeonProfile_768_BaselineASM(b *testing.B) {
	a := make([]float32, 768)
	c := make([]float32, 768)
	for i := 0; i < 768; i++ {
		a[i] = float32(i) * 0.1
		c[i] = float32(i) * 0.2
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = euclideanNEONKernel(a, c)
	}
}

func BenchmarkNeonProfile_768_GenericUnrolled(b *testing.B) {
	a := make([]float32, 768)
	c := make([]float32, 768)
	for i := 0; i < 768; i++ {
		a[i] = float32(i) * 0.1
		c[i] = float32(i) * 0.2
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = euclidean768Unrolled4x(a, c)
	}
}

func BenchmarkNeonProfile_768_Blocked(b *testing.B) {
	a := make([]float32, 768)
	c := make([]float32, 768)
	for i := 0; i < 768; i++ {
		a[i] = float32(i) * 0.1
		c[i] = float32(i) * 0.2
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = euclidean768NEON(a, c)
	}
}

func BenchmarkNeonProfile_768_Direct(b *testing.B) {
	a := make([]float32, 768)
	c := make([]float32, 768)
	for i := 0; i < 768; i++ {
		a[i] = float32(i) * 0.1
		c[i] = float32(i) * 0.2
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = euclideanNEON(a, c)
	}
}

func BenchmarkNeonProfile_768_UnrolledPrefetchedASM(b *testing.B) {
	a := make([]float32, 768)
	c := make([]float32, 768)
	for i := 0; i < 768; i++ {
		a[i] = float32(i) * 0.1
		c[i] = float32(i) * 0.2
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = euclideanHighDimNEONKernel(a, c)
	}
}
