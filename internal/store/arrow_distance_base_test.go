package store

import (
	"testing"
)

// TestDistanceSIMD validates SIMD distance calculation.
func TestDistanceSIMD(t *testing.T) {
	// Test basic distance calculation
	a := []float32{1.0, 2.0, 3.0}
	b := []float32{4.0, 5.0, 6.0}
	
	dist := distanceSIMD(a, b)
	
	// Expected: sqrt((4-1)^2 + (5-2)^2 + (6-3)^2) = sqrt(9+9+9) = sqrt(27) ≈ 5.196
	expected := float32(5.196152)
	if dist < expected-0.001 || dist > expected+0.001 {
		t.Errorf("distanceSIMD = %f, want %f", dist, expected)
	}
}

// TestDistanceSIMD_ZeroVector validates zero vector handling.
func TestDistanceSIMD_ZeroVector(t *testing.T) {
	a := []float32{0.0, 0.0, 0.0}
	b := []float32{0.0, 0.0, 0.0}
	
	dist := distanceSIMD(a, b)
	
	if dist != 0.0 {
		t.Errorf("distance between zero vectors = %f, want 0.0", dist)
	}
}

// TestDistanceSIMD_IdenticalVectors validates identical vector handling.
func TestDistanceSIMD_IdenticalVectors(t *testing.T) {
	a := []float32{1.5, 2.5, 3.5}
	b := []float32{1.5, 2.5, 3.5}
	
	dist := distanceSIMD(a, b)
	
	if dist != 0.0 {
		t.Errorf("distance between identical vectors = %f, want 0.0", dist)
	}
}

// TestDistanceSIMD_LargeVectors validates performance with realistic vector sizes.
func TestDistanceSIMD_LargeVectors(t *testing.T) {
	// 384-dimensional vectors (common embedding size)
	a := make([]float32, 384)
	b := make([]float32, 384)
	
	for i := range a {
		a[i] = float32(i)
		b[i] = float32(i + 1)
	}
	
	dist := distanceSIMD(a, b)
	
	// Distance should be sqrt(384 * 1^2) = sqrt(384) ≈ 19.596
	expected := float32(19.596)
	if dist < expected-0.1 || dist > expected+0.1 {
		t.Errorf("distanceSIMD for 384-dim = %f, want ~%f", dist, expected)
	}
}

// BenchmarkDistanceSIMD benchmarks SIMD distance calculation.
func BenchmarkDistanceSIMD(b *testing.B) {
	a := make([]float32, 384)
	vec := make([]float32, 384)
	
	for i := range a {
		a[i] = float32(i)
		vec[i] = float32(i + 1)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = distanceSIMD(a, vec)
	}
}

// BenchmarkDistanceSIMD_vs_L2 compares SIMD vs simple L2.
func BenchmarkDistanceSIMD_vs_L2(b *testing.B) {
	a := make([]float32, 384)
	vec := make([]float32, 384)
	
	for i := range a {
		a[i] = float32(i)
		vec[i] = float32(i + 1)
	}
	
	b.Run("SIMD", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = distanceSIMD(a, vec)
		}
	})
	
	b.Run("L2", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = l2Distance(a, vec)
		}
	})
}
