package simd

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestADC_FastPath(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	m := 16 // 16 sub-quantizers
	table := make([]float32, m*256)
	for i := range table {
		table[i] = rng.Float32() * 2.0
	}

	n := 32 // 32 candidates (multiple of 8 for AVX-512)
	flatCodes := make([]byte, n*m)
	for i := range flatCodes {
		flatCodes[i] = byte(rng.Intn(256))
	}

	resultsAVX2 := make([]float32, n)
	err := adcBatchAVX2(table, flatCodes, m, resultsAVX2)
	require.NoError(t, err)

	resultsAVX512 := make([]float32, n)
	err = adcBatchAVX512(table, flatCodes, m, resultsAVX512)
	require.NoError(t, err)

	// Validate bit-identity / exact parity between AVX2 and AVX-512 ADC
	for i := 0; i < n; i++ {
		assert.InDelta(t, resultsAVX2[i], resultsAVX512[i], 1e-6, "mismatch at index %d", i)
	}

	// Test DistanceDirectADC
	for i := 0; i < n; i++ {
		singleCode := flatCodes[i*m : (i+1)*m]
		singleDist := DistanceDirectADC(table, singleCode, m)
		assert.InDelta(t, resultsAVX2[i], singleDist, 1e-6, "single mismatch at index %d", i)
	}

	// Test edge cases
	assert.Error(t, adcBatchAVX512(table, flatCodes[:m-1], m, resultsAVX512))
	assert.NoError(t, adcBatchAVX512(table, flatCodes, m, nil))
}

func BenchmarkADC_FastPath(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	m := 16
	table := make([]float32, m*256)
	for i := range table {
		table[i] = rng.Float32()
	}

	n := 64
	flatCodes := make([]byte, n*m)
	for i := range flatCodes {
		flatCodes[i] = byte(rng.Intn(256))
	}
	results := make([]float32, n)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = adcBatchAVX512(table, flatCodes, m, results)
	}
}
