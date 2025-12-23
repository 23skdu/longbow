package store

import (
	"math"
	"math/rand"
	"testing"

	"github.com/23skdu/longbow/internal/simd"
)

func TestADCBatchSIMD(t *testing.T) {
	m := 8
	ksub := 256
	n := 32

	// Create random distance table [m * 256]
	table := make([]float32, m*ksub)
	for i := range table {
		table[i] = rand.Float32()
	}

	// Create random codes [n * m]
	flatCodes := make([]uint8, n*m)
	for i := range flatCodes {
		flatCodes[i] = uint8(rand.Intn(ksub))
	}

	// Results
	simdResults := make([]float32, n)
	scalarResults := make([]float32, n)

	// Run SIMD
	simd.ADCDistanceBatch(table, flatCodes, m, simdResults)

	// Run Scalar (from simd.go)
	for i := 0; i < n; i++ {
		var sum float32
		codes := flatCodes[i*m : (i+1)*m]
		for jj, code := range codes {
			sum += table[jj*ksub+int(code)]
		}
		scalarResults[i] = float32(math.Sqrt(float64(sum)))
	}

	// Compare
	for i := 0; i < n; i++ {
		if math.Abs(float64(simdResults[i]-scalarResults[i])) > 1e-5 {
			t.Errorf("Result %d mismatch: got %f, want %f", i, simdResults[i], scalarResults[i])
		}
	}
}

func BenchmarkADCBatchSIMD(b *testing.B) {
	m := 32
	ksub := 256
	n := 1024

	table := make([]float32, m*ksub)
	flatCodes := make([]uint8, n*m)
	results := make([]float32, n)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		simd.ADCDistanceBatch(table, flatCodes, m, results)
	}
}
