package store


import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/simd"
)

func TestADCBatchCorrectness(t *testing.T) {
	// Setup
	dims := 128
	m := 16
	k := 256
	cfg := PQConfig{
		Dimensions:    dims,
		NumSubVectors: m,
		NumCentroids:  k,
	}

	encoder, _ := NewPQEncoder(cfg)
	// Mock codebooks
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < m; i++ {
		encoder.Codebooks[i] = make([][]float32, k)
		for j := 0; j < k; j++ {
			encoder.Codebooks[i][j] = make([]float32, dims/m)
			for d := 0; d < dims/m; d++ {
				encoder.Codebooks[i][j][d] = rng.Float32()
			}
		}
	}

	query := make([]float32, dims)
	for i := range query {
		query[i] = rng.Float32()
	}
	lut := encoder.ComputeDistanceTableFlat(query)

	batchSize := 32
	flatCodes := make([]byte, batchSize*m)
	_, _ = rng.Read(flatCodes) // Random codes

	// 1. Full Batch
	results := make([]float32, batchSize)
	for i := range results {
		results[i] = -1 // All active
	}

	// Create reference results using scalar math (manually)
	refResults := make([]float32, batchSize)
	for j := 0; j < batchSize; j++ {
		var dist float32
		codes := flatCodes[j*m : (j+1)*m]
		for i, code := range codes {
			dist += lut[i*k+int(code)]
		}
		refResults[j] = float32(math.Sqrt(float64(dist)))
	}

	encoder.ADCDistanceBatch(lut, flatCodes, results)

	for i := 0; i < batchSize; i++ {
		if absDiff(results[i], refResults[i]) > 0.001 {
			t.Errorf("FullBatch mismatch at %d: got %f, want %f", i, results[i], refResults[i])
		}
	}

	// 2. Sparse Batch
	resultsSparse := make([]float32, batchSize)
	for i := range resultsSparse {
		if i%2 == 0 {
			resultsSparse[i] = -1 // Active
		} else {
			resultsSparse[i] = -2 // Skipped
		}
	}

	encoder.ADCDistanceBatch(lut, flatCodes, resultsSparse)

	for i := 0; i < batchSize; i++ {
		if i%2 == 0 {
			if absDiff(resultsSparse[i], refResults[i]) > 0.001 {
				t.Errorf("SparseBatch active mismatch at %d: got %f, want %f", i, resultsSparse[i], refResults[i])
			}
		} else {
			if resultsSparse[i] != -2 {
				t.Errorf("SparseBatch skipped mismatch at %d: got %f, want -2", i, resultsSparse[i])
			}
		}
	}
}

func BenchmarkADCDistanceBatch(b *testing.B) {
	dims := 128
	m := 32
	k := 256
	cfg := PQConfig{
		Dimensions:    dims,
		NumSubVectors: m,
		NumCentroids:  k,
	}

	encoder, _ := NewPQEncoder(cfg)
	// Mock codebooks irrelevant for perf (justLUT access)
	query := make([]float32, dims)
	lut := encoder.ComputeDistanceTableFlat(query) // Zeros/random is fine

	batchSize := 32 // Typical search batch
	flatCodes := make([]byte, batchSize*m)

	results := make([]float32, batchSize)

	b.ResetTimer()
	b.Run(fmt.Sprintf("M=%d_Batch=%d_Dense", m, batchSize), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Reset results
			for j := range results {
				results[j] = -1
			}
			encoder.ADCDistanceBatch(lut, flatCodes, results)
		}
	})

	b.Run(fmt.Sprintf("M=%d_Batch=%d_Sparse", m, batchSize), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Reset results (checkerboard)
			for j := range results {
				if j%2 == 0 {
					results[j] = -1
				} else {
					results[j] = -2
				}
			}
			encoder.ADCDistanceBatch(lut, flatCodes, results)
		}
	})
}

func absDiff(a, b float32) float32 {
	if a < b {
		return b - a
	}
	return a - b
}

func TestSimdFeatures(t *testing.T) {
	features := simd.GetCPUFeatures()
	t.Logf("SIMD Implementation: %s", simd.GetImplementation())
	t.Logf("Features: %+v", features)
}
