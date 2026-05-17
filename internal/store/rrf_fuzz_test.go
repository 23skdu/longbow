package store

import (
	"math/rand"
	"testing"
)

func FuzzReciprocalRankFusion(f *testing.F) {
	// Add some seed corpus
	f.Add(int64(12345), 10, 60, 20, 20)
	f.Add(int64(9999), 5, 60, 50, 10)
	f.Add(int64(0), 100, 60, 0, 0)

	f.Fuzz(func(t *testing.T, seed int64, k int, rrfK int, numDense int, numSparse int) {
		// Cap inputs to prevent OOM / timeout in fuzzing
		if k <= 0 || k > 1000 {
			k = 100
		}
		if rrfK <= 0 || rrfK > 1000 {
			rrfK = 60
		}
		if numDense < 0 || numDense > 1000 {
			numDense = 100
		}
		if numSparse < 0 || numSparse > 1000 {
			numSparse = 100
		}

		r := rand.New(rand.NewSource(seed))

		dense := make([]SearchResult, numDense)
		for i := 0; i < numDense; i++ {
			dense[i] = SearchResult{
				ID:    VectorID(r.Uint32()),
				Score: r.Float32(),
				Source: 0,
			}
		}

		sparse := make([]SearchResult, numSparse)
		for i := 0; i < numSparse; i++ {
			sparse[i] = SearchResult{
				ID:    VectorID(r.Uint32()),
				Score: r.Float32(),
				Source: 1,
			}
		}

		// Function under test
		results := ReciprocalRankFusion("fuzz_dataset", dense, sparse, rrfK, k, nil)

		// Assertions
		if len(results) > k {
			t.Errorf("expected max %d results, got %d", k, len(results))
		}

		// Ensure no panics occurred
	})
}
