package store

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHybridSearch_ParallelExecutor(t *testing.T) {
	hs := NewHybridSearcher()

	// Seed data
	numDocs := 100
	for i := 0; i < numDocs; i++ {
		id := VectorID(i)
		vec := make([]float32, 4)
		for k := range vec {
			vec[k] = rand.Float32()
		}
		text := fmt.Sprintf("document number %d common", i)
		hs.Add(id, vec, text)
	}

	// Query
	qVec := make([]float32, 4)
	qText := "common"

	// Run Hybrid Search
	// This will now use the parallel executor
	results := hs.SearchHybrid(qVec, qText, 10, 0.5, 60)

	assert.NotEmpty(t, results, "Should return results")
	assert.LessOrEqual(t, len(results), 10, "Should respect limit")

	// Run Weighted Search
	wResults := hs.SearchHybridWeighted(qVec, qText, 10, 0.5, 60)
	assert.NotEmpty(t, wResults, "Should return weighted results")
}

func TestHybridSearch_ParallelRace(t *testing.T) {
	// A torture test for the race detector
	hs := NewHybridSearcher()
	hs.Add(1, []float32{0.1, 0.1}, "apple")
	hs.Add(2, []float32{0.9, 0.9}, "banana")

	done := make(chan bool)
	go func() {
		for i := 0; i < 100; i++ {
			hs.SearchHybrid([]float32{0.5, 0.5}, "apple", 5, 0.5, 60)
		}
		done <- true
	}()
	go func() {
		for i := 0; i < 100; i++ {
			hs.SearchHybridWeighted([]float32{0.5, 0.5}, "banana", 5, 0.5, 60)
		}
		done <- true
	}()

	<-done
	<-done
}
