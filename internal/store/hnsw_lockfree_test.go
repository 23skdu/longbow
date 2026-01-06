package store


import (
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLockFreeHNSW_Concurrency(t *testing.T) {
	h := NewLockFreeHNSW()

	const (
		numWorkers = 8
		numVectors = 1000
		dims       = 4
	)

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	// Parallel insertion
	for i := 0; i < numWorkers; i++ {
		go func(processIdx int) {
			defer wg.Done()
			for j := 0; j < numVectors/numWorkers; j++ {
				id := VectorID(processIdx*(numVectors/numWorkers) + j)
				vec := make([]float32, dims)
				for k := range vec {
					vec[k] = rand.Float32()
				}
				h.Add(id, vec)
			}
		}(i)
	}
	wg.Wait()

	// Validation
	// Check if all nodes are present
	count := 0
	h.nodes.Range(func(key, value any) bool {
		count++
		return true
	})
	assert.Equal(t, numVectors, count, "All vectors should be inserted")

	// Basic Search check
	query := make([]float32, dims)
	res := h.Search(query, 5, 100)
	assert.NotEmpty(t, res, "Search should return results")
}
