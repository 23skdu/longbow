package index

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"

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
				id := types.VectorID(processIdx*(numVectors/numWorkers) + j)
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

	// Structural integrity: verify no self-links and no duplicates
	h.nodes.Range(func(key, value any) bool {
		node := value.(*LockFreeNode)
		node.mu.RLock()
		defer node.mu.RUnlock()
		for lvl, friends := range node.Friends {
			seen := make(map[types.VectorID]bool)
			for _, fid := range friends {
				assert.NotEqual(t, node.ID, fid, "Node %d should not have a self-link at level %d", node.ID, lvl)
				assert.False(t, seen[fid], "Node %d has duplicate link to %d at level %d", node.ID, fid, lvl)
				seen[fid] = true
			}
			maxAllowed := MaxConnections
			if lvl == 0 {
				maxAllowed = MaxConnections * 2
			}
			assert.LessOrEqual(t, len(friends), maxAllowed)
		}
		return true
	})
}

func TestLockFreeHNSW_Recall(t *testing.T) {
	h := NewLockFreeHNSW()
	dims := 8
	numVectors := 200

	r := rand.New(rand.NewSource(42))
	vecs := make([][]float32, numVectors)
	for i := 0; i < numVectors; i++ {
		vec := make([]float32, dims)
		for d := range vec {
			vec[d] = r.Float32()
		}
		vecs[i] = vec
		h.Add(types.VectorID(i), vec)
	}

	// Query each vector directly - 1-NN should retrieve the exact vector with distance ~0
	hits := 0
	for i := 0; i < numVectors; i++ {
		res := h.Search(vecs[i], 1, 64)
		if len(res) > 0 && res[0] == types.VectorID(i) {
			hits++
		}
	}
	recall := float64(hits) / float64(numVectors)
	assert.GreaterOrEqual(t, recall, 0.95, "Exact-vector recall should be at least 95%")
}
