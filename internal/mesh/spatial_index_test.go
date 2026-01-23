package mesh

import (
	"math/rand"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/stretchr/testify/assert"
)

func TestVPTree_BuildAndSearch(t *testing.T) {
	// 1. Create random points
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	items := make([]RegionItem, 0, 100)
	for i := 0; i < 100; i++ {
		vec := make([]float32, 2)
		vec[0] = rnd.Float32() * 100
		vec[1] = rnd.Float32() * 100
		items = append(items, RegionItem{
			ID:       uint64(i),
			Centroid: vec,
		})
	}

	// 2. Build Tree
	tree := NewVPTree(items)

	// 3. Search
	// Pick a query point
	query := []float32{50.0, 50.0}
	radius := float32(20.0)

	results := tree.Search(query, radius)

	// 4. Verify against Brute Force
	var expected []RegionItem
	for _, item := range items {
		dist, err := simd.EuclideanDistance(query, item.Centroid)
		assert.NoError(t, err)
		if dist <= radius {
			expected = append(expected, item)
		}
	}

	assert.Equal(t, len(expected), len(results), "Should return same number of results")

	// Check content match (ignoring order)
	resMap := make(map[uint64]bool)
	for _, r := range results {
		resMap[r.ID] = true
	}
	for _, e := range expected {
		assert.True(t, resMap[e.ID], "Result should contain ID %d", e.ID)
	}
}
