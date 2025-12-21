package mesh

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRouter_Route(t *testing.T) {
	router := NewRouter()

	// Region A: Centroid [0, 0], Radius 10. Owner "node1"
	router.UpdateRegion(Region{
		ID:       1,
		Centroid: []float32{0, 0},
		Radius:   10.0,
		OwnerID:  "node1",
	})

	// Region B: Centroid [100, 100], Radius 10. Owner "node2"
	router.UpdateRegion(Region{
		ID:       2,
		Centroid: []float32{100, 100},
		Radius:   10.0,
		OwnerID:  "node2",
	})

	// Query close to A ([2, 2])
	// Dist to A (0,0) = sqrt(8) ~ 2.8 < 10*1.5 -> Match
	// Dist to B (100,100) = massive -> No Match
	peers := router.Route([]float32{2, 2}, 5)
	assert.Contains(t, peers, "node1")
	assert.NotContains(t, peers, "node2")

	// Query close to B ([95, 95])
	peers = router.Route([]float32{95, 95}, 5)
	assert.Contains(t, peers, "node2")
	assert.NotContains(t, peers, "node1")

	// Query in between (but slightly closer to A, outside both radii)
	// [30, 30]
	// Dist A ~ 42 > 15. No match.
	// Dist B ~ 98 > 15. No match.
	peers = router.Route([]float32{30, 30}, 5)
	assert.Empty(t, peers)
}
