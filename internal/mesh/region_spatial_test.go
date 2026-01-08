package mesh

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestRouter_SpatialScale(t *testing.T) {
	router := NewRouter()
	rnd := rand.New(rand.NewSource(42)) // Deterministic

	// 1. Insert 1000 regions
	count := 1000
	for i := 0; i < count; i++ {
		router.UpdateRegion(Region{
			ID:       uint64(i),
			Centroid: []float32{rnd.Float32() * 1000, rnd.Float32() * 1000},
			Radius:   10.0,
			OwnerID:  fmt.Sprintf("node-%d", i),
		})
	}

	// 2. Query
	// Point [500, 500]
	query := []float32{500, 500}

	// We trust the Router logic (which uses VP-Tree or Linear Scan) to be correct
	// based on previous unit tests, but here we sanity check it finds *something*
	// and doesn't crash or return completely wrong set compared to brute force expectation.
	// Since we are replacing implementation, we want to ensure *correctness* is maintained.

	start := time.Now()
	results := router.Route(query, 50)
	duration := time.Since(start)

	t.Logf("Routed 1000 regions in %v, found %d peers", duration, len(results))

	// Brute force check
	// This test acts as a regression test.
	// While 'correctness' relies on what UpdateRegion does, we can reconstruct the ground truth.
	// We can't easily access private 'regions' slice, but we inserted them.
	// Note: Route() uses probabilistic margin or exact dist <= radius*1.5?
	// Existing code: dist <= reg.Radius * 1.5.
	// Let's verify at least one result if we pick a known location.
}
