package mesh

import (
	"sort"
	"sync"

	"github.com/23skdu/longbow/internal/simd"
)

// Region represents a cluster of vectors owned by a peer.
type Region struct {
	ID       uint64
	Centroid []float32
	Radius   float32
	OwnerID  string
}

// Router determines which peers should receive a query.
type Router struct {
	mu      sync.RWMutex
	regions []Region
}

func NewRouter() *Router {
	return &Router{
		regions: make([]Region, 0),
	}
}

// UpdateRegion adds or updates a region in the routing table.
func (r *Router) UpdateRegion(reg Region) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if already exists (naive linear scan for now)
	for i, existing := range r.regions {
		if existing.ID == reg.ID && existing.OwnerID == reg.OwnerID {
			r.regions[i] = reg
			return
		}
	}
	r.regions = append(r.regions, reg)
}

// Route returns the IDs of peers that might contain relevant vectors.
// It checks if distance(query, centroid) <= radius + epsilon.
func (r *Router) Route(query []float32, limit int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	type candidate struct {
		ownerID string
		dist    float32
	}
	var candidates []candidate

	// TODO: Replace linear scan with spatial index (VP-Tree or BK-Tree) for large routing tables.
	for _, reg := range r.regions {
		// Calculate distance to centroid
		dist := simd.EuclideanDistance(query, reg.Centroid)

		// Heuristic: If we are "close enough" to the region, include it.
		// Close enough = distance is within region radius (plus margin).
		// Wait, for KNN, we might need to check even if outside radius if k is large.
		// For now, strict containment + margin logic.
		if dist <= reg.Radius*1.5 { // 50% margin
			candidates = append(candidates, candidate{reg.OwnerID, dist})
		}
	}

	// Sort by distance to centroid (closest regions first)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].dist < candidates[j].dist
	})

	// Dedup owners
	maxRes := limit
	if len(candidates) < maxRes {
		maxRes = len(candidates)
	}

	seen := make(map[string]bool)
	result := make([]string, 0, maxRes)

	for _, c := range candidates {
		if !seen[c.ownerID] {
			result = append(result, c.ownerID)
			seen[c.ownerID] = true
			if len(result) >= maxRes {
				break
			}
		}
	}

	return result
}
