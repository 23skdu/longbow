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
// Router determines which peers should receive a query.
type Router struct {
	mu      sync.RWMutex
	regions []Region
	tree    *VPTree // Spatial index for fast lookups
	dirty   bool    // Tree needs rebuild
}

func NewRouter() *Router {
	return &Router{
		regions: make([]Region, 0),
		dirty:   false,
	}
}

// UpdateRegion adds or updates a region in the routing table.
func (r *Router) UpdateRegion(reg Region) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if already exists (naive linear scan for now)
	found := false
	for i, existing := range r.regions {
		if existing.ID == reg.ID && existing.OwnerID == reg.OwnerID {
			r.regions[i] = reg
			found = true
			break
		}
	}
	if !found {
		r.regions = append(r.regions, reg)
	}
	// Invalidate tree on update
	r.dirty = true
}

// Route returns the IDs of peers that might contain relevant vectors.
// It checks if distance(query, centroid) <= radius + epsilon.
func (r *Router) Route(query []float32, limit int) []string {
	r.mu.RLock()
	// Read-safe check. If dirty, upgradable lock needed or just rebuild.
	// For simplicity, upgrade to write lock if dirty.
	if !r.dirty && r.tree != nil {
		defer r.mu.RUnlock()
		return r.searchTree(query, limit)
	}
	r.mu.RUnlock()

	// Rebuild needed
	r.mu.Lock()
	if r.dirty || r.tree == nil {
		// Rebuild
		items := make([]RegionItem, len(r.regions))
		for i := range r.regions {
			// Careful: use pointer to valid memory or copy data.
			// RegionItem uses copy of centroid?
			// Let's use value copy
			items[i] = RegionItem{
				ID:       r.regions[i].ID,
				Centroid: r.regions[i].Centroid,
				Region:   &r.regions[i],
			}
		}
		r.tree = NewVPTree(items)
		r.dirty = false
	}
	r.mu.Unlock()

	// Now search with read lock
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.searchTree(query, limit)
}

func (r *Router) searchTree(query []float32, limit int) []string {
	// Radius heuristic:
	// For routing, we want ALL regions that *potentially* contain the vector.
	// That is any region where Dist(Query, Centroid) < Region.Radius + Epsilon.
	// But VP-Tree searches with a fixed radius Query+Radius.
	// Since regions have different radii, this is tricky for VP-Tree constructed on Centroids.
	// VP-Tree finds Points within R of Query.
	// We want Points P where metric(Q, P) < P.Radius.

	// Standard VP-Tree stores points. It finds P st Dist(Q, P) < SearchRadius.
	// If P.Radius varies significantly, we can't easily pick a SearchRadius.
	// However, usually shards are balanced. Assume max radius?
	// Strategy:
	// Use SearchRadius = (MaxRegionRadius * 1.5).
	// Then filter results.
	// If regions are huge, this degrades.
	// But "Region" usually implies a localized cluster.

	// Scan for max radius during build? Or just assume a safe default?
	// Let's iterate regions to find MaxRadius during Route or cache it.
	// Simplification: We use a generous SearchRadius, e.g. 2x average or fixed if known.
	// Or we scan the first few levels of tree manually? No.

	// Let's cache MaxRadius in Router.
	// For now, iterate to find max (linear, but in memory). Or allow Build to find it.
	// Better: Keep MaxRadius in Router struct.

	// Fallback logic for simplicity without changing struct too much:
	// Scan regions for MaxRadius? No that's O(N).
	// Let's assume a reasonable bounds or change UpdateRegion to track MaxRadius.
	// Given we touch all regions in Update, we can track it.
	// But deletions/updates might shrink it.
	// Let's try to query with a large enough radius, say 100.0 (arbitrary? No).
	// The test uses Radius=10.0.

	// Correct approach:
	// The routing condition is Dist(Q, C) <= R_region * 1.5.
	// We can query the VP-Tree for points where Dist(Q, C) <= R_max * 1.5.
	// Any region with radius R < R_max will be found if valid.
	// Regions with small radius but far away won't be found (correct).

	// Let's effectively add MaxRadius tracking.
	maxR := float32(0)
	for _, reg := range r.regions {
		if reg.Radius > maxR {
			maxR = reg.Radius
		}
	}

	searchRadius := maxR * 1.5
	items := r.tree.Search(query, searchRadius)

	// Post-filter and Sort
	type candidate struct {
		ownerID string
		dist    float32
	}
	var candidates []candidate

	for _, item := range items {
		// Re-verify specific condition
		dist, err := simd.EuclideanDistance(query, item.Centroid)
		if err != nil {
			continue
		}
		if dist <= item.Region.Radius*1.5 {
			candidates = append(candidates, candidate{item.Region.OwnerID, dist})
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].dist < candidates[j].dist
	})

	// Dedup and Limit
	seen := make(map[string]bool)
	result := make([]string, 0, limit)
	for _, c := range candidates {
		if !seen[c.ownerID] {
			result = append(result, c.ownerID)
			seen[c.ownerID] = true
			if len(result) >= limit {
				break
			}
		}
	}
	return result
}
