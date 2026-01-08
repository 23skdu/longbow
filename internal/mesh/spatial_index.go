package mesh

import (
	"math/rand"
	"sort"

	"github.com/23skdu/longbow/internal/simd"
)

// RegionItem represents a data point in the spatial index.
type RegionItem struct {
	ID       uint64
	Centroid []float32
	// You might keep pointer to original Region or OwnerID here
	Region *Region
}

// SpatialIndex defines the interface for spatial lookups.
type SpatialIndex interface {
	Search(query []float32, radius float32) []RegionItem
}

// VPTree implements a Vantage Point Tree.
type VPTree struct {
	items []RegionItem
	root  *vpNode
}

type vpNode struct {
	item      RegionItem
	threshold float32
	left      *vpNode
	right     *vpNode
}

// NewVPTree builds a static VP-Tree from items.
func NewVPTree(items []RegionItem) *VPTree {
	// Clone items to avoid external mutation affecting tree
	numItems := len(items)
	clone := make([]RegionItem, numItems)
	copy(clone, items)

	root := buildVPTree(clone)
	return &VPTree{
		items: clone,
		root:  root,
	}
}

func buildVPTree(items []RegionItem) *vpNode {
	if len(items) == 0 {
		return nil
	}

	// 1. Pick vantage point (random or first)
	// Random selection is better to avoid worst case
	vpIdx := rand.Intn(len(items))
	// Swap to end
	items[vpIdx], items[len(items)-1] = items[len(items)-1], items[vpIdx]
	vp := items[len(items)-1]

	// Remaining items
	subset := items[:len(items)-1]
	if len(subset) == 0 {
		return &vpNode{item: vp}
	}

	// 2. Compute distances
	type distItem struct {
		item RegionItem
		dist float32
	}
	dists := make([]distItem, len(subset))
	for i, it := range subset {
		dists[i] = distItem{it, simd.EuclideanDistance(vp.Centroid, it.Centroid)}
	}

	// 3. Sort by distance to find median
	sort.Slice(dists, func(i, j int) bool {
		return dists[i].dist < dists[j].dist
	})

	// 4. Split at median
	medianIdx := len(dists) / 2
	threshold := dists[medianIdx].dist

	// Reorder original slice to match split
	// Left: <= threshold, Right: > threshold
	// Note: dists[medianIdx] goes to left? yes.

	leftItems := make([]RegionItem, 0, medianIdx+1)
	rightItems := make([]RegionItem, 0, len(dists)-(medianIdx+1))

	for i, d := range dists {
		if i <= medianIdx {
			leftItems = append(leftItems, d.item)
		} else {
			rightItems = append(rightItems, d.item)
		}
	}

	node := &vpNode{
		item:      vp,
		threshold: threshold,
		left:      buildVPTree(leftItems),
		right:     buildVPTree(rightItems),
	}
	return node
}

// Search finds all items within radius of query.
func (t *VPTree) Search(query []float32, radius float32) []RegionItem {
	var results []RegionItem
	t.searchRecursive(t.root, query, radius, &results)
	return results
}

func (t *VPTree) searchRecursive(node *vpNode, query []float32, radius float32, results *[]RegionItem) {
	if node == nil {
		return
	}

	dist := simd.EuclideanDistance(query, node.item.Centroid)

	// Check node itself
	if dist <= radius {
		*results = append(*results, node.item)
	}

	// Pruning Rules
	// If dist(query, vp) - radius <= threshold -> search Left (inner)
	// If dist(query, vp) + radius >= threshold -> search Right (outer)
	// Note: threshold is median distance to VP.

	// If the query ball overlaps with the "inner" ball (radius threshold around vp)
	// The inner ball contains items where d(vp, item) <= threshold.
	// Triangle inequality: d(item, query) <= radius.
	// We want to find items st d(item, query) <= radius. of course

	// Lower bound of distance from query to any point in Right subtree:
	// Points in Right have d(vp, p) > threshold.
	// d(query, p) >= d(vp, p) - d(vp, query) > threshold - dist.
	// So if threshold - dist > radius, we can prune Right.
	// logic: dist + radius >= threshold.

	// Lower bound of distance from query to any point in Left subtree:
	// Points in Left have d(vp, p) <= threshold.
	// d(query, p) >= d(vp, query) - d(vp, p) >= dist - threshold.
	// So if dist - threshold > radius, we can prune Left.
	// logic: dist - radius <= threshold.

	if dist-radius <= node.threshold {
		t.searchRecursive(node.left, query, radius, results)
	}

	if dist+radius >= node.threshold {
		t.searchRecursive(node.right, query, radius, results)
	}
}
