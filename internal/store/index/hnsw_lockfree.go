package index

// nosec G404 - math/rand/v2 is used for HNSW layer generation, not security-sensitive
import (
	"math"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/store/types"

	"github.com/23skdu/longbow/internal/simd"
)

const (
	// MaxConnections defines the maximum number of neighbors per node per level.
	MaxConnections = 16
	// MaxLayers defines the maximum height of the HNSW graph.
	MaxLayers = 16
	// EfConstruction defines the size of the dynamic candidate list during construction.
	EfConstruction = 128
	// ML is the level multiplier for random level generation.
	ML = 0.36 // 1 / ln(MaxConnections)
)

// LockFreeNode represents a node in the HNSW graph with concurrent access support.
type LockFreeNode struct {
	ID    types.VectorID
	Vec   []float32
	Level int
	// Friends holds adjacency lists for each level.
	// In a true lock-free design, this would be atomic.Pointer to immutable slices.
	// For practicality and performance, we use fine-grained spinlocks or mutexes per node per level.
	// Let's use a simple slice of mutex-protected slices for now, which is "lock-free" at Graph level.
	// To truly meet "CAS-based link updates", we would need to CAS the entire neighbor slice.

	// Friends[level] is a list of neighbor IDs.
	Friends [][]types.VectorID
	mu      sync.RWMutex // Fine-grained lock for this node's connections
}

// LockFreeHNSW implements a high-throughput HNSW index avoiding global locks.
type LockFreeHNSW struct {
	// EntryPoint is the ID of the entry node (highest layer)
	entryPoint atomic.Pointer[LockFreeNode]
	// MaxLayer current max layer in the graph
	maxLayer atomic.Int64

	// Nodes map. For simplicity in this demo, use a sharded map or sync.Map.
	// In production, this would be a pre-allocated array accessed by ID.
	nodes sync.Map // map[types.VectorID]*LockFreeNode

	// Distance function
	distFunc func(a, b []float32) (float32, error)
}

// NewLockFreeHNSW initializes a new LockFreeHNSW index.
func NewLockFreeHNSW() *LockFreeHNSW {
	h := &LockFreeHNSW{
		distFunc: simd.EuclideanDistance,
	}
	h.maxLayer.Store(-1)
	return h
}

func (h *LockFreeHNSW) getNode(id types.VectorID) *LockFreeNode {
	v, ok := h.nodes.Load(id)
	if !ok {
		return nil
	}
	return v.(*LockFreeNode)
}

// Search finds the nearest neighbors for a query vector.
func (h *LockFreeHNSW) Search(query []float32, k, ef int) []types.VectorID {
	ep := h.entryPoint.Load()
	if ep == nil {
		return nil
	}

	// 1. Zoom down to layer 0
	currObj := ep
	dist, err := h.distFunc(query, currObj.Vec)
	if err != nil {
		dist = math.MaxFloat32
	}
	startLevel := currObj.Level

	for level := startLevel; level > 0; level-- {
		changed := true
		for changed {
			changed = false
			currObj.mu.RLock()
			var friends []types.VectorID
			if level < len(currObj.Friends) {
				friends = append([]types.VectorID(nil), currObj.Friends[level]...)
			}
			currObj.mu.RUnlock()

			for _, friendID := range friends {
				fNode := h.getNode(friendID)
				if fNode == nil {
					continue
				}
				d, err := h.distFunc(query, fNode.Vec)
				if err != nil {
					continue
				}
				if d < dist {
					dist = d
					currObj = fNode
					changed = true
				}
			}
		}
	}

	// 2. Search Layer 0 using candidate list
	type candidate struct {
		id   types.VectorID
		dist float32
	}

	efVal := ef
	if efVal < k {
		efVal = k
	}

	visited := make(map[types.VectorID]bool)
	visited[currObj.ID] = true
	candidates := []candidate{{id: currObj.ID, dist: dist}}
	results := []candidate{{id: currObj.ID, dist: dist}}

	for len(candidates) > 0 {
		// Pop best candidate
		bestIdx := 0
		for i := 1; i < len(candidates); i++ {
			if candidates[i].dist < candidates[bestIdx].dist {
				bestIdx = i
			}
		}
		best := candidates[bestIdx]
		candidates = append(candidates[:bestIdx], candidates[bestIdx+1:]...)

		if len(results) >= efVal && best.dist > results[len(results)-1].dist {
			break
		}

		bestNode := h.getNode(best.id)
		if bestNode == nil {
			continue
		}

		bestNode.mu.RLock()
		var friends []types.VectorID
		if len(bestNode.Friends) > 0 {
			friends = append([]types.VectorID(nil), bestNode.Friends[0]...)
		}
		bestNode.mu.RUnlock()

		for _, friendID := range friends {
			if visited[friendID] {
				continue
			}
			visited[friendID] = true
			fNode := h.getNode(friendID)
			if fNode == nil {
				continue
			}
			d, err := h.distFunc(query, fNode.Vec)
			if err != nil {
				continue
			}

			furthestDist := float32(math.MaxFloat32)
			if len(results) >= efVal {
				furthestDist = results[len(results)-1].dist
			}

			if d < furthestDist || len(results) < efVal {
				candidates = append(candidates, candidate{id: friendID, dist: d})

				idx := sort.Search(len(results), func(i int) bool {
					return results[i].dist >= d
				})
				results = append(results, candidate{})
				copy(results[idx+1:], results[idx:])
				results[idx] = candidate{id: friendID, dist: d}
				if len(results) > efVal {
					results = results[:efVal]
				}
			}
		}
	}

	// Return top k results
	if k > len(results) {
		k = len(results)
	}
	ids := make([]types.VectorID, k)
	for i := 0; i < k; i++ {
		ids[i] = results[i].id
	}
	return ids
}

// Add inserts a new vector into the lock-free HNSW index.
func (h *LockFreeHNSW) Add(id types.VectorID, vec []float32) {
	level := h.randomLevel()
	node := &LockFreeNode{
		ID:      id,
		Vec:     vec,
		Level:   level,
		Friends: make([][]types.VectorID, level+1),
	}
	for i := 0; i <= level; i++ {
		node.Friends[i] = make([]types.VectorID, 0, MaxConnections)
	}

	h.nodes.Store(id, node)

	for {
		ep := h.entryPoint.Load()
		if ep == nil {
			if h.entryPoint.CompareAndSwap(nil, node) {
				h.maxLayer.Store(int64(level))
				return
			}
			continue
		}

		currObj := ep
		maxL := int(h.maxLayer.Load())

		// Greedy search down from maxL to level+1
		currDist, err := h.distFunc(vec, currObj.Vec)
		if err != nil {
			currDist = math.MaxFloat32
		}
		for l := maxL; l > level; l-- {
			changed := true
			for changed {
				changed = false
				currObj.mu.RLock()
				var friends []types.VectorID
				if l < len(currObj.Friends) {
					friends = append([]types.VectorID(nil), currObj.Friends[l]...)
				}
				currObj.mu.RUnlock()

				for _, friendID := range friends {
					fNode := h.getNode(friendID)
					if fNode == nil {
						continue
					}
					d, err := h.distFunc(vec, fNode.Vec)
					if err != nil {
						continue
					}
					if d < currDist {
						currDist = d
						currObj = fNode
						changed = true
					}
				}
			}
		}

		// Insert at each level from min(maxL, level) down to 0:
		for l := min(maxL, level); l >= 0; l-- {
			type cand struct {
				node *LockFreeNode
				dist float32
			}
			visited := map[types.VectorID]bool{currObj.ID: true}
			candidates := []cand{{node: currObj, dist: currDist}}
			results := []cand{{node: currObj, dist: currDist}}

			for len(candidates) > 0 {
				bestIdx := 0
				for i := 1; i < len(candidates); i++ {
					if candidates[i].dist < candidates[bestIdx].dist {
						bestIdx = i
					}
				}
				curr := candidates[bestIdx]
				candidates = append(candidates[:bestIdx], candidates[bestIdx+1:]...)

				if len(results) >= EfConstruction && curr.dist > results[len(results)-1].dist {
					break
				}

				curr.node.mu.RLock()
				var friends []types.VectorID
				if l < len(curr.node.Friends) {
					friends = append([]types.VectorID(nil), curr.node.Friends[l]...)
				}
				curr.node.mu.RUnlock()

				for _, friendID := range friends {
					if visited[friendID] {
						continue
					}
					visited[friendID] = true
					fNode := h.getNode(friendID)
					if fNode == nil {
						continue
					}
					d, err := h.distFunc(vec, fNode.Vec)
					if err != nil {
						continue
					}

					furthestDist := float32(math.MaxFloat32)
					if len(results) >= EfConstruction {
						furthestDist = results[len(results)-1].dist
					}

					if d < furthestDist || len(results) < EfConstruction {
						candidates = append(candidates, cand{node: fNode, dist: d})

						idx := sort.Search(len(results), func(i int) bool {
							return results[i].dist >= d
						})
						results = append(results, cand{})
						copy(results[idx+1:], results[idx:])
						results[idx] = cand{node: fNode, dist: d}
						if len(results) > EfConstruction {
							results = results[:EfConstruction]
						}
					}
				}
			}

			// Select top M closest and link bidirectionally
			mMax := MaxConnections
			if l == 0 {
				mMax = MaxConnections * 2
			}
			numToLink := min(len(results), mMax)
			for i := 0; i < numToLink; i++ {
				c := results[i]
				if c.node.ID != node.ID {
					h.link(node, c.node, l)
					h.link(c.node, node, l)
				}
			}
			if len(results) > 0 {
				currObj = results[0].node
				currDist = results[0].dist
			}
		}

		if level > maxL {
			h.maxLayer.Store(int64(level))
			h.entryPoint.Store(node)
		}
		return
	}
}

func (h *LockFreeHNSW) randomLevel() int {
	u := rand.Float64()
	if u == 0 {
		u = 1e-7
	}
	lvl := int(-math.Log(u) * ML)
	if lvl >= MaxLayers {
		lvl = MaxLayers - 1
	}
	return lvl
}

func (h *LockFreeHNSW) link(a, b *LockFreeNode, level int) {
	if b == nil || a.ID == b.ID || level >= len(a.Friends) {
		return
	}
	// Fine-grained lock
	a.mu.Lock()
	defer a.mu.Unlock()

	maxConn := MaxConnections
	if level == 0 {
		maxConn = MaxConnections * 2
	}

	for _, id := range a.Friends[level] {
		if id == b.ID {
			return
		}
	}

	if len(a.Friends[level]) < maxConn {
		a.Friends[level] = append(a.Friends[level], b.ID)
		return
	}

	// Prune furthest neighbor if b is closer
	furthestIdx := -1
	var maxDist float32 = -1
	for idx, fID := range a.Friends[level] {
		fNode := h.getNode(fID)
		if fNode == nil {
			furthestIdx = idx
			maxDist = math.MaxFloat32
			break
		}
		d, err := h.distFunc(a.Vec, fNode.Vec)
		if err != nil {
			d = math.MaxFloat32
		}
		if d > maxDist {
			maxDist = d
			furthestIdx = idx
		}
	}

	newDist, err := h.distFunc(a.Vec, b.Vec)
	if err != nil {
		newDist = math.MaxFloat32
	}
	if furthestIdx != -1 && newDist < maxDist {
		a.Friends[level][furthestIdx] = b.ID
	}
}
