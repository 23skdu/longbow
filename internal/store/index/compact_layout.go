package index

import (
	"errors"
	"math"
	"sort"

	"github.com/23skdu/longbow/internal/store/types"
)

// MortonEncode2D interleaves the bits of two 32-bit coordinates into a 64-bit Z-order code.
// Used for spatial cache-conscious vector ordering in off-heap arenas.
func MortonEncode2D(x, y uint32) uint64 {
	return (part1By1(y) << 1) | part1By1(x)
}

// MortonEncode3D interleaves the bits of three 21-bit coordinates into a 64-bit Z-order code.
func MortonEncode3D(x, y, z uint32) uint64 {
	return (part1By2(z) << 2) | (part1By2(y) << 1) | part1By2(x)
}

// part1By1 spreads the lowest 32 bits of x into 64 bits, inserting a 0 between each bit.
func part1By1(n uint32) uint64 {
	x := uint64(n)
	x &= 0x00000000ffffffff
	x = (x ^ (x << 16)) & 0x0000ffff0000ffff
	x = (x ^ (x << 8)) & 0x00ff00ff00ff00ff
	x = (x ^ (x << 4)) & 0x0f0f0f0f0f0f0f0f
	x = (x ^ (x << 2)) & 0x3333333333333333
	x = (x ^ (x << 1)) & 0x5555555555555555
	return x
}

// part1By2 spreads the lowest 21 bits of x into 63 bits, inserting two 0s between each bit.
func part1By2(n uint32) uint64 {
	x := uint64(n) & 0x1fffff
	x = (x | (x << 32)) & 0x1f00000000ffff
	x = (x | (x << 16)) & 0x1f0000ff0000ff
	x = (x | (x << 8)) & 0x100f00f00f00f00f
	x = (x | (x << 4)) & 0x10c30c30c30c30c3
	x = (x | (x << 2)) & 0x1249249249249249
	return x
}

// ComputeBFSPermutation traverses the layer 0 graph in breadth-first order
// starting from the entry point, returning a mapping oldToNew and newToOld.
func ComputeBFSPermutation(h *ArrowHNSW) ([]uint32, []uint32) {
	nodeCount := int(h.Len())
	if nodeCount <= 0 {
		return nil, nil
	}

	oldToNew := make([]uint32, nodeCount)
	newToOld := make([]uint32, nodeCount)
	visited := make([]bool, nodeCount)

	ep := h.entryPoint.Load()
	if ep == math.MaxUint32 || int(ep) >= nodeCount {
		ep = 0
	}

	queue := make([]uint32, 0, nodeCount)
	queue = append(queue, ep)
	visited[ep] = true

	gd := h.data.Load()
	if gd == nil {
		for i := 0; i < nodeCount; i++ {
			oldToNew[i] = uint32(i)
			newToOld[i] = uint32(i)
		}
		return oldToNew, newToOld
	}

	fa0, _ := gd.PackedNeighbors[0].(*FlatAdjacency)
	newID := uint32(0)

	// BFS traversal
	head := 0
	for head < len(queue) {
		u := queue[head]
		head++

		oldToNew[u] = newID
		newToOld[newID] = u
		newID++

		if fa0 != nil {
			nbrs, ok := fa0.GetNeighbors(u)
			if ok {
				for _, v := range nbrs {
					if int(v) < nodeCount && !visited[v] {
						visited[v] = true
						queue = append(queue, v)
					}
				}
			}
		}
	}

	// Any unvisited disconnected components are appended sequentially
	for i := 0; i < nodeCount; i++ {
		if !visited[i] {
			oldToNew[i] = newID
			newToOld[newID] = uint32(i)
			newID++
		}
	}

	return oldToNew, newToOld
}

// CompactGraphLayout permutes and reorders adjacency lists across all HNSW layers
// according to BFS discovery order (Item 49).
// By ordering neighbor references according to BFS rank, multi-hop traversals
// access contiguous memory and L2/L3 cache miss rates are significantly reduced.
func CompactGraphLayout(h *ArrowHNSW) error {
	if h == nil {
		return errors.New("compact layout: nil index")
	}

	h.growMu.Lock()
	defer h.growMu.Unlock()

	nodeCount := int(h.Len())
	if nodeCount <= 1 {
		return nil
	}

	oldToNew, _ := ComputeBFSPermutation(h)
	if len(oldToNew) == 0 {
		return nil
	}

	gd := h.data.Load()
	if gd == nil {
		return errors.New("compact layout: nil graph data")
	}

	// For each layer, sort all neighbor lists by BFS discovery rank
	for l := 0; l < types.ArrowMaxLayers; l++ {
		fa, ok := gd.PackedNeighbors[l].(*FlatAdjacency)
		if !ok || fa == nil {
			continue
		}

		for u := 0; u < nodeCount; u++ {
			nbrs, has := fa.GetNeighbors(uint32(u))
			if !has || len(nbrs) <= 1 {
				continue
			}

			// Sort neighbors by their BFS rank (oldToNew)
			sortedNbrs := make([]uint32, len(nbrs))
			copy(sortedNbrs, nbrs)
			sort.Slice(sortedNbrs, func(i, j int) bool {
				rankI := uint32(math.MaxUint32)
				if int(sortedNbrs[i]) < len(oldToNew) {
					rankI = oldToNew[sortedNbrs[i]]
				}
				rankJ := uint32(math.MaxUint32)
				if int(sortedNbrs[j]) < len(oldToNew) {
					rankJ = oldToNew[sortedNbrs[j]]
				}
				return rankI < rankJ
			})

			_ = fa.SetNeighbors(uint32(u), sortedNbrs)
		}
	}

	// Ensure the primary entry point is present in multi-centroids
	if h.multiCentroids != nil {
		h.multiCentroids.AddCentroid(h.entryPoint.Load())
	}

	return nil
}
