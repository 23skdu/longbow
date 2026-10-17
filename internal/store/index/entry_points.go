package index

import (
	"math"
	"sync"
)

// MultiCentroidTracker manages a pool of coarse centroids on the upper layers of HNSW.
// For clustered and high-density topologies (500k+ vectors), queries can evaluate
// distance to these candidate entry points and jump directly into the closest local
// cluster, cutting 4-6 hops of top-layer random walking.
type MultiCentroidTracker struct {
	mu        sync.RWMutex
	centroids []uint32 // Top-layer candidate node IDs
	maxCount  int
}

// NewMultiCentroidTracker creates a new tracker with the specified capacity (default 16).
func NewMultiCentroidTracker(maxCount int) *MultiCentroidTracker {
	if maxCount <= 0 {
		maxCount = 16
	}
	return &MultiCentroidTracker{
		centroids: make([]uint32, 0, maxCount),
		maxCount:  maxCount,
	}
}

// AddCentroid registers a candidate top-layer node as a cluster centroid.
func (m *MultiCentroidTracker) AddCentroid(nodeID uint32) {
	if nodeID == math.MaxUint32 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.centroids {
		if c == nodeID {
			return
		}
	}
	if len(m.centroids) < m.maxCount {
		m.centroids = append(m.centroids, nodeID)
	}
}

// GetCentroids returns a snapshot copy of the registered centroids.
func (m *MultiCentroidTracker) GetCentroids() []uint32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]uint32, len(m.centroids))
	copy(out, m.centroids)
	return out
}

// SelectBestEntryPoint evaluates distances from the query to candidate centroids
// and selects the entry node with minimum distance to seed the beam search.
func (m *MultiCentroidTracker) SelectBestEntryPoint(defaultEP uint32, comp DistanceComputer) uint32 {
	m.mu.RLock()
	count := len(m.centroids)
	if count == 0 || comp == nil {
		m.mu.RUnlock()
		return defaultEP
	}

	var local [32]uint32
	n := copy(local[:], m.centroids)
	m.mu.RUnlock()

	bestEP := defaultEP
	bestDist := float32(math.MaxFloat32)

	if defaultEP != math.MaxUint32 {
		if d, err := comp.ComputeSingle(defaultEP); err == nil {
			bestDist = d
		}
	}

	for i := 0; i < n; i++ {
		cID := local[i]
		if cID == defaultEP || cID == math.MaxUint32 {
			continue
		}
		if d, err := comp.ComputeSingle(cID); err == nil {
			if d < bestDist {
				bestDist = d
				bestEP = cID
			}
		}
	}

	return bestEP
}
