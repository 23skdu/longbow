package index

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockComputer struct {
	distances map[uint32]float32
}

func (m *mockComputer) ComputeSingle(id uint32) (float32, error) {
	if d, ok := m.distances[id]; ok {
		return d, nil
	}
	return math.MaxFloat32, nil
}

func (m *mockComputer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	dst = dst[:0]
	for _, id := range ids {
		d, err := m.ComputeSingle(id)
		if err != nil {
			return nil, err
		}
		dst = append(dst, d)
	}
	return dst, nil
}

func (m *mockComputer) Prefetch(id uint32) {}

func TestMultiCentroidTracker_AddAndSelect(t *testing.T) {
	tracker := NewMultiCentroidTracker(4)

	// Test fallback when empty
	comp := &mockComputer{
		distances: map[uint32]float32{
			10: 5.0,
			20: 2.0,
			30: 8.0,
			40: 1.0,
		},
	}
	assert.Equal(t, uint32(10), tracker.SelectBestEntryPoint(10, comp))

	// Register centroids
	tracker.AddCentroid(10)
	tracker.AddCentroid(20)
	tracker.AddCentroid(30)
	tracker.AddCentroid(20) // Duplicate, should be ignored
	tracker.AddCentroid(40)

	centroids := tracker.GetCentroids()
	assert.Len(t, centroids, 4)
	assert.Equal(t, []uint32{10, 20, 30, 40}, centroids)

	// Max count overflow check
	tracker.AddCentroid(50)
	assert.Len(t, tracker.GetCentroids(), 4) // Capped at 4

	// Select best entry point: node 40 has dist 1.0, which is smaller than defaultEP 10 (dist 5.0)
	best := tracker.SelectBestEntryPoint(10, comp)
	assert.Equal(t, uint32(40), best)

	// If defaultEP already has the minimum distance, it should be retained
	comp.distances[10] = 0.5
	best = tracker.SelectBestEntryPoint(10, comp)
	assert.Equal(t, uint32(10), best)
}
