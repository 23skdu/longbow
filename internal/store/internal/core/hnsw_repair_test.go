package core

import (
	"context"
	"github.com/23skdu/longbow/internal/store/types"
	"math"
	"math/rand"
	"testing"
	"time"

	basecore "github.com/23skdu/longbow/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHNSW_TombstoneRepair_WiresAround(t *testing.T) {
	// 1. Setup
	config := types.DefaultArrowHNSWConfig()
	config.M = 16
	config.EfConstruction = 64
	config.Dims = 4
	config.Metric = basecore.MetricL2Squared

	idx := NewArrowHNSW(nil, &config, nil)

	// 2. Build a graph with random vectors
	count := 100
	dim := 4
	rng := rand.New(rand.NewSource(42))

	vecs := make([][]float32, count)
	for i := 0; i < count; i++ {
		vecs[i] = make([]float32, dim)
		for j := 0; j < dim; j++ {
			vecs[i][j] = rng.Float32()
		}
		err := idx.InsertWithVector(uint32(i), vecs[i], 0)
		require.NoError(t, err)
	}

	// 3. Mark "Hub" nodes as deleted (Middle range to ensure they have neighbors on both sides)
	deletedStart := 40
	deletedEnd := 60
	for i := deletedStart; i < deletedEnd; i++ {
		_ = idx.Delete(uint32(i))
	}

	// Verify they are tombstones
	hasTombstoneLinks := false
	data := idx.GetData()

	// 2. Add some connections from non-deleted to deleted
	// SearchContext needs to be acquired from the index pool
	ctx := idx.searchPool.Get()
	defer idx.searchPool.Put(ctx)

	for i := 0; i < count; i++ {
		if i >= deletedStart && i < deletedEnd {
			continue
		} // Skip deleted

		// Use fixed rng
		dist := rng.Float32()
		target := uint32(deletedStart + (i % (deletedEnd - deletedStart)))
		data = idx.AddConnection(ctx, data, uint32(i), target, 0, 10, dist)
	}

	// Commit mutations back to index for visibility before check
	idx.data.Store(data)
	data = idx.GetData()

	for i := 0; i < count; i++ {
		if i >= deletedStart && i < deletedEnd {
			continue
		}
		nid := uint32(i)
		neighbors := idx.GetNeighborsCombined(0, nid, math.MaxUint64)

		for _, neighbor := range neighbors {
			if int(neighbor) >= deletedStart && int(neighbor) < deletedEnd {
				hasTombstoneLinks = true
				break
			}
		}
		if hasTombstoneLinks {
			break
		}
	}
	assert.True(t, hasTombstoneLinks, "Graph should initially contain links to tombstones")

	// 4. Run Repair
	goCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	repairedCount := idx.RepairTombstones(goCtx, 100)

	// We expect some repairs, but it depends if wiring around was needed/possible
	assert.Greater(t, repairedCount, 0, "Should have repaired some connections")

	// 5. Verify Tombstones Gone
	hasTombstoneLinksAfter := false
	for i := 0; i < count; i++ {
		if i >= deletedStart && i < deletedEnd {
			continue
		}
		nid := uint32(i)
		neighbors := idx.GetNeighborsCombined(0, nid, math.MaxUint64)
		for _, neighbor := range neighbors {
			if int(neighbor) >= deletedStart && int(neighbor) < deletedEnd {
				hasTombstoneLinksAfter = true
				break
			}
		}
	}
	assert.False(t, hasTombstoneLinksAfter, "Graph should not contain links to tombstones after repair")

	// 6. Verify Reachability
	for i := 0; i < count; i++ {
		if i >= deletedStart && i < deletedEnd {
			continue
		}

		res, err := idx.SearchVectors(context.Background(), vecs[i], 10, nil, types.SearchOptions{})
		require.NoError(t, err)

		found := false
		for _, r := range res {
			if uint32(r.ID) == uint32(i) {
				found = true
				break
			}
		}
		assert.True(t, found, "Node %d should be reachable and found in top results", i)
	}
}
