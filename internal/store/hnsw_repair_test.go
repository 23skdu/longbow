package store

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHNSW_TombstoneRepair_WiresAround(t *testing.T) {
	// 1. Setup
	config := DefaultArrowHNSWConfig()
	config.M = 8
	config.EfConstruction = 40

	idx := NewArrowHNSW(nil, config, nil)

	// 2. Build a graph with random vectors
	count := 100
	dim := 4
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	vecs := make([][]float32, count)
	for i := 0; i < count; i++ {
		vecs[i] = make([]float32, dim)
		for j := 0; j < dim; j++ {
			vecs[i][j] = rng.Float32()
		}
		err := idx.InsertWithVector(uint32(i), vecs[i], 0)
		require.NoError(t, err)
	}

	// 3. Mark "Hub" nodes as deleted
	deletedCnt := 20
	for i := 0; i < deletedCnt; i++ {
		_ = idx.Delete(uint32(i))
	}

	// Verify they are tombstones
	hasTombstoneLinks := false
	data := idx.data.Load()

	for i := deletedCnt; i < count; i++ {
		nid := uint32(i)
		cID := chunkID(nid)
		cOff := chunkOffset(nid)

		nc := data.GetNeighborsChunk(0, cID)
		cc := data.GetCountsChunk(0, cID)

		if nc != nil && cc != nil {
			cnt := int(atomic.LoadInt32(&cc[cOff]))
			base := int(cOff) * MaxNeighbors
			for k := 0; k < cnt; k++ {
				neighbor := nc[base+k]
				if int(neighbor) < deletedCnt {
					hasTombstoneLinks = true
					break
				}
			}
		}
		if hasTombstoneLinks {
			break
		}
	}
	assert.True(t, hasTombstoneLinks, "Graph should initially contain links to tombstones")

	// 4. Run Repair
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	repairedCount := idx.RepairTombstones(ctx, 100)

	// We expect some repairs, but it depends if wiring around was needed/possible
	assert.Greater(t, repairedCount, 0, "Should have repaired some connections")

	// 5. Verify Tombstones Gone
	hasTombstoneLinksAfter := false
	for i := deletedCnt; i < count; i++ {
		nid := uint32(i)
		cID := chunkID(nid)
		cOff := chunkOffset(nid)

		nc := data.GetNeighborsChunk(0, cID)
		cc := data.GetCountsChunk(0, cID)

		if nc != nil && cc != nil {
			cnt := int(atomic.LoadInt32(&cc[cOff]))
			base := int(cOff) * MaxNeighbors
			for k := 0; k < cnt; k++ {
				neighbor := nc[base+k]
				if int(neighbor) < deletedCnt {
					hasTombstoneLinksAfter = true
					fmt.Printf("Node %d still points to deleted %d\n", nid, neighbor)
					break
				}
			}
		}
	}
	assert.False(t, hasTombstoneLinksAfter, "Graph should not contain links to tombstones after repair")

	// 6. Verify Reachability
	for i := deletedCnt; i < count; i++ {
		res, err := idx.SearchVectors(context.Background(), vecs[i], 1, nil, SearchOptions{})
		require.NoError(t, err)
		if len(res) == 0 {
			t.Errorf("Node %d unreachable", i)
			continue
		}
		assert.Equal(t, uint32(i), uint32(res[0].ID))
	}
}
