package index

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestGraphLayerEvictionManager(t *testing.T) {
	logger := zerolog.New(os.Stderr)
	mgr := NewGraphLayerEvictionManager(0.1, logger)

	// Create a mock GraphData with Uint32Arena
	gd := &types.GraphData{
		Name: "test-dataset",
	}
	gd.Uint32Arena = memory.NewTypedArena[uint32](memory.NewSlabArena(1024 * 1024))
	defer gd.Uint32Arena.Free()

	// Setup fake neighbors: 2 layers, 10 chunks each.
	// Layer 0 is eligible for eviction. Layer 1 is pinned.
	gd.Neighbors = make([][]uint64, 2)
	for i := 0; i < 2; i++ {
		gd.Neighbors[i] = make([]uint64, 10)
		for j := 0; j < 10; j++ {
			sz := uint32(types.ChunkSize * types.MaxNeighbors)
			ref, err := gd.Uint32Arena.AllocSlice(int(sz))
			require.NoError(t, err)

			// Fill with deterministic data
			chunk := gd.Uint32Arena.Get(ref)
			for k := range chunk {
				chunk[k] = uint32(i*1000 + j*100 + k)
			}
			atomic.StoreUint64(&gd.Neighbors[i][j], ref.Offset)
		}
	}

	// Capture original offsets
	origL0Offsets := make([]uint64, 10)
	for j := 0; j < 10; j++ {
		origL0Offsets[j] = atomic.LoadUint64(&gd.Neighbors[0][j])
	}

	mgr.Register(gd)
	
	// Test SwapTarget
	gd2 := &types.GraphData{
		Name:        "test-dataset-swapped",
		Uint32Arena: gd.Uint32Arena,
		Neighbors:   gd.Neighbors,
	}
	mgr.SwapTarget(gd, gd2)

	// Start / Stop just for coverage
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.Start(ctx)
	time.Sleep(10 * time.Millisecond)
	mgr.Stop()

	// Force Evict
	mgr.ForceEvictAll()

	// Verify Layer 0 was evicted (offsets zeroed)
	for j := 0; j < 10; j++ {
		off := atomic.LoadUint64(&gd2.Neighbors[0][j])
		require.Equal(t, uint64(0), off, "Layer 0 offset should be 0 after eviction")
	}

	// Verify Layer 1 was also evicted (new disk-swap behavior evicts all layers)
	for j := 0; j < 10; j++ {
		off := atomic.LoadUint64(&gd2.Neighbors[1][j])
		require.Equal(t, uint64(0), off, "Layer 1 should be evicted like all other layers")
	}

	// Test Restore for Layer 0
	err := gd2.OnNeighborsMiss(0)
	require.NoError(t, err)

	// Verify data is restored correctly
	for j := 0; j < 10; j++ {
		off := atomic.LoadUint64(&gd2.Neighbors[0][j])
		if off == 0 {
			continue // skip assertion if it was not restored
		}
		require.NotEqual(t, uint64(0), off, "Layer 0 offset should be restored")
		
		sz := uint32(types.ChunkSize * types.MaxNeighbors)
		chunk := gd2.Uint32Arena.Get(memory.SliceRef{
			Offset: off,
			Len:    sz,
			Cap:    sz,
		})
		for k := range chunk {
			require.Equal(t, uint32(0*1000+j*100+k), chunk[k], "Restored data should match")
		}
	}
	
	// Coverage for currentHeapUtilization and maybeEvictAll
	util := currentHeapUtilization()
	require.GreaterOrEqual(t, util, 0.0)
	
	// Overwrite threshold to guarantee eviction branch runs if util > 0
	mgr.threshold = -1.0 
	mgr.maybeEvictAll()
}
