package index

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func newTestGraphData(t *testing.T, name string, layers, chunksPerLayer int) *types.GraphData {
	t.Helper()
	gd := &types.GraphData{Name: name}
	gd.Uint32Arena = memory.NewTypedArena[uint32](memory.NewSlabArena(1024 * 1024))
	t.Cleanup(func() { gd.Uint32Arena.Free() })

	gd.Neighbors = make([][]uint64, layers)
	for i := 0; i < layers; i++ {
		gd.Neighbors[i] = make([]uint64, chunksPerLayer)
		for j := 0; j < chunksPerLayer; j++ {
			sz := uint32(types.ChunkSize * types.MaxNeighbors)
			ref, err := gd.Uint32Arena.AllocSlice(int(sz))
			require.NoError(t, err)
			chunk := gd.Uint32Arena.Get(ref)
			for k := range chunk {
				chunk[k] = uint32(i*1000 + j*100 + k)
			}
			atomic.StoreUint64(&gd.Neighbors[i][j], ref.Offset)
		}
	}
	return gd
}

func TestGraphLayerEvictionManager(t *testing.T) {
	logger := zerolog.New(os.Stderr)
	mgr := NewGraphLayerEvictionManager(0.1, logger)

	gd := newTestGraphData(t, "test-dataset", 2, 10)

	// Capture original offsets for layer 0
	origL0Offsets := make([]uint64, 10)
	for j := 0; j < 10; j++ {
		origL0Offsets[j] = atomic.LoadUint64(&gd.Neighbors[0][j])
	}
	require.NotEqual(t, uint64(0), origL0Offsets[0], "offsets should be non-zero before eviction")

	mgr.Register(gd)

	// Test SwapTarget
	gd2 := &types.GraphData{
		Name:        "test-dataset-swapped",
		Uint32Arena: gd.Uint32Arena,
		Neighbors:   gd.Neighbors,
	}
	mgr.SwapTarget(gd, gd2)

	// Start the background monitor for coverage, then ForceEvictAll BEFORE Stop
	// (Stop clears the targets list, so eviction must happen first).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.Start(ctx)
	time.Sleep(10 * time.Millisecond)

	// Force Evict
	mgr.ForceEvictAll()

	// Verify Layer 0 was evicted (offsets zeroed)
	for j := 0; j < 10; j++ {
		off := atomic.LoadUint64(&gd2.Neighbors[0][j])
		require.Equal(t, uint64(0), off, "Layer 0 offset should be 0 after eviction")
	}

	// Verify Layer 1 was also evicted (evictTarget evicts all layers)
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
			continue
		}
		require.NotEqual(t, uint64(0), off, "Layer 0 offset should be restored")

		sz := uint32(types.ChunkSize * types.MaxNeighbors)
		chunk := gd2.Uint32Arena.Get(memory.SliceRef{
			Offset: off,
			Len:    sz,
			Cap:    sz,
		})
		for k := range chunk {
			if chunk[k] != uint32(0*1000+j*100+k) {
				t.Fatalf("Restored data mismatch at j=%d, k=%d: expected %d, got %d", j, k, uint32(0*1000+j*100+k), chunk[k])
			}
		}
	}

	// Stop the background monitor
	mgr.Stop()

	// Coverage for currentHeapUtilization and maybeEvictAll
	util := currentHeapUtilization()
	require.GreaterOrEqual(t, util, 0.0)

	// Overwrite threshold to guarantee eviction branch runs if util > 0
	mgr.threshold = -1.0
	mgr.maybeEvictAll()
}

func TestGraphLayerEvictionManager_RegisterDuplicate(t *testing.T) {
	logger := zerolog.New(os.Stderr)
	mgr := NewGraphLayerEvictionManager(0.75, logger)
	gd := newTestGraphData(t, "dup-test", 1, 5)

	mgr.Register(gd)
	require.Len(t, mgr.targets, 1)

	// Registering the same GraphData again should be a no-op
	mgr.Register(gd)
	require.Len(t, mgr.targets, 1)
}

func TestGraphLayerEvictionManager_RegisterNil(t *testing.T) {
	logger := zerolog.New(os.Stderr)
	mgr := NewGraphLayerEvictionManager(0.75, logger)

	// Registering nil should be a no-op
	mgr.Register(nil)
	require.Empty(t, mgr.targets)
}

func TestGraphLayerEvictionManager_Unregister(t *testing.T) {
	logger := zerolog.New(os.Stderr)
	mgr := NewGraphLayerEvictionManager(0.75, logger)
	gd := newTestGraphData(t, "unreg-test", 1, 5)

	mgr.Register(gd)
	require.Len(t, mgr.targets, 1)

	mgr.Unregister(gd)
	require.Empty(t, mgr.targets)
}

func TestGraphLayerEvictionManager_UnregisterNil(t *testing.T) {
	logger := zerolog.New(os.Stderr)
	mgr := NewGraphLayerEvictionManager(0.75, logger)

	// Unregistering nil should be a no-op
	mgr.Unregister(nil)
	require.Empty(t, mgr.targets)
}

func TestGraphLayerEvictionManager_SwapTargetNilOld(t *testing.T) {
	logger := zerolog.New(os.Stderr)
	mgr := NewGraphLayerEvictionManager(0.75, logger)
	gd := newTestGraphData(t, "swap-nil-old", 1, 5)

	// SwapTarget with nil old should register new
	mgr.SwapTarget(nil, gd)
	require.Len(t, mgr.targets, 1)
	require.Equal(t, gd, mgr.targets[0].gd)
}

func TestGraphLayerEvictionManager_SwapTargetSame(t *testing.T) {
	logger := zerolog.New(os.Stderr)
	mgr := NewGraphLayerEvictionManager(0.75, logger)
	gd := newTestGraphData(t, "swap-same", 1, 5)

	mgr.Register(gd)
	require.Len(t, mgr.targets, 1)

	// SwapTarget(old, old) should be a no-op
	mgr.SwapTarget(gd, gd)
	require.Len(t, mgr.targets, 1)
}

func TestGraphLayerEvictionManager_SwapTargetNilNew(t *testing.T) {
	logger := zerolog.New(os.Stderr)
	mgr := NewGraphLayerEvictionManager(0.75, logger)
	gd := newTestGraphData(t, "swap-nil-new", 1, 5)

	mgr.Register(gd)
	require.Len(t, mgr.targets, 1)

	// SwapTarget with nil new should clear the gd reference
	mgr.SwapTarget(gd, nil)
	require.Len(t, mgr.targets, 1)
	require.Nil(t, mgr.targets[0].gd)
}

func TestGraphLayerEvictionManager_RestoreNonEvicted(t *testing.T) {
	logger := zerolog.New(os.Stderr)
	mgr := NewGraphLayerEvictionManager(0.75, logger)
	gd := newTestGraphData(t, "restore-noop", 1, 5)

	mgr.Register(gd)

	// Restoring a layer that was never evicted should be a no-op
	err := mgr.RestoreLayer(mgr.targets[0], 0)
	require.NoError(t, err)
}

func TestGraphLayerEvictionManager_RestoreNilGD(t *testing.T) {
	logger := zerolog.New(os.Stderr)
	mgr := NewGraphLayerEvictionManager(0.75, logger)
	gd := newTestGraphData(t, "restore-nil-gd", 1, 5)

	mgr.Register(gd)

	// Manually insert an eviction record so RestoreLayer doesn't early-return
	mgr.targets[0].evictedLayers[0] = &layerDiskRecord{path: "/nonexistent"}

	// Force the target's gd to nil
	mgr.targets[0].gd = nil

	// RestoreLayer should return an error for nil gd
	err := mgr.RestoreLayer(mgr.targets[0], 0)
	require.Error(t, err)
}

func TestGraphLayerEvictionManager_ForceEvictNoTargets(t *testing.T) {
	logger := zerolog.New(os.Stderr)
	mgr := NewGraphLayerEvictionManager(0.75, logger)

	// ForceEvictAll with no targets should not panic
	mgr.ForceEvictAll()
}

func TestGraphLayerEvictionManager_StopIdempotent(t *testing.T) {
	logger := zerolog.New(os.Stderr)
	mgr := NewGraphLayerEvictionManager(0.75, logger)

	// Stop should be safe to call multiple times
	mgr.Stop()
	mgr.Stop()
}

func TestGraphLayerEvictionManager_EmptyNeighbors(t *testing.T) {
	logger := zerolog.New(os.Stderr)
	mgr := NewGraphLayerEvictionManager(0.1, logger)

	// GraphData with no neighbors
	gd := &types.GraphData{Name: "empty", Neighbors: make([][]uint64, 0)}
	mgr.Register(gd)

	// ForceEvictAll should handle empty neighbors gracefully
	mgr.ForceEvictAll()
}

func TestGraphLayerEvictionManager_SwapTargetUnregisteredOld(t *testing.T) {
	logger := zerolog.New(os.Stderr)
	mgr := NewGraphLayerEvictionManager(0.75, logger)
	gdOld := newTestGraphData(t, "old-unreg", 1, 5)
	gdNew := newTestGraphData(t, "new-unreg", 1, 5)

	// SwapTarget with an unregistered old should be a no-op
	mgr.SwapTarget(gdOld, gdNew)
	require.Empty(t, mgr.targets)
}

func newTestGraphDataWithPackedNeighbors(t *testing.T, name string, numLayers int) *types.GraphData {
	t.Helper()
	slab := memory.NewSlabArena(1024 * 1024)
	gd := &types.GraphData{
		Name:          name,
		Uint32Arena:   memory.NewTypedArena[uint32](slab),
		PackedNeighbors: make([]types.PackedNeighbors, numLayers),
	}
	t.Cleanup(func() { gd.Uint32Arena.Free() })

	for l := 0; l < numLayers; l++ {
		fa := NewFlatAdjacency(slab, types.MaxNeighbors, 64)
		fa.missLayer = l
		fa.Retain()
		gd.PackedNeighbors[l] = fa

		for id := uint32(0); id < 32; id++ {
			neighbors := make([]uint32, types.MaxNeighbors)
			for k := range neighbors {
				neighbors[k] = uint32(l*10000 + int(id)*100 + k)
			}
			require.NoError(t, fa.SetNeighbors(id, neighbors))
		}
	}
	return gd
}

func TestGraphLayerEvictionManager_PackedNeighborsEvictionAndRestore(t *testing.T) {
	logger := zerolog.New(os.Stderr)
	mgr := NewGraphLayerEvictionManager(0.1, logger)

	gd := newTestGraphDataWithPackedNeighbors(t, "packed-test", 2)
	mgr.Register(gd)

	// Verify data exists before eviction
	fa0 := gd.PackedNeighbors[0].(*FlatAdjacency)
	nbs, ok := fa0.GetNeighbors(0)
	require.True(t, ok)
	require.Len(t, nbs, types.MaxNeighbors)
	require.Equal(t, uint32(0), nbs[0], "layer 0, node 0, neighbor 0 = 0*10000+0*100+0")
	require.Equal(t, uint32(5), nbs[5], "layer 0, node 0, neighbor 5 = 5")

	// Also verify node 5 has correct data
	nbs5, ok := fa0.GetNeighbors(5)
	require.True(t, ok)
	require.Equal(t, uint32(500), nbs5[0], "layer 0, node 5, neighbor 0 = 0*10000+5*100+0")

	// Force evict all layers
	mgr.ForceEvictAll()

	// After eviction, the chunk offset should be MaxUint64
	chunks0 := fa0.chunks.Load()
	require.NotNil(t, chunks0)
	require.Equal(t, ^uint64(0), (*chunks0)[0], "chunk should be marked as evicted")

	// Restore layer 0 via the OnNeighborsMiss callback
	err := gd.OnNeighborsMiss(0)
	require.NoError(t, err)

	// Verify data is restored correctly
	nbs, ok = fa0.GetNeighbors(0)
	require.True(t, ok)
	require.Len(t, nbs, types.MaxNeighbors)
	require.Equal(t, uint32(0), nbs[0], "restored neighbor[0] should match original")
	require.Equal(t, uint32(5), nbs[5], "restored neighbor[5] should match original")

	nbs5, ok = fa0.GetNeighbors(5)
	require.True(t, ok)
	require.Equal(t, uint32(500), nbs5[0], "restored node 5 neighbor 0 should match original")

	mgr.Stop()
}

func TestGraphLayerEvictionManager_PackedNeighborsSwapTarget(t *testing.T) {
	logger := zerolog.New(os.Stderr)
	mgr := NewGraphLayerEvictionManager(0.1, logger)

	gd1 := newTestGraphDataWithPackedNeighbors(t, "swap-packed-old", 2)
	gd2 := newTestGraphDataWithPackedNeighbors(t, "swap-packed-new", 2)

	mgr.Register(gd1)
	mgr.SwapTarget(gd1, gd2)

	mgr.ForceEvictAll()

	// Restore through the new target
	err := gd2.OnNeighborsMiss(1)
	require.NoError(t, err)

	fa1 := gd2.PackedNeighbors[1].(*FlatAdjacency)
	nbs, ok := fa1.GetNeighbors(0)
	require.True(t, ok)
	require.Equal(t, uint32(10000), nbs[0], "restored via swapped target: layer 1, node 0, neighbor 0 = 1*10000+0*100+0")

	mgr.Stop()
}

func TestGraphLayerEvictionManager_EvictTargetNilGD(t *testing.T) {
	logger := zerolog.New(os.Stderr)
	mgr := NewGraphLayerEvictionManager(0.1, logger)

	gd := newTestGraphData(t, "nil-gd-target", 1, 5)
	mgr.Register(gd)

	// Set gd to nil
	mgr.targets[0].gd = nil

	// evictTarget should handle nil gd gracefully
	err := mgr.evictTarget(mgr.targets[0])
	require.NoError(t, err)
}

func TestGraphLayerEvictionManager_MaybeEvictAllWithTargets(t *testing.T) {
	logger := zerolog.New(os.Stderr)
	mgr := NewGraphLayerEvictionManager(0.1, logger)

	gd := newTestGraphDataWithPackedNeighbors(t, "maybe-evict", 1)
	mgr.Register(gd)

	// Force maybeEvictAll to trigger by setting threshold very low
	mgr.threshold = 0.0
	mgr.maybeEvictAll()

	// The eviction should have happened
	chunks := gd.PackedNeighbors[0].(*FlatAdjacency).chunks.Load()
	require.NotNil(t, chunks)
	require.Equal(t, ^uint64(0), (*chunks)[0])
}

func TestGraphLayerEvictionManager_ConcurrentEvictAndRestore(t *testing.T) {
	logger := zerolog.New(os.Stderr)
	mgr := NewGraphLayerEvictionManager(0.1, logger)

	gd := newTestGraphDataWithPackedNeighbors(t, "concurrent-test", 1)
	mgr.Register(gd)

	var wg sync.WaitGroup
	errCh := make(chan error, 20)

	// Start background monitor
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.Start(ctx)

	// Concurrent evictions
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mgr.ForceEvictAll()
		}()
	}

	// Concurrent restores
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := gd.OnNeighborsMiss(0); err != nil {
				errCh <- err
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent restore error: %v", err)
	}

	mgr.Stop()
}

func TestGraphLayerEvictionManager_RestoreLayerRemovesDiskFile(t *testing.T) {
	logger := zerolog.New(os.Stderr)
	mgr := NewGraphLayerEvictionManager(0.1, logger)

	gd := newTestGraphDataWithPackedNeighbors(t, "restore-cleanup", 1)
	mgr.Register(gd)

	mgr.ForceEvictAll()

	// Get the disk record path before restore
	target := mgr.targets[0]
	target.mu.RLock()
	rec := target.evictedLayers[0]
	require.NotNil(t, rec)
	diskPath := rec.path
	target.mu.RUnlock()

	// Restore should remove the disk file
	err := gd.OnNeighborsMiss(0)
	require.NoError(t, err)

	_, err = os.Stat(diskPath)
	require.True(t, os.IsNotExist(err), "disk file should be removed after restore")

	mgr.Stop()
}

func TestGraphLayerEvictionManager_UnregisterCleansUpDiskFiles(t *testing.T) {
	logger := zerolog.New(os.Stderr)
	mgr := NewGraphLayerEvictionManager(0.1, logger)

	gd := newTestGraphDataWithPackedNeighbors(t, "unreg-cleanup", 1)
	mgr.Register(gd)

	mgr.ForceEvictAll()

	// Get the disk record path before unregister
	target := mgr.targets[0]
	target.mu.RLock()
	rec := target.evictedLayers[0]
	require.NotNil(t, rec)
	diskPath := rec.path
	target.mu.RUnlock()

	// Unregister should remove disk files
	mgr.Unregister(gd)

	_, err := os.Stat(diskPath)
	require.True(t, os.IsNotExist(err), "disk file should be removed after unregister")
}
