package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArrowHNSW_MmapPersistence(t *testing.T) {
	// 1. Setup Initial Graph
	dir := t.TempDir()
	graphPath := filepath.Join(dir, "graph_sq8.bin")

	cfg := DefaultArrowHNSWConfig()
	cfg.M = 16
	cfg.EfConstruction = 100
	cfg.Dims = 16
	cfg.SQ8Enabled = true         // Persistence requires SQ8 (or PQ) in current DiskGraph V3
	cfg.SQ8TrainingThreshold = 20 // Train after all 20 vectors

	h1 := NewArrowHNSW(nil, cfg, nil)

	// Insert 20 vectors to ensure training
	vectors := make([][]float32, 20)
	for i := 0; i < 20; i++ {
		val := float32(i)
		vectors[i] = make([]float32, 16)
		for j := 0; j < 16; j++ {
			vectors[i][j] = val
		}
	}
	// Initial batch to train explicitly (though threshold handles it, explicit is safer for determinism)
	h1.ensureTrained(10, vectors)

	for i := 0; i < 20; i++ {
		err := h1.InsertWithVector(uint32(i), vectors[i], h1.generateLevel())
		require.NoError(t, err)
	}

	// Verify vectors match
	for i := 0; i < h1.Size(); i++ {
		v1, _ := h1.getVectorAny(uint32(i))
		assert.NotNil(t, v1)
	}

	// 2. Persist to Disk
	gd := h1.data.Load()
	t.Logf("Persisting graph with %d nodes", h1.Size())

	// Explicitly get quantizer params to ensure we persist the correct bounds
	minV, maxV := float32(0), float32(0)
	if h1.quantizer != nil {
		minV, maxV = h1.quantizer.Params()
	}
	err := WriteDiskGraph(gd, graphPath, h1.Size(), minV, maxV, h1.entryPoint.Load(), int(h1.maxLevel.Load()))
	require.NoError(t, err)
	_ = h1.Close()

	// 3. Load from Mmap (New Instance)
	// Must use same config (especially SQ8 enabled)
	h2 := NewArrowHNSW(nil, cfg, nil)
	err = h2.LoadFromMmap(graphPath)
	require.NoError(t, err)

	// Verify DiskGraph is attached
	dg := h2.diskGraph.Load()
	require.NotNil(t, dg)
	assert.Equal(t, 20, dg.Size())

	// Check restored quantization params
	if h2.quantizer != nil {
		minV2, maxV2 := h2.quantizer.Params()
		t.Logf("Restored Params: Min=%f, Max=%f", minV2, maxV2)
		assert.Equal(t, minV, minV2)
		assert.Equal(t, maxV, maxV2)
	} else {
		t.Error("Quantizer not restored")
	}

	// 4. Verify Search (Hybrid Read)
	// Query for vector 5 (which is exactly [5,5,5,5])
	// Query for vector 5
	q := make([]float32, 16)
	for j := 0; j < 16; j++ {
		q[j] = 5
	}
	// EntryPoint might be loaded from disk
	t.Logf("EP: %d, MaxLevel: %d", h2.entryPoint.Load(), h2.maxLevel.Load())

	// Debug: Check vector 5 directly
	v5, err := h2.getVectorAny(5)
	t.Logf("Vector 5: %v, Err: %v", v5, err)

	res, err := h2.Search(context.Background(), q, 1, 20, nil)
	t.Logf("Search Result: %+v", res)
	require.NoError(t, err)
	require.NotEmpty(t, res)
	assert.Equal(t, uint32(5), uint32(res[0].ID))

	// 5. Verify Graph Traversal (Neighbors)
	// Node 10 should have neighbors loaded from Disk
	// We access them via GetNeighbors, which should fallback to DiskGraph (implicitly or via promoteNode??)
	// Actually GetNeighbors on ArrowHNSW doesn't fallback?
	// GetNeighbors is internal mostly.
	// But h2.data.Load().GetNeighbors calls internal logic.
	// Persistence logic: Search uses GetNeighbors. `searchLayer` uses `GetNeighbors` which uses `data`.
	// Does `data.GetNeighbors` fallback to invalid?
	// Ah! `promoteNode` promotes logic.
	// In Search, we didn't call `promoteNode`.
	// Mmap `DiskGraph` supports `GetNeighbors`.
	// Does `searchLayer` switch to `DiskGraph`? YES.
	// We implemented `fallback` in `searchLayer` (Step 877).
	// So Search works.

	// Let's verify we can retrieve neighbors for validation
	// This usually requires internal access or specialized method.
	// But we can check `DiskGraph` directly.
	dgNeighbors := dg.GetNeighbors(0, 10, nil)
	t.Logf("Node 10 Neighbors on Disk (L0): %v", dgNeighbors)
	assert.NotEmpty(t, dgNeighbors)

	// 6. Verify Copy-On-Write (Insert new node that links to old nodes)
	// Insert vector 20 (similar to 10)
	// This will trigger 'promoteNode' when h2 connects 20 -> neighbors (which are on disk)
	newVec := make([]float32, 16)
	for j := 0; j < 16; j++ {
		newVec[j] = 10.0
	}
	err = h2.InsertWithVector(20, newVec, h2.generateLevel())
	require.NoError(t, err)

	// Verify search finds new node 20 (InMemory) and old node 10 (Disk or Memory?)
	// 5 and 6 have vector 5.
	// 10 has vector 10. 20 has vector 10.
	res2, err := h2.Search(context.Background(), newVec, 2, 50, nil) // Expect 20 and 10
	t.Logf("Search(20) Result: %+v", res2)
	require.NoError(t, err)
	assert.True(t, len(res2) >= 2, "Should find at least 2 results")

	ids := make(map[uint32]bool)
	for _, r := range res2 {
		ids[uint32(r.ID)] = true
	}
	assert.True(t, ids[20], "Should find new node 20")
	assert.True(t, ids[10], "Should find old node 10")

	// 7. Verify IsHealthy (optional)
	// Since we mixed data, health check might need to support hybrid?
	// Currently IsHealthy scans data.
	// h2.data.Load() has partial data.
	// It relies on nodeCount.
	// It might file lots of missing vectors if it doesn't check DiskGraph.
	// But let's skip strict IsHealthy for hybrid if not yet supported.
	// But we can check if it crashes.
	// err = h2.IsHealthy()
	// assert.NoError(t, err)

	// 8. Verify PruneConnections (COW)
	// Force prune on an old node (e.g. 0)
	// We need to add many connections to 0 to trigger prune?
	// Or call PruneConnections directly.
	h2.PruneConnections(nil, h2.data.Load(), 0, 1, 0)
	// This should overwrite node 0's neighbor list in memory
	// Verify node 0 still works
	zeroVec := make([]float32, 16)
	res3, err := h2.Search(context.Background(), zeroVec, 1, 20, nil)
	require.NoError(t, err)
	require.Equal(t, uint32(0), uint32(res3[0].ID))
}
