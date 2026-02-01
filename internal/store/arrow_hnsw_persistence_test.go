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
	cfg.EfSearch = 100
	cfg.EfConstruction = 100
	cfg.Dims = 16
	cfg.SQ8Enabled = true         // Persistence requires SQ8 (or PQ) in current DiskGraph V3
	cfg.SQ8TrainingThreshold = 20 // Train after all 20 vectors

	h1 := NewArrowHNSW(nil, &cfg)

	// Insert 20 vectors [i, i, ...]
	vectors := make([][]float32, 20)
	for i := 0; i < 20; i++ {
		val := float32(i)
		vectors[i] = make([]float32, 16)
		for j := 0; j < 16; j++ {
			vectors[i][j] = val
		}
	}

	// Initial batch to train explicitly
	h1.ensureTrained(20, vectors)
	require.True(t, h1.sq8Ready.Load(), "SQ8 must be ready")

	for i := 0; i < 20; i++ {
		err := h1.InsertWithVector(uint32(i), vectors[i], h1.generateLevel())
		require.NoError(t, err)
	}

	// 2. Persist to Disk
	gd := h1.data.Load()
	minV, maxV := h1.quantizer.Params()
	err := WriteDiskGraph(gd, graphPath, h1.Size(), minV, maxV, h1.entryPoint.Load(), int(h1.maxLevel.Load()))
	require.NoError(t, err)
	_ = h1.Close()

	// 3. Load from Mmap (New Instance)
	h2 := NewArrowHNSW(nil, &cfg)
	err = h2.LoadFromMmap(graphPath)
	require.NoError(t, err)

	// Verify DiskGraph is attached
	dg := h2.diskGraph.Load()
	require.NotNil(t, dg)
	assert.Equal(t, 20, dg.Size())

	// Check restored quantization params
	minV2, maxV2 := h2.quantizer.Params()
	assert.Equal(t, minV, minV2)
	assert.Equal(t, maxV, maxV2)

	// 4. Verify Search (Hybrid Read from Disk)
	q5 := make([]float32, 16)
	for j := 0; j < 16; j++ {
		q5[j] = 5.0
	}

	res, err := h2.Search(context.Background(), q5, 1, nil)
	require.NoError(t, err)
	require.NotEmpty(t, res)
	assert.Equal(t, uint32(5), uint32(res[0].ID))

	// 5. Verify Copy-On-Write (Insert new node 20)
	q10 := make([]float32, 16)
	for j := 0; j < 16; j++ {
		q10[j] = 10.0
	}
	err = h2.InsertWithVector(20, q10, h2.generateLevel())
	require.NoError(t, err)

	// Node 20 (InMemory) and Node 10 (Disk) should both be close to q10
	res2, err := h2.Search(context.Background(), q10, 5, nil)
	require.NoError(t, err)

	ids := make(map[uint32]bool)
	for _, r := range res2 {
		ids[uint32(r.ID)] = true
	}
	assert.True(t, ids[20], "New node 20 should be found")
	assert.True(t, ids[10], "Disk node 10 should be found")

	// 6. Verify PruneConnections (Force COW on node 0)
	// This triggers promotion of node 0's neighbors to memory
	h2.PruneConnections(nil, h2.data.Load(), 0, 1, 0)

	q0 := make([]float32, 16) // all zeros
	res3, err := h2.Search(context.Background(), q0, 1, nil)
	require.NoError(t, err)
	require.NotEmpty(t, res3)
	assert.Equal(t, uint32(0), uint32(res3[0].ID), "Node 0 should still be found after COW")

	_ = h2.Close()
}
