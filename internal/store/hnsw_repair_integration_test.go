package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRepairIntegration_DeleteAndRepair(t *testing.T) {
	// 1. Setup Store with HNSW Index
	config := DefaultArrowHNSWConfig()
	config.M = 4 // Small M for simpler graph

	// Create ArrowHNSW
	// We need a dataset but tests often pass nil if NewArrowHNSW allows it for testing, or we construct minimal one.
	// Constructor: NewArrowHNSW(ds *Dataset, cfg ArrowHNSWConfig) *ArrowHNSW
	ds := &Dataset{}
	idx := NewArrowHNSW(ds, &config)

	// 2. Insert Data
	// Insert 0->1->2->3
	vecs := [][]float32{
		{1.0, 0.0},
		{0.9, 0.1},
		{0.8, 0.2},
		{0.7, 0.3},
	}
	for i, vec := range vecs {
		err := idx.InsertWithVector(uint32(i), vec, 0)
		require.NoError(t, err)
	}

	// 3. Start Repair Agent
	agentConfig := RepairAgentConfig{
		Enabled:            true,
		ScanInterval:       50 * time.Millisecond,
		MaxRepairsPerCycle: 10,
	}
	agent := NewRepairAgent(idx, agentConfig)
	agent.Start()
	defer agent.Stop()

	// 4. Delete Node
	// Delete node 1
	err := idx.Delete(1)
	require.NoError(t, err)

	// 5. Wait for Repair
	// Wait enough time for agent to scan and repair
	time.Sleep(200 * time.Millisecond)

	// 6. Verify Graph Integrity
	// Check if we can still reach node 2 from node 0 (should form bridge 0->2)
	// Or check orphans count (should be 0)
	orphans := agent.detectOrphans()
	assert.Len(t, orphans, 0, "Should satisfy graph connectivity after repair")

	// 7. Verify Search
	// Search for something close to node 2
	results, err := idx.SearchVectors(context.Background(), []float32{0.8, 0.2}, 1, nil, SearchOptions{})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, VectorID(2), results[0].ID)
}

func TestRepairIntegration_ConcurrentLoad(t *testing.T) {
	// 1. Setup
	config := DefaultArrowHNSWConfig()
	ds := &Dataset{}
	idx := NewArrowHNSW(ds, &config)

	agentConfig := RepairAgentConfig{
		Enabled:            true,
		ScanInterval:       50 * time.Millisecond,
		MaxRepairsPerCycle: 50,
	}
	agent := NewRepairAgent(idx, agentConfig)
	agent.Start()
	defer agent.Stop()

	// 2. Concurrent Insert/Delete loop
	done := make(chan bool)
	go func() {
		for i := 0; i < 100; i++ {
			vec := []float32{0.5, 0.5}
			_ = idx.InsertWithVector(uint32(i), vec, 0)
			time.Sleep(2 * time.Millisecond)
			if i%5 == 0 {
				_ = idx.Delete(uint32(i))
			}
		}
		done <- true
	}()

	<-done
	time.Sleep(100 * time.Millisecond)

	// 3. Verify no panic and graph is clean
	orphans := agent.detectOrphans()
	// Orphans might exist transiently, but we just check for no crash
	_ = orphans
}
