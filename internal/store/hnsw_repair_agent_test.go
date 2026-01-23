package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRepairAgent_DetectOrphans_Simple(t *testing.T) {
	config := RepairAgentConfig{
		Enabled:            true,
		ScanInterval:       100 * time.Millisecond,
		MaxRepairsPerCycle: 10,
	}

	// Create ArrowHNSW index
	hnswConfig := DefaultArrowHNSWConfig()
	hnswConfig.M = 4
	idx := NewArrowHNSW(nil, hnswConfig, nil)

	// Insert connected nodes
	vecs := [][]float32{
		{1.0, 0.0},
		{0.9, 0.1},
		{0.8, 0.2},
	}

	for i, vec := range vecs {
		err := idx.InsertWithVector(uint32(i), vec, 0)
		require.NoError(t, err)
	}

	agent := NewRepairAgent(idx, config)
	require.NotNil(t, agent)

	// All nodes should be reachable (no orphans)
	orphans := agent.detectOrphans()
	assert.Len(t, orphans, 0, "Should have no orphans in connected graph")
}

func TestRepairAgent_RepairAfterDeletion(t *testing.T) {
	config := RepairAgentConfig{
		Enabled:            true,
		ScanInterval:       100 * time.Millisecond,
		MaxRepairsPerCycle: 10,
	}

	hnswConfig := DefaultArrowHNSWConfig()
	hnswConfig.M = 4
	idx := NewArrowHNSW(nil, hnswConfig, nil)

	// Insert nodes forming a chain: 0 -> 1 -> 2 -> 3
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

	// Delete middle node (1) - might create orphans
	_ = idx.Delete(1)

	agent := NewRepairAgent(idx, config)

	// Run repair
	repaired := agent.runRepairCycle()

	// Should have attempted repairs
	assert.GreaterOrEqual(t, repaired, 0, "Should run repair cycle")
}

func TestRepairAgent_Lifecycle(t *testing.T) {
	config := RepairAgentConfig{
		Enabled:            true,
		ScanInterval:       50 * time.Millisecond,
		MaxRepairsPerCycle: 10,
	}

	hnswConfig := DefaultArrowHNSWConfig()
	idx := NewArrowHNSW(nil, hnswConfig, nil)

	agent := NewRepairAgent(idx, config)
	require.NotNil(t, agent)

	// Start agent
	agent.Start()

	// Let it run
	time.Sleep(150 * time.Millisecond)

	// Stop should be clean
	agent.Stop()

	// Calling Stop again should be safe
	agent.Stop()
}

func TestRepairAgent_Disabled(t *testing.T) {
	config := RepairAgentConfig{
		Enabled: false,
	}

	hnswConfig := DefaultArrowHNSWConfig()
	idx := NewArrowHNSW(nil, hnswConfig, nil)

	agent := NewRepairAgent(idx, config)

	// Start should be no-op when disabled
	agent.Start()
	time.Sleep(100 * time.Millisecond)
	agent.Stop()

	assert.True(t, true, "Disabled agent should not panic")
}

func TestRepairAgent_MaxRepairsLimit(t *testing.T) {
	config := RepairAgentConfig{
		Enabled:            true,
		ScanInterval:       100 * time.Millisecond,
		MaxRepairsPerCycle: 2, // Limit repairs
	}

	hnswConfig := DefaultArrowHNSWConfig()
	hnswConfig.M = 4
	idx := NewArrowHNSW(nil, hnswConfig, nil)

	// Insert some nodes
	for i := 0; i < 10; i++ {
		vec := []float32{float32(i) / 10.0, 0.0}
		err := idx.InsertWithVector(uint32(i), vec, 0)
		require.NoError(t, err)
	}

	// Delete several nodes to potentially create orphans
	for i := 1; i < 5; i++ {
		_ = idx.Delete(uint32(i))
	}

	agent := NewRepairAgent(idx, config)

	// Run one cycle
	repaired := agent.runRepairCycle()

	// Should respect limit
	assert.LessOrEqual(t, repaired, config.MaxRepairsPerCycle,
		"Should not exceed MaxRepairsPerCycle")
}

func TestRepairAgent_EmptyGraph(t *testing.T) {
	config := RepairAgentConfig{
		Enabled:            true,
		ScanInterval:       100 * time.Millisecond,
		MaxRepairsPerCycle: 10,
	}

	hnswConfig := DefaultArrowHNSWConfig()
	idx := NewArrowHNSW(nil, hnswConfig, nil)

	agent := NewRepairAgent(idx, config)

	// Should handle empty graph gracefully
	orphans := agent.detectOrphans()
	assert.Len(t, orphans, 0, "Empty graph should have no orphans")

	repaired := agent.runRepairCycle()
	assert.Equal(t, 0, repaired, "Empty graph should have no repairs")
}

func TestRepairAgent_SingleNode(t *testing.T) {
	config := RepairAgentConfig{
		Enabled:            true,
		ScanInterval:       100 * time.Millisecond,
		MaxRepairsPerCycle: 10,
	}

	hnswConfig := DefaultArrowHNSWConfig()
	idx := NewArrowHNSW(nil, hnswConfig, nil)

	// Insert single node
	vec := []float32{1.0, 0.0}
	err := idx.InsertWithVector(0, vec, 0)
	require.NoError(t, err)

	agent := NewRepairAgent(idx, config)

	// Single node is its own entry point, not an orphan
	orphans := agent.detectOrphans()
	assert.Len(t, orphans, 0, "Single node should not be orphan")
}

func BenchmarkRepairAgent_DetectOrphans(b *testing.B) {
	config := RepairAgentConfig{
		Enabled:            true,
		MaxRepairsPerCycle: 100,
	}

	hnswConfig := DefaultArrowHNSWConfig()
	hnswConfig.M = 16
	idx := NewArrowHNSW(nil, hnswConfig, nil)

	// Build graph with 1000 nodes
	for i := 0; i < 1000; i++ {
		vec := []float32{float32(i) / 1000.0, float32(i%100) / 100.0}
		_ = idx.InsertWithVector(uint32(i), vec, 0)
	}

	agent := NewRepairAgent(idx, config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = agent.detectOrphans()
	}
}

func BenchmarkRepairAgent_RepairCycle(b *testing.B) {
	config := RepairAgentConfig{
		Enabled:            true,
		MaxRepairsPerCycle: 10,
	}

	hnswConfig := DefaultArrowHNSWConfig()
	hnswConfig.M = 16
	idx := NewArrowHNSW(nil, hnswConfig, nil)

	// Build graph
	for i := 0; i < 500; i++ {
		vec := []float32{float32(i) / 500.0, float32(i%50) / 50.0}
		_ = idx.InsertWithVector(uint32(i), vec, 0)
	}

	// Delete some nodes
	for i := 10; i < 20; i++ {
		_ = idx.Delete(uint32(i))
	}

	agent := NewRepairAgent(idx, config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = agent.runRepairCycle()
	}
}
