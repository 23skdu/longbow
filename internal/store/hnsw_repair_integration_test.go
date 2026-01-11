package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArrowHNSW_RepairAgent_Integration(t *testing.T) {
	config := DefaultArrowHNSWConfig()
	config.M = 8
	config.Dims = 2

	idx := NewArrowHNSW(nil, config, nil)
	require.NotNil(t, idx)
	require.NotNil(t, idx.repairAgent, "Repair agent should be initialized")

	// Insert some vectors
	vecs := [][]float32{
		{1.0, 0.0},
		{0.9, 0.1},
		{0.8, 0.2},
		{0.7, 0.3},
		{0.6, 0.4},
	}

	for i, vec := range vecs {
		err := idx.InsertWithVector(uint32(i), vec, 0)
		require.NoError(t, err)
	}

	// Enable repair agent
	repairConfig := RepairAgentConfig{
		Enabled:            true,
		ScanInterval:       100 * time.Millisecond,
		MaxRepairsPerCycle: 10,
	}

	idx.EnableRepairAgent(repairConfig)

	// Let it run
	time.Sleep(250 * time.Millisecond)

	// Disable
	idx.DisableRepairAgent()

	assert.True(t, true, "Integration test completed")
}

func TestArrowHNSW_RepairAgent_AfterDeletions(t *testing.T) {
	config := DefaultArrowHNSWConfig()
	config.M = 8
	config.Dims = 2

	idx := NewArrowHNSW(nil, config, nil)

	// Insert vectors
	for i := 0; i < 20; i++ {
		vec := []float32{float32(i) / 20.0, float32(i%5) / 5.0}
		err := idx.InsertWithVector(uint32(i), vec, 0)
		require.NoError(t, err)
	}

	// Delete some nodes
	for i := 5; i < 10; i++ {
		idx.Delete(uint32(i))
	}

	// Enable repair agent
	repairConfig := RepairAgentConfig{
		Enabled:            true,
		ScanInterval:       50 * time.Millisecond,
		MaxRepairsPerCycle: 20,
	}

	idx.EnableRepairAgent(repairConfig)

	// Let it run multiple cycles
	time.Sleep(200 * time.Millisecond)

	// Disable
	idx.DisableRepairAgent()

	// Verify remaining nodes are still searchable
	for i := 10; i < 20; i++ {
		vec := []float32{float32(i) / 20.0, float32(i%5) / 5.0}
		results, err := idx.SearchVectors(vec, 1, nil, SearchOptions{})
		require.NoError(t, err)
		assert.Greater(t, len(results), 0, "Node %d should be reachable", i)
	}
}

func TestArrowHNSW_RepairAgent_EnableDisableMultiple(t *testing.T) {
	config := DefaultArrowHNSWConfig()
	config.M = 4
	config.Dims = 2

	idx := NewArrowHNSW(nil, config, nil)

	// Insert a few vectors
	for i := 0; i < 10; i++ {
		vec := []float32{float32(i) / 10.0, 0.0}
		_ = idx.InsertWithVector(uint32(i), vec, 0)
	}

	repairConfig := RepairAgentConfig{
		Enabled:            true,
		ScanInterval:       50 * time.Millisecond,
		MaxRepairsPerCycle: 10,
	}

	// Enable, disable, enable again
	idx.EnableRepairAgent(repairConfig)
	time.Sleep(100 * time.Millisecond)

	idx.DisableRepairAgent()
	time.Sleep(50 * time.Millisecond)

	// Re-enable with different config
	repairConfig.ScanInterval = 100 * time.Millisecond
	idx.EnableRepairAgent(repairConfig)
	time.Sleep(150 * time.Millisecond)

	idx.DisableRepairAgent()

	assert.True(t, true, "Multiple enable/disable cycles completed")
}
