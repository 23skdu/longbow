package store

import (
	"testing"
)

// FuzzRepairAgent_GraphSize fuzzes the graph size to ensure the agent
// handles graphs of various sizes without panicking
func FuzzRepairAgent_GraphSize(f *testing.F) {
	// Seed corpus
	f.Add(0)    // Empty
	f.Add(1)    // Single node
	f.Add(10)   // Small
	f.Add(100)  // Medium
	f.Add(1000) // Large

	f.Fuzz(func(t *testing.T, nodeCount int) {
		if nodeCount < 0 || nodeCount > 10000 {
			t.Skip("Invalid node count")
		}

		config := RepairAgentConfig{
			Enabled:            true,
			MaxRepairsPerCycle: 10,
		}

		hnswConfig := DefaultArrowHNSWConfig()
		hnswConfig.M = 8
		idx := NewArrowHNSW(nil, hnswConfig, nil)

		// Insert nodes
		for i := 0; i < nodeCount; i++ {
			vec := []float32{float32(i) / float32(nodeCount+1), 0.0}
			_ = idx.InsertWithVector(uint32(i), vec, 0)
		}

		agent := NewRepairAgent(idx, config)

		// Should not panic
		_ = agent.detectOrphans()
		_ = agent.runRepairCycle()
	})
}

// FuzzRepairAgent_DeletionPattern fuzzes deletion patterns
func FuzzRepairAgent_DeletionPattern(f *testing.F) {
	// Seed corpus
	f.Add(0, 10)  // Delete none
	f.Add(5, 10)  // Delete half
	f.Add(9, 10)  // Delete most
	f.Add(10, 10) // Delete all

	f.Fuzz(func(t *testing.T, deleteCount, totalCount int) {
		if deleteCount < 0 || totalCount < 0 || totalCount > 1000 {
			t.Skip("Invalid parameters")
		}
		if deleteCount > totalCount {
			deleteCount = totalCount
		}

		config := RepairAgentConfig{
			Enabled:            true,
			MaxRepairsPerCycle: 20,
		}

		hnswConfig := DefaultArrowHNSWConfig()
		hnswConfig.M = 8
		idx := NewArrowHNSW(nil, hnswConfig, nil)

		// Insert nodes
		for i := 0; i < totalCount; i++ {
			vec := []float32{float32(i) / float32(totalCount+1), 0.0}
			_ = idx.InsertWithVector(uint32(i), vec, 0)
		}

		// Delete nodes
		for i := 0; i < deleteCount; i++ {
			_ = idx.Delete(uint32(i))
		}

		agent := NewRepairAgent(idx, config)

		// Should not panic
		_ = agent.detectOrphans()
		_ = agent.runRepairCycle()
	})
}

// FuzzRepairAgent_MaxRepairs fuzzes the MaxRepairsPerCycle parameter
func FuzzRepairAgent_MaxRepairs(f *testing.F) {
	// Seed corpus
	f.Add(0)
	f.Add(1)
	f.Add(10)
	f.Add(100)
	f.Add(-1) // Invalid

	f.Fuzz(func(t *testing.T, maxRepairs int) {
		config := RepairAgentConfig{
			Enabled:            true,
			MaxRepairsPerCycle: maxRepairs,
		}

		hnswConfig := DefaultArrowHNSWConfig()
		idx := NewArrowHNSW(nil, hnswConfig, nil)

		// Insert some nodes
		for i := 0; i < 50; i++ {
			vec := []float32{float32(i) / 50.0, 0.0}
			_ = idx.InsertWithVector(uint32(i), vec, 0)
		}

		// Delete some
		for i := 0; i < 10; i++ {
			_ = idx.Delete(uint32(i))
		}

		agent := NewRepairAgent(idx, config)

		// Should not panic regardless of maxRepairs value
		repaired := agent.runRepairCycle()

		// If maxRepairs is valid and positive, repaired should not exceed it
		if maxRepairs > 0 {
			if repaired > maxRepairs {
				t.Errorf("Repaired %d exceeds max %d", repaired, maxRepairs)
			}
		}
	})
}

// FuzzRepairAgent_MParameter fuzzes the HNSW M parameter
func FuzzRepairAgent_MParameter(f *testing.F) {
	// Seed corpus
	f.Add(2)
	f.Add(8)
	f.Add(16)
	f.Add(32)
	f.Add(64)

	f.Fuzz(func(t *testing.T, m int) {
		if m < 2 || m > 128 {
			t.Skip("Invalid M parameter")
		}

		config := RepairAgentConfig{
			Enabled:            true,
			MaxRepairsPerCycle: 10,
		}

		hnswConfig := DefaultArrowHNSWConfig()
		hnswConfig.M = m
		idx := NewArrowHNSW(nil, hnswConfig, nil)

		// Insert nodes
		for i := 0; i < 100; i++ {
			vec := []float32{float32(i) / 100.0, float32(i%10) / 10.0}
			_ = idx.InsertWithVector(uint32(i), vec, 0)
		}

		// Delete some
		for i := 10; i < 20; i++ {
			_ = idx.Delete(uint32(i))
		}

		agent := NewRepairAgent(idx, config)

		// Should not panic
		_ = agent.detectOrphans()
		_ = agent.runRepairCycle()
	})
}

// FuzzRepairAgent_Combined fuzzes multiple parameters together
func FuzzRepairAgent_Combined(f *testing.F) {
	// Seed corpus
	f.Add(10, 5, 8, 5)
	f.Add(100, 20, 16, 10)
	f.Add(50, 0, 8, 10)
	f.Add(0, 0, 8, 10)

	f.Fuzz(func(t *testing.T, nodeCount, deleteCount, m, maxRepairs int) {
		if nodeCount < 0 || nodeCount > 500 {
			t.Skip("Invalid node count")
		}
		if deleteCount < 0 || deleteCount > nodeCount {
			deleteCount = nodeCount
		}
		if m < 2 || m > 64 {
			t.Skip("Invalid M")
		}
		if maxRepairs < 0 {
			maxRepairs = 0
		}
		if maxRepairs > 100 {
			maxRepairs = 100
		}

		config := RepairAgentConfig{
			Enabled:            true,
			MaxRepairsPerCycle: maxRepairs,
		}

		hnswConfig := DefaultArrowHNSWConfig()
		hnswConfig.M = m
		idx := NewArrowHNSW(nil, hnswConfig, nil)

		// Insert nodes
		for i := 0; i < nodeCount; i++ {
			vec := []float32{float32(i) / float32(nodeCount+1), 0.0}
			_ = idx.InsertWithVector(uint32(i), vec, 0)
		}

		// Delete nodes
		for i := 0; i < deleteCount; i++ {
			_ = idx.Delete(uint32(i))
		}

		agent := NewRepairAgent(idx, config)

		// Should not panic
		orphans := agent.detectOrphans()
		repaired := agent.runRepairCycle()

		// Validate constraints
		if maxRepairs > 0 && repaired > maxRepairs {
			t.Errorf("Repaired %d exceeds max %d", repaired, maxRepairs)
		}

		// Orphans should be non-negative
		// len() always returns non-negative, so just check it exists
		_ = orphans
	})
}
