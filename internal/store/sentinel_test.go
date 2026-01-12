package store

import (
	"testing"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestSentinelVector_Fallback(t *testing.T) {
	// 1. Setup GraphData with no data
	// func NewGraphData(capacity, dims int, sq8Enabled, pqEnabled bool, pqDims int, bqEnabled, float16Enabled, packedAdjacencyEnabled bool)
	gd := NewGraphData(100, 384, false, false, 0, false, false, false, VectorTypeFloat32)

	// 2. Setup ArrowHNSW
	// We need a dummy HNSW structure. We can't easily create a full one without data,
	// but mustGetVectorFromData is a method on ArrowHNSW.
	// We'll create a minimal struct.
	hnsw := &ArrowHNSW{
		// data: atomic pointer usually
	}
	// We don't strictly need h.data to be set if we pass 'gd' directly to the method,
	// but the method definition is: func (h *ArrowHNSW) mustGetVectorFromData(data *GraphData, id uint32) []float32

	// 3. Call mustGetVectorFromData for an ID that doesn't exist in chunks
	missingID := uint32(9999)

	// Reset metric before test (though it's global, dealing with concurrent tests might be tricky,
	// but usually unit tests run sequentially or we check delta).
	initialCount := testutil.ToFloat64(metrics.VectorSentinelHitTotal)

	vec := hnsw.mustGetVectorFromData(gd, missingID)

	// 4. Assertions
	assert.NotNil(t, vec, "Sentinel vector should not be nil")
	assert.Equal(t, 384, len(vec), "Sentinel vector should have correct dimensions")

	// Check content matches zeros
	isZero := true
	for _, v := range vec {
		if v != 0 {
			isZero = false
			break
		}
	}
	assert.True(t, isZero, "Sentinel vector should be all zeros")

	// Check metric increment via testutil or simple check if supported
	// Since we can't easily reset globals in concurrent tests without race, we check delta.
	finalCount := testutil.ToFloat64(metrics.VectorSentinelHitTotal)
	assert.Equal(t, initialCount+1, finalCount, "Sentinel hit metric should increment")
}
