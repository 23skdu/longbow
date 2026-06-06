package index

import (
	"testing"

	types "github.com/23skdu/longbow/internal/store/types"

	"github.com/stretchr/testify/assert"
)

func TestSentinelVector_Fallback(t *testing.T) {
	// 1. Setup GraphData with no data
	// func NewGraphData(capacity, dims int, sq8Enabled, pqEnabled bool, pqDims int, bqEnabled, float16Enabled, packedAdjacencyEnabled bool, "test")
	gd := types.NewGraphData(100, 384, false, false, 0, false, false, false, types.VectorTypeFloat32, false, false, false, 8, "test", nil, false)

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

	vecAny := hnsw.mustGetVectorFromData(gd, missingID)
	vec, ok := vecAny.([]float32)
	assert.False(t, ok, "Sentinel should return nil, not []float32")
	assert.Nil(t, vec, "Sentinel vector should be nil")
}
