package store

import (
	"testing"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getMetricValue(g prometheus.Gauge) float64 {
	var m dto.Metric
	_ = g.Write(&m)
	return m.GetGauge().GetValue()
}

func TestMemVectorStore_WithArena(t *testing.T) {
	// 1. Initial State
	initialManaged := getMetricValue(metrics.StoreVectorsManagedCount)

	// 2. Create Arena-backed store
	// Assumption: New constructor or option to enable Arena
	// We will create a new function `NewArenaMemVectorStore` for now or use options.
	// TDD: This will fail to compile first.
	ms, err := NewMemVectorStore(MemStoreOptions{
		UseArena: true,
		Dim:      4,
	})
	require.NoError(t, err)

	// 3. Insert Vectors
	// Insert 10 vectors of dim 4
	vec := []float32{1.0, 2.0, 3.0, 4.0}
	for i := 0; i < 10; i++ {
		err := ms.Set("vec"+string(rune(i)), vec)
		assert.NoError(t, err)
	}

	// 4. Verify Metric increase
	// If arena is used, managed count should increase by 10
	finalManaged := getMetricValue(metrics.StoreVectorsManagedCount)
	assert.Equal(t, float64(10), finalManaged-initialManaged, "Expected managed vector count to increase by 10")

	// 5. Verify Data Retrieval correctness
	// Ensure we can still read them back correctly via the Arena indirections
	out, exists := ms.Get("vec" + string(rune(0)))
	assert.True(t, exists)
	assert.Equal(t, vec, out)
}

// Temporary Stub types to verify compilation failure logic (or lack thereof)
// depending on how strict we want the TDD red phase to be.
// But real TDD means using the actual code.
// MemMemVectorStore might define Set/Get but we need the Config struct to exist.
