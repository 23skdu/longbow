package store

import (
	"context"
	"math/rand"
	"testing"
	"time"

	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArrowHNSW_Float16_ZeroCopy(t *testing.T) {
	// 1. Setup
	config := DefaultArrowHNSWConfig()
	config.Dims = 8
	config.M = 16
	config.EfConstruction = 100
	config.Float16Enabled = true
	config.DataType = lbtypes.VectorTypeFloat16 // Explicitly use native Float16 storage

	idx := NewArrowHNSW(nil, config)
	defer func() { _ = idx.Close() }()

	count := 100
	vecsF16 := make([][]float16.Num, count)

	// 2. Generate Data
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < count; i++ {
		vecsF16[i] = make([]float16.Num, config.Dims)
		for j := 0; j < config.Dims; j++ {
			f := rng.Float32() * 10
			vecsF16[i][j] = float16.New(f)
		}
	}

	// 3. Insert specific vectors using generic InsertWithVector
	ctx := context.Background()
	_ = ctx

	start := time.Now()
	for i := 0; i < count; i++ {
		// Use generic method which should dispatch to SetVector
		err := idx.InsertWithVector(uint32(i), vecsF16[i], idx.generateLevel())
		require.NoError(t, err)
	}
	t.Logf("Inserted %d Float16 vectors in %v", count, time.Since(start))

	// 4. Verify Content (Zero-Copy Read)
	// We should be able to read back generic type
	vAny, err := idx.getVectorAny(0)
	require.NoError(t, err)

	// Should be float16 slice
	vRead, ok := vAny.([]float16.Num)
	assert.True(t, ok, "Expected []float16.Num, got %T", vAny)
	assert.Equal(t, vecsF16[0], vRead)

	// 5. Search with Native Type
	// Search for vector 0, expect ID 0
	query := vecsF16[0]
	results, err := idx.Search(context.Background(), query, 10, nil)
	require.NoError(t, err)
	require.NotEmpty(t, results)

	assert.Equal(t, uint32(0), uint32(results[0].ID))
	// Score for 0 distance might be 0 (Euclidean)
	if config.Metric == MetricEuclidean {
		assert.InDelta(t, float32(0), results[0].Dist, 1e-4)
	}

	// 6. Test Neighbors fallback (optional/internal)
	// Just Ensure no panic
}
