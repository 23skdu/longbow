package store

import (
	"context"
	"math/rand"
	"runtime"
	"testing"
	"time"

	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestHighDimGrowth(t *testing.T) {
	// Test High-Dimensional Vector Growth (3072 dims)
	// This exercises the optimized Clone / copy() logic and memory stability.

	dims := 3072
	numVecs := 500
	initialCap := 100

	mem := memory.NewGoAllocator()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	vectors := make([][]float32, numVecs)
	for i := 0; i < numVecs; i++ {
		vec := make([]float32, dims)
		for j := 0; j < dims; j++ {
			vec[j] = rng.Float32()
		}
		vectors[i] = vec
	}

	rec := makeBatchTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "growth_test",
		Records: []arrow.RecordBatch{rec},
	}

	config := DefaultArrowHNSWConfig()
	config.InitialCapacity = initialCap
	config.M = 16
	config.EfConstruction = 100
	config.Dims = dims
	config.DataType = lbtypes.VectorTypeFloat32

	idx := NewArrowHNSW(ds, &config)
	defer func() { _ = idx.Close() }()

	require.Equal(t, 0, idx.Len())

	start := time.Now()
	for i := 0; i < numVecs; i++ {
		_, err := idx.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err, "Insert failed at index %d", i)
	}
	duration := time.Since(start)
	t.Logf("Inserted %d high-dim vectors in %v", numVecs, duration)

	require.Equal(t, numVecs, idx.Len())

	for i := 0; i < numVecs; i += 50 {
		query := vectors[i]
		res, err := idx.Search(context.Background(), query, 1, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(res), 1)
		require.Equal(t, uint32(i), uint32(res[0].ID), "Should find self at rank 0")
		require.InDelta(t, 0.0, res[0].Dist, 1e-4, "Distance should be ~0")
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	t.Logf("HeapAlloc: %d MB", m.HeapAlloc/1024/1024)
}
