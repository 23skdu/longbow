package store

import (
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"
)

func TestHighDimGrowth(t *testing.T) {
	// Test High-Dimensional Vector Growth (3072 dims)
	// This exercises the optimized Clone / copy() logic and memory stability.

	dims := 3072
	numVecs := 500 // Enough to trigger multiple grows if initial cap is small
	initialCap := 100

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	// Create random vectors
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	vectors := make([][]float32, numVecs)
	for i := 0; i < numVecs; i++ {
		vec := make([]float32, dims)
		for j := 0; j < dims; j++ {
			vec[j] = rng.Float32()
		}
		vectors[i] = vec
	}

	ds := &Dataset{
		Name:    "growth_test",
		Records: nil, // We'll add incrementally via AddByRecord or just manual setup not needed if we use internal Insert logic exposed via simple harness?
		// Actually HNSW needs records to extract vectors if we use AddBatch.
		// Or we can use lower level Insert if we handle storage.
		// NewArrowHNSW requires dataset to infer schema?
		// We passed manual config.
		Schema: schema,
	}

	config := DefaultArrowHNSWConfig()
	config.InitialCapacity = initialCap
	config.M = 16
	config.EfConstruction = 100
	config.Dims = dims
	config.DataType = VectorTypeFloat32

	idx := NewArrowHNSW(ds, config, NewChunkedLocationStore())
	defer idx.Close()

	// Initial State
	require.Equal(t, 0, idx.Len())

	// Insert vectors individually to force incremental growth
	// We need to bypass AddByLocation/extract overhead and use InsertWithVector if possible?
	// But InsertWithVector is internal/unexported?
	// Wait, InsertWithVector is exported in arrow_hnsw_insert.go?
	// Let's check. Yes, func (h *ArrowHNSW) InsertWithVector(id uint32, vec any, level int) error

	start := time.Now()
	for i := 0; i < numVecs; i++ {
		// Mock ID
		id := uint32(i)

		// We must ensure the vector is in "storage" if we were using zero-copy,
		// but InsertWithVector takes the vector directly.
		// However, it expects 'h.getVectorAny' to work for distance calcs if pass 2??
		// No, InsertWithVector passes the vector to searchLayerForInsert.
		// BUT subsequent inserts search against the graph.
		// The graph needs to be able to retrieve vectors for existing nodes.
		// Existing nodes are in 'h.data'.
		// InsertWithVector writes to 'h.data' (via `data.SetVectorFromFloat32` inside `InsertWithVector`?).
		// Let's verify InsertWithVector logic.
		// ... "InsertWithVector calls h.ensureChunk ... data.SetVectorFromFloat32".
		// So it persists to memory graph.

		// Level generation (random)
		level := int(rng.Intn(10))
		// Actually use graph's generator or fixed 0 for simplicity?
		// Better use decent levels distribution.

		err := idx.InsertWithVector(id, vectors[i], level)
		require.NoError(t, err, "Insert failed at index %d", i)
	}
	duration := time.Since(start)
	t.Logf("Inserted %d high-dim vectors in %v", numVecs, duration)

	// Verify Data Integrity
	// Search for specific vectors and ensure they are found as top result (distance 0)
	for i := 0; i < numVecs; i += 50 { // Sample check
		query := vectors[i]
		res, err := idx.Search(query, 1, 100, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(res), 1)
		require.Equal(t, uint32(i), uint32(res[0].ID), "Should find self at rank 0")
		require.InDelta(t, 0.0, res[0].Score, 1e-4, "Distance should be ~0")
	}

	// Verify Memory Stats optional (just log them)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	t.Logf("HeapAlloc: %d MB", m.HeapAlloc/1024/1024)
}
