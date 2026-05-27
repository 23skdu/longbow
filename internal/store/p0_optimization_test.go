package store

import (
	"context"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/logging"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestP0_GPUEnabledDefault verifies that GPUEnabled defaults to false.
func TestP0_GPUEnabledDefault(t *testing.T) {
	logger, _ := logging.NewLogger(logging.Config{Format: "text", Level: "warn"})
	mem := NewPooledAllocator()
	vs := NewVectorStore(mem, logger, 1024*1024, 1024*1024, 0)
	defer vs.Close()

	assert.False(t, vs.gpuEnabled, "GPU acceleration must be disabled by default in the store")
}

// TestP0_DefaultPrecisionPromotion verifies that unspecified vector types default to TurboQuant8.
func TestP0_DefaultPrecisionPromotion(t *testing.T) {
	logger, _ := logging.NewLogger(logging.Config{Format: "text", Level: "warn"})
	mem := NewPooledAllocator()
	vs := NewVectorStore(mem, logger, 100*1024*1024, 10*1024*1024, 0)
	defer vs.Close()

	// Initialize indexing workers so we don't hang
	vs.StartIndexingWorkers(2)
	vs.StartIngestionWorkers(2)

	// Create test vectors
	vectors := [][]float32{make([]float32, 128)}
	for i := 0; i < 128; i++ {
		vectors[0][i] = float32(i)
	}

	// Create a record batch without schema metadata (unspecified type) using standard helper
	rec := MakeBatchTestRecord(mem, 128, vectors)
	defer rec.Release()

	// Ingest into dataset
	err := vs.StoreRecordBatch(context.Background(), "test_unspecified_default", rec)
	require.NoError(t, err)

	ds, exists := vs.getDataset("test_unspecified_default")
	require.True(t, exists)

	// Wait for indexing to complete so the lazy index gets initialized
	ds.WaitForIndexing()

	// PreferredVectorType should have been promoted to VectorTypeTQ (TurboQuant)
	assert.Equal(t, types.VectorTypeTQ, ds.PreferredVectorType, "Unspecified precision must promote to TurboQuant")

	// Underlying index dataType must be VectorTypeTQ
	idx := ds.GetVectorIndex()
	require.NotNil(t, idx)
	
	var datatype types.VectorDataType
	if hnsw, ok := idx.(*ArrowHNSW); ok {
		datatype = hnsw.GetConfig().DataType
	} else if asi, ok := idx.(*AutoShardingIndex); ok {
		asi.mu.RLock()
		if hnsw, ok := asi.current.(*ArrowHNSW); ok {
			datatype = hnsw.GetConfig().DataType
		}
		asi.mu.RUnlock()
	}
	assert.Equal(t, types.VectorTypeTQ, datatype, "Inferred index type must be TurboQuant")
}

// TestP0_ExplicitFloat32Preserved verifies that explicit float32 is preserved.
func TestP0_ExplicitFloat32Preserved(t *testing.T) {
	logger, _ := logging.NewLogger(logging.Config{Format: "text", Level: "warn"})
	mem := NewPooledAllocator()
	vs := NewVectorStore(mem, logger, 100*1024*1024, 10*1024*1024, 0)
	defer vs.Close()

	vs.StartIndexingWorkers(2)
	vs.StartIngestionWorkers(2)

	// Explicitly request float32 via schema metadata
	md := arrow.NewMetadata([]string{"longbow.vector_type"}, []string{"float32"})
	fields := []arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32), Metadata: md},
	}
	schema := arrow.NewSchema(fields, &md)

	vectors := [][]float32{make([]float32, 128)}
	for i := 0; i < 128; i++ {
		vectors[0][i] = float32(i)
	}

	// Create test record
	rec := MakeBatchTestRecord(mem, 128, vectors)
	
	// Register the dataset using getOrCreateDataset so it's globally tracked
	ds, _ := vs.getOrCreateDataset("test_explicit_float32", func() *Dataset {
		d := NewDataset("test_explicit_float32", schema)
		d.PreferredVectorType = types.VectorTypeFloat32
		return d
	})

	err := vs.applyBatchToMemory(ds, rec, time.Now().UnixNano())
	require.NoError(t, err)

	ds.WaitForIndexing()

	// Should be float32, not promoted to TQ
	assert.Equal(t, types.VectorTypeFloat32, ds.PreferredVectorType, "Explicit float32 must be preserved")
}

// TestP0_AutoQuantization verifies auto-quantization triggers under >70% memory pressure.
func TestP0_AutoQuantization(t *testing.T) {
	logger, _ := logging.NewLogger(logging.Config{Format: "text", Level: "warn"})
	mem := NewPooledAllocator()
	
	// Set MaxMemory limit (e.g. 10000 bytes)
	vs := NewVectorStore(mem, logger, 10000, 100, 0)
	defer vs.Close()

	vs.StartIndexingWorkers(2)
	vs.StartIngestionWorkers(2)

	// Force currentMemory to exceed 70% pressure (e.g. 7500 bytes)
	vs.currentMemory.Store(7500)

	vectors := [][]float32{make([]float32, 128)}
	for i := 0; i < 128; i++ {
		vectors[0][i] = float32(i)
	}

	rec := MakeBatchTestRecord(mem, 128, vectors)
	defer rec.Release()

	// Disable reject writes under memory pressure just for this test so we can proceed
	vs.memoryConfig.RejectWrites = false

	md := arrow.NewMetadata([]string{"longbow.vector_type"}, []string{"float32"})
	fields := []arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
	}
	schema := arrow.NewSchema(fields, &md)

	// Register using getOrCreateDataset so it is tracked
	ds, _ := vs.getOrCreateDataset("test_auto_quant", func() *Dataset {
		return NewDataset("test_auto_quant", schema)
	})

	err := vs.applyBatchToMemory(ds, rec, time.Now().UnixNano())
	require.NoError(t, err)

	ds.WaitForIndexing()

	// PreferredVectorType should have been dynamically promoted to VectorTypeTQ
	assert.Equal(t, types.VectorTypeTQ, ds.PreferredVectorType, "Dataset must dynamically promote to TurboQuant8 under high memory pressure")
}

// TestP0_Eviction_And_FallbackSearch verifies that eviction, cache-miss transparent restore, and exact float32 search fallback work.
func TestP0_Eviction_And_FallbackSearch(t *testing.T) {
	logger, _ := logging.NewLogger(logging.Config{Format: "text", Level: "warn"})
	mem := NewPooledAllocator()
	vs := NewVectorStore(mem, logger, 100*1024*1024, 10*1024*1024, 0)
	defer vs.Close()

	vs.StartIndexingWorkers(2)
	vs.StartIngestionWorkers(2)

	md := arrow.NewMetadata([]string{"longbow.vector_type"}, []string{"float32"})
	fields := []arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32), Metadata: md},
	}
	schema := arrow.NewSchema(fields, &md)

	ds, _ := vs.getOrCreateDataset("test_eviction_fallback", func() *Dataset {
		return NewDataset("test_eviction_fallback", schema)
	})

	// Insert vectors sequentially until we have at least one layer >= 1
	var lastVector []float32
	batchSize := 20
	totalCount := 0

	for {
		var vectors [][]float32
		for k := 0; k < batchSize; k++ {
			vec := make([]float32, 128)
			for i := 0; i < 128; i++ {
				vec[i] = float32(totalCount + k + i)
			}
			vectors = append(vectors, vec)
		}
		lastVector = vectors[0]
		totalCount += batchSize

		rec := MakeBatchTestRecord(mem, 128, vectors)
		err := vs.applyBatchToMemory(ds, rec, time.Now().UnixNano())
		require.NoError(t, err)
		rec.Release()

		ds.WaitForIndexing()

		// Retrieve underlying ArrowHNSW index
		idx := ds.GetVectorIndex()
		var hnsw *ArrowHNSW
		if h, ok := idx.(*ArrowHNSW); ok {
			hnsw = h
		} else if asi, ok := idx.(*AutoShardingIndex); ok {
			asi.mu.RLock()
			if h, ok := asi.current.(*ArrowHNSW); ok {
				hnsw = h
			}
			asi.mu.RUnlock()
		}

		if hnsw != nil && hnsw.GetMaxLevel() >= 1 {
			break
		}

		if totalCount > 200 {
			t.Fatal("Failed to reach MaxLevel >= 1 after inserting 200 vectors")
		}
	}

	// Retrieve underlying ArrowHNSW index again
	idx := ds.GetVectorIndex()
	var hnsw *ArrowHNSW
	if h, ok := idx.(*ArrowHNSW); ok {
		hnsw = h
	} else if asi, ok := idx.(*AutoShardingIndex); ok {
		asi.mu.RLock()
		if h, ok := asi.current.(*ArrowHNSW); ok {
			hnsw = h
		}
		asi.mu.RUnlock()
	}
	require.NotNil(t, hnsw)

	gd := hnsw.GetData()
	require.NotNil(t, gd)

	// Force HNSW search to fall back to standard gd.Neighbors by clearing PackedNeighbors
	gd.PackedNeighbors = nil

	t.Logf("DEBUG: gd.Neighbors length: %d, MaxLevel: %d", len(gd.Neighbors), hnsw.GetMaxLevel())
	for l := 0; l < len(gd.Neighbors); l++ {
		t.Logf("DEBUG: Layer %d neighbors chunk count: %d", l, len(gd.Neighbors[l]))
		if len(gd.Neighbors[l]) > 0 {
			t.Logf("DEBUG: Layer %d chunk 0 offset: %d", l, gd.Neighbors[l][0])
		}
	}

	// Verify we can register with eviction manager
	require.NotNil(t, vs.evictionManager)
	vs.evictionManager.Register(gd)

	// Trigger manual eviction of layers >= 1
	vs.evictionManager.ForceEvictAll()

	t.Logf("DEBUG: After ForceEvictAll, Layer 1 chunk 0 offset: %d", gd.Neighbors[1][0])

	// Verify layer 1 is marked as evicted (offset is 0)
	assert.Equal(t, uint64(0), gd.Neighbors[1][0], "Layer 1 must be evicted to 0 offset")

	// Query with Exact Float32 Option to verify fallback search and transparent restore
	queryOptions := types.SearchOptions{
		VectorType: types.VectorTypeFloat32,
	}

	// Perturb the query vector slightly to force HNSW layer traversal rather than immediate hit
	perturbedQuery := make([]float32, 128)
	for i := 0; i < 128; i++ {
		perturbedQuery[i] = lastVector[i] + 0.1
	}

	res, err := idx.SearchVectors(context.Background(), perturbedQuery, 1, nil, queryOptions)
	require.NoError(t, err)
	assert.NotEmpty(t, res)

	t.Logf("DEBUG: After search, Layer 1 chunk 0 offset: %d", gd.Neighbors[1][0])

	// Verify transparent restore has successfully re-populated neighbors offset
	assert.NotEqual(t, uint64(0), gd.Neighbors[1][0], "Evicted layer must be transparently restored upon cache miss")
}
