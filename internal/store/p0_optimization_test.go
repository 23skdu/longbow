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
