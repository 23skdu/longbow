package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestPQPersistence(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "pq_persistence_test")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := zerolog.New(os.Stdout)
	mem := memory.NewGoAllocator()
	store := NewVectorStore(mem, logger, 1024*1024*1024, 1024*1024*1024, 0)

	// Use InitPersistence
	err = store.InitPersistence(storage.StorageConfig{DataPath: tmpDir})
	require.NoError(t, err)
	store.StartIndexingWorkers(1)

	_ = os.Setenv("LONGBOW_USE_HNSW2", "true")
	defer func() { _ = os.Unsetenv("LONGBOW_USE_HNSW2") }()

	store.datasetInitHook = func(ds *Dataset) {
		cfg := DefaultArrowHNSWConfig()
		idx := NewArrowHNSW(ds, &cfg)
		ds.Index = idx
	}

	defer func() { _ = store.Close() }()

	datasetName := "pq_test"
	dims := 128

	// 1. Create dataset and vectors
	vectors := make([][]float32, 1000)
	for i := range vectors {
		vectors[i] = make([]float32, dims)
		for j := range vectors[i] {
			vectors[i][j] = float32(i + j) // Predictable pattern
		}
	}

	// Create Arrow Record
	rec := createFloat32Record(mem, vectors, dims)
	defer rec.Release()

	err = store.ApplyDelta(datasetName, rec, 1, time.Now().UnixNano())
	require.NoError(t, err)

	// 2. Train PQ
	store.WaitForIndexing(datasetName)

	ds, ok := store.getDataset(datasetName)
	require.True(t, ok)
	require.NotNil(t, ds)
	fmt.Printf("DEBUG: Found dataset %s, index type: %T\n", datasetName, ds.Index)

	trained := false

	// Manually inject PQ Config and Train

	// Check if we can use TrainPQ on the index
	// For ArrowHNSW we can cast it.
	// ds.Index might be ShardedHNSW or ArrowHNSW.
	switch idx := ds.Index.(type) {
	case *ArrowHNSW:
		err = idx.TrainPQ(vectors)
		require.NoError(t, err)
		trained = true
		ds.PQEncoder = idx.pqEncoder
	case *ShardedHNSW:
		fmt.Printf("DEBUG: Index is ShardedHNSW, not yet supported for TrainPQ in test\n")
	case *AutoShardingIndex:
		fmt.Printf("DEBUG: Index is AutoShardingIndex, current: %T\n", idx.current)
		if arrowIdx, ok := idx.current.(*ArrowHNSW); ok {
			err = arrowIdx.TrainPQ(vectors)
			require.NoError(t, err)
			trained = true
			ds.PQEncoder = arrowIdx.pqEncoder
		}
	default:
		fmt.Printf("DEBUG: Index type not supported for TrainPQ: %T\n", ds.Index)
	}

	require.True(t, trained, "Should have found and trained index")

	// 3. Snapshot
	err = store.Snapshot(context.Background())
	require.NoError(t, err)

	// Verify .pq file exists
	pqPath := tmpDir + "/snapshots/" + datasetName + ".pq"
	_, err = os.Stat(pqPath)
	require.NoError(t, err, "PQ snapshot file should exist")

	// 4. Close and Reopen
	_ = store.Close()

	store2 := NewVectorStore(mem, logger, 1024*1024*1024, 1024*1024*1024, 0)
	defer func() { _ = store2.Close() }()

	// Force load snapshots via InitPersistence
	err = store2.InitPersistence(storage.StorageConfig{DataPath: tmpDir})
	require.NoError(t, err)

	ds2, ok := store2.getDataset(datasetName)
	require.True(t, ok)
	require.NotNil(t, ds2.PQEncoder, "PQEncoder should be restored in Dataset")

	// 5. Verify Encoder Data
	require.Equal(t, 16, ds2.PQEncoder.M)
	require.Equal(t, 256, ds2.PQEncoder.K)

	// 6. Verify Index Picks it up
	cfg2 := DefaultArrowHNSWConfig()
	idx2 := NewArrowHNSW(ds2, &cfg2)
	idx2.m = 16
	idx2.dims.Store(128)

	// Trigger "Train" ensuring it picks up
	err = idx2.TrainPQ(vectors)
	require.NoError(t, err)

	require.NotNil(t, idx2.GetPQEncoder())
}

func createFloat32Record(mem memory.Allocator, vectors [][]float32, dims int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
		{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	listB := b.Field(0).(*array.FixedSizeListBuilder)
	floatB := listB.ValueBuilder().(*array.Float32Builder)

	idB := b.Field(1).(*array.Uint32Builder)

	for i, vec := range vectors {
		listB.Append(true)
		floatB.AppendValues(vec, nil)
		idB.Append(uint32(i))
	}

	return b.NewRecordBatch()
}
