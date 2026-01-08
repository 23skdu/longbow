package store

import (
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
	time.Sleep(500 * time.Millisecond) // Allow async indexing

	store.mu.RLock()
	ds := store.datasets[datasetName]
	store.mu.RUnlock()
	require.NotNil(t, ds)

	// Manually inject PQ Config and Train
	ds.dataMu.Lock()
	var trained bool

	// We need to access AutoShardingIndex internals but we can't easily without helpers or reflection if fields private.
	// Since we are in `package store`, if fields are unexported we can see them.
	// Assuming structure from original test.
	switch idx := ds.Index.(type) {
	case *AutoShardingIndex:
		// idx.current is private field?
		// We use NewIndex usually.
		// If we can't access, we skip training validation or rely on test structure being valid in this package.
		if hnswIdx, ok := idx.current.(*HNSWIndex); ok {
			hnswIdx.pqEnabled = true
			cfg := PQConfig{Dimensions: dims, NumSubVectors: 16, NumCentroids: 256}
			enc, _ := NewPQEncoder(cfg)
			for i := 0; i < 16; i++ {
				enc.Codebooks[i] = make([][]float32, 256)
				for j := 0; j < 256; j++ {
					enc.Codebooks[i][j] = make([]float32, dims/16)
				}
			}
			hnswIdx.pqEncoder = enc
			ds.PQEncoder = enc
			trained = true
		}
	case *HNSWIndex:
		cfg := PQConfig{Dimensions: dims, NumSubVectors: 16, NumCentroids: 256}
		enc, _ := NewPQEncoder(cfg)
		idx.pqEncoder = enc
		ds.PQEncoder = enc
		trained = true
	}
	ds.dataMu.Unlock()
	require.True(t, trained, "Should have found and trained index")

	// 3. Snapshot
	err = store.Snapshot()
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

	store2.mu.RLock()
	ds2, ok := store2.datasets[datasetName]
	store2.mu.RUnlock()
	require.True(t, ok)
	require.NotNil(t, ds2.PQEncoder, "PQEncoder should be restored in Dataset")

	// 5. Verify Encoder Data
	require.Equal(t, 16, ds2.PQEncoder.config.NumSubVectors)
	require.Equal(t, 256, ds2.PQEncoder.config.NumCentroids)

	// 6. Verify Index Picks it up
	idx2 := NewArrowHNSW(ds2, DefaultArrowHNSWConfig(), nil)
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
