package store

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestVectorStore_RCU_Integration(t *testing.T) {
	if testing.Short() {
			t.Skip("skipping test in short mode")
	}
	vs := &VectorStore{}
	m := make(map[string]*Dataset)
	vs.datasets.Store(&m)

	vs.updateDatasets(func(m map[string]*Dataset) {
		m["test"] = &Dataset{Name: "test"}
	})

	ds, ok := vs.getDataset("test")
	require.True(t, ok)
	require.Equal(t, "test", ds.Name)

	var wg sync.WaitGroup
	// Readers
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				vs.getDataset("test")
			}
		}()
	}

	// Writers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("test-%d", id)
			for j := 0; j < 100; j++ {
				vs.updateDatasets(func(m map[string]*Dataset) {
					m[key] = &Dataset{Name: key}
				})
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	wg.Wait()

	finalMap := vs.loadDatasets()
	require.GreaterOrEqual(t, len(finalMap), 11)
}

func TestVectorStore_Persistence_FullFlow(t *testing.T) {
	if testing.Short() {
			t.Skip("skipping test in short mode")
	}
	tmpDir, err := os.MkdirTemp("", "longbow_persistence_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	mem := memory.NewGoAllocator()
	logger := zerolog.New(os.Stderr)

	// 1. Initialize Store
	vs := NewVectorStore(mem, logger, 1024*1024*100, 0, 0)
	err = vs.InitPersistence(storage.StorageConfig{
		DataPath:         tmpDir,
		SnapshotInterval: 0, // Manual snapshots
	})
	require.NoError(t, err)

	// 2. Ingest Data
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "vector", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)},
	}, nil)

	vs.PrewarmDataset("test_ds", schema)
	dsIngest, ok := vs.getDataset("test_ds")
	require.True(t, ok)

	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2}, nil)
	vlb := b.Field(1).(*array.FixedSizeListBuilder)
	vb := vlb.ValueBuilder().(*array.Float32Builder)
	vlb.Append(true)
	vb.AppendValues([]float32{1.0, 2.0}, nil)
	vlb.Append(true)
	vb.AppendValues([]float32{3.0, 4.0}, nil)
	rec := b.NewRecordBatch()

	err = vs.applyBatchToMemory(dsIngest, rec, time.Now().UnixNano())
	require.NoError(t, err)
	rec.Release()
	b.Release()

	// 3. Snapshot
	err = vs.Snapshot(context.Background())
	require.NoError(t, err)
	vs.Close()

	// 4. Re-initialize Store (Recovery)
	vs2 := NewVectorStore(mem, logger, 1024*1024*100, 0, 0)
	err = vs2.InitPersistence(storage.StorageConfig{
		DataPath: tmpDir,
	})
	require.NoError(t, err)
	defer vs2.Close()

	// 5. Verify Data
	ds, ok := vs2.getDataset("test_ds")
	require.True(t, ok)
	require.True(t, ds.SizeBytes.Load() > 0) // Should have data

	records := ds.Records.Read()
	require.Len(t, records, 1)
	require.Equal(t, int64(2), records[0].NumRows())
}
