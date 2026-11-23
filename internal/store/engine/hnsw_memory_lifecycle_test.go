package engine

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"testing"

	lbmem "github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func liveOffHeapBytes() int64 {
	lbmem.ReleaseGlobalSlabPoolsUnused()
	runtime.GC()
	runtime.GC()
	lbmem.ReleaseGlobalSlabPoolsUnused()
	return lbmem.GetGlobalOffHeapAllocator().Allocated() - lbmem.GetGlobalSlabPoolUnusedMemory()
}

func makeLifecycleBatch(mem memory.Allocator, dim, rows int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
	}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	idB := b.Field(0).(*array.StringBuilder)
	vecB := b.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)
	for i := 0; i < rows; i++ {
		idB.Append(fmt.Sprintf("id-%d", i))
		vecB.Append(true)
		for d := 0; d < dim; d++ {
			valB.Append(float32((i+d)%31) * 0.01)
		}
	}
	return b.NewRecordBatch()
}

// TestP0_ArrowHNSW_OffHeapLifecycleBounded verifies that repeatedly creating,
// populating, searching, snapshotting, exporting/importing, and closing ArrowHNSW
// releases all off-heap slab arenas without net off-heap retention.
func TestP0_ArrowHNSW_OffHeapLifecycleBounded(t *testing.T) {
	const (
		dim        = 64
		numVectors = 128
		iterations = 5
	)
	mem := memory.NewGoAllocator()
	rec := makeLifecycleBatch(mem, dim, numVectors)
	defer rec.Release()

	runCycle := func() {
		ds := NewDataset("p0_lifecycle_hnsw", rec.Schema())
		ds.Records = NewLockFreeSliceFrom([]arrow.RecordBatch{rec})
		rec.Retain()
		defer func() {
			for _, r := range ds.Records.Read() {
				r.Release()
			}
		}()

		cfg := DefaultArrowHNSWConfig()
		cfg.Dims = dim
		cfg.InitialCapacity = 512

		idx := NewArrowHNSW(ds, &cfg, nil)
		for i := 0; i < numVectors; i++ {
			_, err := idx.AddByLocation(context.Background(), 0, i)
			require.NoError(t, err)
		}

		q := make([]float32, dim)
		res, err := idx.SearchVectors(context.Background(), q, 5, nil, SearchOptions{})
		require.NoError(t, err)
		require.NotEmpty(t, res)

		snap, _, err := idx.SnapshotGraph()
		require.NoError(t, err)
		snap.Release()

		analyticsSnap := idx.GetData().CloneForAnalytics()
		require.NotNil(t, analyticsSnap)

		var exported bytes.Buffer
		require.NoError(t, idx.ExportGraph(&exported))

		idx2 := NewArrowHNSW(ds, &cfg, nil)
		require.NoError(t, idx2.ImportGraph(&exported))
		require.NoError(t, idx2.Close())
		require.NoError(t, idx.Close())
	}

	// Warm one cycle so fixed process singletons (e.g. shared pools) are initialized
	// before establishing the baseline.
	runCycle()
	baseLive := liveOffHeapBytes()

	for i := 0; i < iterations; i++ {
		runCycle()
	}

	afterLive := liveOffHeapBytes()
	require.LessOrEqualf(
		t,
		afterLive,
		baseLive,
		"off-heap live bytes grew across HNSW lifecycle cycles: base=%d after=%d\n%s",
		baseLive,
		afterLive,
		lbmem.DebugSlabPoolsSnapshot(),
	)
}

// TestP0_VectorStore_SnapshotLifecycleBounded verifies that repeated VectorStore
// ingestion, TemporalIndex batches, snapshots, and Close calls return active
// off-heap allocations to baseline without leaking slabs across cycles.
func TestP0_VectorStore_SnapshotLifecycleBounded(t *testing.T) {
	const (
		dim        = 64
		numVectors = 96
		iterations = 3
	)
	mem := memory.NewGoAllocator()

	runStoreCycle := func() {
		dataDir := t.TempDir()
		vs := NewVectorStore(mem, zerolog.Nop(), 128*1024*1024, 16*1024*1024, 0)
		vs.dataPath = dataDir
		vs.StartIngestionWorkers(2)
		vs.StartIndexingWorkers(2)
		vs.SetTemporalIndex(TemporalConfig{Enabled: true})
		require.NoError(t, vs.InitPersistence(storage.StorageConfig{
			DataPath:         dataDir,
			SnapshotInterval: 0,
		}))

		rec := makeLifecycleBatch(mem, dim, numVectors)
		require.NoError(t, vs.StoreRecordBatch(context.Background(), "p0_store_ds", rec))
		rec.Release()

		ds, ok := vs.getDataset("p0_store_ds")
		require.True(t, ok)
		ds.WaitForIndexing()

		require.NoError(t, vs.Snapshot(context.Background()))
		require.NoError(t, vs.Close())
	}

	runStoreCycle()
	baseLive := liveOffHeapBytes()

	for i := 0; i < iterations; i++ {
		runStoreCycle()
	}

	afterLive := liveOffHeapBytes()
	require.LessOrEqualf(
		t,
		afterLive,
		baseLive,
		"off-heap live bytes grew across VectorStore/Snapshot cycles: base=%d after=%d\n%s",
		baseLive,
		afterLive,
		lbmem.DebugSlabPoolsSnapshot(),
	)
}
