package hnsw

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"
	arrowarray "github.com/apache/arrow-go/v18/arrow/array"
	arrowmemory "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

// TestIntersectFilterBitmaps_MultiPredicate verifies smallest-first roaring
// bitmap intersection and dense filterMask parity (roadmap D1).
func TestIntersectFilterBitmaps_MultiPredicate(t *testing.T) {
	require.Nil(t, IntersectFilterBitmaps(nil))
	require.Nil(t, IntersectFilterBitmaps([]*roaring.Bitmap{}))

	large := roaring.New()
	for i := uint32(0); i < 10000; i += 2 {
		large.Add(i)
	}
	medium := roaring.New()
	for i := uint32(0); i < 10000; i += 5 {
		medium.Add(i)
	}
	small := roaring.New()
	for i := uint32(0); i < 500; i += 10 {
		small.Add(i)
	}

	got := IntersectFilterBitmaps([]*roaring.Bitmap{large, medium, small})
	require.NotNil(t, got)
	require.Equal(t, uint64(50), got.GetCardinality())
	// Inputs must not be mutated.
	require.Equal(t, uint64(5000), large.GetCardinality())
	require.Equal(t, uint64(2000), medium.GetCardinality())
	require.Equal(t, uint64(50), small.GetCardinality())

	for i := uint32(0); i < 1000; i++ {
		want := i < 500 && i%10 == 0
		require.Equal(t, want, got.Contains(i), "id=%d", i)
	}

	// Empty predicate short-circuits to empty bitmap.
	empty := IntersectFilterBitmaps([]*roaring.Bitmap{large, roaring.New(), small})
	require.NotNil(t, empty)
	require.True(t, empty.IsEmpty())

	// buildIntersectedFilterMask parity with roaring intersection.
	mask, _ := buildIntersectedFilterMask([]*roaring.Bitmap{large, medium, small}, nil)
	require.NotNil(t, mask)
	for i := uint32(0); i < 1000; i++ {
		require.Equal(t, got.Contains(i), mask.allows(i), "mask id=%d", i)
	}
}

// TestRepairAgent_TriggerAfterBulkInsert verifies that a running RepairAgent is
// woken immediately after bulk insertion without waiting for ScanInterval (roadmap D4).
func TestRepairAgent_TriggerAfterBulkInsert(t *testing.T) {
	const (
		n    = 400
		dims = 16
	)
	cfg := types.DefaultArrowHNSWConfig()
	cfg.Dims = dims
	cfg.M = 16
	cfg.MMax = 16
	cfg.MMax0 = 32
	cfg.EfConstruction = 64
	cfg.InitialCapacity = n + 64

	idx := NewArrowHNSW(nil, &cfg, nil)
	idx.repairAgent = NewRepairAgent(idx, RepairAgentConfig{
		Enabled:            true,
		ScanInterval:       time.Hour, // Long interval so only TriggerRepair wakes it
		MaxRepairsPerCycle: 50,
	})
	idx.repairAgent.Start()
	defer func() { _ = idx.Close() }()
	require.NotNil(t, idx.repairAgent)

	mem := arrowmemory.NewGoAllocator()
	vecB := arrowarray.NewFixedSizeListBuilder(mem, int32(dims), arrow.PrimitiveTypes.Float32)
	defer vecB.Release()
	valB := vecB.ValueBuilder().(*arrowarray.Float32Builder)
	rng := rand.New(rand.NewSource(20261118)) // #nosec G404 -- deterministic test
	for range n {
		vecB.Append(true)
		for range dims {
			valB.Append(rng.Float32()*2 - 1)
		}
	}
	vecArr := vecB.NewArray()
	defer vecArr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "vector", Type: vecArr.DataType()}}, nil)
	rec := arrowarray.NewRecordBatch(schema, []arrow.Array{vecArr}, int64(n))
	defer rec.Release()

	rowIdxs := make([]int, n)
	batchIdxs := make([]int, n)
	for i := range n {
		rowIdxs[i] = i
	}

	before := idx.repairAgent.CyclesCompleted()
	ids, err := idx.AddBatch(context.Background(), []arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
	require.NoError(t, err)
	require.Len(t, ids, n)

	deadline := time.Now().Add(5 * time.Second)
	for idx.repairAgent.CyclesCompleted() == before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	require.Greater(t, idx.repairAgent.CyclesCompleted(), before,
		"bulk insert should trigger an immediate RepairAgent cycle")
}
