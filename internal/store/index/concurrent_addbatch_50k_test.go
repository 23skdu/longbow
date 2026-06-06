package index

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestArrowHNSW_ConcurrentAddBatch_Int8_50k_Stress is a regression test for
// the P0 "arena is nil" failure at int8 50k+ scale discovered in the
// 12-run benchmark matrix (docs/performance.md, 2026-06-06).
//
// The bug: in addBatchBulkInternal, after the bootstrap loop the code did
//
//	data = h.data.Load()                  // published, not private
//
// then later did data.Clone() and compareAndSwapData(...). A concurrent
// addBatchBulkInternal from another goroutine could CAS-publish a newer
// snapshot and Release() the snapshot we held, nilling the typed-arenas.
// The next data.Clone() then propagated nil-typed-arenas into the global
// published data, and any subsequent AllocSlice logged "arena is nil".
//
// The fix: change that line to data = h.data.Load().Clone() so the local
// data is a private copy with a Retain() on the underlying Slab. The
// Slab stays alive for the rest of addBatchBulkInternal, so all
// data.Clone() and data.SetVector calls have a valid live GraphData.
//
// This test exercises:
//   - 5 concurrent AddBatch calls at 10k rows each = 50k total
//   - 384 dims int8 (matches the benchmark configs that triggered the bug)
//   - Without the fix, this test fails with "arena is nil" errors logged
//     during one or more of the batches (typically 1-3 of the 5 fail).
func TestArrowHNSW_ConcurrentAddBatch_Int8_50k_Stress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 50k stress test in short mode")
	}

	mem := memory.NewGoAllocator()
	dims := 384
	batchSize := 10_000
	numBatches := 5

	recs := make([]arrow.RecordBatch, numBatches)
	ds := NewMockDataset("concurrent_int8_50k", nil)
	for i := 0; i < numBatches; i++ {
		rec := makeInt8TestRecordBatch(mem, dims, batchSize)
		rec.Retain()
		recs[i] = rec
		ds.Records = append(ds.Records, rec)
	}

	config := types.DefaultArrowHNSWConfig()
	config.DataType = types.VectorTypeInt8
	config.Dims = dims
	idx := NewArrowHNSW(ds, &config, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	start := time.Now()
	for b := 0; b < numBatches; b++ {
		wg.Add(1)
		go func(batchIdx int, r arrow.RecordBatch) {
			defer wg.Done()
			rowIdxs := make([]int, batchSize)
			batchIdxs := make([]int, batchSize)
			for k := 0; k < batchSize; k++ {
				rowIdxs[k] = k
				batchIdxs[k] = 0
			}
			_, err := idx.AddBatch(ctx, []arrow.RecordBatch{r}, rowIdxs, batchIdxs)
			if err != nil {
				t.Errorf("batch %d AddBatch failed: %v", batchIdx, err)
			}
		}(b, recs[b])
	}
	wg.Wait()
	elapsed := time.Since(start)
	t.Logf("%dx%dk int8 concurrent AddBatch completed in %v (%.0f vec/s)",
		numBatches, batchSize/1000, elapsed,
		float64(numBatches*batchSize)/elapsed.Seconds())

	meta := idx.GetMetadataSnapshot()
	expected := int64(numBatches * batchSize)
	if meta.NodeCount < expected {
		t.Errorf("expected nodeCount >= %d, got %d", expected, meta.NodeCount)
	}

	// Sanity: data must still be loadable and not nil
	if d := idx.data.Load(); d == nil {
		t.Fatal("h.data.Load() returned nil after stress test")
	} else if d.Int8Arena == nil {
		t.Fatal("h.data.Int8Arena is nil after stress test")
	}
}
