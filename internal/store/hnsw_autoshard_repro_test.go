package store

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestAutoSharding_ConcurrentWrites_Repro reproduces the race condition where
// the underlying index is closed during migration while concurrent writes are occurring.
func TestAutoSharding_ConcurrentWrites_Repro(t *testing.T) {
	// Setup
	ds := &Dataset{
		Name:    "repro_dataset",
		Records: make([]arrow.RecordBatch, 0),
		dataMu:  sync.RWMutex{},
	}

	// Create a mock allocator
	mem := memory.NewGoAllocator()

	// Config to trigger sharding quickly
	config := AutoShardingConfig{
		Enabled:        true,
		ShardThreshold: 10000, // Higher threshold to slow down migration
		ShardCount:     2,
	}

	index := NewAutoShardingIndex(ds, config)
	// Initialize dims
	index.SetInitialDimension(4)

	// Pre-load data to near threshold
	fmt.Println("Pre-loading data...")
	preloadBatch := createMockBatch(mem, 9900, 4)
	defer preloadBatch.Release()
	rowIdxs := make([]int, 9900)
	batchIdxs := make([]int, 9900)
	for i := range rowIdxs {
		rowIdxs[i] = i
		batchIdxs[i] = 0
	}
	index.AddBatch([]arrow.RecordBatch{preloadBatch}, rowIdxs, batchIdxs)
	fmt.Println("Pre-load complete. Starting race...")

	var wg sync.WaitGroup
	workers := 20
	ops := 1000

	// We need valid record batches for AddBatch
	// Create a shared set of batches to reuse
	batches := make([]arrow.RecordBatch, 0, 10)
	for i := 0; i < 10; i++ {
		b := createMockBatch(mem, 10, 4) // 10 rows, 4 dims
		batches = append(batches, b)
		defer b.Release()
	}

	// Flag to detect panic or error
	var panicCount atomic.Int32

	// Start concurrent writers
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("Worker %d panicked: %v\n", id, r)
					panicCount.Add(1)
				}
			}()

			for i := 0; i < ops; i++ {
				// Pick a batch
				bIdx := rand.Intn(len(batches))
				batch := batches[bIdx]

				// Mock row indices/batch indices
				rowIdxs := make([]int, batch.NumRows())
				batchIdxs := make([]int, batch.NumRows())
				for r := 0; r < int(batch.NumRows()); r++ {
					rowIdxs[r] = r
					batchIdxs[r] = bIdx
				}

				// The call that might panic if index is closed underneath
				_, _ = index.AddBatch([]arrow.RecordBatch{batch}, rowIdxs, batchIdxs)

				// Small sleep to allow migration to interleave
				if i%10 == 0 {
					runtime.Gosched()
				}
			}
		}(w)
	}

	// Wait for workers
	wg.Wait()

	if panicCount.Load() > 0 {
		t.Fatalf("Test panicked %d times! Race condition reproduced.", panicCount.Load())
	} else {
		fmt.Println("No panics detected.")
	}
}

func createMockBatch(mem memory.Allocator, rows int, dim int) arrow.RecordBatch {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	listB := b.Field(0).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < rows; i++ {
		listB.Append(true)
		for d := 0; d < dim; d++ {
			valB.Append(rand.Float32())
		}
	}

	return b.NewRecordBatch()
}
