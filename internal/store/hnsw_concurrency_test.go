package store

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func makeConcurrencyTestRecord(mem memory.Allocator, dims, numVectors int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "int64", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).Reserve(numVectors)
	listB := b.Field(1).(*array.FixedSizeListBuilder)
	listB.Reserve(numVectors)
	valB := listB.ValueBuilder().(*array.Float32Builder)
	valB.Reserve(numVectors * dims)

	for i := 0; i < numVectors; i++ {
		b.Field(0).(*array.Int64Builder).UnsafeAppend(int64(i))
		listB.Append(true)
		for j := 0; j < dims; j++ {
			valB.UnsafeAppend(rand.Float32())
		}
	}

	return b.NewRecordBatch()
}

func TestHNSW_Concurrency_HighContention(t *testing.T) {
	mem := memory.NewGoAllocator()
	rec := makeConcurrencyTestRecord(mem, 128, 500)
	defer rec.Release()

	ds := &Dataset{
		Records: nil,
		Name:    "stress_test",
		dataMu:  sync.RWMutex{},
	}

	idx := NewHNSWIndex(ds)
	// Create stripe locks created by NewHNSWIndex, but let's ensure they are initialized
	// NewHNSWIndex initializes them based on NumCPU.

	// Phase 1: Concurrent Adds
	const numGoroutines = 8
	const vectorsPerGoroutine = 50
	var wg sync.WaitGroup
	var errCount atomic.Int32

	start := time.Now()
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < vectorsPerGoroutine; j++ {
				rowIdx := rand.Intn(500)
				// AddSafe copies vector, so rowIdx reuse is fine for testing concurrency of graph
				_, err := idx.AddSafe(rec, rowIdx, 0)
				if err != nil {
					errCount.Add(1)
					fmt.Printf("Add error: %v\n", err)
				}
			}
		}()
	}
	wg.Wait()
	duration := time.Since(start)

	if errCount.Load() > 0 {
		t.Fatalf("Encountered %d errors during concurrent adds", errCount.Load())
	}
	t.Logf("Added %d vectors in %v", numGoroutines*vectorsPerGoroutine, duration)

	if idx.Len() != numGoroutines*vectorsPerGoroutine {
		t.Errorf("Expected %d vectors, got %d", numGoroutines*vectorsPerGoroutine, idx.Len())
	}
}

func TestHNSW_Concurrency_Mixed(t *testing.T) {
	mem := memory.NewGoAllocator()
	rec := makeConcurrencyTestRecord(mem, 128, 500)
	defer rec.Release()

	ds := &Dataset{Name: "mixed_test", dataMu: sync.RWMutex{}}
	idx := NewHNSWIndex(ds)

	// Pre-populate
	for i := 0; i < 100; i++ {
		_, _ = idx.AddSafe(rec, i, 0)
	}

	var wg sync.WaitGroup
	var ops atomic.Int64
	done := make(chan struct{})

	// 10 concurrent writers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Keep writing until told to stop
			for {
				select {
				case <-done:
					return
				default:
					rowIdx := rand.Intn(500)
					_, _ = idx.AddSafe(rec, rowIdx, 0)
					ops.Add(1)
					runtime.Gosched()
				}
			}
		}()
	}

	// 10 concurrent readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			q := make([]float32, 128)
			for {
				select {
				case <-done:
					return
				default:
					_, _ = idx.Search(q, 5)
					ops.Add(1)
					runtime.Gosched()
				}
			}
		}()
	}

	// Run for a fixed duration
	time.Sleep(200 * time.Millisecond)
	close(done)
	wg.Wait()

	t.Logf("Completed %d mixed operations in 200ms", ops.Load())

	// Validate final state
	if idx.Len() < 100 {
		t.Error("Index shrank?")
	}
}
