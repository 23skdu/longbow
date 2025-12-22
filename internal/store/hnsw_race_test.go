package store

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestHNSWRaceCompaction(t *testing.T) {
	mem := memory.NewGoAllocator()
	vs := NewVectorStore(mem, nil, 0, 0, 0)
	vs.startIndexingWorkers(8) // More workers to increase race chance
	// VectorStore doesn't have a public Stop, but we can close stopChan if it was exported.
	// For this test, we'll just let it be cleaned up by GC or add a Stop method if possible.

	datasetName := "race_test"
	dim := 128

	// Create a record batch
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32), Nullable: false},
	}, nil)

	createRecord := func(rows int) arrow.RecordBatch {
		bldr := array.NewRecordBuilder(mem, schema)
		defer bldr.Release()

		vBldr := bldr.Field(0).(*array.FixedSizeListBuilder)
		fBldr := vBldr.ValueBuilder().(*array.Float32Builder)

		for i := 0; i < rows; i++ {
			vBldr.Append(true)
			for j := 0; j < dim; j++ {
				fBldr.Append(rand.Float32())
			}
		}
		return bldr.NewRecordBatch()
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stopCh := make(chan struct{})

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			select {
			case <-stopCh:
				return
			case <-ctx.Done():
				return
			default:
				rec := createRecord(20)
				// We need to simulate DoPut behavior
				vs.mu.Lock()
				ds, ok := vs.datasets[datasetName]
				if !ok {
					newDs := &Dataset{Name: datasetName, Records: []arrow.RecordBatch{}} // Fixed struct init
					newDs.Index = NewHNSWIndex(newDs)
					vs.datasets[datasetName] = newDs
					ds = newDs
				}
				vs.mu.Unlock()

				ds.dataMu.Lock()
				batchIdx := len(ds.Records)
				ds.Records = append(ds.Records, rec)
				ds.dataMu.Unlock()

				// Queue indexing jobs (simulating DoPut logic)
				numRows := int(rec.NumRows())
				for j := 0; j < numRows; j++ {
					rec.Retain()
					if !vs.indexQueue.Send(IndexJob{
						DatasetName: datasetName,
						Record:      rec,
						BatchIdx:    batchIdx,
						RowIdx:      j,
						CreatedAt:   time.Now(),
					}) {
						rec.Release()
					}
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	// Compactor goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			case <-ctx.Done():
				return
			default:
				time.Sleep(20 * time.Millisecond)
				_ = vs.CompactDataset(datasetName)
			}
		}
	}()

	// Search goroutine (to check if index is stable)
	wg.Add(1)
	go func() {
		defer wg.Done()
		query := make([]float32, dim)
		for i := range query {
			query[i] = rand.Float32()
		}
		for {
			select {
			case <-stopCh:
				return
			case <-ctx.Done():
				return
			default:
				vs.mu.RLock()
				ds, ok := vs.datasets[datasetName]
				vs.mu.RUnlock()
				if ok && ds.Index != nil {
					_ = ds.Index.SearchVectors(query, 5, nil)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Run for a bit
	time.Sleep(3 * time.Second)
	close(stopCh)
	wg.Wait()

	// Final check
	vs.mu.RLock()
	ds, ok := vs.datasets[datasetName]
	vs.mu.RUnlock()
	assert.True(t, ok)
	assert.NotNil(t, ds.Index)
	fmt.Printf("Final index size: %d\n", ds.Index.Len())
	fmt.Printf("Final record count: %d\n", len(ds.Records))
}
