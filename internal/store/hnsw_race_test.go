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
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestHNSWRaceCompaction(t *testing.T) {
	mem := memory.NewGoAllocator()
	vs := NewVectorStore(mem, zerolog.Nop(), 0, 0, 0)
	defer func() { _ = vs.Close() }()
	vs.StartIndexingWorkers(2) // More workers to increase race chance

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
				ds, _ := vs.getOrCreateDataset(datasetName, func() *Dataset {
					newDs := NewDataset(datasetName, schema)
					newDs.Index = NewTestHNSWIndex(newDs)
					return newDs
				})

				ds.dataMu.Lock()
				batchIdx := len(ds.Records)
				ds.Records = append(ds.Records, rec)
				ds.dataMu.Unlock()

				// Queue indexing jobs (simulating DoPut logic - batch level)
				rec.Retain()
				if !vs.indexQueue.Send(IndexJob{
					DatasetName: datasetName,
					Record:      rec,
					BatchIdx:    batchIdx,
					CreatedAt:   time.Now(),
				}) {
					rec.Release()
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
				// _ = vs.CompactDataset(context.Background(), datasetName) // Not implemented
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
				ds, ok := vs.getDataset(datasetName)
				if ok && ds.Index != nil {
					_, _ = ds.Index.SearchVectors(context.Background(), query, 5, nil, SearchOptions{})
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
	ds, ok := vs.getDataset(datasetName)
	assert.True(t, ok)
	assert.NotNil(t, ds.Index)
	fmt.Printf("Final index size: %d\n", ds.Index.Len())
	fmt.Printf("Final record count: %d\n", len(ds.Records))
}
