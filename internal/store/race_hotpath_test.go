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
)

func TestRace_ConcurrentSearchAndInsert(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping race test in short mode")
	}

	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	s := NewVectorStore(mem, logger, 1024*1024*100, 0, 0)
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.BinaryTypes.String},
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	datasetName := "race_dataset"
	s.PrewarmDataset(datasetName, schema)

	var insertWg sync.WaitGroup
	var searchWg sync.WaitGroup
	numInserters := 2
	numSearchers := 4
	rowsPerBatch := 100
	numBatches := 20

	// Inserters
	for i := 0; i < numInserters; i++ {
		insertWg.Add(1)
		go func(id int) {
			defer insertWg.Done()
			for b := 0; b < numBatches; b++ {
				builder := array.NewRecordBuilder(mem, schema)
				ids := builder.Field(0).(*array.StringBuilder)
				vecs := builder.Field(1).(*array.FixedSizeListBuilder)
				vBuilder := vecs.ValueBuilder().(*array.Float32Builder)

				for r := 0; r < rowsPerBatch; r++ {
					ids.Append(fmt.Sprintf("worker-%d-batch-%d-row-%d", id, b, r))
					vecs.Append(true)
					v := make([]float32, 128)
					for i := range v {
						v[i] = rand.Float32()
					}
					vBuilder.AppendValues(v, nil)
				}
				rec := builder.NewRecord()
				builder.Release()

				if err := s.StoreRecordBatch(ctx, datasetName, rec); err != nil {
					t.Errorf("Inserter %d failed: %v", id, err)
					rec.Release()
					return
				}
				rec.Release()
				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}

	searchCtx, searchCancel := context.WithCancel(ctx)
	defer searchCancel()
	for i := 0; i < numSearchers; i++ {
		searchWg.Add(1)
		go func(id int) {
			defer searchWg.Done()
			for {
				select {
				case <-searchCtx.Done():
					return
				default:
					query := make([]float32, 128)
					for i := range query {
						query[i] = rand.Float32()
					}
					// Use SearchHybrid as it exercises RCU and Index
					_, err := s.SearchHybrid(searchCtx, datasetName, query, "", 10, 1.0, 60, 0.1, 2)
					if err != nil {
						// Dataset might be empty initially, which is fine
						_ = err
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
		}(i)
	}

	insertWg.Wait()
	searchCancel()
	searchWg.Wait()
}
