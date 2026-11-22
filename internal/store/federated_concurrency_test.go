package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

func makeTestBatch(t *testing.T, dim int, n int) arrow.RecordBatch {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
	}, nil)
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	idB := b.Field(0).(*array.StringBuilder)
	vecB := b.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)
	for i := 0; i < n; i++ {
		idB.Append(fmt.Sprintf("v%d", i))
		vecB.Append(true)
		for j := 0; j < dim; j++ {
			valB.Append(float32(i+j) / 10.0)
		}
	}
	return b.NewRecordBatch()
}

func TestFederatedSearchConcurrent(t *testing.T) {
	s := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1024*1024*1024, 0, 0)
	s.StartIndexingWorkers(2)
	s.StartIngestionWorkers(2)
	defer func() { s.StopIndexingWorkers(2); s.StopIngestionWorkers(2) }()

	dsName := "ds1"
	batch := makeTestBatch(t, 4, 10)
	defer batch.Release()
	if err := s.StoreRecordBatch(context.Background(), dsName, batch); err != nil {
		t.Fatal(err)
	}
	// wait for indexing
	ds, _ := s.getDataset(dsName)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for ds.Index == nil || ds.Index.Len() < 10 {
		select {
		case <-ctx.Done():
			t.Fatal("indexing timeout")
		case <-time.After(50 * time.Millisecond):
		}
	}

	query := make([]float32, 4)
	for i := range query {
		query[i] = 0.1
	}

	var wg sync.WaitGroup
	var errs atomic.Int64
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if _, err := s.FederatedSearch(context.Background(), []string{dsName}, nil, query, 5, 60); err != nil {
					errs.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if errs.Load() > 0 {
		t.Fatalf("concurrent federated search errors: %d", errs.Load())
	}
}

func FuzzFederatedSearchDirtyFlag(f *testing.F) {
	f.Add(1)
	f.Fuzz(func(t *testing.T, n int) {
		if n < 1 {
			n = 1
		}
		if n > 5 {
			n = 5
		}
		s := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1024*1024*1024, 0, 0)
		s.federatedDirty.Add(1)
		if s.federatedDirty.Load() != 1 {
			t.Fatal("dirty flag not set")
		}
		s.federatedDirty.Store(0)
		if s.federatedDirty.Load() != 0 {
			t.Fatal("dirty flag not cleared")
		}
	})
}
