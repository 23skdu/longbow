package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

// TestDeleteVectorByIDConcurrency runs the test delete vector by id concurrency operation.
func TestDeleteVectorByIDConcurrency(t *testing.T) {
	s := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1024*1024*1024, 0, 0)
	s.StartIndexingWorkers(2)
	s.StartIngestionWorkers(2)
	defer func() { _ = s.Close() }()

	dsName := "dsdel"
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
	}, nil)
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	idB := b.Field(0).(*array.StringBuilder)
	vecB := b.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)
	for i := 0; i < 50; i++ {
		idB.Append(fmt.Sprintf("v%d", i))
		vecB.Append(true)
		for j := 0; j < 4; j++ {
			valB.Append(float32(i + j))
		}
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	if err := s.StoreRecordBatch(context.Background(), dsName, rec); err != nil {
		t.Fatal(err)
	}
	ds, _ := s.getDataset(dsName)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for ds.Index == nil || ds.Index.Len() < 50 {
		select {
		case <-ctx.Done():
			t.Fatal("indexing timeout")
		case <-time.After(50 * time.Millisecond):
		}
	}

	var wg sync.WaitGroup
	var deletes atomic.Int64
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				id := fmt.Sprintf("v%d", idx*10+j)
				if err := s.DeleteVectorByID(context.Background(), dsName, id); err == nil {
					deletes.Add(1)
				}
			}
		}(i)
	}
	// concurrent searches should not block
	var searches atomic.Int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 200; j++ {
			if _, err := s.GetVectorByID(dsName, "v0"); err == nil || err != nil {
				searches.Add(1)
			}
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Wait()
	if deletes.Load() == 0 {
		t.Fatal("no deletes succeeded")
	}
}

// FuzzSetTombstone runs the fuzz set tombstone operation.
func FuzzSetTombstone(f *testing.F) {
	f.Add(0, 0)
	f.Fuzz(func(t *testing.T, batch, row int) {
		if batch < 0 {
			batch = -batch
		}
		if row < 0 {
			row = -row
		}
		if batch > 1000 {
			batch = 1000
		}
		if row > 1000 {
			row = 1000
		}
		ds := &Dataset{Tombstones: make(map[int]*types.Bitset)}
		setTombstone(ds, batch, row)
		ds.TombstoneMu.RLock()
		ts := ds.Tombstones[batch]
		if ts == nil || !ts.Contains(row) {
			ds.TombstoneMu.RUnlock()
			t.Fatal("tombstone not set")
		}
		ds.TombstoneMu.RUnlock()
	})
}
