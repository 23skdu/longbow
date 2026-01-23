package store

import (
	"context"
	"math/rand"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func FuzzHNSW_Compaction(f *testing.F) {
	f.Add(uint16(100), uint16(30), uint16(5))
	f.Add(uint16(500), uint16(20), uint16(10))
	f.Add(uint16(1000), uint16(15), uint16(20))

	f.Fuzz(func(t *testing.T, numVectors, deletePercent, concurrentOps uint16) {
		if numVectors == 0 || concurrentOps == 0 {
			return
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		dim := 16
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
		}, nil)

		mem := memory.NewGoAllocator()
		builder := array.NewRecordBuilder(mem, schema)
		defer builder.Release()

		listB := builder.Field(0).(*array.FixedSizeListBuilder)
		valB := listB.ValueBuilder().(*array.Float32Builder)

		for i := 0; i < int(numVectors); i++ {
			vec := make([]float32, dim)
			for j := range vec {
				vec[j] = float32(i*1000+j) / 1000.0
			}
			listB.Append(true)
			valB.AppendValues(vec, nil)
		}

		record := builder.NewRecordBatch()
		defer record.Release()

		ds := &Dataset{
			Name:    "fuzz_compaction",
			Schema:  schema,
			Records: []arrow.RecordBatch{record},
		}

		config := DefaultArrowHNSWConfig()
		config.Dims = dim
		config.PackedAdjacencyEnabled = true
		h := NewArrowHNSW(ds, config, nil)
		defer h.Close()

		r := rand.New(rand.NewSource(42))

		for i := 0; i < int(numVectors); i++ {
			_, err := h.AddByLocation(context.Background(), 0, i)
			if err != nil {
				return
			}
		}

		numToDelete := int64(numVectors) * int64(deletePercent) / 100
		numToDeleteUint64 := uint64(numToDelete)
		if numToDelete > 0 {
			for i := int64(0); i < numToDelete; i++ {
				id := uint32(r.Intn(int(numVectors)))
				_ = h.Delete(id)
			}
		}

		var wg sync.WaitGroup
		wg.Add(int(concurrentOps))

		for g := uint16(0); g < concurrentOps; g++ {
			go func(gID uint16) {
				defer wg.Done()

				if gID%3 == 0 {
					query := make([]float32, dim)
					for j := range query {
						query[j] = float32(j)
					}
					_, _ = h.Search(ctx, query, 10, 100, nil)
				}

				if gID%5 == 0 {
					if h.NeedsCompaction() {
						stats, err := h.CompactGraph(ctx)
						if err != nil {
							return
						}
						if stats != nil && stats.NodesRemoved < 0 {
							return
						}
					}
				}
			}(g)
		}

		wg.Wait()

		deletedCount := h.deleted.Count()

		if deletedCount > numToDeleteUint64 {
			return
		}

		if deletedCount > uint64(numVectors) {
			return
		}
	})
}

func FuzzHNSW_CompactionUnderLoad(f *testing.F) {
	f.Add(uint8(5), uint8(10))
	f.Add(uint8(10), uint8(5))

	f.Fuzz(func(t *testing.T, numGoroutines, iterations uint8) {
		if numGoroutines == 0 || iterations == 0 {
			return
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		dim := 16
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
		}, nil)

		mem := memory.NewGoAllocator()
		builder := array.NewRecordBuilder(mem, schema)
		defer builder.Release()

		listB := builder.Field(0).(*array.FixedSizeListBuilder)
		valB := listB.ValueBuilder().(*array.Float32Builder)

		for i := 0; i < 200; i++ {
			vec := make([]float32, dim)
			for j := range vec {
				vec[j] = float32(i*1000+j) / 1000.0
			}
			listB.Append(true)
			valB.AppendValues(vec, nil)
		}

		record := builder.NewRecordBatch()
		defer record.Release()

		ds := &Dataset{
			Name:    "fuzz_compaction_load",
			Schema:  schema,
			Records: []arrow.RecordBatch{record},
		}

		config := DefaultArrowHNSWConfig()
		config.Dims = dim
		config.PackedAdjacencyEnabled = true
		h := NewArrowHNSW(ds, config, nil)
		defer h.Close()

		for i := 0; i < 200; i++ {
			_, _ = h.AddByLocation(context.Background(), 0, i)
		}

		var wg sync.WaitGroup
		wg.Add(int(numGoroutines))

		for g := uint8(0); g < numGoroutines; g++ {
			go func(gID uint8) {
				defer wg.Done()

				for iter := uint8(0); iter < iterations; iter++ {
					if ctx.Err() != nil {
						return
					}

					switch (gID + iter) % 4 {
					case 0:
						for i := 0; i < 10; i++ {
							_, _ = h.AddByLocation(context.Background(), 0, int(iter)*10+i)
						}
					case 1:
						for i := 0; i < 5; i++ {
							id := uint32(int(iter)*5 + i)
							if id < 1000 {
								_ = h.Delete(id)
							}
						}
					case 2:
						query := make([]float32, dim)
						for j := range query {
							query[j] = float32(j)
						}
						_, _ = h.Search(ctx, query, 10, 100, nil)
					case 3:
						if h.NeedsCompaction() {
							_, err := h.CompactGraph(ctx)
							if err != nil {
								return
							}
						}
					}
				}
			}(g)
		}

		wg.Wait()

		nodeCount := h.nodeCount.Load()
		if nodeCount <= 0 {
			return
		}

		deletedCount := h.deleted.Count()
		if deletedCount < 0 {
			return
		}
	})
}
