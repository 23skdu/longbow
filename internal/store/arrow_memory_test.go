package store

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestMemoryUsage(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory test in short mode")
	}

	runtime.GC()
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)

	// ArrowHNSWConfig
	count := 2_000
	dims := 384 // Realistic for measuring overhead percentage

	// Create Dataset
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)
	ds := NewDataset("mem_test", schema)

	// Generate Data
	fmt.Printf("Generating %d vectors (%d dims)...\n", count, dims)
	// Create single large batch for dataset
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	vecBuilder := builder.Field(0).(*array.FixedSizeListBuilder)
	floatBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	vecBuilder.Reserve(count)
	floatBuilder.Reserve(count * dims)

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < count; i++ {
		vecBuilder.Append(true)
		for j := 0; j < dims; j++ {
			floatBuilder.Append(rng.Float32())
		}
	}

	rec := builder.NewRecordBatch()
	defer rec.Release()
	ds.Records = append(ds.Records, rec)
	rec.Retain()

	runtime.GC()
	runtime.GC() // Double GC
	runtime.ReadMemStats(&m2)
	dataSize := m2.HeapAlloc - m1.HeapAlloc
	fmt.Printf("Dataset Memory: %d MB\n", dataSize/1024/1024)

	// Indexing
	fmt.Println("Building Index...")
	cfg := DefaultArrowHNSWConfig()
	cfg.SQ8Enabled = false // Baseline float32
	cfg.M = 32             // Explicit M

	idx := NewArrowHNSW(ds, cfg, nil)

	start := time.Now()
	// Insert one by one (simulating stream) or batch?
	// AddBatch is better for bulk.
	// But let's use AddBatch to mimic usage.
	// AddBatch wrapper? idx.AddBatch needs dummy mapping?
	// Manually map first.
	// Actually idx.AddBatch takes record batches.

	// Just use internal insert for raw graph memory measurement
	// We need to map global IDs to local.
	// For simple ArrowHNSW, ID is direct.
	// But we need to use Insert(id, level).

	// Just use dummy loop calling Insert
	for i := 0; i < count; i++ {
		if i%1000 == 0 {
			fmt.Printf("Inserting %d/%d\n", i, count)
		}
		// Directly appending to locationStore might be unsafe if concurrently accessed but this is test
		idx.locationStore.Append(Location{BatchIdx: 0, RowIdx: i})
		// level generator
		level := 0 // default
		// Simulate level gen
		if i%32 == 0 {
			level = 1
		} // rude approx
		_ = idx.Insert(uint32(i), level)
	}

	dur := time.Since(start)
	fmt.Printf("Indexing took %v (%.2f vec/s)\n", dur, float64(count)/dur.Seconds())

	runtime.GC()
	runtime.GC()
	var m3 runtime.MemStats
	runtime.ReadMemStats(&m3)

	var indexSize = int64(m3.HeapAlloc) - int64(m2.HeapAlloc)
	fmt.Printf("Index Overhead: %d KB\n", indexSize/1024)

	overheadBytesPerVec := float64(indexSize) / float64(count)
	fmt.Printf("Overhead per vector: %.2f bytes\n", overheadBytesPerVec)

	// Check against expectation
	// Graph:
	// - Neighbors: M * 4 bytes * 2 (bidirectional approx) * layers?
	// - Levels: 1 byte
	// - Counts: 4 bytes * layers
	// - Vectors: 4 bytes * dims (dense storage)
	// Approx 32 neighbors * 4 bytes = 128 bytes per layer. Avg 1 layer?
	// 500-1000 bytes/vec expected?
}
