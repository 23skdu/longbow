package hnsw2

import (
	"math/rand"
	"testing"
	"github.com/23skdu/longbow/internal/store"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// BenchmarkArrowHNSWInsert benchmarks the insertion performance.
func BenchmarkArrowHNSWInsert(b *testing.B) {
	// Setup
	config := DefaultConfig()
	dims := 384
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	// Create a large batch of random vectors
	// We'll reuse this batch for the benchmark to avoid allocation noise
	count := 10000 // Block size
	if b.N > count {
		// Just handle b.N cases by wrapping or generating more if essential
		// But for standard benchmarking, reusing same data is fine.
		// However, we need unique IDs.
	}
	
	// Generate random vectors
	data := make([]float32, count*dims)
	rng := rand.New(rand.NewSource(42))
	for i := range data {
		data[i] = rng.Float32() * 2 - 1
	}
	
	bldr := array.NewFixedSizeListBuilder(pool, int32(dims), arrow.PrimitiveTypes.Float32)
	defer bldr.Release()
	vb := bldr.ValueBuilder().(*array.Float32Builder)
	
	bldr.Reserve(count)
	vb.Reserve(count * dims)
	
	for i := 0; i < count; i++ {
		bldr.Append(true)
		vb.AppendValues(data[i*dims:(i+1)*dims], nil)
	}
	
	rec := bldr.NewArray()
	defer rec.Release()
	
	// Create simplified record batch
	batch := array.NewRecordBatch(schema, []arrow.Array{rec}, int64(count))
	defer batch.Release()
	
	// Create Dataset
	ds := &store.Dataset{
		Schema: schema,
		Records: []arrow.RecordBatch{batch},
	}
	
	// Initialize Index
	// We create a NEW index for the benchmark run to strictly measure insertion?
	// If b.N is large, we might fill it up.
	// We want to measure amortized insertion time.
	
	index := NewArrowHNSW(ds, config)
	
	// Pre-train PQ if configured


	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		// Cycle through the dataset records
		rowIdx := i % count
		// ID is just i
		id := uint32(i)
		
		// Insert
		// We manually call Insert like AddByLocation would
		// Note: we bypass locationStore for raw Insert benchmark or use AddByLocation?
		// Let's use AddByLocation to be realistic?
		// But AddByLocation adds to locationStore, which grows.
		// Let's call Insert directly to isolate Graph performance.
		// We need to ensure getVector works.
		// getVector uses locationStore if available or relies on...
		// check getVector logic.
		
		// If we use index.Insert(id), getVector(id) is called.
		// We need to map id -> location.
		// So we must add to locationStore first.
		
		index.locationStore.Append(store.Location{BatchIdx: 0, RowIdx: rowIdx})
		
		level := index.generateLevel()
		_ = index.Insert(id, level)
	}
}
