package store

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryLeak_CreateDropDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	logger := zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	mem := memory.NewGoAllocator()
	s := NewVectorStore(mem, logger, 10*1024*1024*1024, 0, 0)
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
	}, nil)

	// 1. Warm up and get baseline memory
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("warmup_%d", i)
		runDatasetCycle(t, ctx, s, name, schema, &logger)
	}
	s.ReleaseMemory()

	baselineCurrentMemory := s.currentMemory.Load()
	logger.Info().Int64("current_memory", baselineCurrentMemory).Msg("Baseline memory")

	// 2. Run iterations with GC between each
	iterations := 20 // Fewer iterations for cleaner signal
	for i := 0; i < iterations; i++ {
		name := fmt.Sprintf("dataset_%d", i)
		runDatasetCycle(t, ctx, s, name, schema, &logger)

		// Force GC and memory release between iterations
		runtime.GC()

		if i%5 == 0 {
			currentMem := s.currentMemory.Load()
			logger.Info().Int("iter", i).Int64("current_memory", currentMem).Msg("Progress")
		}
	}

	// 3. Final cleanup and check
	s.ReleaseMemory()
	time.Sleep(500 * time.Millisecond)

	finalCurrentMemory := s.currentMemory.Load()
	logger.Info().Int64("current_memory", finalCurrentMemory).Msg("Final memory")

	// Check that currentMemory (our internal accounting) is back to baseline
	// Allow for some variance due to timing
	memoryDelta := finalCurrentMemory - baselineCurrentMemory
	logger.Info().Int64("delta", memoryDelta).Msg("Memory delta")

	// The internal memory tracking should show minimal growth
	// (ideally zero, but allowing for small transient allocations)
	assert.True(t, memoryDelta < 100*1024*1024, "Memory leak detected! Delta: %d bytes", memoryDelta)
}

func runDatasetCycle(t *testing.T, ctx context.Context, s *VectorStore, name string, schema *arrow.Schema, logger *zerolog.Logger) {
	// Create dataset
	beforeCreate := s.currentMemory.Load()
	s.PrewarmDataset(name, schema)
	afterCreate := s.currentMemory.Load()
	logger.Debug().Int64("before", beforeCreate).Int64("after", afterCreate).Int64("delta", afterCreate-beforeCreate).Msg("After PrewarmDataset")

	// Add data
	numRows := 1000
	bld := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bld.Release()

	idBld := bld.Field(0).(*array.Uint32Builder)
	vecBld := bld.Field(1).(*array.FixedSizeListBuilder)
	valBld := vecBld.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < numRows; i++ {
		idBld.Append(uint32(i))
		vecBld.Append(true)
		for j := 0; j < 128; j++ {
			valBld.Append(float32(j))
		}
	}

	rec := bld.NewRecordBatch()
	defer rec.Release()

	beforeStore := s.currentMemory.Load()
	err := s.StoreRecordBatch(ctx, name, rec)
	require.NoError(t, err)

	// Wait for indexing
	s.WaitForIndexing(name)
	afterStore := s.currentMemory.Load()

	// Get dataset for search
	ds, ok := s.getDataset(name)
	require.True(t, ok)

	dsSize := ds.SizeBytes.Load()
	logger.Debug().Int64("before_store", beforeStore).Int64("after_store", afterStore).Int64("store_delta", afterStore-beforeStore).Int64("ds_size", dsSize).Msg("After StoreRecordBatch")

	if len(ds.Records) == 0 {
		logger.Warn().Str("dataset", name).Msg("Dataset has NO records after indexing")
	}

	// Search
	query := make([]float32, 128)
	for i := range query {
		query[i] = 1.0
	}
	results, err := ds.Index.SearchVectors(ctx, query, 10, nil, SearchOptions{})
	require.NoError(t, err)

	if len(results) == 0 {
		logger.Warn().Str("dataset", name).Int("records", len(ds.Records)).Int("index_len", ds.Index.Len()).Msg("Search returned 0 results")
	}
	assert.NotEmpty(t, results)

	// Drop dataset
	beforeDrop := s.currentMemory.Load()
	err = s.DropDataset(ctx, name)
	require.NoError(t, err)

	// Give async cleanup time to complete
	time.Sleep(100 * time.Millisecond)
	afterDrop := s.currentMemory.Load()
	logger.Debug().Int64("before_drop", beforeDrop).Int64("after_drop", afterDrop).Int64("drop_delta", afterDrop-beforeDrop).Int64("expected_delta", -dsSize).Msg("After DropDataset")
}

func TestMemoryLeak_GoroutineCount(t *testing.T) {
	// Simple test to ensure goroutines return to baseline
	baseline := runtime.NumGoroutine()

	logger := zerolog.Nop()
	mem := memory.NewGoAllocator()
	s := NewVectorStore(mem, logger, 1024*1024*1024, 0, 0)

	ctx := context.Background()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
	}, nil)

	for i := 0; i < 50; i++ {
		name := fmt.Sprintf("ds_%d", i)
		s.PrewarmDataset(name, schema)
		_ = s.DropDataset(ctx, name)
	}

	_ = s.Close()

	// Wait for workers to stop
	time.Sleep(500 * time.Millisecond)

	final := runtime.NumGoroutine()
	// Allow for some background runtime goroutines
	assert.True(t, final <= baseline+5, "Goroutine leak detected: baseline %d, final %d", baseline, final)
}
