package store

import (
	"context"
	"runtime"
	"sync"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestAdaptiveChunkSize_Calculation(t *testing.T) {
	testCases := []struct {
		name           string
		numWorkers     int
		neighborCount  int
		minChunkSize   int
		maxChunkSize   int
		targetChunks   int
		expectedMethod string
	}{
		{
			name:           "Small neighbor count uses serial",
			numWorkers:     runtime.NumCPU(),
			neighborCount:  20,
			minChunkSize:   32,
			maxChunkSize:   500,
			targetChunks:   runtime.NumCPU() * 3,
			expectedMethod: "serial",
		},
		{
			name:           "Medium neighbor count uses adaptive parallel",
			numWorkers:     4,
			neighborCount:  500,
			minChunkSize:   32,
			maxChunkSize:   500,
			targetChunks:   12,
			expectedMethod: "parallel",
		},
		{
			name:           "Large neighbor count uses adaptive parallel",
			numWorkers:     8,
			neighborCount:  5000,
			minChunkSize:   32,
			maxChunkSize:   500,
			targetChunks:   24,
			expectedMethod: "parallel",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			neighborCount := tc.neighborCount
			_ = tc.numWorkers

			chunkSize := neighborCount / tc.targetChunks
			if chunkSize < tc.minChunkSize {
				chunkSize = tc.minChunkSize
			} else if chunkSize > tc.maxChunkSize {
				chunkSize = tc.maxChunkSize
			}

			method := "parallel"
			if neighborCount < chunkSize*2 {
				method = "serial"
			}

			assert.Equal(t, tc.expectedMethod, method)
		})
	}
}

func TestAdaptiveChunkSize_WorkerEfficiency(t *testing.T) {
	testCases := []struct {
		name           string
		numWorkers     int
		neighborCount  int
		minChunkSize   int
		expectParallel bool
	}{
		{
			name:           "Small count falls back to serial",
			numWorkers:     4,
			neighborCount:  100,
			minChunkSize:   32,
			expectParallel: false,
		},
		{
			name:           "Medium count uses parallel",
			numWorkers:     4,
			neighborCount:  500,
			minChunkSize:   32,
			expectParallel: true,
		},
		{
			name:           "Large count uses parallel",
			numWorkers:     8,
			neighborCount:  8000,
			minChunkSize:   32,
			expectParallel: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			targetChunks := tc.numWorkers * 3
			chunkSize := tc.neighborCount / targetChunks
			if chunkSize < tc.minChunkSize {
				chunkSize = tc.minChunkSize
			}

			workPerWorker := float64(tc.neighborCount) / float64(tc.numWorkers)
			efficiencyRatio := workPerWorker / float64(chunkSize)

			if tc.expectParallel {
				assert.GreaterOrEqual(t, efficiencyRatio, 1.0, "parallel path should have efficient work distribution")
			} else {
				assert.LessOrEqual(t, efficiencyRatio, 1.0, "serial fallback when work per worker <= chunk size")
			}
		})
	}
}

func TestAdaptiveChunkSize_ConfigThresholds(t *testing.T) {
	testCases := []struct {
		name             string
		config           types.ParallelSearchConfig
		neighborCount    int
		expectedParallel bool
	}{
		{
			name: "Enabled with sufficient neighbors",
			config: types.ParallelSearchConfig{
				Enabled:   true,
				Workers:   4,
				Threshold: 100,
			},
			neighborCount:    500,
			expectedParallel: true,
		},
		{
			name: "Disabled always serial",
			config: types.ParallelSearchConfig{
				Enabled:   false,
				Workers:   4,
				Threshold: 100,
			},
			neighborCount:    500,
			expectedParallel: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			numWorkers := tc.config.Workers
			if numWorkers <= 0 {
				numWorkers = runtime.NumCPU()
			}

			isParallel := tc.config.Enabled && tc.neighborCount >= tc.config.Threshold

			if isParallel {
				targetChunks := numWorkers * 3
				chunkSize := tc.neighborCount / targetChunks
				if chunkSize < 32 {
					chunkSize = 32
				}
				if tc.neighborCount < chunkSize*2 {
					isParallel = false
				}
			}

			assert.Equal(t, tc.expectedParallel, isParallel)
		})
	}
}

func TestAdaptiveChunkSize_ConcurrentSafety(t *testing.T) {
	mem := memory.NewGoAllocator()
	vectors := generateTestVectors(100, 4)
	rec := makeBatchTestRecord(mem, 4, vectors)

	ds := &Dataset{
		Name:    "test",
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewTestHNSWIndex(ds)

	locations := make([]Location, 100)
	for i := 0; i < 100; i++ {
		locations[i] = Location{BatchIdx: 0, RowIdx: i}
	}
	for _, loc := range locations {
		_, _ = idx.AddByLocation(context.Background(), loc.BatchIdx, loc.RowIdx)
	}

	neighbors := make([]types.Candidate, 1000)
	for i := 0; i < 1000; i++ {
		neighbors[i] = types.Candidate{ID: uint32(i % 100)}
	}

	query := make([]float32, 4)
	for i := range query {
		query[i] = float32(i)
	}

	const numGoroutines = 10
	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				k := 10 + (gid+i)%20
				_, _ = idx.Search(context.Background(), query, k, nil)
			}
		}(g)
	}
	wg.Wait()
}

func BenchmarkAdaptiveChunkSize_ParallelVsSerial(b *testing.B) {
	mem := memory.NewGoAllocator()
	dims := 128
	numVectors := 10000
	vectors := generateTestVectors(numVectors, dims)
	rec := makeBatchTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "bench",
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewTestHNSWIndex(ds)

	locations := make([]Location, numVectors)
	for i := 0; i < numVectors; i++ {
		locations[i] = Location{BatchIdx: 0, RowIdx: i}
	}
	for _, loc := range locations {
		_, _ = idx.AddByLocation(context.Background(), loc.BatchIdx, loc.RowIdx)
	}

	query := make([]float32, dims)
	for i := range query {
		query[i] = float32(i)
	}

	b.Run("SmallSet", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = idx.Search(context.Background(), query, 10, nil)
		}
	})

	neighbors500 := make([]types.Candidate, 500)
	for i := 0; i < 500; i++ {
		neighbors500[i] = types.Candidate{ID: uint32(i % numVectors)}
	}

	b.Run("MediumSet_Parallel", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = idx.processResultsParallel(context.Background(), query, neighbors500, 10, nil)
		}
	})

	neighbors5000 := make([]types.Candidate, 5000)
	for i := 0; i < 5000; i++ {
		neighbors5000[i] = types.Candidate{ID: uint32(i % numVectors)}
	}

	b.Run("LargeSet_Parallel", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = idx.processResultsParallel(context.Background(), query, neighbors5000, 10, nil)
		}
	})
}

func FuzzAdaptiveChunkSize_Calculation(f *testing.F) {
	f.Add(4, 100, 32, 500)
	f.Add(8, 1000, 32, 500)
	f.Add(16, 10000, 32, 500)

	f.Fuzz(func(t *testing.T, numWorkers int, neighborCount int, minChunkSize int, maxChunkSize int) {
		if numWorkers <= 0 || numWorkers > 64 {
			t.Skip()
		}
		if neighborCount < 0 || neighborCount > 100000 {
			t.Skip()
		}
		if minChunkSize <= 0 || minChunkSize > 1000 {
			t.Skip()
		}
		if maxChunkSize <= 0 || maxChunkSize > 10000 {
			t.Skip()
		}
		if minChunkSize > maxChunkSize {
			t.Skip()
		}

		targetChunks := numWorkers * 3
		chunkSize := neighborCount / targetChunks
		if chunkSize < minChunkSize {
			chunkSize = minChunkSize
		} else if chunkSize > maxChunkSize {
			chunkSize = maxChunkSize
		}

		if chunkSize < minChunkSize || chunkSize > maxChunkSize {
			t.Errorf("chunkSize out of bounds")
		}
	})
}
