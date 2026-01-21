package store

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func FuzzGoroutineLeak_RapidCycles(f *testing.F) {
	f.Add(uint8(1), uint8(10))
	f.Add(uint8(5), uint8(50))
	f.Add(uint8(10), uint8(25))

	f.Fuzz(func(t *testing.T, iterations, operations uint8) {
		if iterations == 0 || operations == 0 {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		initialGoroutines := runtime.NumGoroutine()

		for i := uint8(0); i < iterations; i++ {
			// Create and immediately close store multiple times
			// NewVectorStore: mem, logger, maxMemoryBytes, _, _
			store := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<30, 0, 0)

			// Start workers to create goroutines
			store.StartLifecycleManager(ctx)
			store.StartEvictionTicker(10 * time.Millisecond)

			// Do some operations
			for j := uint8(0); j < operations; j++ {
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
				}, nil)

				store.PrewarmDataset("fuzz_test", schema)
				time.Sleep(time.Duration(j) * time.Millisecond)
			}

			// Close store
			err := store.Close()
			assert.NoError(t, err)

			// Give time for cleanup
			time.Sleep(time.Duration(i) * time.Millisecond)
		}

		// Allow final cleanup
		time.Sleep(100 * time.Millisecond)

		finalGoroutines := runtime.NumGoroutine()
		goroutineDiff := int(finalGoroutines - initialGoroutines)

		// Allow some margin for runtime goroutines (up to +10)
		if goroutineDiff > 10 {
			t.Errorf("Too many goroutines leaked: %d (expected <10)", goroutineDiff)
		}
	})
}
