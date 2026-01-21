package store

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestVectorStore_GoroutineLeak(t *testing.T) {
	// Verify if goleak is working and if we have leaks
	defer goleak.VerifyNone(t)

	logger := zerolog.Nop()
	mem := memory.DefaultAllocator

	for i := 0; i < 3; i++ {
		s := NewVectorStore(mem, logger, 1024*1024, 0, 0)

		// Start some workers that might not be started by default
		ctx, cancel := context.WithCancel(context.Background())
		s.StartLifecycleManager(ctx)
		s.StartEvictionTicker(10 * time.Millisecond)

		// Create a dataset to test DropDataset leak
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
		}, nil)
		s.PrewarmDataset("test_ds", schema)

		// Start repair worker for HNSW
		if ds, ok := s.getDataset("test_ds"); ok {
			if hnsw, ok := ds.Index.(*ArrowHNSW); ok {
				hnsw.StartRepairWorker(ctx, 10*time.Millisecond, 100)
			}
		}

		// Drop dataset - this spawns an untracked goroutine in current implementation
		err := s.DropDataset(context.Background(), "test_ds")
		assert.NoError(t, err)

		// Give them a moment to start
		time.Sleep(50 * time.Millisecond)

		// Shutdown
		cancel()
		err = s.Close()
		assert.NoError(t, err)
	}
}
