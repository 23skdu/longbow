package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompaction_RateLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping rate limit integration test in short mode")
	}

	// Setup: 10KB/s limit
	limitBytes := int64(10 * 1024)
	compactionCfg := DefaultCompactionConfig()
	compactionCfg.Enabled = true
	compactionCfg.RateLimitBytesPerSec = limitBytes
	compactionCfg.TargetBatchSize = 1000 // Small target to force merge

	mem := memory.NewGoAllocator()
	store := NewVectorStoreWithCompaction(mem, zerolog.Nop(), 1<<30, 1<<20, time.Hour, compactionCfg)
	defer func() { _ = store.Close() }()

	// Create dataset
	dsName := "test_rate_limit"

	// Create some data
	// Limit = 100KB/s.
	// Rate limiter burst = 100KB (1s worth).
	limitBytes = 100 * 1024 // 100KB/s
	store.rateLimiter.SetLimit(limitBytes)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "embedding", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
	}, nil)

	// Create 30 batches.
	// 30 batches * 100 rows = 3000 rows.
	// TargetBatchSize = 1000.
	// We expect 3 compaction candidates (3 merges).
	// Total data: 3000 * 100 = 300,000 bytes.
	// Limit: 100,000 bytes/sec.
	// Burst: 100,000 bytes.
	// 1st candidate (100KB): Consumes burst. Instant.
	// 2nd candidate (100KB): Waits 1s.
	// 3rd candidate (100KB): Waits 1s.
	// Total expected wait: ~2s.
	for i := 0; i < 30; i++ {
		b := array.NewRecordBuilder(mem, schema)
		// Check manual release at end of loop

		for j := 0; j < 100; j++ {
			b.Field(0).(*array.Uint32Builder).Append(uint32(i*100 + j))
			listB := b.Field(1).(*array.FixedSizeListBuilder)
			listB.Append(true)
			valB := listB.ValueBuilder().(*array.Float32Builder)
			for k := 0; k < 128; k++ {
				valB.Append(float32(k))
			}
		}
		rec := b.NewRecordBatch()
		err := store.ApplyDelta(dsName, rec, uint64(i), time.Now().UnixNano())
		require.NoError(t, err)
		rec.Release()
		b.Release()
	}

	// Trigger compaction
	start := time.Now()
	err := store.CompactDataset(dsName)
	require.NoError(t, err)
	duration := time.Since(start)

	fmt.Printf("Compaction took %v\n", duration)
	assert.Greater(t, duration.Seconds(), 1.5, "Compaction should be throttled (took too fast: %v)", duration)
}
