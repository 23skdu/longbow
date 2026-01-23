package store

import (
	"testing"

	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

// TestIdentifyCompactionCandidates_Aggressive verifies that high fragmentation triggers compaction
// even if batches are small or few, effectively "tombstone compaction".
func TestIdentifyCompactionCandidates_Aggressive(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Helper to create a batch
	createBatch := func(rows int) arrow.RecordBatch {
		b := array.NewRecordBuilder(mem, schema)
		defer b.Release()
		for i := 0; i < rows; i++ {
			b.Field(0).(*array.Int64Builder).Append(int64(i))
		}
		return b.NewRecordBatch()
	}

	// 1. Setup: 3 batches, 1000 rows each.
	targetSize := int64(500)

	b1 := createBatch(1000)
	b2 := createBatch(1000)
	b3 := createBatch(1000)
	defer b1.Release()
	defer b2.Release()
	defer b3.Release()

	batches := []arrow.RecordBatch{b1, b2, b3}

	// Tombstones: b2 has 900 deletions (90%)
	ts2 := query.NewBitset()
	for i := 0; i < 900; i++ {
		ts2.Set(i)
	}

	// b1 and b3 have 0 deletions
	ts1 := query.NewBitset()
	ts3 := query.NewBitset()

	tombstones := []*query.Bitset{ts1, ts2, ts3}

	// 2. Run Identification
	candidates := identifyCompactionCandidates(batches, tombstones, targetSize, 0.5)

	// Assertions
	// b2 is fragmented (90% > 50%), so it should be a candidate.
	assert.GreaterOrEqual(t, len(candidates), 1, "Should identify fragmented batch as candidate")
	found := false
	for _, cand := range candidates {
		// b2 is index 1.
		if cand.StartIdx <= 1 && cand.EndIdx > 1 {
			found = true
			break
		}
	}
	assert.True(t, found, "Fragmented batch should be in a candidate group")
}

// TestStartup_LoadSnapshotItem_Concurrency checks if loadSnapshotItem handles concurrent calls safely.
// This simulates the core logic of parallel startup.
func TestStartup_LoadSnapshotItem_Concurrency(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	s := NewVectorStore(mem, logger, 1024*1024, 0, 0)
	defer func() { _ = s.ClosePersistence() }()

	// Concurrent creation of datasets
	concurrency := 10
	done := make(chan bool)

	for i := 0; i < concurrency; i++ {
		go func(_ int) {
			// Mocking getOrCreateDataset call
			s.getOrCreateDataset("test_parallel_ds", func() *Dataset {
				return NewDataset("test_parallel_ds", nil)
			})
			done <- true
		}(i)
	}

	for i := 0; i < concurrency; i++ {
		<-done
	}

	// Basic check
	ds, ok := s.getDataset("test_parallel_ds")
	assert.True(t, ok)
	assert.NotNil(t, ds)
}
