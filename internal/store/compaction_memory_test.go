package store

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCompactDataset_InsufficientMemory verifies that compaction aborts if there isn't enough memory headroom.
func TestCompactDataset_InsufficientMemory(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()

	// MaxMemory = 200MB
	maxMem := int64(200 * 1024 * 1024)
	s := NewVectorStore(mem, logger, maxMem, 1024, time.Hour)
	defer func() { _ = s.Close() }()

	// Create dummy dataset
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	ds := NewDataset("heavy_dataset", schema)

	// Artificially set Dataset Size to 150MB
	ds.SizeBytes.Store(150 * 1024 * 1024)

	// Add two fake records so it has something to compact (checks len >= 2)
	// We don't need real data matching size for this check, just non-empty records list
	// to pass "len < 2" check.
	// However, compactRecords might fail if records are empty/incompatible, but the headroom check
	// happens BEFORE compactRecords.
	// So we just need 2 dummy records.
	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.Int64Builder).Append(1)
	rec1 := b.NewRecordBatch()
	b.Field(0).(*array.Int64Builder).Append(2)
	rec2 := b.NewRecordBatch()

	ds.dataMu.Lock()
	ds.Records = []arrow.RecordBatch{rec1, rec2}
	ds.dataMu.Unlock()

	s.updateDatasets(func(m map[string]*Dataset) {
		m["heavy_dataset"] = ds
	})

	// Simulate current global memory usage = 100MB
	// Available = 200 - 100 = 100MB
	// Required for safe compaction of 150MB dataset is > 150 * 1.1 = 165MB
	// 100MB < 165MB -> Insufficient
	s.currentMemory.Store(100 * 1024 * 1024)

	// Enable compaction config target
	s.compactionConfig.TargetBatchSize = 100

	err := s.CompactDataset("heavy_dataset")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient memory headroom")

	// Cleanup
	rec1.Release()
	rec2.Release()
	b.Release()
}
