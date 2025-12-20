package store

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Helper to create a test RecordBatch with n rows
func makeTestBatch(t *testing.T, n int64) arrow.RecordBatch {
	t.Helper()
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int64Builder)
	valBuilder := builder.Field(1).(*array.Float64Builder)

	for i := int64(0); i < n; i++ {
		idBuilder.Append(i)
		valBuilder.Append(float64(i) * 1.5)
	}

	return builder.NewRecordBatch()
}

// releaseBatches releases all batches in slice
func releaseBatches(batches []arrow.RecordBatch) {
	for _, b := range batches {
		b.Release()
	}
}

// TestCompactRecordsEmpty verifies empty input returns empty output
func TestCompactRecordsEmpty(t *testing.T) {
	result := compactRecords(nil, 1000)
	if len(result) != 0 {
		t.Errorf("expected empty result, got %d batches", len(result))
	}

	result = compactRecords([]arrow.RecordBatch{}, 1000)
	if len(result) != 0 {
		t.Errorf("expected empty result, got %d batches", len(result))
	}
}

// TestCompactRecordsMergesSmallBatches verifies small batches are merged
func TestCompactRecordsMergesSmallBatches(t *testing.T) {
	// Create 10 batches of 100 rows each = 1000 total rows
	batches := make([]arrow.RecordBatch, 10)
	for i := 0; i < 10; i++ {
		batches[i] = makeTestBatch(t, 100)
	}
	defer releaseBatches(batches)

	// Target 500 rows per batch - should result in 2 batches
	result := compactRecords(batches, 500)
	defer releaseBatches(result)

	if len(result) != 2 {
		t.Errorf("expected 2 compacted batches, got %d", len(result))
	}

	// Verify total row count preserved
	var totalRows int64
	for _, r := range result {
		totalRows += r.NumRows()
	}
	if totalRows != 1000 {
		t.Errorf("expected 1000 total rows, got %d", totalRows)
	}
}

// TestCompactRecordsLargeBatchUnchanged verifies large batches are not split
func TestCompactRecordsLargeBatchUnchanged(t *testing.T) {
	// Create one batch with 1000 rows
	batch := makeTestBatch(t, 1000)
	defer batch.Release()

	// Target 500 rows - but we don't split, just don't merge
	result := compactRecords([]arrow.RecordBatch{batch}, 500)
	defer releaseBatches(result)

	if len(result) != 1 {
		t.Errorf("expected 1 batch (unchanged), got %d", len(result))
	}
	if result[0].NumRows() != 1000 {
		t.Errorf("expected 1000 rows, got %d", result[0].NumRows())
	}
}

// TestCompactRecordsPreservesSchema verifies schema is preserved
func TestCompactRecordsPreservesSchema(t *testing.T) {
	batches := make([]arrow.RecordBatch, 3)
	for i := 0; i < 3; i++ {
		batches[i] = makeTestBatch(t, 50)
	}
	defer releaseBatches(batches)

	originalSchema := batches[0].Schema()
	result := compactRecords(batches, 200)
	defer releaseBatches(result)

	if len(result) != 1 {
		t.Errorf("expected 1 batch, got %d", len(result))
	}
	if !result[0].Schema().Equal(originalSchema) {
		t.Error("schema not preserved after compaction")
	}
}

// TestCompactRecordsPreservesData verifies data integrity
func TestCompactRecordsPreservesData(t *testing.T) {
	// Create 2 batches with known values
	batches := make([]arrow.RecordBatch, 2)
	batches[0] = makeTestBatch(t, 5) // ids: 0,1,2,3,4
	batches[1] = makeTestBatch(t, 5) // ids: 0,1,2,3,4
	defer releaseBatches(batches)

	result := compactRecords(batches, 100)
	defer releaseBatches(result)

	if len(result) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(result))
	}
	if result[0].NumRows() != 10 {
		t.Fatalf("expected 10 rows, got %d", result[0].NumRows())
	}

	// Verify ID column contains expected values
	idCol := result[0].Column(0).(*array.Int64)
	expected := []int64{0, 1, 2, 3, 4, 0, 1, 2, 3, 4}
	for i, exp := range expected {
		if idCol.Value(i) != exp {
			t.Errorf("row %d: expected id=%d, got %d", i, exp, idCol.Value(i))
		}
	}
}
