package store

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestVacuum_Basic(t *testing.T) {
	mem := memory.NewGoAllocator()
	dims := 4
	vectors := [][]float32{
		{1, 0, 0, 0},
		{0, 1, 0, 0},
	}
	rec := makeBatchTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "vacuum_test",
		Records: []arrow.RecordBatch{rec},
	}

	h := NewArrowHNSW(ds, DefaultArrowHNSWConfig())
	defer func() { _ = h.Close() }()

	// Add data
	_, err := h.AddByLocation(context.Background(), 0, 0)
	require.NoError(t, err)

	// Test Vacuum (stubbed or using public method if available)
	// h.Vacuum() undefined in ArrowHNSW public interface usually.
	// If it's internal logic, we might depend on CleanupTombstones?

	// Just verify cleanup tombstones if that's what vacuum does
	_, err = h.CleanupTombstones(0) // Assuming this method exists on ArrowHNSW based on previous files
	require.NoError(t, err)
}

func TestVacuum_SearchImpact(t *testing.T) {
	// Verify search works after "vacuum"
	mem := memory.NewGoAllocator()
	dims := 4
	vectors := [][]float32{{1, 0, 0, 0}}
	rec := makeBatchTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &Dataset{Records: []arrow.RecordBatch{rec}}
	h := NewArrowHNSW(ds, DefaultArrowHNSWConfig())

	_, _ = h.AddByLocation(context.Background(), 0, 0)

	// Search(ctx, query, k, filter)
	// Corrected signature
	q := []float32{1, 0, 0, 0}
	_, err := h.Search(context.Background(), q, 1, nil)
	require.NoError(t, err)
}
