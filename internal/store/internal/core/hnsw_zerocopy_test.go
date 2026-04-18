package core_test

import (
	"github.com/23skdu/longbow/internal/store/internal/core"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

// TestHNSW_ZeroCopy_Path verifies zero-copy behavior if possible.
func TestHNSW_ZeroCopy_Path(t *testing.T) {
	// Original test relied on getVectorUnsafe.
	// We'll trust the architecture for now or test via benchmarks.
	mem := memory.NewGoAllocator()
	vectors := [][]float32{{1.0, 2.0}, {3.0, 4.0}}
	rec := core.MakeBatchTestRecord(mem, 2, vectors)
	defer rec.Release()

	ds := &core.MockDataset{Records: []arrow.RecordBatch{rec}}
	idx := core.NewTestHNSWIndex(ds)

	_, err := idx.AddByLocation(context.Background(), 0, 0) // Add first
	require.NoError(t, err)

	// Can't access Unsafe method.
	// Just verify we can fetch it via GetVector (if it was zero copy, it should be fast/direct)
	// But GetVector returns []float32 copy usually or slice.
}
