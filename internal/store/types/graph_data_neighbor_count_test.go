package types

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGetNeighbors_RejectsOutOfRangeCount pins the read-side invariant on the
// per-node neighbor count.
//
// SetNeighbors clamps every write to [0, MaxNeighbors], so a count outside
// that range can only come from memory that is not a live count: a torn
// seqlock read, or a counts chunk swapped underneath the reader by a
// concurrent EnsureChunk (the counts, neighbors and versions offsets are each
// loaded independently, so the three slices are not guaranteed to agree).
//
// Previously such a value flowed straight into the slice bound, and a
// negative one panicked with "slice bounds out of range [:-1084976126]",
// crashing whichever search or insert happened to be running.
func TestGetNeighbors_RejectsOutOfRangeCount(t *testing.T) {
	const dims = 8

	newGraph := func(t *testing.T) *GraphData {
		t.Helper()
		g := NewGraphData(10, dims, false, false, -1, false, false, false, VectorTypeFloat32, false, false, false, 8, "test-neighbor-count", nil, false)
		require.NoError(t, g.EnsureChunk(0, 0, dims))
		return g
	}

	// Counts that must all be rejected rather than trusted as a length.
	// The negatives are the values observed in the wild panic.
	badCounts := []int32{
		-1,
		-2,
		-1084976126,
		MaxNeighbors + 1,
		1 << 20,
	}

	for _, bad := range badCounts {
		g := newGraph(t)

		// Write the out-of-range count directly, simulating the corrupted
		// or torn slot that the read path must survive.
		counts := g.GetCountsChunk(0, 0)
		require.NotNil(t, counts)
		atomic.StoreInt32(&counts[0], bad)

		require.NotPanics(t, func() {
			require.Empty(t, g.GetNeighbors(0, 0, nil), "count %d must not yield neighbors", bad)
			require.Empty(t, g.GetNeighborsWithGen(0, 0, nil, ^uint64(0)), "count %d must not yield neighbors", bad)
			require.Empty(t, g.GetNeighborsLockFree(0, 0), "count %d must not yield neighbors", bad)
		}, "count %d must not panic", bad)
	}

	// The guard must not reject the boundary values SetNeighbors can write.
	for _, ok := range []int32{1, MaxNeighbors} {
		g := newGraph(t)
		counts := g.GetCountsChunk(0, 0)
		require.NotNil(t, counts)
		atomic.StoreInt32(&counts[0], ok)

		got := g.GetNeighbors(0, 0, nil)
		require.Len(t, got, int(ok), "count %d should be honoured", ok)
	}
}
