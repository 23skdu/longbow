package index

// Roadmap Item 33: a pooled search context must not pin the graph.
//
// The neighbour appliers (neighbor_appliers.go) live on the pooled context so
// the bulk linker stops heap-allocating one closure per call. They point at the
// GraphData and the index they were last used against, and a pooled context is
// reachable for the life of the process - so without clearing them on the way
// back to the pool, the last snapshot a search wrote to stays reachable
// forever. GraphData releases its Arrow references from a finalizer, so a
// pinned snapshot never reaches one and its vector buffers stay resident. It
// showed up as an Arrow allocator imbalance in TestHNSW_BulkInsert (801,024
// bytes outstanding at exit) and nowhere else, which is the argument for
// testing the invariant rather than the symptom.

import (
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/stretchr/testify/require"
)

func TestSearchContextPoolDoesNotPinGraphData(t *testing.T) {
	pool := NewArrowSearchContextPool()
	ctx := pool.Get()

	data := &types.GraphData{Name: "pinned-by-a-pooled-context"}
	ctx.batchApplier = batchNeighborApplier{h: NewArrowHNSW(nil, &types.ArrowHNSWConfig{}, nil), data: data, target: 7, maxConn: 16}
	ctx.addConnApplier = addConnNeighborApplier{data: data, source: 1, target: 2}
	ctx.pruneApplier = pruneNeighborApplier{data: data, id: 3}
	ctx.inboundApplier = inboundNeighborApplier{data: data, host: 4, target: 5}

	pool.Put(ctx)

	// The context is back in the pool and reachable from it for the rest of the
	// process, so nothing it points at may still be pinned.
	require.Nil(t, ctx.batchApplier.h, "batch applier must not keep the index alive in the pool")
	require.Nil(t, ctx.batchApplier.data, "batch applier must not keep the graph alive in the pool")
	require.Nil(t, ctx.addConnApplier.data, "add applier must not keep the graph alive in the pool")
	require.Nil(t, ctx.pruneApplier.data, "prune applier must not keep the graph alive in the pool")
	require.Nil(t, ctx.inboundApplier.data, "inbound applier must not keep the graph alive in the pool")

	// And a context handed out again starts clean even if a caller bypassed Put.
	again := pool.Get()
	require.Nil(t, again.batchApplier.data, "Reset must clear the appliers")
	pool.Put(again)
}
