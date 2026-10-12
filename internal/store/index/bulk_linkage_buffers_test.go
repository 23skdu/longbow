package index

import (
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	arrowarray "github.com/apache/arrow-go/v18/arrow/array"
	arrowmemory "github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

// TestAddConnectionsBatchReusesPooledBuffers is the allocation guard for the
// bulk-linkage hot path.
//
// AddConnectionsBatch is called once per inserted node and once more per
// selected neighbour (roughly 33 times per node at M0=32). The old shape
// allocated a fresh `newSources` (grown from nil), a fresh `next`, and two
// slices.Clone for the in-degree diff on every one of those calls; profiling a
// 1M-node build attributed 39.4 GB to this function, 32.3 GB to
// computePrunedNeighbors and 28.9 GB to the retry loop in UpdateNeighbors.
//
// All of it now comes from buffers on the caller's pooled ArrowSearchContext,
// so repeated calls must not allocate at all.
func TestAddConnectionsBatchReusesPooledBuffers(t *testing.T) {
	h := newLinkageTestIndex(t)
	data := h.data.Load()
	ctx := h.searchPool.Get()
	defer h.searchPool.PutWithMetrics(ctx, "float32", "8")

	const maxConn = 32

	// Pre-built argument slices: the call does not retain them, so any
	// allocation inside the measured region belongs to AddConnectionsBatch
	// and not to the test.
	src := make([]uint32, 1)
	dst := make([]float32, 1)

	// Warm the context so one-time growth is not counted.
	for i := range 64 {
		src[0] = uint32(i)
		h.AddConnectionsBatch(ctx, data, 0, src, dst, 0, maxConn)
	}

	const callsPerRun = 8
	allocs := testing.AllocsPerRun(200, func() {
		for i := range callsPerRun {
			src[0] = uint32(100 + i)
			dst[0] = float32(i)
			h.AddConnectionsBatch(ctx, data, uint32(1+i%4), src, dst, 0, maxConn)
		}
	}) / callsPerRun

	// One allocation is the floor: UpdateNeighbors takes a func through an
	// interface, so the closure environment must be heap allocated. The old
	// shape sat at four - the closure plus a nil-grown newSources, a freshly
	// made next, and a slices.Clone for the in-degree diff - and grew
	// logarithmically with the number of sources.
	require.LessOrEqualf(t, allocs, 2.0,
		"AddConnectionsBatch must not allocate per call beyond the closure (saw %.2f allocs per run)", allocs)
}

// TestAddConnectionsBatchPreservesSemanticsWithPooledBuffers is the correctness
// guard for the pooling: reusing a buffer is only safe if the committed
// neighbour list is a copy, not a view of the scratch.
//
// Run with -race.
func TestAddConnectionsBatchPreservesSemanticsWithPooledBuffers(t *testing.T) {
	h := newLinkageTestIndex(t)
	data := h.data.Load()
	ctx := h.searchPool.Get()
	defer h.searchPool.PutWithMetrics(ctx, "float32", "8")

	const maxConn = 32

	// Fill node 0 past its own link count and interleave writes to its
	// neighbours, so a stale view of any scratch buffer would show up as a
	// wrong neighbour list rather than as a crash.
	for i := range maxConn {
		h.AddConnectionsBatch(ctx, data, 0, []uint32{uint32(100 + i)}, []float32{float32(i)}, 0, maxConn)
		h.AddConnectionsBatch(ctx, data, uint32(100+i), []uint32{0}, []float32{float32(i)}, 0, maxConn)
	}

	nb, ok := data.PackedNeighbors[0].GetNeighbors(0)
	require.True(t, ok)
	require.Len(t, nb, maxConn)
	for i := range maxConn {
		require.Equalf(t, uint32(100+i), nb[i], "neighbour %d", i)
	}

	// Captured now, then used after many more writes: the committed list must
	// not have been overwritten by subsequent reuse of the scratch buffers.
	snapshot := append([]uint32(nil), nb...)

	for i := range 200 {
		h.AddConnectionsBatch(ctx, data, 5, []uint32{uint32(300 + i)}, []float32{1}, 0, maxConn)
	}

	require.Equal(t, snapshot, nb,
		"a previously returned neighbour view must not be clobbered by scratch reuse")
}

// TestAddConnectionPruneKeepsInDegreeAccurate covers the layer-0 in-degree
// bookkeeping, which now reads the pre/post snapshots out of pooled buffers
// instead of two slices.Clone per call.
//
// If the snapshots aliased the scratch buffer that was just handed to
// SetNeighbors, the diff would compare the new list against itself and every
// link would look like it was added and never removed.
func TestAddConnectionPruneKeepsInDegreeAccurate(t *testing.T) {
	h := newLinkageTestIndex(t)
	data := h.data.Load()
	ctx := h.searchPool.Get()
	defer h.searchPool.PutWithMetrics(ctx, "float32", "8")

	const maxConn = 8
	for i := range maxConn {
		h.AddConnectionsBatch(ctx, data, 0, []uint32{100 + uint32(i)}, []float32{float32(i)}, 0, maxConn)
	}
	for i := range maxConn {
		require.Equalf(t, int32(1), h.inDegreeL0.Get(uint32(100+i)),
			"node %d must show exactly one inbound edge", 100+i)
	}

	// Node 100 is now full; one more link forces a prune through the same
	// pooled path.
	h.AddConnectionsBatch(ctx, data, 100, []uint32{101}, []float32{0}, 0, maxConn)

	nb, ok := data.PackedNeighbors[0].GetNeighbors(100)
	require.True(t, ok)
	require.LessOrEqual(t, len(nb), maxConn, "pruning must respect the connection limit")

	// The tracker must agree with the committed graph exactly: count the
	// inbound edges by walking every node's list and compare.
	actual := map[uint32]int32{}
	for node := range 512 {
		list, ok := data.PackedNeighbors[0].GetNeighbors(uint32(node))
		if !ok {
			continue
		}
		for _, n := range list {
			actual[n]++
		}
	}
	for node := range 512 {
		require.Equalf(t, actual[uint32(node)], h.inDegreeL0.Get(uint32(node)),
			"layer-0 in-degree of node %d disagrees with the committed graph", node)
	}

	// And no node the prune kept may have been stranded at zero inbound
	// edges: that is the invariant the guard exists to protect.
	for _, n := range nb {
		require.Positivef(t, h.inDegreeL0.Get(n),
			"kept neighbour %d was left unreachable", n)
	}
}

// TestBulkCandidateBuffersArePooled pins the bulk linkage scratch buffers that
// replaced the per-node make()s in addBatchBulkInternal.
func TestBulkCandidateBuffersArePooled(t *testing.T) {
	ctx := NewArrowSearchContext()

	require.NotNil(t, ctx.bulkCandidates)
	require.NotNil(t, ctx.bulkSources)
	require.NotNil(t, ctx.bulkDists)
	require.NotNil(t, ctx.linkSrc)
	require.NotNil(t, ctx.linkDist)
	require.NotNil(t, ctx.batchSrc)
	require.NotNil(t, ctx.batchNext)
	require.NotNil(t, ctx.diffOld)
	require.NotNil(t, ctx.diffNew)

	// Reset must truncate without dropping the backing arrays, so the pool
	// keeps its high-water mark across uses.
	candAddr := &ctx.bulkCandidates[:1][0]
	srcAddr := &ctx.bulkSources[:1][0]

	ctx.bulkCandidates = append(ctx.bulkCandidates, types.Candidate{ID: 1, Dist: 1})
	ctx.bulkSources = append(ctx.bulkSources, 7)
	ctx.Reset()

	require.Empty(t, ctx.bulkCandidates)
	require.Empty(t, ctx.bulkSources)
	require.Equal(t, candAddr, &ctx.bulkCandidates[:1][0], "Reset must not reallocate bulkCandidates")
	require.Equal(t, srcAddr, &ctx.bulkSources[:1][0], "Reset must not reallocate bulkSources")
}

// TestResolveArrowBatchLayoutLocatesOwningBatch covers the bulk preparation
// fast path. The row -> batch mapping used to be a linear scan with nil
// skipping, re-run for every row of every batch.
func TestResolveArrowBatchLayoutLocatesOwningBatch(t *testing.T) {
	recA := makeArrowRecord(t, 3)
	recB := makeArrowRecord(t, 5)
	recC := makeArrowRecord(t, 2)

	start, col := resolveArrowBatchLayout([]arrow.RecordBatch{recA, recB, recC})
	require.Equal(t, []int{0, 3, 8, 10}, start)
	require.Len(t, col, 3)

	// Prefix sums must locate the owning batch for every row, including the
	// boundaries.
	for row := range 10 {
		idx := sortSearch(start, row)
		require.GreaterOrEqual(t, idx, 0)
		require.Less(t, idx, 3)
		require.LessOrEqual(t, start[idx], row)
		require.Greater(t, start[idx+1], row)
	}

	// nil batches contribute no rows and must be skipped, exactly as the
	// linear scan did.
	_, _ = resolveArrowBatchLayout(nil)
	nilStart, _ := resolveArrowBatchLayout([]arrow.RecordBatch{nil, recA})
	require.Equal(t, []int{0, 0, 3}, nilStart)
}

func sortSearch(start []int, row int) int {
	idx := 0
	for idx < len(start) && start[idx] <= row {
		idx++
	}
	return idx - 1
}

// newLinkageTestIndex builds a small ArrowHNSW with room to add neighbours.
func newLinkageTestIndex(t *testing.T) *ArrowHNSW {
	t.Helper()

	cfg := DefaultArrowHNSWConfig()
	cfg.DataType = types.VectorTypeFloat32
	cfg.M = 32
	cfg.MMax0 = 32
	cfg.EfConstruction = 64

	h := NewArrowHNSW(&MockDataset{Name: "linkage"}, &cfg, nil)
	data := h.data.Load()
	for i := range 512 {
		vec := make([]float32, 8)
		for d := range 8 {
			vec[d] = float32(i + d)
		}
		require.NoError(t, data.SetVector(uint32(i), vec))
	}
	return h
}

// makeArrowRecord builds a record of rows float32 vectors of dimension 8.
func makeArrowRecord(t *testing.T, rows int) arrow.RecordBatch {
	t.Helper()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(8, arrow.PrimitiveTypes.Float32)},
	}, nil)

	bld := arrowarray.NewRecordBuilder(arrowmemory.NewGoAllocator(), schema)
	defer bld.Release()

	listBld := bld.Field(0).(*arrowarray.FixedSizeListBuilder)
	valBld := listBld.ValueBuilder().(*arrowarray.Float32Builder)
	for range rows {
		listBld.Append(true)
		for range 8 {
			valBld.Append(0)
		}
	}

	return bld.NewRecordBatch()
}
