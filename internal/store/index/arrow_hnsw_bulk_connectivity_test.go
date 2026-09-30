package index_test

import (
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/store/index"
	"github.com/23skdu/longbow/internal/store/types"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collinearBulkRecord builds n vectors that all lie on one straight line, so
// that dist(i, j) depends only on |i - j|. This is the worst case for a graph
// index: every candidate is closer to an already-linked neighbour than to the
// query, so neighbour selection has nothing diverse to pick and the whole batch
// concentrates its links on the same handful of targets.
func collinearBulkRecord(t *testing.T, dt types.VectorDataType, dims, n int) arrow.RecordBatch {
	t.Helper()

	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, arrow.NewSchema(
		[]arrow.Field{{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), getArrowType(dt))}}, nil,
	))
	defer builder.Release()
	listB := builder.Field(0).(*array.FixedSizeListBuilder)

	for i := 0; i < n; i++ {
		listB.Append(true)
		switch vb := listB.ValueBuilder().(type) {
		case *array.Int64Builder:
			for j := 0; j < dims; j++ {
				vb.Append(int64(i + j))
			}
		case *array.Int32Builder:
			for j := 0; j < dims; j++ {
				vb.Append(int32(i + j))
			}
		case *array.Uint32Builder:
			for j := 0; j < dims; j++ {
				vb.Append(uint32(i + j))
			}
		case *array.Float64Builder:
			for j := 0; j < dims; j++ {
				vb.Append(float64(i) + float64(j)*0.1)
			}
		default:
			t.Fatalf("unsupported builder %T", vb)
		}
	}
	return builder.NewRecordBatch()
}

func newCollinearConfig(dt types.VectorDataType, dims int) types.ArrowHNSWConfig {
	config := types.DefaultArrowHNSWConfig()
	config.M = 32
	config.MMax = 32
	config.MMax0 = 64
	config.EfConstruction = 128
	config.DataType = dt
	config.Dims = dims
	return config
}

// reachableFromEntryPoint returns the set of layer-0 node ids reachable from
// the index entry point by following stored neighbour lists.
func reachableFromEntryPoint(t *testing.T, idx *index.ArrowHNSW, n int) map[uint32]bool {
	t.Helper()

	seen := make(map[uint32]bool, n)
	queue := []uint32{idx.GetEntryPoint()}
	seen[idx.GetEntryPoint()] = true
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		neighbors, err := idx.GetLayerNeighbors(cur, 0)
		require.NoError(t, err)
		for _, nb := range neighbors {
			if !seen[nb] {
				seen[nb] = true
				queue = append(queue, nb)
			}
		}
	}
	return seen
}

func unreachableIDs(reach map[uint32]bool, n int) []int {
	var out []int
	for i := 0; i < n; i++ {
		if !reach[uint32(i)] {
			out = append(out, i)
		}
	}
	return out
}

// TestBulkInsert_CollinearGraphStaysConnected pins the invariant that the bulk
// insert path must not strand nodes: every inserted node has to stay reachable
// from the entry point.
//
// Both insert paths spend one degree budget on both directions of a link - the
// fresh node's forward links and the reverse link it hands to each of its
// neighbours. If the forward links are allowed to consume the whole MMax0 = 2M
// budget, the targets are already full when the reverse links arrive, pruning
// drops them, and the new node ends up with no inbound edge at all. Such a node
// is invisible to the traversal no matter how large ef is, which used to make
// TestAddBatch_Bulk_Typed fail intermittently: on collinear data roughly 200 of
// 1100 nodes lost their only path to the entry point on every single run.
func TestBulkInsert_CollinearGraphStaysConnected(t *testing.T) {
	const n = 512

	for _, tc := range []struct {
		desc string
		dt   types.VectorDataType
		dims int
	}{
		{"Int64", types.VectorTypeInt64, 16},
		{"Int32", types.VectorTypeInt32, 16},
		{"Float64", types.VectorTypeFloat64, 8},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			config := newCollinearConfig(tc.dt, tc.dims)
			idx := index.NewArrowHNSW(nil, &config, nil)
			defer func() { _ = idx.Close() }()

			rec := collinearBulkRecord(t, tc.dt, tc.dims, n)
			defer rec.Release()

			rowIdxs := make([]int, n)
			for i := range rowIdxs {
				rowIdxs[i] = i
			}
			ids, err := idx.AddBatch(context.Background(), []arrow.RecordBatch{rec}, rowIdxs, make([]int, n))
			require.NoError(t, err)
			require.Len(t, ids, n)
			require.Equal(t, n, idx.Len())

			reach := reachableFromEntryPoint(t, idx, n)
			assert.Empty(t, unreachableIDs(reach, n),
				"%d/%d nodes unreachable from entry point %d", n-len(reach), n, idx.GetEntryPoint())

			// An exhaustive search can only return nodes the traversal reaches.
			vec, err := idx.GetVector(n / 2)
			require.NoError(t, err)
			opts := types.DefaultSearchOptions()
			opts.Ef = n
			res, err := idx.SearchVectors(context.Background(), vec, n, nil, opts)
			require.NoError(t, err)
			assert.Len(t, res, n, "exhaustive search (ef = k = n) must return every node")

			found := false
			for _, c := range res {
				if uint32(c.ID) == n/2 {
					found = true
					break
				}
			}
			assert.True(t, found, "exact match %d missing from exhaustive search", n/2)
		})
	}
}

// TestSequentialInsert_CollinearGraphStaysConnected covers the same invariant
// for the one-at-a-time insert path, which shares the degree budget with the
// bulk path.
func TestSequentialInsert_CollinearGraphStaysConnected(t *testing.T) {
	const n = 512

	for _, tc := range []struct {
		desc string
		dt   types.VectorDataType
		dims int
	}{
		{"Int64", types.VectorTypeInt64, 16},
		{"Float64", types.VectorTypeFloat64, 8},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			config := newCollinearConfig(tc.dt, tc.dims)
			idx := index.NewArrowHNSW(nil, &config, nil)
			defer func() { _ = idx.Close() }()

			rec := collinearBulkRecord(t, tc.dt, tc.dims, n)
			defer rec.Release()
			fsl := rec.Column(0).(*array.FixedSizeList)

			for i := 0; i < n; i++ {
				vec := extractTestVector(t, fsl, i, tc.dt)
				require.NoError(t, idx.InsertWithVector(uint32(i), vec, -1))
			}
			require.Equal(t, n, idx.Len())

			reach := reachableFromEntryPoint(t, idx, n)
			assert.Empty(t, unreachableIDs(reach, n),
				"%d/%d nodes unreachable from entry point %d", n-len(reach), n, idx.GetEntryPoint())
		})
	}
}

func extractTestVector(t *testing.T, fsl *array.FixedSizeList, row int, dt types.VectorDataType) any {
	t.Helper()

	size := int(fsl.DataType().(*arrow.FixedSizeListType).Len())
	start := (fsl.Offset() + row) * size
	switch dt {
	case types.VectorTypeInt64:
		return append([]int64(nil), fsl.ListValues().(*array.Int64).Int64Values()[start:start+size]...)
	case types.VectorTypeInt32:
		return append([]int32(nil), fsl.ListValues().(*array.Int32).Int32Values()[start:start+size]...)
	case types.VectorTypeFloat64:
		return append([]float64(nil), fsl.ListValues().(*array.Float64).Float64Values()[start:start+size]...)
	}
	t.Fatalf("unsupported type %v", dt)
	return nil
}
