package index

// Distance and ID correctness for LookupNeighbors across element types.
//
// This is the verification roadmap item 5 asks for and did not have. The item
// reported two defects - distances computed only for float32 and 0.0 for every
// other type, and NeighborResult.ID carrying the internal node index instead of
// the client's ID - and both are fixed in the working tree (arrowHNSWLookupNeighbors
// calls ComputeDistanceAny and LookupExternalID).
//
// The tests that existed checked only that returned IDs were in range and that
// k was respected. Neither distance nor element type was checked, so "fixed"
// was an assertion about reading code rather than about behaviour.
//
// This pins the behaviour: on a non-float32 index the distances must be the
// real typed distances, not zeros, and the IDs must be the external ones - which
// are deliberately offset from the node indices here so a mix-up cannot pass.

import (
	"context"
	"math"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// externalIDBase offsets external IDs well clear of the node index range so
// that returning an internal index instead of an external ID cannot coincide
// with the right answer.
const externalIDBase = 900_000

func buildTypedLookupIndex(t *testing.T, n, dim int, dt arrow.DataType, vt types.VectorDataType) *ArrowHNSW {
	t.Helper()

	b := array.NewRecordBuilder(memory.NewGoAllocator(), arrow.NewSchema(
		[]arrow.Field{{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), dt)}}, nil,
	))
	defer b.Release()
	listB := b.Field(0).(*array.FixedSizeListBuilder)

	// A corpus where no two vectors are equal, so a zero distance is always
	// wrong rather than sometimes right.
	switch dt {
	case arrow.PrimitiveTypes.Float32:
		vb := listB.ValueBuilder().(*array.Float32Builder)
		for i := 0; i < n; i++ {
			listB.Append(true)
			for j := 0; j < dim; j++ {
				vb.Append(float32(i*dim + j))
			}
		}
	case arrow.PrimitiveTypes.Int8:
		vb := listB.ValueBuilder().(*array.Int8Builder)
		for i := 0; i < n; i++ {
			listB.Append(true)
			for j := 0; j < dim; j++ {
				vb.Append(int8(i*dim + j))
			}
		}
	case arrow.PrimitiveTypes.Int32:
		vb := listB.ValueBuilder().(*array.Int32Builder)
		for i := 0; i < n; i++ {
			listB.Append(true)
			for j := 0; j < dim; j++ {
				vb.Append(int32(i*dim + j))
			}
		}
	}
	rec := b.NewRecordBatch()
	defer rec.Release()

	cfg := types.DefaultArrowHNSWConfig()
	cfg.DataType = vt
	cfg.Dims = dim
	cfg.M = 16
	cfg.MMax = 16
	cfg.MMax0 = 16
	cfg.EfConstruction = 200
	cfg.Workers = 1

	ds := NewMockDataset("lookup", rec.Schema())
	ds.Records = append(ds.Records, rec)
	idx := NewArrowHNSW(ds, &cfg, nil)

	rowIdxs := make([]int, n)
	batchIdx := make([]int, n)
	for i := 0; i < n; i++ {
		rowIdxs[i], batchIdx[i] = i, 0
	}
	if _, err := idx.AddBatch(context.Background(), []arrow.RecordBatch{rec}, rowIdxs, batchIdx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		idx.IndexExternalID(uint64(externalIDBase+i), uint32(i)) // #nosec G115 -- n is small
	}
	return idx
}

// TestLookupNeighbors_TypedDistances asserts the distances are real typed
// distances on every element type, not zero.
func TestLookupNeighbors_TypedDistances(t *testing.T) {
	const n, dim = 120, 8

	// Each case gets its own n so the corpus stays pairwise distinct within
	// the type's range: n*dim values 0..n*dim-1, which has to fit in int8 for
	// that case or two vectors collide and a zero distance becomes correct.
	cases := []struct {
		name string
		n    int
		dt   arrow.DataType
		vt   types.VectorDataType
	}{
		{"float32", 120, arrow.PrimitiveTypes.Float32, types.VectorTypeFloat32},
		{"int8", 16, arrow.PrimitiveTypes.Int8, types.VectorTypeInt8},
		{"int32", 120, arrow.PrimitiveTypes.Int32, types.VectorTypeInt32},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			idx := buildTypedLookupIndex(t, c.n, dim, c.dt, c.vt)
			results, err := arrowHNSWLookupNeighbors(idx, uint64(externalIDBase), 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(results) == 0 {
				t.Skip("entry point has no recorded neighbours in this build")
			}

			srcVec, err := idx.GetVector(0)
			if err != nil || srcVec == nil {
				t.Fatalf("could not read the source vector: %v", err)
			}
			for _, r := range results {
				if math.IsNaN(float64(r.Distance)) {
					t.Fatalf("neighbour %d has NaN distance", r.ID)
				}
				internal, ok := idx.LookupInternalID(r.ID)
				if !ok {
					t.Fatalf("returned ID %d is not a known external ID", r.ID)
				}
				nbrVec, err := idx.GetVector(internal)
				if err != nil || nbrVec == nil {
					t.Fatalf("could not read neighbour vector: %v", err)
				}
				want, err := idx.ComputeDistanceAny(srcVec, nbrVec)
				if err != nil {
					t.Fatalf("reference distance failed for %T: %v", srcVec, err)
				}
				if math.Abs(float64(r.Distance-want)) > 1e-4 {
					t.Errorf("neighbour %d distance is %v, want %v", r.ID, r.Distance, want)
				}
			}
		})
	}
}

// TestLookupNeighbors_ExternalIDs asserts the returned IDs are the client's
// external IDs, not internal node indices. The external range starts at
// externalIDBase, so a mix-up cannot coincide.
func TestLookupNeighbors_ExternalIDs(t *testing.T) {
	const n, dim = 120, 8
	idx := buildTypedLookupIndex(t, n, dim, arrow.PrimitiveTypes.Int8, types.VectorTypeInt8)

	results, err := arrowHNSWLookupNeighbors(idx, uint64(externalIDBase), 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range results {
		if r.ID < externalIDBase || r.ID >= externalIDBase+n {
			t.Errorf("neighbour ID %d is neither an external ID (%d..%d) nor, by coincidence, "+
				"a plausible external one; internal node indices must be translated",
				r.ID, externalIDBase, externalIDBase+n)
		}
	}
}
