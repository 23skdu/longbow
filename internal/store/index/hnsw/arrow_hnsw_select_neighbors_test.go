package hnsw

// Roadmap Item 29: selectNeighbors is the single entry point for neighbour
// selection.
//
// Every insert path used to have its own idea of what "pick M neighbours"
// means: the bulk linker called selectNeighbors, the narrow path handed the
// whole ef-wide result set to AddConnection and let a per-add prune converge,
// and the GPU batch build sorted by distance and took the first M - a
// truncation, not a selection, with no diversity check at all. The uint8
// default branch was the same defect one level down and it cost a graph mean
// degree of 1.3 against 16.0 for int8.
//
// The paths are unified now; this pins the entry point itself, because a test
// that reaches the narrow path or the GPU path only proves the wiring, not the
// heuristic.

import (
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	arrowarray "github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestSelectNeighborsIsTheSingleEntryPoint runs the test select neighbors is the single entry point operation.
func TestSelectNeighborsIsTheSingleEntryPoint(t *testing.T) {
	const dims = 4

	data := types.NewGraphData(64, dims, false, false, 0, false, false, false,
		types.VectorTypeFloat32, false, false, false, 8, "select-entry", nil, false)
	defer data.Release()

	fill := func(id uint32, v float32) {
		t.Helper()
		vec := make([]float32, dims)
		for i := range vec {
			vec[i] = v
		}
		if err := data.SetVector(id, vec); err != nil {
			t.Fatalf("SetVector(%d): %v", id, err)
		}
	}
	fill(0, 0)
	fill(1, 1.00) // accepted: first candidate
	fill(2, 1.02) // rejected: 0.04 from candidate 1, but claims 1.04 from the query
	fill(3, 5.00) // accepted: 8.0 from candidate 1, claims 5.0 from the query
	fill(4, 9.00)

	cfg := types.DefaultArrowHNSWConfig()
	cfg.DataType = types.VectorTypeFloat32
	cfg.Dims = dims
	h := NewArrowHNSW(&MockDataset{}, &cfg, nil)
	h.distFunc = simd.EuclideanDistance

	// In ascending distance order, which is the contract every caller sorts to.
	candidates := []types.Candidate{
		{ID: 1, Dist: 1.00},
		{ID: 2, Dist: 1.04},
		{ID: 3, Dist: 5.00},
		{ID: 4, Dist: 9.00},
	}

	selected := h.selectNeighbors(nil, candidates, 2, data)
	if len(selected) != 2 {
		t.Fatalf("selectNeighbors kept %d of %d candidates, want exactly m=2", len(selected), len(candidates))
	}
	if selected[0].ID != 1 {
		t.Errorf("first selected neighbour is %d, want the nearest candidate 1", selected[0].ID)
	}
	if selected[1].ID != 3 {
		t.Errorf("second selected neighbour is %d, want 3: candidate 2 is only 0.04 from "+
			"candidate 1 while claiming 1.04 from the query, which is what the diversity "+
			"check exists to reject (got IDs [%d %d])", selected[1].ID, selected[0].ID, selected[1].ID)
	}

	// Fewer candidates than m: every path shares the same pass-through.
	if got := h.selectNeighbors(nil, candidates, 8, data); len(got) != len(candidates) {
		t.Errorf("selectNeighbors with m >= len(candidates) returned %d, want all %d",
			len(got), len(candidates))
	}

	// No candidates: no selection, and no panic on the empty input.
	if got := h.selectNeighbors(nil, nil, 2, data); len(got) != 0 {
		t.Errorf("selectNeighbors(nil) returned %d candidates, want 0", len(got))
	}
}

// TestSelectNeighborsFloat64Batched runs the test select neighbors float64 batched operation.
func TestSelectNeighborsFloat64Batched(t *testing.T) {
	const dims = 8
	data := types.NewGraphData(128, dims, false, false, 0, false, false, false,
		types.VectorTypeFloat64, false, false, false, 16, "f64-select", nil, false)
	defer data.Release()

	for id := uint32(0); id < 32; id++ {
		vec := make([]float64, dims)
		for d := range vec {
			vec[d] = float64(id) * 0.5
		}
		if err := data.SetVector(id, vec); err != nil {
			t.Fatalf("SetVector(%d): %v", id, err)
		}
	}

	cfg := types.DefaultArrowHNSWConfig()
	cfg.DataType = types.VectorTypeFloat64
	cfg.Dims = dims
	cfg.M = 8
	h := NewArrowHNSW(&MockDataset{}, &cfg, nil)

	cands := make([]types.Candidate, 24)
	for i := range cands {
		cands[i] = types.Candidate{ID: uint32(i + 1), Dist: float32(i+1) * 1.4}
	}

	ctx := h.searchPool.Get()
	defer h.searchPool.Put(ctx)
	selected := h.selectNeighbors(ctx, cands, 8, data)
	if len(selected) != 8 {
		t.Fatalf("expected 8 selected neighbors, got %d", len(selected))
	}
}

// BenchmarkSelectNeighborsFloat64 runs the benchmark select neighbors float64 operation.
func BenchmarkSelectNeighborsFloat64(b *testing.B) {
	const dims = 128
	data := types.NewGraphData(256, dims, false, false, 0, false, false, false,
		types.VectorTypeFloat64, false, false, false, 16, "f64-bench", nil, false)
	defer data.Release()

	for id := uint32(0); id < 64; id++ {
		vec := make([]float64, dims)
		for d := range vec {
			vec[d] = float64((int(id)*31+d)%97) * 0.1
		}
		if err := data.SetVector(id, vec); err != nil {
			b.Fatal(err)
		}
	}

	cfg := types.DefaultArrowHNSWConfig()
	cfg.DataType = types.VectorTypeFloat64
	cfg.Dims = dims
	cfg.M = 16
	h := NewArrowHNSW(&MockDataset{}, &cfg, nil)

	cands := make([]types.Candidate, 48)
	for i := range cands {
		cands[i] = types.Candidate{ID: uint32(i), Dist: float32(i+1) * 0.25}
	}

	ctx := h.searchPool.Get()
	defer h.searchPool.Put(ctx)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = h.selectNeighbors(ctx, cands, 16, data)
	}
}

// TestComplex128ChunkedBulkInsert runs the test complex128 chunked bulk insert operation.
func TestComplex128ChunkedBulkInsert(t *testing.T) {
	const (
		numRows = 64
		dims    = 4
	)
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(dims*2, arrow.PrimitiveTypes.Float32)},
	}, nil)

	bldr := arrowarray.NewFixedSizeListBuilder(alloc, dims*2, arrow.PrimitiveTypes.Float32)
	defer bldr.Release()
	vb := bldr.ValueBuilder().(*arrowarray.Float32Builder)
	for r := 0; r < numRows; r++ {
		bldr.Append(true)
		for d := 0; d < dims*2; d++ {
			vb.Append(float32(r*dims*2 + d))
		}
	}
	arr := bldr.NewArray()
	defer arr.Release()
	rec := arrowarray.NewRecordBatch(schema, []arrow.Array{arr}, numRows)
	defer rec.Release()

	cfg := types.DefaultArrowHNSWConfig()
	cfg.DataType = types.VectorTypeComplex128
	cfg.Dims = dims
	cfg.M = 8
	cfg.EfConstruction = 32
	h := NewArrowHNSW(&MockDataset{Schema: schema}, &cfg, nil)

	rowIdxs := make([]int, numRows)
	batchIdxs := make([]int, numRows)
	for i := range rowIdxs {
		rowIdxs[i] = i
	}
	ids, err := h.AddBatch(context.Background(), []arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
	if err != nil {
		t.Fatalf("AddBatch Complex128: %v", err)
	}
	if len(ids) != numRows {
		t.Fatalf("expected %d ids, got %d", numRows, len(ids))
	}
}
