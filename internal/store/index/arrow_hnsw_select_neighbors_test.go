package index

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
	"testing"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
)

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
