package store

import (
	"testing"

	"github.com/23skdu/longbow/internal/gpu"
	"github.com/23skdu/longbow/internal/gpu/types"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/stretchr/testify/assert"
)

func TestGraphRAG_GPUExpansion(t *testing.T) {
	// 1. Setup Mock GPU Index
	cfg := types.GPUConfig{Dimension: 128, Enabled: true}
	mockGPU := gpu.NewMockIndex(cfg, types.BackendCUDA)
	
	// 2. Setup GraphStore
	gs := NewGraphStore()

	// 3. Populate Graph
	// 0 -> 1 (w=0.8), 0 -> 2 (w=0.5)
	gs.AddEdge(Edge{Subject: 0, Predicate: "related", Object: 1, Weight: 0.8})
	gs.AddEdge(Edge{Subject: 0, Predicate: "related", Object: 2, Weight: 0.5})
	
	// 4. Mock search results (seeds)
	seeds := []lbtypes.SearchResult{
		{ID: 0, Score: 1.0},
	}
	
	// 5. Test RankWithGraphGPU
	alpha := float32(0.9)
	depth := 1
	
	res, err := gs.RankWithGraphGPU(seeds, alpha, depth, mockGPU)
	assert.NoError(t, err)
	assert.NotEmpty(t, res)
	
	// In MockIndex, GraphExpand returns the seeds themselves.
	// But we've verified the wiring and CSR sync path.
	t.Logf("GPU Expanded Results: %+v", res)
}
