package index

import (
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/stretchr/testify/require"
)

// buildNavGrid creates a cols×rows grid GraphData where each node links to
// its orthogonal neighbours — a realistic multi-hop navigation workload.
func buildNavGrid(cols, rows int) *types.GraphData {
	n := cols * rows
	g := types.NewGraphData(n, 2, false, false, -1, false, false, false,
		types.VectorTypeFloat32, false, false, false, 8, "nav_bench", nil, false)
	_ = g.EnsureChunk(0, 0, 2)

	for y := 0; y < rows; y++ {
		for x := 0; x < cols; x++ {
			id := uint32(y*cols + x) // #nosec G115
			var nb []uint32
			if y > 0 {
				nb = append(nb, uint32((y-1)*cols+x)) // #nosec G115
			}
			if y < rows-1 {
				nb = append(nb, uint32((y+1)*cols+x)) // #nosec G115
			}
			if x > 0 {
				nb = append(nb, uint32(y*cols+x-1)) // #nosec G115
			}
			if x < cols-1 {
				nb = append(nb, uint32(y*cols+x+1)) // #nosec G115
			}
			_ = g.SetNeighbors(id, nb)
			_ = g.SetVector(id, []float32{float32(x), float32(y)}) // #nosec G115
		}
	}
	return g
}

func BenchmarkGraphNavigator_FindPath(b *testing.B) {
	const cols, rows = 32, 32
	gd := buildNavGrid(cols, rows)

	nav := NewGraphNavigator("bench", func() *types.GraphData { return gd },
		NavigatorConfig{MaxHops: 64, EnableCaching: false}, nil)
	if err := nav.Initialize(); err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	query := NavigatorQuery{StartID: 0, TargetID: uint32(cols*rows - 1), MaxHops: 64} // #nosec G115

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := nav.FindPath(ctx, query); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGraphNavigator_FindPathCached(b *testing.B) {
	const cols, rows = 32, 32
	gd := buildNavGrid(cols, rows)

	nav := NewGraphNavigator("bench", func() *types.GraphData { return gd },
		NavigatorConfig{MaxHops: 64, EnableCaching: true, CacheTTL: 0}, nil)
	if err := nav.Initialize(); err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	query := NavigatorQuery{StartID: 0, TargetID: uint32(cols*rows - 1), MaxHops: 64} // #nosec G115

	// Warm the cache once.
	if _, err := nav.FindPath(ctx, query); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := nav.FindPath(ctx, query); err != nil {
			b.Fatal(err)
		}
	}
}

func TestGraphNavigator_GetNeighborsBufScratch(t *testing.T) {
	g := buildNavGrid(8, 8)
	nav := NewGraphNavigator("test", func() *types.GraphData { return g },
		NavigatorConfig{MaxHops: 10}, nil)
	require.NoError(t, nav.Initialize())

	scratch := make([]uint32, 0, types.MaxNeighbors)
	n1, ok := nav.getNeighborsBuf(0, scratch)
	require.True(t, ok)
	require.NotEmpty(t, n1)

	// Reuse the returned slice as the next scratch — must not corrupt results.
	scratch = n1
	n2, ok := nav.getNeighborsBuf(1, scratch)
	require.True(t, ok)
	require.NotEmpty(t, n2)

	// The legacy getNeighbors entry point still works (nil scratch).
	n3, ok := nav.getNeighbors(0)
	require.True(t, ok)
	require.NotEmpty(t, n3)
}
