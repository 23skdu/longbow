package index

import (
	"path/filepath"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildTestGraphData constructs a small in-memory graph suitable for
// WriteDiskGraph round-trip tests. Layer 0 forms a chain 0→1→…→n-1 and
// layer 1 links every other node so sparse-layer lookup is exercised.
func buildTestGraphData(t *testing.T, n int) *types.GraphData {
	t.Helper()
	g := types.NewGraphData(n, 4, false, false, -1, false, false, false,
		types.VectorTypeFloat32, false, false, false, 8, "disk_graph_test", nil, false)
	require.NoError(t, g.EnsureChunk(0, 0, 4))

	for i := 0; i < n; i++ {
		require.NoError(t, g.SetVector(uint32(i), []float32{float32(i), 0, 0, 1})) // #nosec G115
	}

	// Layer 0: chain
	for i := 0; i < n-1; i++ {
		require.NoError(t, g.SetNeighbors(uint32(i), []uint32{uint32(i + 1)})) // #nosec G115
	}
	require.NoError(t, g.SetNeighbors(uint32(n-1), nil)) // #nosec G115

	// Layer 1: every other node (sparse)
	for i := 0; i+2 < n; i += 2 {
		require.NoError(t, g.SetNeighborsAtLayer(1, uint32(i), []uint32{uint32(i + 2)})) // #nosec G115
	}
	return g
}

func TestDiskGraph_RoundTrip(t *testing.T) {
	const n = 32
	gd := buildTestGraphData(t, n)
	path := filepath.Join(t.TempDir(), "graph.hnsw")

	require.NoError(t, WriteDiskGraph(gd, path, n, 0, 0, 0, 1))

	dg, err := NewDiskGraph(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, dg.Close()) }()

	assert.Equal(t, n, dg.Size())
	assert.Equal(t, n, dg.Capacity())

	// Layer-0 chain must survive the round trip.
	for i := 0; i < n-1; i++ {
		neighbors := dg.GetNeighbors(0, uint32(i), nil) // #nosec G115
		require.Len(t, neighbors, 1, "node %d layer 0", i)
		assert.Equal(t, uint32(i+1), neighbors[0]) // #nosec G115
	}

	// Sparse layer-1 entries for even nodes.
	for i := 0; i+2 < n; i += 2 {
		neighbors := dg.GetNeighbors(1, uint32(i), nil) // #nosec G115
		require.Len(t, neighbors, 1, "node %d layer 1", i)
		assert.Equal(t, uint32(i+2), neighbors[0]) // #nosec G115
	}

	// Odd nodes are absent from layer 1.
	for i := 1; i+2 < n; i += 2 {
		assert.Nil(t, dg.GetNeighbors(1, uint32(i), nil)) // #nosec G115
	}
}

func TestDiskGraph_GetNeighborsBufReuse(t *testing.T) {
	const n = 16
	gd := buildTestGraphData(t, n)
	path := filepath.Join(t.TempDir(), "graph.hnsw")
	require.NoError(t, WriteDiskGraph(gd, path, n, 0, 0, 0, 1))

	dg, err := NewDiskGraph(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, dg.Close()) }()

	// First call with nil allocates; subsequent calls with the same buf
	// must return the same backing array (no re-allocation).
	first := dg.GetNeighbors(0, 0, nil)
	require.NotEmpty(t, first)

	buf := make([]uint32, len(first), len(first))
	second := dg.GetNeighbors(0, 0, buf)
	require.Equal(t, first, second)
	// same backing array → buf was reused
	require.Equal(t, len(first), cap(second))
}

func TestDiskGraph_GetLevel(t *testing.T) {
	const n = 16
	gd := buildTestGraphData(t, n)
	path := filepath.Join(t.TempDir(), "graph.hnsw")
	require.NoError(t, WriteDiskGraph(gd, path, n, 0, 0, 0, 1))

	dg, err := NewDiskGraph(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, dg.Close()) }()

	// Even nodes < n-2 appear in layer 1 → GetLevel ≥ 1.
	assert.GreaterOrEqual(t, dg.GetLevel(0), 1)
	// Odd nodes only exist at layer 0.
	assert.Equal(t, 0, dg.GetLevel(1))
	// Out-of-range node returns 0 (default).
	assert.Equal(t, 0, dg.GetLevel(uint32(n+10)))
}

func TestDiskGraph_GetNeighborsOutOfRange(t *testing.T) {
	const n = 8
	gd := buildTestGraphData(t, n)
	path := filepath.Join(t.TempDir(), "graph.hnsw")
	require.NoError(t, WriteDiskGraph(gd, path, n, 0, 0, 0, 1))

	dg, err := NewDiskGraph(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, dg.Close()) }()

	assert.Nil(t, dg.GetNeighbors(0, uint32(n), nil))
	assert.Nil(t, dg.GetNeighbors(types.ArrowMaxLayers, 0, nil))
	assert.Nil(t, dg.GetNeighbors(1, 5, nil)) // odd node not on layer 1
}

func BenchmarkDiskGraph_GetNeighborsNilBuf(b *testing.B) {
	const n = 1024
	gd := buildTestGraphData(&testing.T{}, n)
	path := filepath.Join(b.TempDir(), "graph.hnsw")
	if err := WriteDiskGraph(gd, path, n, 0, 0, 0, 1); err != nil {
		b.Fatal(err)
	}
	dg, err := NewDiskGraph(path)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = dg.Close() }()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = dg.GetNeighbors(0, uint32(i%n), nil) // #nosec G115
	}
}

func BenchmarkDiskGraph_GetNeighborsReusedBuf(b *testing.B) {
	const n = 1024
	gd := buildTestGraphData(&testing.T{}, n)
	path := filepath.Join(b.TempDir(), "graph.hnsw")
	if err := WriteDiskGraph(gd, path, n, 0, 0, 0, 1); err != nil {
		b.Fatal(err)
	}
	dg, err := NewDiskGraph(path)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = dg.Close() }()

	buf := make([]uint32, 0, types.MaxNeighbors)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf = dg.GetNeighbors(0, uint32(i%n), buf) // #nosec G115
	}
}
