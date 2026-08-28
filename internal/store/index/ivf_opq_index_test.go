package index

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIVFOPQIndex_Basic(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	dim := 16
	n := 100
	config := IVFOPQConfig{
		Nlist:         10,
		M:             4,
		K:             32,
		Nprobe:        2,
		OPQIterations: 2,
	}

	idx, err := NewIVFOPQIndex(dim, config)
	require.NoError(t, err)

	// Generate data
	vectors := make([][]float32, n)
	for i := 0; i < n; i++ {
		vectors[i] = make([]float32, dim)
		for j := 0; j < dim; j++ {
			vectors[i][j] = rand.Float32()
		}
	}

	// Train
	err = idx.Train(vectors)
	require.NoError(t, err)

	// Add
	err = idx.Add(context.Background(), vectors)
	require.NoError(t, err)

	// Search
	query := vectors[0]
	results, err := idx.SearchVectorsWithBitmap(context.Background(), query, 5, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, results)

	// The first result should be the query vector itself (ID 0)
	assert.Equal(t, uint32(0), uint32(results[0].ID))
}

func TestIVFOPQIndex_Empty(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	idx, _ := NewIVFOPQIndex(8, IVFOPQConfig{Nlist: 5})
	results, err := idx.SearchVectorsWithBitmap(context.Background(), make([]float32, 8), 5, nil, nil)
	require.NoError(t, err)
	assert.Empty(t, results)
}

func FuzzIVFOPQIndexBuild(f *testing.F) {
	if testing.Short() {
		f.Skip("skipping fuzz test in short mode")
	}
	f.Fuzz(func(t *testing.T, dim int, nlist int, n int) {
		if dim <= 0 || dim > 8192 || nlist <= 0 || nlist > 512 || n <= 0 || n > 100000 {
			t.Skip()
		}

		cfg := IVFOPQConfig{
			Nlist:         nlist,
			M:             4,
			K:             10,
			Nprobe:        2,
			OPQIterations: 2,
		}

		idx, err := NewIVFOPQIndex(dim, cfg)
		if err != nil {
			t.Skip()
		}

		rng := rand.New(rand.NewSource(int64(dim ^ nlist ^ n)))

		vectors := make([][]float32, n)
		for i := 0; i < n; i++ {
			vectors[i] = make([]float32, dim)
			for j := 0; j < dim; j++ {
				vectors[i][j] = rng.Float32()
			}
		}

		err = idx.Train(vectors)
		if err != nil {
			t.Skip()
		}

		err = idx.Add(context.Background(), vectors)
		if err != nil {
			t.Skip()
		}

		query := vectors[0]
		results, err := idx.SearchVectorsWithBitmap(context.Background(), query, 5, nil, nil)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		if len(results) == 0 {
			t.Fatalf("No results returned")
		}
	})
}

func TestIVFOPQIndex_RecallK(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	dim := 64
	n := 100
	k := 10

	config := IVFOPQConfig{
		Nlist:         10,
		M:             8,
		K:             32,
		Nprobe:        5,
		OPQIterations: 2,
	}

	idx, err := NewIVFOPQIndex(dim, config)
	require.NoError(t, err)

	vectors := make([][]float32, n)
	for i := 0; i < n; i++ {
		vectors[i] = make([]float32, dim)
		for j := 0; j < dim; j++ {
			vectors[i][j] = rand.Float32()
		}
	}

	err = idx.Train(vectors)
	require.NoError(t, err)

	err = idx.Add(context.Background(), vectors)
	require.NoError(t, err)

	query := vectors[0]
	results, err := idx.SearchVectorsWithBitmap(context.Background(), query, k, nil, nil)
	require.NoError(t, err)

	matchCount := 0
	for _, r := range results {
		if r.ID == 0 {
			matchCount++
		}
	}

	t.Logf("Recall@%d: %d/%d (%.2f%%)", k, matchCount, k, float64(matchCount)*100/float64(k))
}

func BenchmarkIVFOPQIndex_1M_3072dim(b *testing.B) {
	dim := 3072
	n := 100000
	nlist := 256

	config := IVFOPQConfig{
		Nlist:         nlist,
		M:             32,
		K:             100,
		Nprobe:        32,
		OPQIterations: 5,
	}

	idx, err := NewIVFOPQIndex(dim, config)
	if err != nil {
		b.Fatal(err)
	}

	vectors := make([][]float32, n)
	for i := 0; i < n; i++ {
		vectors[i] = make([]float32, dim)
		for j := 0; j < dim; j++ {
			vectors[i][j] = rand.Float32()
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i == 0 {
			idx.Train(vectors)
		}
		idx.Add(context.Background(), vectors)
	}

	query := make([]float32, dim)
	for i := 0; i < dim; i++ {
		query[i] = rand.Float32()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.SearchVectorsWithBitmap(context.Background(), query, 10, nil, nil)
	}
}

func TestIVFOPQ_makeClusterDists(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	dim := 8
	nlist := 4
	qv := make([]float32, dim)
	for i := 0; i < dim; i++ {
		qv[i] = 0.5
	}
	centroids := []float32{
		0, 0, 0, 0, 0, 0, 0, 0,
		1, 1, 1, 1, 1, 1, 1, 1,
		2, 2, 2, 2, 2, 2, 2, 2,
		3, 3, 3, 3, 3, 3, 3, 3,
	}

	dists := makeClusterDists(qv, centroids, nlist, dim)

	assert.Equal(t, nlist, len(dists))
	for i := 0; i < nlist; i++ {
		assert.Equal(t, i, dists[i].id)
		assert.Greater(t, dists[i].dist, float32(0))
	}
}

func TestIVFOPQ_decodeVector(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	dim := 16
	n := 20
	config := IVFOPQConfig{
		Nlist:         4,
		M:             4,
		K:             8,
		Nprobe:        2,
		OPQIterations: 2,
	}

	idx, err := NewIVFOPQIndex(dim, config)
	require.NoError(t, err)

	vectors := make([][]float32, n)
	for i := 0; i < n; i++ {
		vectors[i] = make([]float32, dim)
		for j := 0; j < dim; j++ {
			vectors[i][j] = rand.Float32()
		}
	}

	err = idx.Train(vectors)
	require.NoError(t, err)

	err = idx.Add(context.Background(), vectors)
	require.NoError(t, err)

	decoded, err := idx.decodeVector(0)
	require.NoError(t, err)
	assert.NotNil(t, decoded)
	assert.Equal(t, dim, len(decoded))

	_, err = idx.decodeVector(int(idx.nextID))
	assert.Error(t, err)
}

func TestIVFOPQ_computeResidualScore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	dim := 16
	n := 20
	config := IVFOPQConfig{
		Nlist:         4,
		M:             4,
		K:             8,
		Nprobe:        2,
		OPQIterations: 2,
	}

	idx, err := NewIVFOPQIndex(dim, config)
	require.NoError(t, err)

	vectors := make([][]float32, n)
	for i := 0; i < n; i++ {
		vectors[i] = make([]float32, dim)
		for j := 0; j < dim; j++ {
			vectors[i][j] = rand.Float32()
		}
	}

	err = idx.Train(vectors)
	require.NoError(t, err)

	err = idx.Add(context.Background(), vectors)
	require.NoError(t, err)

	score := idx.computeResidualScore(0, vectors[0])
	assert.GreaterOrEqual(t, score, float32(0))

	scoreBad := idx.computeResidualScore(int(idx.nextID)+100, vectors[0])
	assert.Equal(t, float32(0), scoreBad)
}
