package store

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiskVectorStore_Compression(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "vectors.bin")
	dim := 128

	dvs, err := NewDiskVectorStore(path, dim)
	require.NoError(t, err)
	defer dvs.Close()

	// Test case: 10 vectors of 128 floats
	vectors := make([][]float32, 10)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for j := range vectors[i] {
			vectors[i][j] = float32(i + j)
		}
	}

	n, err := dvs.BatchAppend(vectors)
	require.NoError(t, err)
	require.Equal(t, 10, n)

	// Check file size
	fi, err := os.Stat(path)
	require.NoError(t, err)
	require.Greater(t, fi.Size(), int64(0))

	t.Logf("Zstd compressed size for 10 vectors: %d bytes", fi.Size())

	// Test LZ4
	path2 := filepath.Join(tmpDir, "vectors_lz4.bin")
	dvs2, _ := NewDiskVectorStore(path2, dim)
	dvs2.SetCompression("lz4")
	_, err = dvs2.BatchAppend(vectors)
	require.NoError(t, err)
	dvs2.Close()

	fi2, _ := os.Stat(path2)
	t.Logf("LZ4 compressed size for 10 vectors: %d bytes", fi2.Size())
}

func TestDiskVectorStore_Read(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "read_v.bin")
	dim := 4

	dvs, err := NewDiskVectorStore(path, dim)
	require.NoError(t, err)
	defer dvs.Close()

	// Append 2 batches
	batch1 := [][]float32{
		{1, 1, 1, 1},
		{2, 2, 2, 2},
	}
	batch2 := [][]float32{
		{3, 3, 3, 3},
		{4, 4, 4, 4},
		{5, 5, 5, 5},
	}

	_, err = dvs.BatchAppend(batch1)
	require.NoError(t, err)
	_, err = dvs.BatchAppend(batch2)
	require.NoError(t, err)

	// Read back
	indices := []int{0, 1, 2, 3, 4}
	results, err := dvs.GetBatch(indices)
	require.NoError(t, err)
	require.Equal(t, 5, len(results))

	require.Equal(t, float32(1), results[0][0])
	require.Equal(t, float32(2), results[1][0])
	require.Equal(t, float32(3), results[2][0])
	require.Equal(t, float32(4), results[3][0])
	require.Equal(t, float32(5), results[4][0])

	// Read subset across blocks
	results2, err := dvs.GetBatch([]int{1, 3})
	require.NoError(t, err)
	require.Equal(t, 2, len(results2))
	require.Equal(t, float32(2), results2[0][0])
	require.Equal(t, float32(4), results2[1][0])
}

func BenchmarkDiskVectorStore_Read(b *testing.B) {
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "bench.bin")
	dim := 128
	numVectors := 10000

	dvs, _ := NewDiskVectorStore(path, dim)

	// Create some data
	batch := make([][]float32, 100)
	for i := range batch {
		batch[i] = make([]float32, dim)
	}

	for i := 0; i < numVectors/100; i++ {
		_, _ = dvs.BatchAppend(batch)
	}

	b.Run("StandardIO", func(b *testing.B) {
		indices := make([]int, 10)
		for i := 0; i < b.N; i++ {
			for j := 0; j < 10; j++ {
				indices[j] = (i + j) % numVectors
			}
			_, _ = dvs.GetBatch(indices)
		}
	})

	b.Run("DirectIO", func(b *testing.B) {
		dvsDirect, _ := NewDiskVectorStoreWithConfig(path+"_direct", dim, false, true)
		for i := 0; i < numVectors/100; i++ {
			_, _ = dvsDirect.BatchAppend(batch)
		}

		indices := make([]int, 10)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < 10; j++ {
				indices[j] = (i + j) % numVectors
			}
			_, _ = dvsDirect.GetBatch(indices)
		}
		dvsDirect.Close()
	})
}
