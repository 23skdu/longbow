package store

import (
	"path/filepath"
	"testing"

	"github.com/23skdu/longbow/internal/store/index"
	"github.com/stretchr/testify/require"
)

func TestRowBufferPool(t *testing.T) {
	pool := index.NewFloat32RowPool(128)
	b1 := pool.Get()
	require.Len(t, b1, 128)
	b1[0] = 42.0
	pool.Put(b1)

	b2 := pool.Get()
	require.Len(t, b2, 128)

	bytePool := index.NewByteRowPool(512)
	bb1 := bytePool.Get()
	require.Len(t, bb1, 512)
	bb1[0] = 0xAA
	bytePool.Put(bb1)

	bb2 := bytePool.Get()
	require.Len(t, bb2, 512)
}

func TestDiskVectorStore_GetVectorInto(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "store.bin")
	dim := 4

	dvs, err := NewDiskVectorStore(path, dim)
	require.NoError(t, err)
	defer dvs.Close()

	vecs := [][]float32{
		{1.0, 2.0, 3.0, 4.0},
		{5.0, 6.0, 7.0, 8.0},
		{9.0, 10.0, 11.0, 12.0},
	}
	n, err := dvs.BatchAppend(vecs)
	require.NoError(t, err)
	require.Equal(t, 3, n)

	buf := make([]float32, dim)

	// Fetch each vector using GetVectorInto
	for i, expected := range vecs {
		err := dvs.GetVectorInto(i, buf)
		require.NoError(t, err)
		require.Equal(t, expected, buf)
	}

	// Out of bounds checks
	err = dvs.GetVectorInto(-1, buf)
	require.Error(t, err)
	err = dvs.GetVectorInto(3, buf)
	require.Error(t, err)

	// Buffer too small check
	smallBuf := make([]float32, dim-1)
	err = dvs.GetVectorInto(0, smallBuf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "dst buffer too small")

	// Tombstone deletion check
	dvs.Delete(1)
	err = dvs.GetVectorInto(1, buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "deleted")
}

func TestDiskVectorStore_ScanRows(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "scan.bin")
	dim := 4

	dvs, err := NewDiskVectorStore(path, dim)
	require.NoError(t, err)
	defer dvs.Close()

	vecs := [][]float32{
		{1.1, 2.2, 3.3, 4.4},
		{5.5, 6.6, 7.7, 8.8},
		{9.9, 10.1, 11.2, 12.3},
	}
	_, err = dvs.BatchAppend(vecs)
	require.NoError(t, err)

	scanned := make([][]float32, 0, len(vecs))
	err = dvs.ScanRows([]int{0, 1, 2}, func(idx int, row []float32) bool {
		r := make([]float32, len(row))
		copy(r, row)
		scanned = append(scanned, r)
		return true
	})
	require.NoError(t, err)
	require.Equal(t, vecs, scanned)

	// Test early stop
	count := 0
	err = dvs.ScanRows([]int{0, 1, 2}, func(idx int, row []float32) bool {
		count++
		return false // stop after first row
	})
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestDiskVectorStore_GetVectorAnyInto(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "u8_any.bin")
	dim := 4

	dvs, err := NewDiskVectorStore(path, dim)
	require.NoError(t, err)
	defer dvs.Close()

	u8Vecs := [][]uint8{
		{10, 20, 30, 40},
		{50, 60, 70, 80},
	}
	_, err = dvs.BatchAppendAny(u8Vecs)
	require.NoError(t, err)

	dst := make([]uint8, dim)
	for i, expected := range u8Vecs {
		err := dvs.GetVectorAnyInto(i, dst)
		require.NoError(t, err)
		require.Equal(t, expected, dst)
	}

	scanned := make([][]uint8, 0, len(u8Vecs))
	err = dvs.ScanRowsAny([]int{0, 1}, func(idx int, row any) bool {
		r := make([]uint8, dim)
		copy(r, row.([]uint8))
		scanned = append(scanned, r)
		return true
	})
	require.NoError(t, err)
	require.Equal(t, u8Vecs, scanned)
}

func BenchmarkDiskVectorStore_GetVectorInto(b *testing.B) {
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "bench_into.bin")
	dim := 128
	numVectors := 1000

	dvs, err := NewDiskVectorStore(path, dim)
	if err != nil {
		b.Fatal(err)
	}
	defer dvs.Close()

	batch := make([][]float32, 100)
	for i := range batch {
		batch[i] = make([]float32, dim)
		for j := range batch[i] {
			batch[i][j] = float32(i + j)
		}
	}
	for i := 0; i < numVectors/100; i++ {
		_, _ = dvs.BatchAppend(batch)
	}

	buf := make([]float32, dim)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		idx := i % numVectors
		_ = dvs.GetVectorInto(idx, buf)
	}
}

func BenchmarkDiskVectorStore_ScanRows(b *testing.B) {
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "bench_scan.bin")
	dim := 128
	numVectors := 1000

	dvs, err := NewDiskVectorStore(path, dim)
	if err != nil {
		b.Fatal(err)
	}
	defer dvs.Close()

	batch := make([][]float32, 100)
	for i := range batch {
		batch[i] = make([]float32, dim)
	}
	for i := 0; i < numVectors/100; i++ {
		_, _ = dvs.BatchAppend(batch)
	}

	indices := make([]int, 10)
	for i := range indices {
		indices[i] = i
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = dvs.ScanRows(indices, func(idx int, row []float32) bool {
			return true
		})
	}
}

func BenchmarkDiskVectorStore_GetBatchInto(b *testing.B) {
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "bench_batch_into.bin")
	dim := 128
	numVectors := 1000

	dvs, err := NewDiskVectorStore(path, dim)
	if err != nil {
		b.Fatal(err)
	}
	defer dvs.Close()

	batch := make([][]float32, 100)
	for i := range batch {
		batch[i] = make([]float32, dim)
	}
	for i := 0; i < numVectors/100; i++ {
		_, _ = dvs.BatchAppend(batch)
	}

	indices := make([]int, 10)
	for i := range indices {
		indices[i] = i
	}

	dst := make([][]float32, 10)
	flat := make([]float32, 10*dim)
	for i := range dst {
		dst[i] = flat[i*dim : (i+1)*dim]
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = dvs.GetBatchInto(indices, dst)
	}
}
