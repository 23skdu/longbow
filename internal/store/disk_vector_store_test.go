package store

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestDiskVectorStore_Compression(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
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
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
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

func TestDiskVectorStore_Uint8(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "uint8_vectors.bin")
	dim := 4

	dvs, err := NewDiskVectorStore(path, dim)
	require.NoError(t, err)
	defer dvs.Close()

	mem := memory.NewGoAllocator()
	builder := array.NewFixedSizeListBuilder(mem, int32(dim), arrow.PrimitiveTypes.Uint8)
	defer builder.Release()

	vb := builder.ValueBuilder().(*array.Uint8Builder)
	vb.AppendValues([]uint8{10, 20, 30, 40}, nil)
	builder.Append(true)
	vb.AppendValues([]uint8{50, 60, 70, 80}, nil)
	builder.Append(true)
	vb.AppendValues([]uint8{90, 100, 110, 120}, nil)
	builder.Append(true)

	arr := builder.NewArray().(*array.FixedSizeList)
	defer arr.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "vector", Type: arr.DataType()}}, nil)
	rec := array.NewRecordBatch(schema, []arrow.Array{arr}, 3)
	defer rec.Release()

	n, err := dvs.BatchAppendArrow(rec, 0)
	require.NoError(t, err)
	require.Equal(t, 3, n)

	// Read back using GetBatchAny
	rawResults, err := dvs.GetBatchAny([]int{0, 1, 2})
	require.NoError(t, err)

	u8Results, ok := rawResults.([][]uint8)
	require.True(t, ok, "Expected [][]uint8 from GetBatchAny")
	require.Len(t, u8Results, 3)
	require.Equal(t, []uint8{10, 20, 30, 40}, u8Results[0])
	require.Equal(t, []uint8{50, 60, 70, 80}, u8Results[1])
	require.Equal(t, []uint8{90, 100, 110, 120}, u8Results[2])
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

// TestDiskVectorStore_HotBlockCache verifies the hot-tier LRU serves repeated
// GetBatch reads of the same block without re-hitting the backend after the
// first fetch, and that cached payloads stay correct across reads.
func TestDiskVectorStore_HotBlockCache(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "hot_cache.bin")
	dim := 8

	dvs, err := NewDiskVectorStore(path, dim)
	require.NoError(t, err)
	defer dvs.Close()

	// Append 3 blocks (BatchAppend flushes per call → one block each).
	for b := 0; b < 3; b++ {
		batch := make([][]float32, 4)
		for i := range batch {
			batch[i] = make([]float32, dim)
			for j := range batch[i] {
				batch[i][j] = float32(b*100 + i*10 + j)
			}
		}
		n, err := dvs.BatchAppend(batch)
		require.NoError(t, err)
		require.Equal(t, 4, n)
	}
	require.Equal(t, 3, len(dvs.blocks))
	require.NotNil(t, dvs.blockCache, "constructor must install a default hot cache")

	// First read populates the cache for block 0 and block 1 (indices 4-7).
	idx01 := []int{0, 1, 2, 3, 4, 5, 6, 7}
	first, err := dvs.GetBatch(idx01)
	require.NoError(t, err)
	require.Len(t, first, 8)

	key := hotCacheKey(0)
	_, cached := dvs.blockCache.Get(key)
	require.True(t, cached, "block 0 must be cached after first read")
	_, cached1 := dvs.blockCache.Get(hotCacheKey(1))
	require.True(t, cached1, "block 1 must be cached after first read")

	// Second read: identical results from cache.
	second, err := dvs.GetBatch(idx01)
	require.NoError(t, err)
	require.Equal(t, first, second)

	// Hijack the cached payload; a subsequent read must reflect it (proving
	// the cache path, not disk, was used).
	src := mustGet(t, dvs, key)
	alt := make([]byte, len(src))
	copy(alt, src)
	alt[0] ^= 0xFF
	dvs.blockCache.Put(key, alt)

	third, err := dvs.GetBatch(idx01)
	require.NoError(t, err)
	require.Len(t, third, 8)
	require.NotEqual(t, first[0], third[0], "cache-hijacked read must differ → cache was used")
	require.Equal(t, first[4], third[4], "block 1 payload unchanged")
}

func mustGet(t *testing.T, dvs *DiskVectorStore, key string) []byte {
	t.Helper()
	data, ok := dvs.blockCache.Get(key)
	require.True(t, ok, "key %q", key)
	return data
}
