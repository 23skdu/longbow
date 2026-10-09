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
	_ = dvs2.Close()

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
		_ = dvsDirect.Close()
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

func TestDiskVectorStore_OutOfBounds(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "oob.bin")
	dim := 4

	dvs, err := NewDiskVectorStore(path, dim)
	require.NoError(t, err)
	defer dvs.Close()

	vecs := [][]float32{
		{1, 2, 3, 4},
		{5, 6, 7, 8},
	}
	n, err := dvs.BatchAppend(vecs)
	require.NoError(t, err)
	require.Equal(t, 2, n)

	// In-bounds query
	res, err := dvs.GetBatch([]int{0, 1})
	require.NoError(t, err)
	require.Len(t, res, 2)

	// Negative index
	_, err = dvs.GetBatch([]int{-1})
	require.Error(t, err)

	// Index >= totalCount
	_, err = dvs.GetBatch([]int{2})
	require.Error(t, err)

	// Huge out-of-bounds index
	_, err = dvs.GetBatch([]int{99999})
	require.Error(t, err)

	// Mixed valid and invalid indices
	_, err = dvs.GetBatch([]int{0, 5, 1})
	require.Error(t, err)

	// Same checks with GetBatchAny
	_, err = dvs.GetBatchAny([]int{-1})
	require.Error(t, err)

	_, err = dvs.GetBatchAny([]int{2})
	require.Error(t, err)
}

func TestDiskVectorStore_BatchAppendAny_AllTypes(t *testing.T) {
	dim := 4

	t.Run("Float64", func(t *testing.T) {
		tmpDir := t.TempDir()
		dvs, err := NewDiskVectorStore(filepath.Join(tmpDir, "f64.bin"), dim)
		require.NoError(t, err)
		defer dvs.Close()

		data := [][]float64{
			{1.5, 2.5, 3.5, 4.5},
			{5.5, 6.5, 7.5, 8.5},
		}
		n, err := dvs.BatchAppendAny(data)
		require.NoError(t, err)
		require.Equal(t, 2, n)

		got, err := dvs.GetBatchAny([]int{0, 1})
		require.NoError(t, err)
		gotSlice, ok := got.([][]float64)
		require.True(t, ok)
		require.Equal(t, data, gotSlice)
	})

	t.Run("Int16", func(t *testing.T) {
		tmpDir := t.TempDir()
		dvs, err := NewDiskVectorStore(filepath.Join(tmpDir, "i16.bin"), dim)
		require.NoError(t, err)
		defer dvs.Close()

		data := [][]int16{
			{100, -200, 300, -400},
			{1000, -2000, 3000, -4000},
		}
		n, err := dvs.BatchAppendAny(data)
		require.NoError(t, err)
		require.Equal(t, 2, n)

		got, err := dvs.GetBatchAny([]int{0, 1})
		require.NoError(t, err)
		gotSlice, ok := got.([][]int16)
		require.True(t, ok)
		require.Equal(t, data, gotSlice)
	})

	t.Run("Uint16", func(t *testing.T) {
		tmpDir := t.TempDir()
		dvs, err := NewDiskVectorStore(filepath.Join(tmpDir, "u16.bin"), dim)
		require.NoError(t, err)
		defer dvs.Close()

		data := [][]uint16{
			{100, 200, 300, 400},
			{1000, 2000, 3000, 4000},
		}
		n, err := dvs.BatchAppendAny(data)
		require.NoError(t, err)
		require.Equal(t, 2, n)

		got, err := dvs.GetBatchAny([]int{0, 1})
		require.NoError(t, err)
		gotSlice, ok := got.([][]uint16)
		require.True(t, ok)
		require.Equal(t, data, gotSlice)
	})

	t.Run("Int32", func(t *testing.T) {
		tmpDir := t.TempDir()
		dvs, err := NewDiskVectorStore(filepath.Join(tmpDir, "i32.bin"), dim)
		require.NoError(t, err)
		defer dvs.Close()

		data := [][]int32{
			{100000, -200000, 300000, -400000},
			{500000, -600000, 700000, -800000},
		}
		n, err := dvs.BatchAppendAny(data)
		require.NoError(t, err)
		require.Equal(t, 2, n)

		got, err := dvs.GetBatchAny([]int{0, 1})
		require.NoError(t, err)
		gotSlice, ok := got.([][]int32)
		require.True(t, ok)
		require.Equal(t, data, gotSlice)
	})

	t.Run("Uint32", func(t *testing.T) {
		tmpDir := t.TempDir()
		dvs, err := NewDiskVectorStore(filepath.Join(tmpDir, "u32.bin"), dim)
		require.NoError(t, err)
		defer dvs.Close()

		data := [][]uint32{
			{100000, 200000, 300000, 400000},
			{500000, 600000, 700000, 800000},
		}
		n, err := dvs.BatchAppendAny(data)
		require.NoError(t, err)
		require.Equal(t, 2, n)

		got, err := dvs.GetBatchAny([]int{0, 1})
		require.NoError(t, err)
		gotSlice, ok := got.([][]uint32)
		require.True(t, ok)
		require.Equal(t, data, gotSlice)
	})

	t.Run("Int64", func(t *testing.T) {
		tmpDir := t.TempDir()
		dvs, err := NewDiskVectorStore(filepath.Join(tmpDir, "i64.bin"), dim)
		require.NoError(t, err)
		defer dvs.Close()

		data := [][]int64{
			{10000000000, -20000000000, 30000000000, -40000000000},
			{50000000000, -60000000000, 70000000000, -80000000000},
		}
		n, err := dvs.BatchAppendAny(data)
		require.NoError(t, err)
		require.Equal(t, 2, n)

		got, err := dvs.GetBatchAny([]int{0, 1})
		require.NoError(t, err)
		gotSlice, ok := got.([][]int64)
		require.True(t, ok)
		require.Equal(t, data, gotSlice)
	})

	t.Run("Uint64", func(t *testing.T) {
		tmpDir := t.TempDir()
		dvs, err := NewDiskVectorStore(filepath.Join(tmpDir, "u64.bin"), dim)
		require.NoError(t, err)
		defer dvs.Close()

		data := [][]uint64{
			{10000000000, 20000000000, 30000000000, 40000000000},
			{50000000000, 60000000000, 70000000000, 80000000000},
		}
		n, err := dvs.BatchAppendAny(data)
		require.NoError(t, err)
		require.Equal(t, 2, n)

		got, err := dvs.GetBatchAny([]int{0, 1})
		require.NoError(t, err)
		gotSlice, ok := got.([][]uint64)
		require.True(t, ok)
		require.Equal(t, data, gotSlice)
	})

	t.Run("Complex64", func(t *testing.T) {
		tmpDir := t.TempDir()
		dvs, err := NewDiskVectorStore(filepath.Join(tmpDir, "c64.bin"), dim)
		require.NoError(t, err)
		defer dvs.Close()

		data := [][]complex64{
			{complex(1, 2), complex(3, 4), complex(5, 6), complex(7, 8)},
			{complex(-1, -2), complex(-3, -4), complex(-5, -6), complex(-7, -8)},
		}
		n, err := dvs.BatchAppendAny(data)
		require.NoError(t, err)
		require.Equal(t, 2, n)

		got, err := dvs.GetBatchAny([]int{0, 1})
		require.NoError(t, err)
		gotSlice, ok := got.([][]complex64)
		require.True(t, ok)
		require.Equal(t, data, gotSlice)
	})

	t.Run("Complex128", func(t *testing.T) {
		tmpDir := t.TempDir()
		dvs, err := NewDiskVectorStore(filepath.Join(tmpDir, "c128.bin"), dim)
		require.NoError(t, err)
		defer dvs.Close()

		data := [][]complex128{
			{complex(1.5, 2.5), complex(3.5, 4.5), complex(5.5, 6.5), complex(7.5, 8.5)},
			{complex(-1.5, -2.5), complex(-3.5, -4.5), complex(-5.5, -6.5), complex(-7.5, -8.5)},
		}
		n, err := dvs.BatchAppendAny(data)
		require.NoError(t, err)
		require.Equal(t, 2, n)

		got, err := dvs.GetBatchAny([]int{0, 1})
		require.NoError(t, err)
		gotSlice, ok := got.([][]complex128)
		require.True(t, ok)
		require.Equal(t, data, gotSlice)
	})
}

// TestDiskVectorStore_BatchAppendAny_Float32AndTurboQuant covers the two paths
// that GetBatchAny dispatches separately rather than through the generic typed
// list: float32, which also has to notice that a block holding TurboQuant rows
// is not dim*4 bytes wide, and TurboQuant itself.
//
// The existing AllTypes test covers the nine widths that map straight onto a
// typed case and so exercised neither of these.
//
// These are characterisation tests, not regression tests for a demonstrated
// defect: float32 and TurboQuant both round-tripped correctly before this
// change. They pin the two paths GetBatchAny dispatches separately so that a
// future change to them fails here rather than in production.
func TestDiskVectorStore_BatchAppendAny_Float32AndTurboQuant(t *testing.T) {
	dim := 8

	t.Run("Float32", func(t *testing.T) {
		dvs, err := NewDiskVectorStore(filepath.Join(t.TempDir(), "f32.bin"), dim)
		require.NoError(t, err)
		defer dvs.Close()

		// Distinct magnitudes so a wrong stride or a stale row cannot pass.
		data := [][]float32{
			{0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5},
			{10.25, 20.5, 30.75, 40.125, 50.25, 60.5, 70.75, 80.125},
		}
		n, err := dvs.BatchAppendAny(data)
		require.NoError(t, err)
		require.Equal(t, 2, n)

		got, err := dvs.GetBatchAny([]int{0, 1})
		require.NoError(t, err)
		gotSlice, ok := got.([][]float32)
		require.True(t, ok, "float32 must come back as [][]float32, got %T", got)
		require.Equal(t, data, gotSlice)

		// Out-of-order and partial request: blockOf must not assume the
		// requested order matches storage order.
		got2, err := dvs.GetBatchAny([]int{1})
		require.NoError(t, err)
		require.Equal(t, [][]float32{data[1]}, got2)
	})

	t.Run("TurboQuantRowWidth", func(t *testing.T) {
		dvs, err := NewDiskVectorStore(filepath.Join(t.TempDir(), "tq.bin"), dim)
		require.NoError(t, err)
		defer dvs.Close()

		// TurboQuant rows are PackedSize() bytes, not dim*4, so this block
		// must be decoded by stride rather than read as float32. Note the
		// encoder is created by SetTurboQuant, which sets tqBits at the same
		// time, so a TurboQuant block without an encoder is not reachable
		// through the append path - decodeTQRow still errors on it rather
		// than guessing a stride.
		dvs.SetTurboQuant(4)
		data := [][]float32{
			{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8},
			{0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2},
		}
		n, err := dvs.BatchAppendAny(data)
		require.NoError(t, err)
		require.Equal(t, 2, n)

		got, err := dvs.GetBatchAny([]int{0, 1})
		require.NoError(t, err)
		gotSlice, ok := got.([][]float32)
		require.True(t, ok, "turboquant decodes to [][]float32, got %T", got)
		require.Len(t, gotSlice, 2)
		for i := range gotSlice {
			require.Len(t, gotSlice[i], dim, "row %d has wrong width", i)
		}
	})
}

// TestDiskVectorStore_GetBatchAny_OutOfRange pins that an index past the end of
// the data is an error naming the index, not an alias onto the last block.
func TestDiskVectorStore_GetBatchAny_OutOfRange(t *testing.T) {
	dim := 4
	dvs, err := NewDiskVectorStore(filepath.Join(t.TempDir(), "oob.bin"), dim)
	require.NoError(t, err)
	defer dvs.Close()

	n, err := dvs.BatchAppendAny([][]float32{{1, 2, 3, 4}, {5, 6, 7, 8}})
	require.NoError(t, err)
	require.Equal(t, 2, n)

	for _, idx := range []int{2, 3, 1 << 20, -1} {
		got, err := dvs.GetBatchAny([]int{idx})
		require.Error(t, err, "index %d is past the end and must not succeed", idx)
		require.Nil(t, got)
		require.Contains(t, err.Error(), "out of bounds")
	}
}

// TestDiskVectorStore_ExtractTypedRows_Bounds pins the two bounds checks that
// now live in one shared place rather than in twelve copies of the same unsafe
// expression, where only one of them had them.
//
// It calls extractTypedRows directly with a hand-built block so the row can be
// asked for out of range without having to construct a store that would produce
// one, which is the point: the corruption this guards against is a truncated
// block, and a test that only ever writes well-formed blocks cannot reach it.
func TestDiskVectorStore_ExtractTypedRows_Bounds(t *testing.T) {
	const dim, rows = 4, 2
	block := BlockEntry{StartIdx: 10, NumVectors: rows}
	payload := make([]byte, rows*dim*8) // float64 rows
	blockData := map[int][]byte{0: payload}
	blockOf := []int{0, 0}
	copies := map[int]BlockEntry{0: block}

	t.Run("in range", func(t *testing.T) {
		got, err := extractTypedRows[float64](blockData, blockOf, copies, []int{10, 11}, dim)
		require.NoError(t, err)
		require.Len(t, got, 2)
		for _, r := range got {
			require.Len(t, r, dim)
		}
	})

	t.Run("missing block mapping", func(t *testing.T) {
		_, err := extractTypedRows[float64](blockData, []int{0}, copies, []int{10, 11}, dim)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no block mapping")
	})

	t.Run("local index past the block", func(t *testing.T) {
		_, err := extractTypedRows[float64](blockData, blockOf, copies, []int{12}, dim)
		require.Error(t, err)
		require.Contains(t, err.Error(), "outside block")
	})

	t.Run("local index before the block", func(t *testing.T) {
		_, err := extractTypedRows[float64](blockData, blockOf, copies, []int{9}, dim)
		require.Error(t, err)
		require.Contains(t, err.Error(), "outside block")
	})

	t.Run("truncated payload", func(t *testing.T) {
		// Block claims two rows but only carries one and a half. Without the
		// payload check this reads past the end of raw.
		short := map[int][]byte{0: payload[:dim*8+8]}
		_, err := extractTypedRows[float64](short, blockOf, copies, []int{11}, dim)
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeds block payload")
	})
}
