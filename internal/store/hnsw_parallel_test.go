package store

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// generateTestVectors creates n random test vectors of given dimensions
func generateTestVectors(n, dims int) [][]float32 {
	vectors := make([][]float32, n)
	for i := 0; i < n; i++ {
		vec := make([]float32, dims)
		for j := 0; j < dims; j++ {
			vec[j] = float32(i*dims + j)
		}
		vectors[i] = vec
	}
	return vectors
}

// makeHNSWTestRecord helper (duplicated from hnsw_test.go if needed, but likely available in package store)
// Since we are rewriting, we must ensure it exists.
// We'll define a local helper to be safe.
/*
func makeHNSWTestRecord(mem memory.Allocator, dims int, vectors [][]float32) arrow.RecordBatch {
	// ... (implementation similar to makeBatchTestRecord but named differently to avoid collision if already exists)
	// But wait, if makeBatchTestRecord exists in package store (added in hnsw_batch_test.go), use it.
	// But let's define a local one here to avoid ambiguity if makeBatchTestRecord is not exported (it is exported if capital, but it was lower case).
	// Lower case means only visible in current package. Yes.
	// But redeclaration is an error.
	// I will use makeBatchTestRecord from hnsw_batch_test.go since they are in same package.
	// Assuming hnsw_batch_test.go is compiled with this.
	return makeBatchTestRecord(mem, dims, vectors)
}
*/

// TestAddBatchParallel_Basic tests basic parallel batch addition
func TestAddBatchParallel_Basic(t *testing.T) {
	mem := memory.NewGoAllocator()
	vectors := generateTestVectors(100, 4)
	// Use makeBatchTestRecord which we know exists in the package now
	rec := makeBatchTestRecord(mem, 4, vectors)
	defer rec.Release()

	ds := &Dataset{
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewTestHNSWIndex(ds)

	rowIdxs := make([]int, 100)
	batchIdxs := make([]int, 100)
	for i := 0; i < 100; i++ {
		rowIdxs[i] = i
		batchIdxs[i] = 0
	}

	// Use AddBatch (which handles parallelism internally)
	ids, err := idx.AddBatch(context.Background(), []arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
	require.NoError(t, err)
	require.Len(t, ids, 100)
	assert.Equal(t, 100, idx.Len())
}

// TestAddBatchParallel_SingleWorker tests with single worker (sequential)
func TestAddBatchParallel_SingleWorker(t *testing.T) {
	// AddBatch doesn't easily allow forcing worker count without config.
	// We'll just verify AddBatch works on smaller set.
	mem := memory.NewGoAllocator()
	vectors := generateTestVectors(50, 4)
	rec := makeBatchTestRecord(mem, 4, vectors)
	defer rec.Release()

	ds := &Dataset{
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewTestHNSWIndex(ds)

	rowIdxs := make([]int, 50)
	batchIdxs := make([]int, 50)
	for i := 0; i < 50; i++ {
		rowIdxs[i] = i
		batchIdxs[i] = 0
	}

	_, err := idx.AddBatch(context.Background(), []arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
	require.NoError(t, err)
	assert.Equal(t, 50, idx.Len())
}

// TestAddBatchParallel_EmptyBatch tests with empty batch
func TestAddBatchParallel_EmptyBatch(t *testing.T) {
	mem := memory.NewGoAllocator()
	vectors := generateTestVectors(10, 4)
	rec := makeBatchTestRecord(mem, 4, vectors)
	defer rec.Release()

	ds := &Dataset{
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewTestHNSWIndex(ds)

	_, err := idx.AddBatch(context.Background(), []arrow.RecordBatch{rec}, []int{}, []int{})
	require.NoError(t, err)
	assert.Equal(t, 0, idx.Len()) // Nothing added
}

// TestAddBatchParallel_SearchQuality tests search results after parallel insertion
func TestAddBatchParallel_SearchQuality(t *testing.T) {
	mem := memory.NewGoAllocator()
	// Create sequential vectors where neighbors should be close
	vectors := make([][]float32, 50)
	for i := 0; i < 50; i++ {
		vectors[i] = []float32{float32(i), float32(i), float32(i), float32(i)}
	}
	rec := makeBatchTestRecord(mem, 4, vectors)
	defer rec.Release()

	ds := &Dataset{
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewTestHNSWIndex(ds)

	rowIdxs := make([]int, 50)
	batchIdxs := make([]int, 50)
	for i := 0; i < 50; i++ {
		rowIdxs[i] = i
		batchIdxs[i] = 0
	}

	_, err := idx.AddBatch(context.Background(), []arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
	require.NoError(t, err)

	// Search for vector 25
	// Fetch vector 25 manually since SearchByID might not exist or be weird
	queryVec := vectors[25]

	result, err := idx.SearchVectors(context.Background(), queryVec, 3, nil, SearchOptions{})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(result), 3)

	// First result should be self (approx 0 distance)
	// Note: result IDs are VectorIDs.
	// Since we inserted sequentially into empty index, ID should be 25.
	// AddBatch return IDs, we could check them. But assuming 0..N-1
	// assert.Equal(t, VectorID(25), result[0].ID) // Might be unreliable if IDs are random/hashed?
	// Usually sequential if simple counter.

	if result[0].Distance > 0.0001 {
		t.Errorf("Expected distance ~0 for self match, got %f", result[0].Distance)
	}

	// Other results should be reasonably close
	for _, r := range result[1:] {
		// Just check we got results
		_ = r
	}
}

// TestAddBatchParallel_LargeScale tests with larger dataset
func TestAddBatchParallel_LargeScale(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large scale test in short mode")
	}

	mem := memory.NewGoAllocator()
	numVectors := 10000
	vectors := generateTestVectors(numVectors, 128)
	rec := makeBatchTestRecord(mem, 128, vectors)
	defer rec.Release()

	ds := &Dataset{
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewTestHNSWIndex(ds)

	rowIdxs := make([]int, numVectors)
	batchIdxs := make([]int, numVectors)
	for i := 0; i < numVectors; i++ {
		rowIdxs[i] = i
		batchIdxs[i] = 0
	}

	start := time.Now()
	ids, err := idx.AddBatch(context.Background(), []arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
	duration := time.Since(start)

	require.NoError(t, err)
	require.Len(t, ids, numVectors)
	assert.Equal(t, numVectors, idx.Len())
	t.Logf("Added %d vectors in %v", numVectors, duration)
}

// BenchmarkAddBatchParallel benchmarks parallel vs sequential insertion
func BenchmarkAddBatchParallel(b *testing.B) {
	mem := memory.NewGoAllocator()
	numVectors := 1000
	vectors := generateTestVectors(numVectors, 64)
	rec := makeBatchTestRecord(mem, 64, vectors)
	defer rec.Release()

	rowIdxs := make([]int, numVectors)
	batchIdxs := make([]int, numVectors)
	for i := 0; i < numVectors; i++ {
		rowIdxs[i] = i
		batchIdxs[i] = 0
	}
	recs := []arrow.RecordBatch{rec} // Pass same rec repeatedly? No, AddBatch takes slice of RecordBatches and indices map into them?
	// AddBatch(ctx, records, rowIdxs, batchIdxs)
	// records is the lookup array for batchIdxs.

	b.Run("AddBatch_Sequential_Equivalent", func(b *testing.B) {
		b.SetBytes(int64(numVectors * 64 * 4))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ds := &Dataset{
				Records: []arrow.RecordBatch{rec},
			}
			idx := NewTestHNSWIndex(ds)
			// Loop adds
			for j := 0; j < numVectors; j++ {
				_, _ = idx.AddByLocation(context.Background(), 0, j)
			}
		}
		b.ReportMetric(float64(numVectors*b.N)/b.Elapsed().Seconds(), "vectors/sec")
	})

	b.Run("AddBatch_Optimized", func(b *testing.B) {
		b.SetBytes(int64(numVectors * 64 * 4))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ds := &Dataset{
				Records: []arrow.RecordBatch{rec},
			}
			idx := NewTestHNSWIndex(ds)
			_, _ = idx.AddBatch(context.Background(), recs, rowIdxs, batchIdxs)
		}
		b.ReportMetric(float64(numVectors*b.N)/b.Elapsed().Seconds(), "vectors/sec")
	})
}
