package store

import (
	"context"
	"runtime"
	"testing"

	"golang.org/x/sync/errgroup"
)

func TestArrowHNSW_AddBatchBulk_EnsureChunkRace(t *testing.T) {
	t.Run("Concurrent_EnsureChunk_NoRace", func(t *testing.T) {
		cfg := DefaultArrowHNSWConfig()
		cfg.InitialCapacity = 1          // Force small initial capacity
		cfg.Dims = 4                     // Small dimensions
		cfg.DataType = VectorTypeFloat32 // Explicitly set data type

		// Create a dummy dataset
		mockDataset := &Dataset{
			Name: "test-dataset",
			// No actual records needed for this test, as we are only checking allocation
			// and not actual data retrieval through the dataset.
		}

		h := NewArrowHNSW(mockDataset, cfg, NewChunkedLocationStore())

		// Helper function to get vector from ArrowHNSW (mimicking GetVector)
		getVectorFloat32 := func(hnsw *ArrowHNSW, id uint32) []float32 {
			data := hnsw.data.Load()
			if data == nil {
				return nil
			}
			cID := chunkID(id)
			cOff := chunkOffset(id)

			chunk := data.GetVectorsChunk(cID)
			if chunk == nil {
				return nil
			}
			// Ensure dims is loaded
			dims := int(hnsw.dims.Load())
			if dims == 0 {
				return nil
			}
			paddedDims := data.GetPaddedDims() // Use padded dims for indexing into chunk
			start := int(cOff) * paddedDims
			if start+dims > len(chunk) {
				return nil
			}
			return chunk[start : start+dims]
		}

		defer h.Close()

		numVectors := 2048 // Enough vectors to span multiple chunks
		vecs := make([][]float32, numVectors)
		for i := 0; i < numVectors; i++ {
			vecs[i] = make([]float32, cfg.Dims)
			for j := 0; j < cfg.Dims; j++ {
				vecs[i][j] = float32(i + j)
			}
		}

		// Use errgroup to simulate concurrent AddBatchBulk calls
		g, ctx := errgroup.WithContext(context.Background())
		// Each goroutine adds a small batch of vectors, designed to hit the same initial chunk.
		// Chunk 0 covers IDs 0-1023, Chunk 1 covers 1024-2047.
		// We'll have multiple goroutines trying to insert into the beginning of a chunk.

		concurrency := runtime.GOMAXPROCS(0) * 2 // More goroutines than CPUs
		if concurrency < 4 {
			concurrency = 4
		}

		batchSize := 20 // Each goroutine adds a small batch
		for i := 0; i < concurrency; i++ {
			startID := uint32(i * batchSize)
			endID := startID + uint32(batchSize)
			if int(endID) > numVectors {
				endID = uint32(numVectors)
			}
			if startID >= endID {
				continue
			}

			localStartID := startID
			localNum := int(endID - startID)
			localVecs := vecs[localStartID:endID]

			g.Go(func() error {
				return h.AddBatchBulk(ctx, localStartID, localNum, localVecs)
			})
		}

		if err := g.Wait(); err != nil {
			t.Errorf("concurrent AddBatchBulk failed: %v", err)
		}

		// Additional checks: ensure all nodes are present and correctly leveled.
		if h.nodeCount.Load() != int64(numVectors) {
			t.Errorf("expected node count %d, got %d", numVectors, h.nodeCount.Load())
		}
		for i := 0; i < numVectors; i++ {
			// Check if vector exists - getVectorFloat32 internally uses ensureChunk read path
			vec := getVectorFloat32(h, uint32(i))
			if vec == nil {
				t.Errorf("vector for ID %d not found", i)
			}
			if len(vec) != cfg.Dims {
				t.Errorf("vector for ID %d has incorrect dimensions: expected %d, got %d", i, cfg.Dims, len(vec))
			}
		}
	})
}
