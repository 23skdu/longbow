package types

import (
	"math"
	"sync"
	"testing"
	"sync/atomic"
)

func TestFastPathChunkAccessors(t *testing.T) {
	g := &GraphData{
		Type: VectorTypeFloat32,
		Dims: 128,
	}
	g.GrowMetadataSlices(2)

	err := g.EnsureChunk(0, 0, 128)
	if err != nil {
		t.Fatalf("failed to ensure chunk: %v", err)
	}

	vecs := make([][]float32, 1)
	vecs[0] = make([]float32, 128)
	for i := range vecs[0] {
		vecs[0][i] = float32(i)
	}

	err = g.SetVector(0, vecs[0])
	if err != nil {
		t.Fatalf("failed to set vector: %v", err)
	}

	// Test fast path accessor
	chunkFast := g.GetVectorsChunkFast(0)
	chunkNormal := g.GetVectorsChunk(0)

	if len(chunkFast) != len(chunkNormal) {
		t.Fatalf("chunk size mismatch: %d vs %d", len(chunkFast), len(chunkNormal))
	}

	for i := range vecs[0] {
		if chunkFast[i] != vecs[0][i] {
			t.Errorf("fast path returned incorrect data at %d: got %f, want %f", i, chunkFast[i], vecs[0][i])
		}
	}
}

func TestGenerationBypass(t *testing.T) {
	g := &GraphData{
		Type: VectorTypeFloat32,
		Dims: 128,
	}
	g.GrowMetadataSlices(2)
	g.EnsureChunk(0, 0, 128) // chunk 0 is generation 0

	// Bump generation to 5
	g.SetGeneration(5)
	
	// Create chunk 1 at generation 5
	g.EnsureChunk(1, 0, 128)

	// Bypass generation with math.MaxUint64 for chunk 1
	chunk1 := g.GetVectorsChunkFastWithGen(1, math.MaxUint64)
	if chunk1 == nil {
		t.Fatalf("expected chunk with bypass, got nil")
	}

	// Try to read chunk 1 (gen 5) with maxGen 4. Should fail!
	chunkOld := g.GetVectorsChunkFastWithGen(1, 4)
	if chunkOld != nil {
		t.Fatalf("expected nil chunk due to generation isolation (chunk gen=5, maxGen=4), got data")
	}
	
	// Try to read chunk 0 (gen 0) with maxGen 4. Should succeed!
	chunk0 := g.GetVectorsChunkFastWithGen(0, 4)
	if chunk0 == nil {
		t.Fatalf("expected chunk 0 to be visible to maxGen 4, got nil")
	}
}

func FuzzConcurrentChunkIO(f *testing.F) {
	f.Add(uint32(0))
	f.Add(uint32(5))
	
	f.Fuzz(func(t *testing.T, delay uint32) {
		g := &GraphData{
			Type: VectorTypeFloat32,
			Dims: 8,
		}
		g.GrowMetadataSlices(2)
		
		g.EnsureChunk(0, 0, 8)
		vec := []float32{1, 2, 3, 4, 5, 6, 7, 8}
		g.SetVector(0, vec)
		
		var wg sync.WaitGroup
		wg.Add(2)
		
		var readCount int32
		
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				// Fast path read
				chunk := g.GetVectorsChunkFast(0)
				if chunk != nil {
					_ = chunk[0]
					atomic.AddInt32(&readCount, 1)
				}
			}
		}()
		
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				g.ReleaseChunk(0)
				g.EnsureChunk(0, 0, 8)
			}
		}()
		
		wg.Wait()
	})
}
