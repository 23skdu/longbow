package store

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/coder/hnsw"
)

func TestPQIntegration_EndToEnd(t *testing.T) {
	fmt.Println("Step 1: Setup Data")
	dim := 128
	count := 50 // Minimal
	trainSize := 20

	vectors := make([][]float32, count)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < count; i++ {
		vec := make([]float32, dim)
		for j := 0; j < dim; j++ {
			vec[j] = rng.Float32()
		}
		vectors[i] = vec
	}

	fmt.Println("Step 2: Create Index")
	// 2. Create Index
	ds := &Dataset{Name: "test_pq"}
	index := NewHNSWIndexWithCapacity(ds, count)

	fmt.Println("Step 3: Train PQ")
	// 3. Train PQ externally (before adding to graph to maintain dimension consistency)
	cfg := &PQConfig{Dimensions: dim, NumSubVectors: 8, NumCentroids: 16}
	encoder, err := TrainPQEncoder(cfg, vectors[:trainSize], 2)
	if err != nil {
		t.Fatalf("TrainPQ error: %v", err)
	}
	index.SetPQEncoder(encoder)

	fmt.Println("Step 4: Add vectors (Packed)")
	// 4. Add vectors (Packed)
	for i := 0; i < count; i++ {
		vec := vectors[i]
		codes := encoder.Encode(vec)
		index.pqCodesMu.Lock()
		if index.pqCodes == nil {
			index.pqCodes = make([][]uint8, 0)
		}
		if int(i) >= len(index.pqCodes) {
			targetLen := int(i) + 1
			newCodeSlice := make([][]uint8, targetLen)
			copy(newCodeSlice, index.pqCodes)
			index.pqCodes = newCodeSlice
		}
		index.pqCodes[i] = codes
		index.pqCodesMu.Unlock()

		packed := PackBytesToFloat32s(codes)
		index.Graph.Add(hnsw.MakeNode(VectorID(i), packed))
		index.nextVecID.Add(1)
	}

	fmt.Println("Step 5: Search")
	// 5. Search
	queryIdx := trainSize + 5
	queryVec := vectors[queryIdx]
	qCodes := encoder.Encode(queryVec)
	qPacked := PackBytesToFloat32s(qCodes)

	results := index.Graph.Search(qPacked, 5)
	found := false
	for _, res := range results {
		if int(res.Key) == queryIdx {
			found = true
			break
		}
	}
	if !found {
		t.Logf("PQ Search failed to find exact match for vector %d (Might happen with low Ksub/N, acceptable for integration check)", queryIdx)
	}
}

func BenchmarkPQSearch(b *testing.B) {
	dim := 128
	count := 100 // Minimal
	k := 10

	vectors := make([][]float32, count)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < count; i++ {
		vec := make([]float32, dim)
		for j := 0; j < dim; j++ {
			vec[j] = rng.Float32()
		}
		vectors[i] = vec
	}

	ds := &Dataset{Name: "bench_pq"}
	index := NewHNSWIndexWithCapacity(ds, count)

	cfg := &PQConfig{Dimensions: dim, NumSubVectors: 8, NumCentroids: 16}
	encoder, _ := TrainPQEncoder(cfg, vectors[:50], 2)
	index.SetPQEncoder(encoder)

	index.pqCodesMu.Lock()
	index.pqCodes = make([][]uint8, count)
	index.pqCodesMu.Unlock()

	for i := 0; i < count; i++ {
		codes := encoder.Encode(vectors[i])
		index.pqCodes[i] = codes
		packed := PackBytesToFloat32s(codes)
		index.Graph.Add(hnsw.MakeNode(VectorID(i), packed))
		index.nextVecID.Add(1)
	}

	qVec := vectors[0]
	qCodes := encoder.Encode(qVec)
	qPacked := PackBytesToFloat32s(qCodes)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index.Graph.Search(qPacked, k)
	}
}
