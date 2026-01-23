package store

import (
	"context"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createSQ8TestDataset(t *testing.T, vectors [][]float32) *Dataset {
	mem := memory.NewGoAllocator()
	dims := len(vectors[0])
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	listB := builder.Field(0).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float32Builder)

	for _, vec := range vectors {
		listB.Append(true)
		valB.AppendValues(vec, nil)
	}

	rec := builder.NewRecordBatch()

	// Check that we created exactly one batch with correct length
	require.Equal(t, int64(len(vectors)), rec.NumRows())

	return &Dataset{
		Name:    "test_sq8",
		Schema:  schema,
		Records: []arrow.RecordBatch{rec},
	}
}

func TestSQ8_EndToEnd(t *testing.T) {
	// Setup
	dims := 128
	numVecs := 2000 // Enough to trigger training

	// Create random vectors
	vectors := make([][]float32, numVecs)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < numVecs; i++ {
		vectors[i] = make([]float32, dims)
		for j := 0; j < dims; j++ {
			vectors[i][j] = rng.Float32()
		}
	}

	ds := createSQ8TestDataset(t, vectors)
	defer ds.Records[0].Release()

	config := DefaultArrowHNSWConfig()
	config.M = 16
	config.EfConstruction = 64
	config.SQ8Enabled = true
	config.SQ8TrainingThreshold = 1000

	h := NewArrowHNSW(ds, config, nil)

	// Insert vectors
	t.Log("Inserting vectors...")
	for i := 0; i < numVecs; i++ {
		// Use AddByLocation which resolves via Dataset
		// ID i maps to Batch 0, Row i
		_, err := h.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	t.Log("Verifying SQ8 Training...")
	require.NotNil(t, h.quantizer, "Quantizer should be initialized")
	assert.True(t, h.quantizer.IsTrained(), "Quantizer should be trained")

	// Verify GraphData has SQ8 vectors
	data := h.data.Load()
	require.NotNil(t, data, "GraphData should not be nil")
	require.NotNil(t, data.VectorsSQ8, "VectorsSQ8 should be allocated")

	chunkID := uint32(0)

	sq8Chunk := data.GetVectorsSQ8Chunk(chunkID)
	require.NotNil(t, sq8Chunk, "First SQ8 chunk should exist")

	v0SQ8 := sq8Chunk[0:dims]
	isAllZero := true
	for _, b := range v0SQ8 {
		if b != 0 {
			isAllZero = false
			break
		}
	}
	assert.False(t, isAllZero, "SQ8 vector should not be all zeros")

	// Verify Search
	t.Log("Comparing Search Results (SQ8 vs Float32)...")

	queryVec := vectors[0]
	k := 10

	// Search
	results, err := h.Search(context.Background(), queryVec, k, 100, nil)
	require.NoError(t, err)

	require.GreaterOrEqual(t, len(results), 1)
	assert.Equal(t, uint32(0), uint32(results[0].ID), "Should find self")

	t.Logf("Self distance (SQ8): %f", results[0].Score)
	assert.Less(t, results[0].Score, float32(1.0), "Self distance should be small")

	// Verify Decoding
	t.Log("Verifying Decoding Accuracy...")
	v0Encoded := sq8Chunk[0:dims]
	v0Decoded := h.quantizer.Decode(v0Encoded)

	var sumSqErr float32
	for j := 0; j < dims; j++ {
		diff := vectors[0][j] - v0Decoded[j]
		sumSqErr += diff * diff
	}
	t.Logf("Decoded Vector L2 Error: %f", sumSqErr)
	assert.Less(t, sumSqErr, float32(dims)*0.01, "Decoding error should be low")
}

func TestSQ8_Persistence(t *testing.T) {
	// Setup Graph
	dims := 64
	numVecs := 1500

	vectors := make([][]float32, numVecs)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < numVecs; i++ {
		vectors[i] = make([]float32, dims)
		for j := 0; j < dims; j++ {
			vectors[i][j] = rng.Float32()
		}
	}

	ds := createSQ8TestDataset(t, vectors)
	defer ds.Records[0].Release()

	config := DefaultArrowHNSWConfig()
	config.SQ8Enabled = true
	config.SQ8TrainingThreshold = 500

	h := NewArrowHNSW(ds, config, nil)

	for i := 0; i < numVecs; i++ {
		_, err := h.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	require.True(t, h.quantizer.IsTrained())

	// Write to Disk
	tmpDir := t.TempDir()
	idxPath := filepath.Join(tmpDir, "index.bin")

	data := h.data.Load()
	err := WriteDiskGraph(data, idxPath, int(h.nodeCount.Load()), 0, 0, 0, 0)
	require.NoError(t, err)

	// Load from Disk
	t.Log("Loading from Disk...")
	dg, err := NewDiskGraph(idxPath)
	require.NoError(t, err)
	defer func() { _ = dg.Close() }()

	// Verify Header
	assert.Equal(t, uint32(dims), dg.header.Dims)
	assert.Greater(t, dg.header.SQ8Offset, uint64(0), "SQ8 Offset should be set")

	// Verify Vector Data
	t.Log("Verifying Disk Vectors...")
	vec0Disk := dg.GetVectorSQ8(0)
	require.NotNil(t, vec0Disk)
	assert.Equal(t, dims, len(vec0Disk))

	// Compare with memory version
	vec0Mem := data.GetVectorsSQ8Chunk(0)[0:dims]
	assert.Equal(t, vec0Mem, vec0Disk, "Disk vector should match Memory vector")

	// Verify Neighbors
	neighbors := dg.GetNeighbors(0, 0, nil)
	require.NotNil(t, neighbors)

	t.Log("Persistence Verified.")
}
