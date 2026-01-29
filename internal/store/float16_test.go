package store

import (
	"math"
	"math/rand"

	lbtypes "github.com/23skdu/longbow/internal/store/types"

	"testing"

	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFloat16_RoundTrip(t *testing.T) {
	// Goal: Verify we can store a float32 vector as float16 and retrieve it
	// with acceptable precision loss.

	dims := 128
	capacity := 100

	// Create GraphData with Float16 enabled
	// NewGraphData(capacity, dims, sq8, pq, pqDims, bq, float16, false)
	gd := lbtypes.NewGraphData(capacity, dims, false, false, 0, false, true, false, lbtypes.VectorTypeFloat16)

	require.NotNil(t, gd.Float16Arena)
	require.NotNil(t, gd.VectorsF16)

	// Create a random vector
	vec := make([]float32, dims)
	for i := 0; i < dims; i++ {
		vec[i] = rand.Float32()*2 - 1 // -1 to 1
	}

	// 1. Manually Simulate Insertion (since ArrowHNSW logic not yet updated)
	// We need to:
	// a. Ensure chunk exists
	// b. Allocate in arena
	// c. Convert and Copy

	id := uint32(1)
	cID := chunkID(id)
	cOff := chunkOffset(id)

	// Ensure VectorsF16 chunk exists
	// Mimic ensureChunk logic for F16
	chunk := gd.GetVectorsF16Chunk(int(cID))
	if chunk == nil {
		// Allocating in TypedArena:
		// Test uses ChunkSize=1024
		ref, err := gd.Float16Arena.AllocSlice(lbtypes.ChunkSize * dims)
		require.NoError(t, err)

		// Store the offset
		gd.VectorsF16[cID] = ref.Offset
		chunk = gd.Float16Arena.Get(ref)
	}

	require.NotNil(t, chunk)

	// 2. Convert and Store
	f16Vec := make([]float16.Num, dims)
	for i := 0; i < dims; i++ {
		f16Vec[i] = float16.New(vec[i])
	}

	start := int(cOff) * dims
	copy(chunk[start:start+dims], f16Vec)

	// 3. Retrieve and Verify
	// We need a way to read it back.
	// Since we have 'chunk', we can read directly.

	retrievedF16 := chunk[start : start+dims]
	retrievedVec := make([]float32, dims)
	for i := 0; i < dims; i++ {
		retrievedVec[i] = retrievedF16[i].Float32()
	}

	// 4. Check Precision
	for i := 0; i < dims; i++ {
		// Float16 has ~3-4 significant digits.
		// Diff should be small.
		diff := math.Abs(float64(vec[i] - retrievedVec[i]))
		assert.Less(t, diff, 0.01, "Precision loss too high at index %d: original=%f, retrieved=%f", i, vec[i], retrievedVec[i])
	}

	t.Logf("Float16 Round Trip Successful for %d dims", dims)
}

func TestArrowHNSW_Float16_Integration(t *testing.T) {
	// Goal: Verify end-to-end usage via ArrowHNSW

	dims := 128
	cfg := DefaultArrowHNSWConfig()
	cfg.Dims = dims
	cfg.Float16Enabled = true
	cfg.InitialCapacity = 100

	// Create HNSW
	hnsw := NewArrowHNSW(nil, &cfg)

	// Insert 10 vectors
	n := 10
	vecs := make([][]float32, n)
	for i := 0; i < n; i++ {
		vecs[i] = make([]float32, dims)
		for j := 0; j < dims; j++ {
			vecs[i][j] = rand.Float32()*2 - 1
		}
	}

	// Use manual insertion bypass? No, use AddByLocation would be best but it requires LocationStore logic.
	// But InsertWithVector is internal.
	// AddByLocation calls Insert(id, level) which calls InsertWithVector(id, vector, level).
	// But AddByLocation doesn't provide vector?
	// Ah, AddByLocation READS vector from Dataset.
	// We didn't provide dataset.

	// We can use internal Insert method directly if we mock vector retrieval?
	// Or we create a Dataset.
	// Creating Dataset is heavy.
	// What did other tests do?
	// TestArrowHNSW_AddBatch uses AddBatch.

	// Let's assume we can call an internal helper or just create a mock logical flow.
	// InsertWithVector signature?
	// It's in arrow_hnsw_insert.go? No, func (h *ArrowHNSW) InsertWithVector(id uint32, vec []float32, level int) error
	// Wait, I saw it in Step 3081: func (h *ArrowHNSW) InsertWithVector(id uint32, vec []float32, level int) error
	// Is it exported? No (capital I means exported? No, struct method).
	// Yes, InsertWithVector starts with Capital I.
	// So we can call it!

	for i := 0; i < n; i++ {
		id := uint32(i + 1)
		err := hnsw.InsertWithVector(id, vecs[i], 0) // Level 0 for simplicity
		require.NoError(t, err)
	}

	// Verify
	gd := hnsw.data.Load()
	require.NotNil(t, gd.VectorsF16)

	// Check internal state
	// F32 Vectors should be empty (offsets 0)
	// Actually, we need to check if chunk allocated.
	// cID ends up being 0 for id=1..10.
	// gd.Vectors[0] should be 0.
	// Check if Vectors[0] is nil instead of atomic Load on slice
	assert.Nil(t, gd.GetVectorsChunk(0), "Float32 vector storage (Vectors) should be empty/zero when Float16 is enabled")

	// F16 Vectors should be populated
	// Need unsafe cast as VectorsF16 is []float16.Num (uint16)
	// We want to check if chunk is allocated/non-zero
	chunkF16 := gd.GetVectorsF16Chunk(0)
	assert.NotNil(t, chunkF16)
	assert.Greater(t, len(chunkF16), 0)

	// Retrieve and Check Precision
	for i := 0; i < n; i++ {
		id := uint32(i + 1)
		// Internal helper mustGetVectorFromData is NOT exported.
		// But maybe there is exported GetVector?
		// We'll use mustGetVectorFromData if we can (internals test).
		// Since we are in package store, we can access unexported methods.

		retrieved := hnsw.mustGetVectorFromData(gd, id)

		var retrievedF32 []float32

		switch v := retrieved.(type) {
		case []float32:
			retrievedF32 = v
		case []float16.Num:
			retrievedF32 = make([]float32, dims)
			for idx, val := range v {
				retrievedF32[idx] = val.Float32()
			}
		default:
			require.Failf(t, "Unexpected vector type", "Got %T", v)
		}

		for j := 0; j < dims; j++ {
			diff := math.Abs(float64(vecs[i][j] - retrievedF32[j]))
			assert.Less(t, diff, 0.01)
		}
	}
}
