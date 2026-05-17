package store

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMemVectorStore_OffHeap(t *testing.T) {
	opts := MemStoreOptions{
		UseOffHeap: true,
		Dim:        128,
	}

	ms, err := NewMemVectorStore(opts)
	assert.NoError(t, err)
	defer ms.Close()

	assert.True(t, ms.baseArena.IsOffHeap())

	// Test Set and Get
	key := "vec1"
	vec := make([]float32, 128)
	for i := range vec {
		vec[i] = float32(i)
	}

	err = ms.Set(key, vec)
	assert.NoError(t, err)

	out, ok := ms.Get(key)
	assert.True(t, ok)
	assert.Equal(t, vec, out)

	// Test Get not found
	_, ok = ms.Get("nonexistent")
	assert.False(t, ok)

	// Verify metrics
	// metrics.ArenaOffHeapBytes should be around 128 * 4 = 512 bytes
	val := testutil.ToFloat64(metrics.ArenaOffHeapBytes)
	assert.GreaterOrEqual(t, val, float64(512))

	// Test Delete
	ok = ms.Delete(key)
	assert.True(t, ok)
	assert.True(t, ms.IsDeleted(key))

	// Test Delete not found
	ok = ms.Delete("nonexistent")
	assert.False(t, ok)

	// Test legacy path coverage
	msLegacy, _ := NewMemVectorStore(MemStoreOptions{Dim: 128})
	_ = msLegacy.Set("l1", vec)
	_, ok = msLegacy.Get("l1")
	assert.True(t, ok)
	_, ok = msLegacy.Get("l2")
	assert.False(t, ok)
	ok = msLegacy.Delete("l1")
	assert.True(t, ok)
	ok = msLegacy.Delete("l2")
	assert.False(t, ok)
}

func TestMemVectorStore_DeleteBatch(t *testing.T) {
	opts := MemStoreOptions{
		UseOffHeap: true,
		Dim:        4,
	}

	ms, err := NewMemVectorStore(opts)
	assert.NoError(t, err)
	defer ms.Close()

	keys := []string{"v1", "v2", "v3"}
	vec := []float32{1, 2, 3, 4}

	for _, k := range keys {
		_ = ms.Set(k, vec)
	}

	deleted := ms.DeleteBatch(keys[:2])
	assert.Equal(t, 2, deleted)
	assert.True(t, ms.IsDeleted("v1"))
	assert.True(t, ms.IsDeleted("v2"))
	assert.False(t, ms.IsDeleted("v3"))

	// Test legacy path (no arena)
	msLegacy, _ := NewMemVectorStore(MemStoreOptions{Dim: 4})
	for _, k := range keys {
		_ = msLegacy.Set(k, vec)
	}
	deleted = msLegacy.DeleteBatch(keys)
	assert.Equal(t, 3, deleted)
}

func TestMemVectorStore_OffHeapMismatch(t *testing.T) {
	opts := MemStoreOptions{
		UseOffHeap: true,
		Dim:        128,
	}

	ms, err := NewMemVectorStore(opts)
	assert.NoError(t, err)
	defer ms.Close()

	vec := make([]float32, 64) // Wrong dimension
	err = ms.Set("wrong", vec)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dimension mismatch")
}

func FuzzOffHeapVectorStore(f *testing.F) {
	f.Add(128, 10, int64(42))
	f.Fuzz(func(t *testing.T, dim int, count int, seed int64) {
		if dim <= 0 || dim > 2048 || count <= 0 || count > 100 {
			t.Skip()
		}

		opts := MemStoreOptions{
			UseOffHeap: true,
			Dim:        dim,
		}

		ms, err := NewMemVectorStore(opts)
		if err != nil {
			t.Fatalf("Failed to create store: %v", err)
		}
		defer ms.Close()

		rng := rand.New(rand.NewSource(seed))
		keys := make([]string, count)
		vecs := make([][]float32, count)

		for i := 0; i < count; i++ {
			keys[i] = fmt.Sprintf("key_%d", i)
			vecs[i] = make([]float32, dim)
			for j := range vecs[i] {
				vecs[i][j] = rng.Float32()
			}

			err := ms.Set(keys[i], vecs[i])
			if err != nil {
				t.Fatalf("Set failed at %d: %v", i, err)
			}
		}

		for i := 0; i < count; i++ {
			out, ok := ms.Get(keys[i])
			if !ok {
				t.Fatalf("Get failed at %d", i)
			}
			for j := range vecs[i] {
				if out[j] != vecs[i][j] {
					t.Fatalf("Data mismatch at %d index %d", i, j)
				}
			}
		}
	})
}
