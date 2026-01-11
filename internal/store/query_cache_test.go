package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueryCache_Basic(t *testing.T) {
	cache := NewQueryCache(2, 1*time.Second)

	q1 := []float32{1.0, 0.0}
	res1 := []SearchResult{{ID: 1, Score: 0.1}}
	params1 := "k=10,ef=100"

	// 1. Add to cache
	cache.Set(q1, params1, res1)

	// 2. Get from cache
	got, ok := cache.Get(q1, params1)
	assert.True(t, ok)
	assert.Equal(t, res1, got)

	// 3. Different query should miss
	q2 := []float32{0.0, 1.0}
	_, ok = cache.Get(q2, params1)
	assert.False(t, ok)

	// 4. Different params should miss
	params2 := "k=5,ef=50"
	_, ok = cache.Get(q1, params2)
	assert.False(t, ok)
}

func TestQueryCache_LRU(t *testing.T) {
	cache := NewQueryCache(2, 1*time.Minute)

	q1 := []float32{1.0}
	q2 := []float32{2.0}
	q3 := []float32{3.0}
	res := []SearchResult{{ID: 1}}

	cache.Set(q1, "", res)
	cache.Set(q2, "", res)

	// Access q1 to make it MRU
	cache.Get(q1, "")

	// Add q3, should evict q2
	cache.Set(q3, "", res)

	_, ok := cache.Get(q1, "")
	assert.True(t, ok, "q1 should still be in cache")

	_, ok = cache.Get(q3, "")
	assert.True(t, ok, "q3 should be in cache")

	_, ok = cache.Get(q2, "")
	assert.False(t, ok, "q2 should have been evicted")
}

func TestQueryCache_TTL(t *testing.T) {
	cache := NewQueryCache(10, 100*time.Millisecond)

	q := []float32{1.0}
	res := []SearchResult{{ID: 1}}

	cache.Set(q, "", res)

	got, ok := cache.Get(q, "")
	assert.True(t, ok)
	assert.Equal(t, res, got)

	// Wait for TTL to expire
	time.Sleep(150 * time.Millisecond)

	_, ok = cache.Get(q, "")
	assert.False(t, ok, "should have expired due to TTL")
}

func FuzzQueryCache(f *testing.F) {
	f.Add(float32(1.0), "k=10", 0)
	f.Fuzz(func(t *testing.T, val float32, params string, seed int) {
		cache := NewQueryCache(10, 1*time.Second)
		q := []float32{val}
		res := []SearchResult{{ID: VectorID(seed)}}

		cache.Set(q, params, res)
		got, ok := cache.Get(q, params)
		if !ok {
			t.Errorf("expected to find value in cache")
		}
		if len(got) != 1 || got[0].ID != VectorID(seed) {
			t.Errorf("wrong value returned from cache")
		}
	})
}
