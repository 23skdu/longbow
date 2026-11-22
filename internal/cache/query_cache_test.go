package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestNewQueryCache runs the test new query cache operation.
func TestNewQueryCache(t *testing.T) {
	c := NewQueryCache[string](100, time.Minute, "test")
	assert.NotNil(t, c)
}

// TestQueryCacheGetSet runs the test query cache get set operation.
func TestQueryCacheGetSet(t *testing.T) {
	c := NewQueryCache[string](10, time.Minute, "test")

	val, ok := c.Get("missing")
	assert.False(t, ok)
	assert.Empty(t, val)

	c.Set("key1", "value1")
	val, ok = c.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, "value1", val)
}

// TestQueryCacheGetInt runs the test query cache get int operation.
func TestQueryCacheGetInt(t *testing.T) {
	c := NewQueryCache[string](10, time.Minute, "test")
	c.SetInt(42, "forty-two")
	val, ok := c.GetInt(42)
	assert.True(t, ok)
	assert.Equal(t, "forty-two", val)
}

// TestQueryCacheGetUint64 runs the test query cache get uint64 operation.
func TestQueryCacheGetUint64(t *testing.T) {
	c := NewQueryCache[string](10, time.Minute, "test")
	c.Set("100", "hundred")
	val, ok := c.GetUint64(100)
	assert.True(t, ok)
	assert.Equal(t, "hundred", val)
}

// TestQueryCachePut runs the test query cache put operation.
func TestQueryCachePut(t *testing.T) {
	c := NewQueryCache[string](10, time.Minute, "test")
	c.Put("a", "alpha")
	val, ok := c.Get("a")
	assert.True(t, ok)
	assert.Equal(t, "alpha", val)
}

// TestQueryCachePutInt runs the test query cache put int operation.
func TestQueryCachePutInt(t *testing.T) {
	c := NewQueryCache[string](10, time.Minute, "test")
	c.PutInt(1, "one")
	val, ok := c.GetInt(1)
	assert.True(t, ok)
	assert.Equal(t, "one", val)
}

// TestQueryCachePutUint64 runs the test query cache put uint64 operation.
func TestQueryCachePutUint64(t *testing.T) {
	c := NewQueryCache[string](10, time.Minute, "test")
	c.PutUint64(uint64(99), "ninety-nine")
	val, ok := c.GetUint64(99)
	assert.True(t, ok)
	assert.Equal(t, "ninety-nine", val)
}

// TestQueryCacheClear runs the test query cache clear operation.
func TestQueryCacheClear(t *testing.T) {
	c := NewQueryCache[string](10, time.Minute, "test")
	c.Set("k", "v")
	c.Clear()
	_, ok := c.Get("k")
	assert.False(t, ok)
}

// TestQueryCacheOverride runs the test query cache override operation.
func TestQueryCacheOverride(t *testing.T) {
	c := NewQueryCache[string](10, time.Minute, "test")
	c.Set("k", "v1")
	c.Set("k", "v2")
	val, ok := c.Get("k")
	assert.True(t, ok)
	assert.Equal(t, "v2", val)
}
