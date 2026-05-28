package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewQueryCache(t *testing.T) {
	c := NewQueryCache[string](100, time.Minute, "test")
	assert.NotNil(t, c)
}

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

func TestQueryCacheGetInt(t *testing.T) {
	c := NewQueryCache[string](10, time.Minute, "test")
	c.SetInt(42, "forty-two")
	val, ok := c.GetInt(42)
	assert.True(t, ok)
	assert.Equal(t, "forty-two", val)
}

func TestQueryCacheGetUint64(t *testing.T) {
	c := NewQueryCache[string](10, time.Minute, "test")
	c.Set("100", "hundred")
	val, ok := c.GetUint64(100)
	assert.True(t, ok)
	assert.Equal(t, "hundred", val)
}

func TestQueryCachePut(t *testing.T) {
	c := NewQueryCache[string](10, time.Minute, "test")
	c.Put("a", "alpha")
	val, ok := c.Get("a")
	assert.True(t, ok)
	assert.Equal(t, "alpha", val)
}

func TestQueryCachePutInt(t *testing.T) {
	c := NewQueryCache[string](10, time.Minute, "test")
	c.PutInt(1, "one")
	val, ok := c.GetInt(1)
	assert.True(t, ok)
	assert.Equal(t, "one", val)
}

func TestQueryCachePutUint64(t *testing.T) {
	c := NewQueryCache[string](10, time.Minute, "test")
	c.PutUint64(uint64(99), "ninety-nine")
	val, ok := c.GetUint64(99)
	assert.True(t, ok)
	assert.Equal(t, "ninety-nine", val)
}

func TestQueryCacheClear(t *testing.T) {
	c := NewQueryCache[string](10, time.Minute, "test")
	c.Set("k", "v")
	c.Clear()
	_, ok := c.Get("k")
	assert.False(t, ok)
}

func TestQueryCacheOverride(t *testing.T) {
	c := NewQueryCache[string](10, time.Minute, "test")
	c.Set("k", "v1")
	c.Set("k", "v2")
	val, ok := c.Get("k")
	assert.True(t, ok)
	assert.Equal(t, "v2", val)
}
