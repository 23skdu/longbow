package cache

import (
	"fmt"
	"sync"
	"time"
)

type QueryCache[T any] struct {
	mu      sync.RWMutex
	data    map[string]T
	ttl     time.Duration
	expires map[string]time.Time
}

func NewQueryCache[T any](capacity int, ttl time.Duration, namespace string) *QueryCache[T] {
	return &QueryCache[T]{
		data:    make(map[string]T, capacity),
		ttl:     ttl,
		expires: make(map[string]time.Time, capacity),
	}
}

func (c *QueryCache[T]) Get(key string) (T, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	val, ok := c.data[key]
	return val, ok
}

func (c *QueryCache[T]) GetInt(key int) (T, bool) {
	return c.Get(fmt.Sprintf("%d", key))
}

func (c *QueryCache[T]) GetUint64(key uint64) (T, bool) {
	return c.Get(fmt.Sprintf("%d", key))
}

func (c *QueryCache[T]) Set(key string, val T) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = val
	c.expires[key] = time.Now().Add(c.ttl)
}

func (c *QueryCache[T]) SetInt(key int, val T) {
	c.Set(fmt.Sprintf("%d", key), val)
}

func (c *QueryCache[T]) Put(key string, val T) {
	c.Set(key, val)
}

func (c *QueryCache[T]) PutInt(key int, val T) {
	c.SetInt(key, val)
}

func (c *QueryCache[T]) PutUint64(key uint64, val T) {
	c.Put(fmt.Sprintf("%d", key), val)
}

func (c *QueryCache[T]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data = make(map[string]T)
	c.expires = make(map[string]time.Time)
}

type CacheStats struct {
	Hits   int64
	Misses int64
	Items  int
}
