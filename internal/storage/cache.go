package storage

import (
	"container/list"
	"sync"
)

// LRUCache is a simple thread-safe LRU cache.
type LRUCache struct {
	mu        sync.Mutex
	maxBytes  int64
	currBytes int64
	items     map[string]*list.Element
	evictList *list.List
}

type entry struct {
	key   string
	value []byte
}

func NewLRUCache(maxBytes int64) *LRUCache {
	return &LRUCache{
		maxBytes:  maxBytes,
		items:     make(map[string]*list.Element),
		evictList: list.New(),
	}
}

func (c *LRUCache) Put(key string, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	size := int64(len(value))
	if size > c.maxBytes {
		return // Too big
	}

	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		c.currBytes -= int64(len(ent.Value.(*entry).value))
		ent.Value.(*entry).value = value
		c.currBytes += size
	} else {
		ent := c.evictList.PushFront(&entry{key, value})
		c.items[key] = ent
		c.currBytes += size
	}

	for c.currBytes > c.maxBytes {
		c.removeOldest()
	}
}

func (c *LRUCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		return ent.Value.(*entry).value, true
	}
	return nil, false
}

func (c *LRUCache) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.evictList.Remove(ent)
		kv := ent.Value.(*entry)
		delete(c.items, kv.key)
		c.currBytes -= int64(len(kv.value))
	}
}
