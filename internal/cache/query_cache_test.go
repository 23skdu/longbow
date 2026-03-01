package cache

import (
	"testing"
	"time"
)

func TestQueryCache_PutAndGet(t *testing.T) {
	cache := NewQueryCache[string](2, time.Minute, "test")

	// Put values
	cache.Put(1, "value1")
	cache.Put(2, "value2")

	// Get values
	v, ok := cache.Get(1)
	if !ok {
		t.Error("expected to find key 1")
	}
	if v != "value1" {
		t.Errorf("expected value1, got %v", v)
	}

	v, ok = cache.Get(2)
	if !ok {
		t.Error("expected to find key 2")
	}
	if v != "value2" {
		t.Errorf("expected value2, got %v", v)
	}
}

func TestQueryCache_GetNotFound(t *testing.T) {
	cache := NewQueryCache[string](2, time.Minute, "test")

	_, ok := cache.Get(999)
	if ok {
		t.Error("expected not to find key 999")
	}
}

func TestQueryCache_Eviction(t *testing.T) {
	cache := NewQueryCache[string](2, time.Minute, "test")

	cache.Put(1, "value1")
	cache.Put(2, "value2")
	cache.Put(3, "value3") // Should evict key 1

	// Key 1 should be evicted
	_, ok := cache.Get(1)
	if ok {
		t.Error("expected key 1 to be evicted")
	}

	// Key 2 and 3 should still exist
	_, ok = cache.Get(2)
	if !ok {
		t.Error("expected to find key 2")
	}

	_, ok = cache.Get(3)
	if !ok {
		t.Error("expected to find key 3")
	}
}

func TestQueryCache_Expiration(t *testing.T) {
	cache := NewQueryCache[string](2, 50*time.Millisecond, "test")

	cache.Put(1, "value1")

	// Should exist initially
	v, ok := cache.Get(1)
	if !ok || v != "value1" {
		t.Error("expected to find key 1 initially")
	}

	// Wait for expiration
	time.Sleep(100 * time.Millisecond)

	// Should be expired
	_, ok = cache.Get(1)
	if ok {
		t.Error("expected key 1 to be expired")
	}
}

func TestQueryCache_Clear(t *testing.T) {
	cache := NewQueryCache[string](2, time.Minute, "test")

	cache.Put(1, "value1")
	cache.Put(2, "value2")

	cache.Clear()

	// All keys should be gone
	_, ok := cache.Get(1)
	if ok {
		t.Error("expected key 1 to be cleared")
	}
	_, ok = cache.Get(2)
	if ok {
		t.Error("expected key 2 to be cleared")
	}
}

func TestQueryCache_UpdateExisting(t *testing.T) {
	cache := NewQueryCache[string](2, time.Minute, "test")

	cache.Put(1, "value1")
	cache.Put(1, "value2") // Update existing

	v, ok := cache.Get(1)
	if !ok {
		t.Error("expected to find key 1")
	}
	if v != "value2" {
		t.Errorf("expected value2, got %v", v)
	}
}
