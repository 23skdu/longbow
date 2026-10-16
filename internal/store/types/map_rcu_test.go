package types

import (
	"fmt"
	"sync"
	"testing"
)

func TestMapRCUBasic(t *testing.T) {
	m := NewMapRCU[string, int]()

	if m.Len() != 0 {
		t.Fatalf("expected 0 len, got %d", m.Len())
	}

	m.Store("apple", 1)
	m.Store("banana", 2)
	m.Store("cherry", 3)

	if m.Len() != 3 {
		t.Fatalf("expected 3 len, got %d", m.Len())
	}

	val, ok := m.Get("apple")
	if !ok || val != 1 {
		t.Fatalf("expected 1, got %v (ok=%v)", val, ok)
	}

	val, ok = m.Get("banana")
	if !ok || val != 2 {
		t.Fatalf("expected 2, got %v (ok=%v)", val, ok)
	}

	val, ok = m.Get("unknown")
	if ok {
		t.Fatalf("expected not found for unknown key")
	}

	// Update key
	m.Store("apple", 100)
	val, ok = m.Get("apple")
	if !ok || val != 100 {
		t.Fatalf("expected 100, got %v (ok=%v)", val, ok)
	}

	// Delete key
	m.Delete("apple")
	_, ok = m.Get("apple")
	if ok {
		t.Fatalf("expected apple to be deleted")
	}
	if m.Len() != 2 {
		t.Fatalf("expected 2 len, got %d", m.Len())
	}
}

func TestMapRCUBulkStoreAndBulkDelete(t *testing.T) {
	m := NewMapRCU[int, string]()

	// Bulk store 1000 items
	batch1 := make(map[int]string, 1000)
	for i := 0; i < 1000; i++ {
		batch1[i] = fmt.Sprintf("val-%d", i)
	}
	m.BulkStore(batch1)

	if m.Len() != 1000 {
		t.Fatalf("expected 1000 items, got %d", m.Len())
	}

	for i := 0; i < 1000; i++ {
		v, ok := m.Get(i)
		expected := fmt.Sprintf("val-%d", i)
		if !ok || v != expected {
			t.Fatalf("key %d: expected %s, got %s (ok=%v)", i, expected, v, ok)
		}
	}

	// Bulk delete first 500 items
	toDelete := make([]int, 500)
	for i := 0; i < 500; i++ {
		toDelete[i] = i
	}
	m.BulkDelete(toDelete)

	if m.Len() != 500 {
		t.Fatalf("expected 500 items after delete, got %d", m.Len())
	}

	for i := 0; i < 500; i++ {
		if _, ok := m.Get(i); ok {
			t.Fatalf("key %d should have been deleted", i)
		}
	}
	for i := 500; i < 1000; i++ {
		v, ok := m.Get(i)
		expected := fmt.Sprintf("val-%d", i)
		if !ok || v != expected {
			t.Fatalf("key %d: expected %s, got %s (ok=%v)", i, expected, v, ok)
		}
	}

	// BulkStore small batch (<= 8 items branch)
	smallBatch := map[int]string{
		1001: "one",
		1002: "two",
	}
	m.BulkStore(smallBatch)
	if v, ok := m.Get(1001); !ok || v != "one" {
		t.Fatalf("small batch store failed")
	}

	// BulkDelete small batch (<= 8 items branch)
	m.BulkDelete([]int{1001, 1002})
	if _, ok := m.Get(1001); ok {
		t.Fatalf("small batch delete failed")
	}
}

func TestMapRCURangeAndLoad(t *testing.T) {
	m := NewMapRCU[string, int]()
	expected := map[string]int{
		"a": 10,
		"b": 20,
		"c": 30,
		"d": 40,
	}
	m.BulkStore(expected)

	// Test Range
	visited := make(map[string]int)
	m.Range(func(k string, v int) bool {
		visited[k] = v
		return true
	})
	if len(visited) != len(expected) {
		t.Fatalf("expected %d items visited, got %d", len(expected), len(visited))
	}

	// Test Range early stop
	count := 0
	m.Range(func(k string, v int) bool {
		count++
		return count < 2
	})
	if count != 2 {
		t.Fatalf("expected early stop at 2, got %d", count)
	}

	// Test Load
	snap := m.Load()
	if len(snap) != len(expected) {
		t.Fatalf("expected snapshot len %d, got %d", len(expected), len(snap))
	}
	for k, v := range expected {
		if snap[k] != v {
			t.Fatalf("snapshot mismatch for key %s: expected %d, got %d", k, v, snap[k])
		}
	}
}

func TestMapRCUKeyTypes(t *testing.T) {
	// uint64
	mU64 := NewMapRCU[uint64, bool]()
	mU64.Store(123456789012345, true)
	if v, ok := mU64.Get(123456789012345); !ok || !v {
		t.Fatalf("uint64 key failed")
	}

	// uint32
	mU32 := NewMapRCU[uint32, string]()
	mU32.Store(42, "hello")
	if v, ok := mU32.Get(42); !ok || v != "hello" {
		t.Fatalf("uint32 key failed")
	}

	// int64
	mI64 := NewMapRCU[int64, string]()
	mI64.Store(-987654321, "neg")
	if v, ok := mI64.Get(-987654321); !ok || v != "neg" {
		t.Fatalf("int64 key failed")
	}

	// int32
	mI32 := NewMapRCU[int32, string]()
	mI32.Store(-42, "neg32")
	if v, ok := mI32.Get(-42); !ok || v != "neg32" {
		t.Fatalf("int32 key failed")
	}

	// Generic struct key fallback
	type customKey struct {
		id   int
		name string
	}
	mCustom := NewMapRCU[customKey, float64]()
	k := customKey{id: 7, name: "seven"}
	mCustom.Store(k, 7.77)
	if v, ok := mCustom.Get(k); !ok || v != 7.77 {
		t.Fatalf("customKey failed")
	}
}

func TestMapRCUConcurrentRace(t *testing.T) {
	m := NewMapRCU[int, int]()
	const numGoroutines = 8
	const opsPerGoroutine = 500

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 3)

	// Writers (single Store)
	for g := 0; g < numGoroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := (gid * opsPerGoroutine) + i
				m.Store(key, i)
			}
		}(g)
	}

	// Bulk Writers and Deletions
	for g := 0; g < numGoroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				batch := make(map[int]int, 10)
				for j := 0; j < 10; j++ {
					k := 100000 + (gid * 100) + j
					batch[k] = j
				}
				m.BulkStore(batch)

				deletes := make([]int, 5)
				for j := 0; j < 5; j++ {
					deletes[j] = 100000 + (gid * 100) + j
				}
				m.BulkDelete(deletes)
			}
		}(g)
	}

	// Readers
	for g := 0; g < numGoroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := (gid * opsPerGoroutine) + i
				m.Get(key)
				_ = m.Len()
			}
		}(g)
	}

	wg.Wait()
}
