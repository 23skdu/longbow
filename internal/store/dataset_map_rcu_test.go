package store

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ProtoRCU is a prototype of the logic we want to implement in VectorStore.
// It manages a map[string]int atomically.
type ProtoRCU struct {
	// data holds *map[string]int
	data atomic.Pointer[map[string]int]
}

func NewProtoRCU() *ProtoRCU {
	p := &ProtoRCU{}
	m := make(map[string]int)
	p.data.Store(&m)
	return p
}

func (p *ProtoRCU) Get(key string) (int, bool) {
	m := *p.data.Load()
	val, ok := m[key]
	return val, ok
}

func (p *ProtoRCU) Len() int {
	return len(*p.data.Load())
}

// Set implements the Read-Copy-Update loop
func (p *ProtoRCU) Set(key string, val int) {
	for {
		// 1. Read
		oldPtr := p.data.Load()
		oldMap := *oldPtr

		// 2. Copy
		newMap := make(map[string]int, len(oldMap)+1)
		for k, v := range oldMap {
			newMap[k] = v
		}

		// 3. Update
		newMap[key] = val

		// 4. Swap (CAS)
		if p.data.CompareAndSwap(oldPtr, &newMap) {
			return
		}
		// Contention detected, retry
	}
}

// Delete implements RCU delete
func (p *ProtoRCU) Delete(key string) {
	for {
		oldPtr := p.data.Load()
		oldMap := *oldPtr

		// Optimization: If key didn't exist, do nothing
		// (But in atomic map, checking existence in old map is safe,
		// but we must be careful if we want strictly serializable behavior.
		// For RCU, we just proceed or check.)
		if _, ok := oldMap[key]; !ok {
			return
		}

		newMap := make(map[string]int, len(oldMap))
		for k, v := range oldMap {
			if k != key {
				newMap[k] = v
			}
		}

		if p.data.CompareAndSwap(oldPtr, &newMap) {
			return
		}
	}
}

func TestProtoRCU_Correctness(t *testing.T) {
	rcu := NewProtoRCU()

	// Basic Set/Get
	rcu.Set("a", 1)
	if v, ok := rcu.Get("a"); !ok || v != 1 {
		t.Errorf("Get failed: got %v, %v", v, ok)
	}

	// Update
	rcu.Set("a", 2)
	if v, _ := rcu.Get("a"); v != 2 {
		t.Errorf("Update failed: got %v", v)
	}

	// Delete
	rcu.Delete("a")
	if _, ok := rcu.Get("a"); ok {
		t.Error("Delete failed: key still exists")
	}
}

func TestProtoRCU_Concurrency(t *testing.T) {
	rcu := NewProtoRCU()
	var wg sync.WaitGroup

	// 100 Readers
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				// Just read, expect no panic
				rcu.Get("foo")
				rcu.Len()
			}
		}()
	}

	// 10 Writers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", id)
			for j := 0; j < 100; j++ {
				rcu.Set(key, j)
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	wg.Wait()

	// Verify final state roughly (writers finished)
	if rcu.Len() < 10 {
		t.Errorf("Expected at least 10 keys, got %d", rcu.Len())
	}
}

func TestProtoRCU_CompareAndSwap_Spin(t *testing.T) {
	rcu := NewProtoRCU()
	rcu.Set("race", 0)

	var wg sync.WaitGroup
	// High contention on single key
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				oldPtr := rcu.data.Load()
				oldMap := *oldPtr
				val := oldMap["race"]

				newMap := make(map[string]int, len(oldMap))
				for k, v := range oldMap {
					newMap[k] = v
				}
				newMap["race"] = val + 1

				if rcu.data.CompareAndSwap(oldPtr, &newMap) {
					return
				}
				// yield/sleep not needed for correctness but helps test finish
			}
		}()
	}
	wg.Wait()

	if v, _ := rcu.Get("race"); v != 100 {
		t.Errorf("Race increments failed: got %d, want 100", v)
	}
}

func TestVectorStore_RCU_Integration_Stub(t *testing.T) {
	// This test will be filled in once VectorStore is updated
	t.Skip("Pending implementation")
}
