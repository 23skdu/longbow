package concurrency

import (
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestLockFreeQueue_BasicOperations(t *testing.T) {
	q := NewLockFreeQueue[int]()

	if !q.IsEmpty() {
		t.Error("New queue should be empty")
	}

	q.Enqueue(42)
	q.Enqueue(17)
	q.Enqueue(89)

	if q.IsEmpty() {
		t.Error("Queue should not be empty after enqueuing")
	}

	val, ok := q.Dequeue()
	if !ok {
		t.Error("Dequeue failed")
	}
	if val != 42 {
		t.Errorf("Expected 42, got %d", val)
	}

	val, ok = q.Dequeue()
	if !ok {
		t.Error("Second dequeue failed")
	}
	if val != 17 {
		t.Errorf("Expected 17, got %d", val)
	}

	val, ok = q.Dequeue()
	if !ok {
		t.Error("Third dequeue failed")
	}
	if val != 89 {
		t.Errorf("Expected 89, got %d", val)
	}

	if !q.IsEmpty() {
		t.Error("Queue should be empty after dequeuing all items")
	}
}

func TestLockFreeQueue_ConcurrentAccess(t *testing.T) {
	q := NewLockFreeQueue[int]()

	const numGoroutines = 10
	const numItems = 1000

	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numItems/numGoroutines; j++ {
				q.Enqueue(i*100 + j)
			}
		}()
	}

	wg.Wait()

	items := make([]int, 0, numItems)

	for !q.IsEmpty() {
		val, ok := q.Dequeue()
		if !ok {
			t.Error("Concurrent dequeue failed")
			return
		}
		items = append(items, val)
	}

	if len(items) != numItems {
		t.Errorf("Expected %d items, got %d", numItems, len(items))
	}
}

func TestLockFreeStack_BasicOperations(t *testing.T) {
	s := NewLockFreeStack[int]()

	s.Push(42)
	s.Push(17)
	s.Push(89)

	val, ok := s.Pop()
	if !ok {
		t.Error("Pop failed")
	}
	if val != 89 {
		t.Errorf("Expected 89 (LIFO), got %d", val)
	}

	val, ok = s.Pop()
	if !ok {
		t.Error("Second pop failed")
	}
	if val != 17 {
		t.Errorf("Expected 17 (LIFO), got %d", val)
	}

	val, ok = s.Pop()
	if !ok {
		t.Error("Third pop failed")
	}
	if val != 42 {
		t.Errorf("Expected 42 (LIFO), got %d", val)
	}

	if _, ok := s.Pop(); ok {
		t.Error("Stack should be empty after popping all items")
	}
}

func TestWorkStealingScheduler_Basic(t *testing.T) {
	s := NewWorkStealingScheduler[func()](4)

	submitted := make([]int, 0)

	var noop func()

	s.Submit(func() noop { submitted = append(submitted, 1) })
	s.Submit(func() noop { submitted = append(submitted, 2) })
	s.Submit(func() noop { submitted = append(submitted, 3) })
	s.Submit(func() noop { submitted = append(submitted, 4) })

	s.Start()
	time.Sleep(10 * time.Millisecond)
	s.Stop()

	if len(submitted) != 4 {
		t.Errorf("Expected 4 submitted tasks, got %d", len(submitted))
	}
}

func TestShardedMutex_ReducedContention(t *testing.T) {
	sm := NewShardedMutex[string](16)

	const numGoroutines = 100
	const numOperations = 1000

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numOperations/numGoroutines; j++ {
				sm.Lock("test")
				sm.Unlock("test")
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	t.Logf("ShardedMutex: %d operations in %v with %d goroutines", numOperations, duration, numGoroutines)

	if duration > time.Second {
		t.Error("ShardedMutex took too long")
	}
}

func BenchmarkLockFreeQueue_EnqueueDequeue(b *testing.B) {
	q := NewLockFreeQueue[int]()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Enqueue(i)
		_, ok := q.Dequeue()
		if !ok {
			b.Fatalf("Dequeue failed")
		}
	}
}

func BenchmarkLockFreeStack_PushPop(b *testing.B) {
	s := NewLockFreeStack[int]()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Push(i)
		_, ok := s.Pop()
		if !ok {
			b.Fatalf("Pop failed")
		}
	}
}

func BenchmarkWorkStealingScheduler_Throughput(b *testing.B) {
	s := NewWorkStealingScheduler[func()](runtime.NumCPU())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Submit(func() {})
	}
}

func BenchmarkShardedMutex_Throughput(b *testing.B) {
	sm := NewShardedMutex[string](16)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sm.Lock("test")
		sm.Unlock("test")
	}
}
