package storage

import (
	"sync"
	"sync/atomic"
	"testing"
)

// TestWALRingBuffer_BasicOperations tests basic push and drain operations
func TestWALRingBuffer_BasicOperations(t *testing.T) {
	rb := NewWALRingBuffer(8)

	// Test empty buffer
	if rb.Len() != 0 {
		t.Errorf("Expected empty buffer, got len=%d", rb.Len())
	}

	// Push single entry
	entry := WALEntry{Name: "test1", Seq: 1}
	if !rb.Push(entry) {
		t.Fatal("Failed to push entry to empty buffer")
	}

	if rb.Len() != 1 {
		t.Errorf("Expected len=1, got %d", rb.Len())
	}

	// Drain entries
	drained := make([]WALEntry, 0, 8)
	count := rb.Drain(&drained)
	if count != 1 {
		t.Errorf("Expected to drain 1 entry, got %d", count)
	}
	if len(drained) != 1 || drained[0].Name != "test1" {
		t.Errorf("Drained entry mismatch: %+v", drained)
	}

	// Buffer should be empty again
	if rb.Len() != 0 {
		t.Errorf("Expected empty buffer after drain, got len=%d", rb.Len())
	}
}

// TestWALRingBuffer_FullBuffer tests behavior when buffer is full
func TestWALRingBuffer_FullBuffer(t *testing.T) {
	capacity := 4
	rb := NewWALRingBuffer(capacity)

	// Fill buffer to capacity
	for i := 0; i < capacity; i++ {
		entry := WALEntry{Name: "test", Seq: uint64(i)}
		if !rb.Push(entry) {
			t.Fatalf("Failed to push entry %d to buffer", i)
		}
	}

	// Next push should fail (buffer full)
	overflow := WALEntry{Name: "overflow", Seq: 999}
	if rb.Push(overflow) {
		t.Error("Expected push to fail on full buffer, but it succeeded")
	}

	// Drain should return all entries
	drained := make([]WALEntry, 0, capacity)
	count := rb.Drain(&drained)
	if count != capacity {
		t.Errorf("Expected to drain %d entries, got %d", capacity, count)
	}

	// Now push should succeed again
	if !rb.Push(overflow) {
		t.Error("Failed to push after drain")
	}
}

// TestWALRingBuffer_Wraparound tests that ring buffer correctly wraps around
func TestWALRingBuffer_Wraparound(t *testing.T) {
	rb := NewWALRingBuffer(4)

	// Push and drain multiple times to test wraparound
	for round := 0; round < 3; round++ {
		// Push 3 entries
		for i := 0; i < 3; i++ {
			entry := WALEntry{Name: "test", Seq: uint64(round*10 + i)}
			if !rb.Push(entry) {
				t.Fatalf("Round %d: Failed to push entry %d", round, i)
			}
		}

		// Drain all
		drained := make([]WALEntry, 0, 4)
		count := rb.Drain(&drained)
		if count != 3 {
			t.Errorf("Round %d: Expected 3 entries, got %d", round, count)
		}

		// Verify sequence numbers
		for i, entry := range drained {
			expected := uint64(round*10 + i)
			if entry.Seq != expected {
				t.Errorf("Round %d: Entry %d has seq=%d, expected %d", round, i, entry.Seq, expected)
			}
		}
	}
}

// TestWALRingBuffer_ConcurrentPushDrain tests concurrent producer/consumer
func TestWALRingBuffer_ConcurrentPushDrain(t *testing.T) {
	rb := NewWALRingBuffer(128)
	const numWrites = 10000

	var pushCount, drainCount atomic.Int64
	var wg sync.WaitGroup

	// Producer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numWrites; i++ {
			entry := WALEntry{Name: "test", Seq: uint64(i)}
			for !rb.Push(entry) {
				// Spin if buffer full (consumer will drain)
			}
			pushCount.Add(1)
		}
	}()

	// Consumer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		drained := make([]WALEntry, 0, 128)
		for drainCount.Load() < numWrites {
			drained = drained[:0]
			count := rb.Drain(&drained)
			drainCount.Add(int64(count))
		}
	}()

	wg.Wait()

	if pushCount.Load() != numWrites {
		t.Errorf("Expected %d pushes, got %d", numWrites, pushCount.Load())
	}
	if drainCount.Load() != numWrites {
		t.Errorf("Expected %d drained, got %d", numWrites, drainCount.Load())
	}
}

// BenchmarkWALRingBuffer_Push benchmarks push operations
func BenchmarkWALRingBuffer_Push(b *testing.B) {
	rb := NewWALRingBuffer(1024)
	entry := WALEntry{Name: "benchmark", Seq: 1}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !rb.Push(entry) {
			// Drain when full
			drained := make([]WALEntry, 0, 1024)
			rb.Drain(&drained)
		}
	}
}

// BenchmarkWALRingBuffer_PushDrain benchmarks combined push/drain
func BenchmarkWALRingBuffer_PushDrain(b *testing.B) {
	rb := NewWALRingBuffer(128)
	entry := WALEntry{Name: "benchmark", Seq: 1}
	drained := make([]WALEntry, 0, 128)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Push(entry)
		if i%100 == 0 {
			drained = drained[:0]
			rb.Drain(&drained)
		}
	}
}

// BenchmarkMutexSlice_Baseline benchmarks mutex-protected slice (baseline)
func BenchmarkMutexSlice_Baseline(b *testing.B) {
	var mu sync.Mutex
	batch := make([]WALEntry, 0, 128)
	entry := WALEntry{Name: "benchmark", Seq: 1}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		batch = append(batch, entry)
		if len(batch) >= 100 {
			batch = batch[:0]
		}
		mu.Unlock()
	}
}

// BenchmarkChannel_Baseline benchmarks channel-based batching (baseline)
func BenchmarkChannel_Baseline(b *testing.B) {
	ch := make(chan WALEntry, 128)
	entry := WALEntry{Name: "benchmark", Seq: 1}

	// Consumer
	go func() {
		for range ch {
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch <- entry
	}
	close(ch)
}
