package concurrency

import (
	"runtime"
	"testing"
)

// FuzzLockFreeQueueFIFO replays a fuzzed enqueue/dequeue sequence against a
// reference slice and asserts FIFO ordering plus IsEmpty parity.
func FuzzLockFreeQueueFIFO(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 1, 1, 1})
	f.Add([]byte{1, 1, 1}) // dequeues on an empty queue
	f.Add([]byte{0, 1, 0, 1, 0, 0, 1, 1})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0})
	f.Add(make([]byte, 64))

	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 128 {
			return
		}

		q := NewLockFreeQueue[int]()
		var ref []int
		for i, b := range ops {
			if b%2 == 0 {
				v := i + 1
				q.Enqueue(v)
				ref = append(ref, v)
			} else {
				got, ok := q.Dequeue()
				if len(ref) == 0 {
					if ok {
						t.Fatalf("op %d: dequeue from empty queue returned %d", i, got)
					}
				} else {
					if !ok {
						t.Fatalf("op %d: dequeue failed, want %d", i, ref[0])
					}
					if got != ref[0] {
						t.Fatalf("op %d: FIFO violation: got %d, want %d", i, got, ref[0])
					}
					ref = ref[1:]
				}
			}
			if wantEmpty := len(ref) == 0; q.IsEmpty() != wantEmpty {
				t.Fatalf("op %d: IsEmpty() = %v, want %v", i, q.IsEmpty(), wantEmpty)
			}
		}

		// Drain the remainder in FIFO order.
		for len(ref) > 0 {
			got, ok := q.Dequeue()
			if !ok {
				t.Fatalf("drain failed, want %d", ref[0])
			}
			if got != ref[0] {
				t.Fatalf("drain FIFO violation: got %d, want %d", got, ref[0])
			}
			ref = ref[1:]
		}
		if _, ok := q.Dequeue(); ok {
			t.Fatal("dequeue succeeded after drain")
		}
		if !q.IsEmpty() {
			t.Fatal("queue not empty after drain")
		}
	})
}

// FuzzLockFreeStackLIFO replays a fuzzed push/pop sequence against a
// reference slice and asserts LIFO ordering.
func FuzzLockFreeStackLIFO(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 1, 1, 1})
	f.Add([]byte{1, 1, 1}) // pops on an empty stack
	f.Add([]byte{0, 1, 0, 1, 0, 0, 1, 1})
	f.Add([]byte{0, 0, 0, 0, 1, 1, 1, 1})
	f.Add(make([]byte, 64))

	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 128 {
			return
		}

		s := NewLockFreeStack[int]()
		var ref []int
		for i, b := range ops {
			if b%2 == 0 {
				v := i + 1
				s.Push(v)
				ref = append(ref, v)
			} else {
				got, ok := s.Pop()
				if len(ref) == 0 {
					if ok {
						t.Fatalf("op %d: pop from empty stack returned %d", i, got)
					}
				} else {
					if !ok {
						t.Fatalf("op %d: pop failed, want %d", i, ref[len(ref)-1])
					}
					want := ref[len(ref)-1]
					if got != want {
						t.Fatalf("op %d: LIFO violation: got %d, want %d", i, got, want)
					}
					ref = ref[:len(ref)-1]
				}
			}
		}

		// Drain the remainder in LIFO order.
		for len(ref) > 0 {
			got, ok := s.Pop()
			if !ok {
				t.Fatalf("drain failed, want %d", ref[len(ref)-1])
			}
			want := ref[len(ref)-1]
			if got != want {
				t.Fatalf("drain LIFO violation: got %d, want %d", got, want)
			}
			ref = ref[:len(ref)-1]
		}
		if _, ok := s.Pop(); ok {
			t.Fatal("pop succeeded after drain")
		}
	})
}

// refShardedHash mirrors the string hashing used by ShardedMutex.
func refShardedHash(key string) int {
	h := 0
	for _, c := range key {
		h = h*31 + int(c)
	}
	return h
}

// FuzzShardedMutexHash checks the shard-key hash for determinism and parity
// with a simple reference implementation, and exercises Lock/Unlock plus
// RLock/RUnlock for keys whose shard index is in range.
func FuzzShardedMutexHash(f *testing.F) {
	f.Add("hello", int64(42))
	f.Add("", int64(0))
	f.Add("dataset-key-123", int64(-1))
	f.Add(string(make([]byte, 64)), int64(1<<40))
	f.Add("ünïcödé-ключ-日本語", int64(-987654321))
	f.Add("\xff\xfe\xfd\xfc\xfb", int64(1<<62))

	f.Fuzz(func(t *testing.T, key string, num int64) {
		if len(key) > 4096 {
			return
		}

		smStr := NewShardedMutex[string](0) // numShards < 1 falls back to 16
		if smStr.numShards != 16 {
			t.Fatalf("numShards = %d, want 16", smStr.numShards)
		}

		h := smStr.hash(key)
		if h != refShardedHash(key) {
			t.Fatalf("hash(%q) = %d, want %d", key, h, refShardedHash(key))
		}
		if again := smStr.hash(key); again != h {
			t.Fatalf("hash(%q) not deterministic: %d vs %d", key, h, again)
		}

		smInt := NewShardedMutex[int64](4)
		if got := smInt.hash(num); got != int(num) {
			t.Fatalf("hash(int64(%d)) = %d, want %d", num, got, int(num))
		}

		// Locking round-trips safely for keys whose shard index is valid.
		if idx := h % smStr.numShards; idx >= 0 && idx < smStr.numShards {
			smStr.Lock(key)
			smStr.Unlock(key)
			smStr.RLock(key)
			smStr.RUnlock(key)
		}
	})
}

// FuzzWorkQueueFIFO submits fuzzed tasks to a WorkStealingScheduler (never
// started, so no goroutines run) and asserts per-queue FIFO drain order and
// Stats consistency. All submissions land in the GOMAXPROCS-selected queue.
func FuzzWorkQueueFIFO(f *testing.F) {
	f.Add([]byte{}, uint8(1))
	f.Add([]byte{0, 0, 0, 1, 1, 1}, uint8(4))
	f.Add([]byte{0, 1, 0, 1}, uint8(16))
	f.Add([]byte{1, 1, 1}, uint8(2)) // gets on empty queues
	f.Add(make([]byte, 64), uint8(3))
	f.Add([]byte{0, 1, 2, 3, 4, 5}, uint8(0)) // out of range: workers < 1

	f.Fuzz(func(t *testing.T, ops []byte, workers uint8) {
		if workers < 1 || workers > 16 {
			return
		}
		if len(ops) > 64 {
			return
		}

		ws := NewWorkStealingScheduler[int](int(workers))
		// Submit routes to runtime.GOMAXPROCS(0) % workers; mirror it.
		qid := runtime.GOMAXPROCS(0) % int(workers)

		var ref []int
		for i, b := range ops {
			if b%2 == 0 {
				v := i + 1
				ws.Submit(v)
				ref = append(ref, v)
				continue
			}
			got, ok := ws.GetTask(qid)
			if len(ref) == 0 {
				if ok {
					t.Fatalf("op %d: unexpected task %d", i, got)
				}
			} else {
				if !ok {
					t.Fatalf("op %d: missing task %d", i, ref[0])
				}
				if got != ref[0] {
					t.Fatalf("op %d: FIFO violation: got %d, want %d", i, got, ref[0])
				}
				ref = ref[1:]
			}
		}

		st := ws.Stats()
		if st.NumWorkers != int(workers) {
			t.Fatalf("NumWorkers = %d, want %d", st.NumWorkers, workers)
		}
		if len(st.QueueSizes) != int(workers) {
			t.Fatalf("len(QueueSizes) = %d, want %d", len(st.QueueSizes), workers)
		}
		if st.QueueSizes[qid] != len(ref) {
			t.Fatalf("QueueSizes[%d] = %d, want %d", qid, st.QueueSizes[qid], len(ref))
		}
		if st.StealIndex != 0 {
			t.Fatalf("StealIndex = %d, want 0 (no steals occurred)", st.StealIndex)
		}

		// Drain the remainder from the receiving queue in submission order.
		for len(ref) > 0 {
			got, ok := ws.GetTask(qid)
			if !ok {
				t.Fatalf("drain failed, want %d", ref[0])
			}
			if got != ref[0] {
				t.Fatalf("drain FIFO violation: got %d, want %d", got, ref[0])
			}
			ref = ref[1:]
		}
		if got, ok := ws.GetTask(qid); ok {
			t.Fatalf("extra task after drain: %d", got)
		}

		st = ws.Stats()
		total := 0
		for _, n := range st.QueueSizes {
			total += n
		}
		if total != 0 {
			t.Fatalf("queues not empty after drain: %v", st.QueueSizes)
		}
	})
}
