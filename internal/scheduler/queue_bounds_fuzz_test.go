package scheduler

import (
	"runtime"
	"testing"
)

// FuzzWorkQueueSubmitCapacity verifies the sizing math of NewWorkQueue:
// non-positive sizes fall back to documented defaults, exactly bufferSize
// submissions are accepted before the queue reports full, backlog/len agree,
// and Close is idempotent.
func FuzzWorkQueueSubmitCapacity(f *testing.F) {
	f.Add(4, 2, "item-a")
	f.Add(0, 0, "")
	f.Add(1, 1, "x")
	f.Add(100, 4, "payload")
	f.Add(-1, -1, "negative")
	f.Add(4096, 64, "large")

	f.Fuzz(func(t *testing.T, bufferSize, workers int, item string) {
		if bufferSize < -1 || bufferSize > 4096 {
			return
		}
		if workers < -1 || workers > 64 {
			return
		}

		wq := NewWorkQueue(bufferSize, workers)

		wantCap := bufferSize
		if wantCap <= 0 {
			wantCap = 1024 // documented default
		}
		if got := cap(wq.items); got != wantCap {
			t.Fatalf("channel capacity = %d, want %d", got, wantCap)
		}

		wantWorkers := workers
		if wantWorkers <= 0 {
			wantWorkers = runtime.NumCPU() // documented default
		}
		if wq.workers != wantWorkers {
			t.Fatalf("worker count = %d, want %d", wq.workers, wantWorkers)
		}

		accepted := 0
		for i := 0; i <= wantCap; i++ {
			if wq.Submit(item) {
				accepted++
			}
		}
		if accepted != wantCap {
			t.Fatalf("accepted %d submissions, want exactly %d", accepted, wantCap)
		}
		if got := wq.Backlog(); got != int64(wantCap) {
			t.Fatalf("Backlog() = %d, want %d", got, wantCap)
		}
		if got := wq.Len(); got != wantCap {
			t.Fatalf("Len() = %d, want %d", got, wantCap)
		}

		if err := wq.Close(); err != nil {
			t.Fatalf("Close() = %v, want nil", err)
		}
		if err := wq.Close(); err != nil {
			t.Fatalf("second Close() = %v, want nil (idempotent)", err)
		}
	})
}

// FuzzPriorityWorkQueueClampsPriority verifies that out-of-range priorities
// are clamped into [PriorityLow, PriorityHigh] and that the item is delivered
// to exactly the clamped priority queue.
func FuzzPriorityWorkQueueClampsPriority(f *testing.F) {
	f.Add(-5, "low")
	f.Add(0, "normal")
	f.Add(1, "high")
	f.Add(2, "high-boundary")
	f.Add(3, "over")
	f.Add(100, "way-over")
	f.Add(-1000, "way-under")

	f.Fuzz(func(t *testing.T, priority int, item string) {
		if priority < -1000 || priority > 1000 {
			return
		}
		if len(item) > 256 {
			return
		}

		pwq := NewPriorityWorkQueue(4, 1)

		want := priority
		if want < int(PriorityLow) {
			want = int(PriorityLow)
		}
		if want > int(PriorityHigh) {
			want = int(PriorityHigh)
		}

		if !pwq.Submit(item, Priority(priority)) {
			t.Fatalf("Submit(%q, %d) unexpectedly rejected on empty queue", item, priority)
		}

		for p := 0; p < 3; p++ {
			wantLen := 0
			if p == want {
				wantLen = 1
			}
			if got := len(pwq.queues[p]); got != wantLen {
				t.Fatalf("queue %d holds %d items, want %d (submitted priority %d)",
					p, got, wantLen, priority)
			}
		}

		got := <-pwq.queues[want]
		if got.Item != item {
			t.Fatalf("delivered item = %v, want %q", got.Item, item)
		}
		if int(got.Priority) != want {
			t.Fatalf("delivered priority = %d, want %d", got.Priority, want)
		}

		if err := pwq.Close(); err != nil {
			t.Fatalf("Close() = %v, want nil", err)
		}
	})
}
