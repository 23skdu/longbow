package core

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// SharedWorkerPool is a lightweight pool of persistent workers to avoid
// goroutine churn and scheduler overhead during parallel search and bulk insert.
// SharedWorkerPool is a lightweight pool of persistent workers to avoid
// goroutine churn and scheduler overhead during parallel search and bulk insert.
type SharedWorkerPool struct {
	numWorkers int
	shards     []chan func()
	nextShard  uint32
}

var (
	globalPool atomic.Pointer[SharedWorkerPool]
	poolOnce   sync.Once
)

// GetSharedPool returns the global shared worker pool.
func GetSharedPool() *SharedWorkerPool {
	poolOnce.Do(func() {
		numWorkers := runtime.GOMAXPROCS(0)
		if numWorkers < 1 {
			numWorkers = 1
		}
		p := &SharedWorkerPool{
			numWorkers: numWorkers,
			shards:     make([]chan func(), numWorkers),
		}
		for i := 0; i < numWorkers; i++ {
			p.shards[i] = make(chan func(), 1024)
			go p.worker(p.shards[i])
		}
		globalPool.Store(p)
	})
	return globalPool.Load()
}

func (p *SharedWorkerPool) worker(tasks chan func()) {
	for task := range tasks {
		func() {
			defer func() {
				if r := recover(); r != nil {
					// Log and recover
				}
			}()
			task()
		}()
	}
}

// Submit adds a task to the pool using round-robin distribution.
func (p *SharedWorkerPool) Submit(task func()) {
	shardIdx := atomic.AddUint32(&p.nextShard, 1) % uint32(p.numWorkers) // #nosec G115
	p.shards[shardIdx] <- task
}

// ParallelFor executes a loop in parallel using the worker pool.
func (p *SharedWorkerPool) ParallelFor(n int, chunkSize int, task func(start, end int)) {
	if n <= 0 {
		return
	}
	if chunkSize <= 0 {
		chunkSize = 1
	}

	numChunks := (n + chunkSize - 1) / chunkSize
	var wg sync.WaitGroup
	wg.Add(numChunks)

	for i := 0; i < n; i += chunkSize {
		start := i
		end := i + chunkSize
		if end > n {
			end = n
		}

		p.Submit(func() {
			defer wg.Done()
			task(start, end)
		})
	}
	wg.Wait()
}
