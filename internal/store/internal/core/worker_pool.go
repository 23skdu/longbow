package core

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// SharedWorkerPool is a lightweight pool of persistent workers to avoid
// goroutine churn and scheduler overhead during parallel search and bulk insert.
type SharedWorkerPool struct {
	tasks chan func()
	wg    sync.WaitGroup
}

var (
	globalPool atomic.Pointer[SharedWorkerPool]
	poolOnce   sync.Once
)

// GetSharedPool returns the global shared worker pool.
func GetSharedPool() *SharedWorkerPool {
	poolOnce.Do(func() {
		numWorkers := runtime.GOMAXPROCS(0)
		p := &SharedWorkerPool{
			tasks: make(chan func(), numWorkers*256),
		}
		for i := 0; i < numWorkers; i++ {
			go p.worker()
		}
		globalPool.Store(p)
	})
	return globalPool.Load()
}

func (p *SharedWorkerPool) worker() {
	for task := range p.tasks {
		func() {
			defer func() {
				if r := recover(); r != nil {
					// In a real system, we'd log this.
					// For now, we just ensure the worker doesn't die.
				}
			}()
			task()
		}()
	}
}

// Submit adds a task to the pool.
func (p *SharedWorkerPool) Submit(task func()) {
	p.tasks <- task
}

// ParallelFor executes a loop in parallel using the worker pool.
func (p *SharedWorkerPool) ParallelFor(n int, chunkSize int, task func(start, end int)) {
	if n <= 0 {
		return
	}
	if chunkSize <= 0 {
		chunkSize = 1
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i += chunkSize {
		start := i
		end := i + chunkSize
		if end > n {
			end = n
		}

		wg.Add(1)
		p.Submit(func() {
			defer wg.Done()
			task(start, end)
		})
	}
	wg.Wait()
}
