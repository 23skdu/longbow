package core

import (
	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Tuner interface for observing system load
type Tuner interface {
	IsBursting() bool
}

// SharedWorkerPool is a lightweight pool of persistent workers to avoid
// goroutine churn and scheduler overhead during parallel search and bulk insert.
type SharedWorkerPool struct {
	numWorkers int
	shards     []chan func()
	nextShard  uint32
	tuner      atomic.Value // Stores Tuner interface

	// NUMA-aware pooling
	topo      *memory.NUMATopology
	nodePools [][]chan func()
	nodeRobin []uint32
}

var (
	globalPool atomic.Pointer[SharedWorkerPool]
	poolOnce   sync.Once
)

// GetSharedPool returns the global shared worker pool.
func GetSharedPool() *SharedWorkerPool {
	poolOnce.Do(func() {
		topo, _ := memory.DetectNUMATopology()
		numWorkers := runtime.GOMAXPROCS(0)
		if numWorkers < 1 {
			numWorkers = 1
		}

		p := &SharedWorkerPool{
			numWorkers: numWorkers,
			shards:     make([]chan func(), numWorkers),
			topo:       topo,
			nodePools:  make([][]chan func(), topo.NumNodes),
			nodeRobin:  make([]uint32, topo.NumNodes),
		}

		// Initialize per-node pools
		workersPerNode := numWorkers / topo.NumNodes
		if workersPerNode < 1 {
			workersPerNode = 1
		}

		workerIdx := 0
		for n := 0; n < topo.NumNodes; n++ {
			p.nodePools[n] = make([]chan func(), workersPerNode)
			cpus := topo.CPUs[n]
			for w := 0; w < workersPerNode; w++ {
				ch := make(chan func(), 1024)
				p.nodePools[n][w] = ch
				
				coreID := -1
				if len(cpus) > 0 {
					coreID = cpus[w%len(cpus)]
				}

				if workerIdx < numWorkers {
					p.shards[workerIdx] = ch
					workerIdx++
				}
				go p.numaWorker(ch, n, coreID)
			}
		}

		// Handle remaining workers if numWorkers not divisible by NumNodes
		for workerIdx < numWorkers {
			ch := make(chan func(), 1024)
			p.shards[workerIdx] = ch
			p.nodePools[0] = append(p.nodePools[0], ch)
			
			coreID := -1
			cpus := topo.CPUs[0]
			if len(cpus) > 0 {
				coreID = cpus[workerIdx%len(cpus)]
			}
			
			go p.numaWorker(ch, 0, coreID)
			workerIdx++
		}

		globalPool.Store(p)
	})
	return globalPool.Load()
}

func (p *SharedWorkerPool) numaWorker(tasks chan func(), nodeID int, coreID int) {
	// Pin thread to specific core if available, otherwise NUMA node
	if coreID >= 0 {
		_ = memory.PinThreadToCore(coreID)
	} else if p.topo != nil && p.topo.NumNodes > 1 {
		_ = memory.PinToNUMANode(p.topo, nodeID)
	}

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

// SetTuner attaches a system tuner to the pool.
func (p *SharedWorkerPool) SetTuner(t Tuner) {
	p.tuner.Store(t)
}

// Submit adds a task to the pool using global round-robin distribution.
func (p *SharedWorkerPool) Submit(task func()) {
	shardIdx := atomic.AddUint32(&p.nextShard, 1) % uint32(p.numWorkers) // #nosec G115
	p.shards[shardIdx] <- task
}

// SubmitToNode adds a task to a specific NUMA node's pool.
func (p *SharedWorkerPool) SubmitToNode(nodeID int, task func()) {
	if nodeID < 0 || nodeID >= len(p.nodePools) {
		p.Submit(task)
		return
	}
	pools := p.nodePools[nodeID]
	if len(pools) == 0 {
		p.Submit(task)
		return
	}
	idx := atomic.AddUint32(&p.nodeRobin[nodeID], 1) % uint32(len(pools)) // #nosec G115
	pools[idx] <- task
}

// SubmitLowPriority adds a task that can be delayed if the system is bursting.
func (p *SharedWorkerPool) SubmitLowPriority(task func()) {
	tVal := p.tuner.Load()
	if tVal != nil {
		t := tVal.(Tuner)
		if t.IsBursting() {
			start := time.Now()
			for t.IsBursting() {
				runtime.Gosched()
				time.Sleep(10 * time.Millisecond)
			}
			metrics.IndexingPausedDurationSeconds.Add(time.Since(start).Seconds())
		}
	}
	p.Submit(task)
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
	if numChunks == 1 {
		task(0, n)
		return
	}

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
