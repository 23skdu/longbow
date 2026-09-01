package index

import (
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
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
	topo                  *memory.NUMATopology
	nodePools             [][]chan func()
	highPriorityNodePools [][]chan func() // New: High-priority channels
	nodeRobin             []uint32
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
			numWorkers:            numWorkers,
			shards:                make([]chan func(), numWorkers),
			topo:                  topo,
			nodePools:             make([][]chan func(), topo.NumNodes),
			highPriorityNodePools: make([][]chan func(), topo.NumNodes),
			nodeRobin:             make([]uint32, topo.NumNodes),
		}

		// Initialize per-node pools
		workersPerNode := numWorkers / topo.NumNodes
		if workersPerNode < 1 {
			workersPerNode = 1
		}

		workerIdx := 0
		for n := 0; n < topo.NumNodes; n++ {
			p.nodePools[n] = make([]chan func(), workersPerNode)
			p.highPriorityNodePools[n] = make([]chan func(), workersPerNode)
			for w := 0; w < workersPerNode; w++ {
				ch := make(chan func(), 1024)
				hpCh := make(chan func(), 512) // Smaller buffer for high priority
				p.nodePools[n][w] = ch
				p.highPriorityNodePools[n][w] = hpCh

				coreID := -1
				cpus := topo.PhysicalCPUs[n]
				if len(cpus) == 0 {
					cpus = topo.CPUs[n]
				}
				if len(cpus) > 0 {
					coreID = cpus[w%len(cpus)]
				}

				if workerIdx < numWorkers {
					p.shards[workerIdx] = ch
					workerIdx++
				}
				go p.numaWorker(ch, hpCh, n, coreID)
			}
		}

		// Handle remaining workers if numWorkers not divisible by NumNodes
		for workerIdx < numWorkers {
			ch := make(chan func(), 1024)
			hpCh := make(chan func(), 512)
			p.shards[workerIdx] = ch
			p.nodePools[0] = append(p.nodePools[0], ch)
			p.highPriorityNodePools[0] = append(p.highPriorityNodePools[0], hpCh)

			coreID := -1
			cpus := topo.PhysicalCPUs[0]
			if len(cpus) == 0 {
				cpus = topo.CPUs[0]
			}
			if len(cpus) > 0 {
				coreID = cpus[workerIdx%len(cpus)]
			}

			go p.numaWorker(ch, hpCh, 0, coreID)
			workerIdx++
		}

		globalPool.Store(p)
	})
	return globalPool.Load()
}

func (p *SharedWorkerPool) numaWorker(tasks chan func(), hpTasks chan func(), nodeID int, coreID int) {
	// Pin thread to specific core if available, otherwise NUMA node
	if coreID >= 0 {
		_ = memory.PinThreadToCore(coreID)
	} else if p.topo != nil && p.topo.NumNodes > 1 {
		_ = memory.PinToNUMANode(p.topo, nodeID)
	}

	for {
		select {
		case task, ok := <-hpTasks:
			if !ok {
				return
			}
			executeTask(task)
		default:
			select {
			case task, ok := <-hpTasks:
				if !ok {
					return
				}
				executeTask(task)
			case task, ok := <-tasks:
				if !ok {
					return
				}
				executeTask(task)
			}
		}
	}
}

func executeTask(task func()) {
	defer func() {
		if r := recover(); r != nil {
			// Log panic with stack trace for debugging
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			log.Printf("worker pool: task panicked: %v\n%s", r, buf[:n])
		}
	}()
	task()
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

// SubmitHighPriority adds a high-priority task using global round-robin distribution.
func (p *SharedWorkerPool) SubmitHighPriority(task func()) {
	// Find the NUMA node for this thread or default to 0
	nodeID := 0
	if p.topo != nil && p.topo.NumNodes > 0 {
		// Use round-robin across nodes for high priority global submit
		nodeID = int(atomic.AddUint32(&p.nextShard, 1) % uint32(p.topo.NumNodes)) // #nosec G115
	}
	p.SubmitToNodeHighPriority(nodeID, task)
}

// SubmitToNode adds a task to a specific NUMA node's pool.
func (p *SharedWorkerPool) SubmitToNode(nodeID int, task func()) {
	p.submitToNodeInternal(nodeID, task, false)
}

// SubmitToNodeHighPriority adds a high-priority task to a specific NUMA node's pool.
func (p *SharedWorkerPool) SubmitToNodeHighPriority(nodeID int, task func()) {
	p.submitToNodeInternal(nodeID, task, true)
}

func (p *SharedWorkerPool) submitToNodeInternal(nodeID int, task func(), highPriority bool) {
	if nodeID < 0 || nodeID >= len(p.nodePools) {
		p.Submit(task) // Fallback to global
		return
	}

	var pools []chan func()
	if highPriority {
		pools = p.highPriorityNodePools[nodeID]
	} else {
		pools = p.nodePools[nodeID]
	}

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
	p.parallelForInternal(n, chunkSize, task, false)
}

// ParallelForHighPriority executes a loop in parallel with high priority.
func (p *SharedWorkerPool) ParallelForHighPriority(n int, chunkSize int, task func(start, end int)) {
	p.parallelForInternal(n, chunkSize, task, true)
}

func (p *SharedWorkerPool) parallelForInternal(n int, chunkSize int, task func(start, end int), highPriority bool) {
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

	numHelpers := p.numWorkers - 1
	if numHelpers > numChunks-1 {
		numHelpers = numChunks - 1
	}
	if numHelpers < 0 {
		numHelpers = 0
	}

	// Pre-partition chunks among all workers (helpers + caller) to eliminate
	// shared atomic counter contention. Each worker processes a contiguous
	// range of chunks with zero atomic operations.
	totalWorkers := numHelpers + 1
	base := numChunks / totalWorkers
	rem := numChunks % totalWorkers

	// Worker i (0 = caller, 1+ = helpers) gets:
	//   count = base+1 if i < rem else base
	//   start = i*base + min(i, rem)
	//   end   = start + count
	workerRange := func(workerID int) (int, int) {
		cnt := base
		if workerID < rem {
			cnt = base + 1
		}
		start := workerID*base + workerID
		if workerID > rem {
			start = rem*(base+1) + (workerID-rem)*base
		}
		return start, cnt
	}

	if numHelpers > 0 {
		wg.Add(numHelpers)

		submitFunc := p.Submit
		if highPriority {
			submitFunc = p.SubmitHighPriority
		}

		for i := 0; i < numHelpers; i++ {
			startChunk, count := workerRange(i + 1)
			submitFunc(func() {
				defer wg.Done()
				endChunk := startChunk + count
				for chunkIdx := startChunk; chunkIdx < endChunk; chunkIdx++ {
					s := chunkIdx * chunkSize
					e := s + chunkSize
					if e > n {
						e = n
					}
					task(s, e)
				}
			})
		}
	}

	// Caller thread = worker 0
	startChunk, count := workerRange(0)
	endChunk := startChunk + count
	for chunkIdx := startChunk; chunkIdx < endChunk; chunkIdx++ {
		s := chunkIdx * chunkSize
		e := s + chunkSize
		if e > n {
			e = n
		}
		task(s, e)
	}

	wg.Wait()
}
