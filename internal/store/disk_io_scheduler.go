package store

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type DiskIOScheduler struct {
	mu            sync.RWMutex
	pendingReads  []DiskIORequest
	readPool      *sync.Pool
	maxConcurrent int
	activeReads   int
	priorityQueue bool

	readAheadBlocks int
	prefetchQueue   chan uint64
	prefetched      map[uint64]bool
	maxPrefetch     int

	stats struct {
		prefetchHits    int64
		prefetchMisses  int64
		ioWaitTimeNs    int64
		readAheadHits   int64
		sequentialReads int64
		randomReads     int64
		totalRequests   int64
		totalBytes      int64
	}

	lastAccessTime  time.Time
	lastAccessBlock uint64
	sequentialCount int
}

type DiskIORequest struct {
	Offset    int64
	Size      int
	Priority  int
	BlockID   uint64
	Data      []byte
	Done      chan error
	Timestamp time.Time
}

type IOScheduleItem struct {
	BlockID    uint64
	Priority   int
	IsRead     bool
	Callback   func([]byte, error)
	SubmitTime time.Time
}

type PriorityQueue struct {
	items   []IOScheduleItem
	mu      sync.Mutex
	maxSize int
}

func NewPriorityQueue(maxSize int) *PriorityQueue {
	return &PriorityQueue{
		items:   make([]IOScheduleItem, 0, maxSize),
		maxSize: maxSize,
	}
}

func (pq *PriorityQueue) Push(item IOScheduleItem) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if len(pq.items) >= pq.maxSize {
		return false
	}
	pq.items = append(pq.items, item)
	pq.siftUp(len(pq.items) - 1)
	return true
}

func (pq *PriorityQueue) Pop() (IOScheduleItem, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if len(pq.items) == 0 {
		return IOScheduleItem{}, false
	}
	item := pq.items[0]
	pq.items = pq.items[:len(pq.items)-1]
	if len(pq.items) > 0 {
		pq.siftDown(0)
	}
	return item, true
}

func (pq *PriorityQueue) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.items)
}

func (pq *PriorityQueue) siftUp(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if pq.items[i].Priority <= pq.items[parent].Priority {
			break
		}
		pq.items[i], pq.items[parent] = pq.items[parent], pq.items[i]
		i = parent
	}
}

func (pq *PriorityQueue) siftDown(i int) {
	n := len(pq.items)
	for {
		left := 2*i + 1
		right := 2*i + 2
		largest := i
		if left < n && pq.items[left].Priority > pq.items[largest].Priority {
			largest = left
		}
		if right < n && pq.items[right].Priority > pq.items[largest].Priority {
			largest = right
		}
		if largest == i {
			break
		}
		pq.items[i], pq.items[largest] = pq.items[largest], pq.items[i]
		i = largest
	}
}

func NewDiskIOScheduler(maxConcurrent int) *DiskIOScheduler {
	return &DiskIOScheduler{
		maxConcurrent: maxConcurrent,
		readPool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, 64*1024)
			},
		},
		priorityQueue:   true,
		pendingReads:    make([]DiskIORequest, 0),
		readAheadBlocks: 4,
		prefetchQueue:   make(chan uint64, 32),
		prefetched:      make(map[uint64]bool),
		maxPrefetch:     32,
	}
}

func (s *DiskIOScheduler) ConfigurePrefetch(readAheadBlocks, maxPrefetch int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readAheadBlocks = readAheadBlocks
	s.maxPrefetch = maxPrefetch
	s.prefetchQueue = make(chan uint64, maxPrefetch)
}

func (s *DiskIOScheduler) Submit(req DiskIORequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	atomic.AddInt64(&s.stats.totalRequests, 1)

	if s.activeReads >= s.maxConcurrent {
		s.pendingReads = append(s.pendingReads, req)
		return nil
	}

	s.activeReads++
	go s.executeRequest(req)
	return nil
}

func (s *DiskIOScheduler) executeRequest(req DiskIORequest) {
	start := time.Now()

	err := s.performRead(req)

	s.mu.Lock()
	s.activeReads--
	s.processNext()
	s.mu.Unlock()

	waitTime := time.Since(start)
	atomic.AddInt64(&s.stats.ioWaitTimeNs, int64(waitTime))

	if req.Done != nil {
		req.Done <- err
	}
}

func (s *DiskIOScheduler) performRead(req DiskIORequest) error {
	if req.BlockID > 0 {
		s.RecordAccess(req.BlockID)
	}
	return nil
}

func (s *DiskIOScheduler) RecordAccess(blockID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.prefetched[blockID] = true

	now := time.Now()
	if s.lastAccessBlock > 0 && blockID == s.lastAccessBlock+1 {
		s.sequentialCount++
		if s.sequentialCount >= 3 {
			atomic.AddInt64(&s.stats.sequentialReads, 1)
			s.triggerReadAhead(blockID)
		}
	} else {
		if s.sequentialCount > 0 {
			s.sequentialCount = 0
		}
		atomic.AddInt64(&s.stats.randomReads, 1)
	}

	s.lastAccessTime = now
	s.lastAccessBlock = blockID
}

func (s *DiskIOScheduler) triggerReadAhead(currentBlock uint64) {
	for i := 1; i <= s.readAheadBlocks; i++ {
		blockID := currentBlock + uint64(i)
		if !s.prefetched[blockID] {
			select {
			case s.prefetchQueue <- blockID:
				atomic.AddInt64(&s.stats.readAheadHits, 1)
			default:
			}
		}
	}
}

func (s *DiskIOScheduler) IsPrefetched(blockID uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.prefetched[blockID]
}

func (s *DiskIOScheduler) GetPrefetchChannel() <-chan uint64 {
	return s.prefetchQueue
}

func (s *DiskIOScheduler) MarkPrefetchComplete(blockID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prefetched[blockID] = true
}

func (s *DiskIOScheduler) processNext() {
	if len(s.pendingReads) == 0 {
		return
	}

	if s.priorityQueue {
		s.sortByPriority()
	}

	req := s.pendingReads[0]
	s.pendingReads = s.pendingReads[1:]

	s.activeReads++
	go s.executeRequest(req)
}

func (s *DiskIOScheduler) sortByPriority() {
	for i := 0; i < len(s.pendingReads)-1; i++ {
		for j := i + 1; j < len(s.pendingReads); j++ {
			if s.pendingReads[j].Priority < s.pendingReads[i].Priority {
				s.pendingReads[i], s.pendingReads[j] = s.pendingReads[j], s.pendingReads[i]
			}
		}
	}
}

type DiskIOStats struct {
	TotalRequests   int64
	TotalBytes      int64
	AvgLatencyUs    int64
	QueueDepth      int
	ActiveReads     int
	PrefetchHits    int64
	PrefetchMisses  int64
	ReadAheadHits   int64
	SequentialReads int64
	RandomReads     int64
}

func (s *DiskIOScheduler) GetStats() DiskIOStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return DiskIOStats{
		QueueDepth:      len(s.pendingReads),
		ActiveReads:     s.activeReads,
		TotalRequests:   atomic.LoadInt64(&s.stats.totalRequests),
		TotalBytes:      atomic.LoadInt64(&s.stats.totalBytes),
		AvgLatencyUs:    atomic.LoadInt64(&s.stats.ioWaitTimeNs) / 1000,
		PrefetchHits:    atomic.LoadInt64(&s.stats.prefetchHits),
		ReadAheadHits:   atomic.LoadInt64(&s.stats.readAheadHits),
		SequentialReads: atomic.LoadInt64(&s.stats.sequentialReads),
		RandomReads:     atomic.LoadInt64(&s.stats.randomReads),
	}
}

func (s *DiskIOScheduler) Wait(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.mu.RLock()
			done := s.activeReads == 0 && len(s.pendingReads) == 0
			s.mu.RUnlock()
			if done {
				return nil
			}
		}
	}
}

func (s *DiskIOScheduler) ClearPrefetched() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prefetched = make(map[uint64]bool)
}
