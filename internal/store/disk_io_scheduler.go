package store

import (
	"context"
	"sync"
	"time"
)

type DiskIOScheduler struct {
	mu            sync.RWMutex
	pendingReads  []DiskIORequest
	readPool      *sync.Pool
	maxConcurrent int
	activeReads   int
	priorityQueue bool
}

type DiskIORequest struct {
	Offset    int64
	Size      int
	Priority  int
	Data      []byte
	Done      chan error
	Timestamp time.Time
}

type DiskIOStats struct {
	TotalRequests int64
	TotalBytes    int64
	AvgLatencyUs  int64
	QueueDepth    int
	ActiveReads   int
}

func NewDiskIOScheduler(maxConcurrent int) *DiskIOScheduler {
	return &DiskIOScheduler{
		maxConcurrent: maxConcurrent,
		readPool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, 64*1024) // 64KB default buffer
			},
		},
		priorityQueue: true,
		pendingReads:  make([]DiskIORequest, 0),
	}
}

func (s *DiskIOScheduler) Submit(req DiskIORequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

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

	// Simulated I/O operation - actual implementation would use read(2) or pread(2)
	// In practice, this would read from disk using O_DIRECT or memory-mapped I/O

	err := s.performRead(req)

	_ = start // Used for timing below
	s.mu.Lock()
	s.activeReads--
	s.processNext()
	s.mu.Unlock()

	if req.Done != nil {
		req.Done <- err
	}
}

func (s *DiskIOScheduler) performRead(req DiskIORequest) error {
	// Placeholder for actual disk read implementation
	// Would use pread(2) for aligned reads
	return nil
}

func (s *DiskIOScheduler) processNext() {
	if len(s.pendingReads) == 0 {
		return
	}

	// Sort by priority (lower is higher priority)
	if s.priorityQueue {
		s.sortByPriority()
	}

	// Get next request
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

func (s *DiskIOScheduler) GetStats() DiskIOStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return DiskIOStats{
		QueueDepth:  len(s.pendingReads),
		ActiveReads: s.activeReads,
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
