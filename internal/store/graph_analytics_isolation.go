package store

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	lbtypes "github.com/23skdu/longbow/internal/store/types"
)

// maxConcurrentGraphAnalytics bounds expensive topology scans so a burst of
// PageRank/community/expansion/traversal requests cannot starve normal search
// and ingest. It is deliberately small and package-local: analytics is best
// effort, while search is the hot path.
const maxConcurrentGraphAnalytics = 2

var (
	errAnalyticsIndexUnavailable = errors.New("index not initialized")
	errAnalyticsGraphUnsupported = errors.New("graph analytics not supported for this index type")
)

// analyticsLimiter is the store-local admission control for graph analytics.
// The buffered channel is the semaphore; the counters are used by tests to
// synchronize deterministically without relying on wall-clock timing.
type analyticsLimiter struct {
	slots     chan struct{}
	waiters   atomic.Int64
	snapshots atomic.Int64
	// afterSnapshot is an internal rendezvous point used by tests to hold an
	// analytics operation after its snapshot is taken but before the algorithm
	// runs. It is nil in production.
	afterSnapshot atomic.Pointer[func()]
}

func (l *analyticsLimiter) notifySnapshot() {
	if hook := l.afterSnapshot.Load(); hook != nil {
		(*hook)()
	}
}

// graphAnalyticsSnapshot owns the bounded analytics slot until done is called.
// Topology is copied from the live graph so algorithms never hold the dataset
// (or graph adjacency) read lock for their full duration.
type graphAnalyticsSnapshot struct {
	data    *lbtypes.GraphData
	graph   *GraphStore
	release func()
	once    sync.Once
}

func (s *graphAnalyticsSnapshot) done() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		if s.release != nil {
			s.release()
		}
	})
}

// getAnalyticsLimiter returns this store's lazily created analytics semaphore.
func (s *VectorStore) getAnalyticsLimiter() *analyticsLimiter {
	if existing := s.analyticsGuard.Load(); existing != nil {
		return existing
	}
	created := &analyticsLimiter{slots: make(chan struct{}, maxConcurrentGraphAnalytics)}
	if s.analyticsGuard.CompareAndSwap(nil, created) {
		return created
	}
	return s.analyticsGuard.Load()
}

// acquireAnalyticsSlot waits for a bounded analytics slot or context cancellation.
func (s *VectorStore) acquireAnalyticsSlot(ctx context.Context) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	limiter := s.getAnalyticsLimiter()
	limiter.waiters.Add(1)
	select {
	case limiter.slots <- struct{}{}:
		limiter.waiters.Add(-1)
		var once sync.Once
		return func() { once.Do(func() { <-limiter.slots }) }, nil
	case <-ctx.Done():
		limiter.waiters.Add(-1)
		return nil, ctx.Err()
	}
}

// withAnalyticsSlot runs fn under the bounded analytics semaphore.
func (s *VectorStore) withAnalyticsSlot(ctx context.Context, fn func() error) error {
	release, err := s.acquireAnalyticsSlot(ctx)
	if err != nil {
		return err
	}
	defer release()
	return fn()
}

// graphDataFromIndex finds the layer-0 GraphData backing an index. Sharded
// indexes expose a per-shard graph; PageRank and community detection operate
// on the first shard, matching the pre-isolation handlers.
func graphDataFromIndex(index any) *lbtypes.GraphData {
	switch h := index.(type) {
	case interface{ GetData() *lbtypes.GraphData }:
		return h.GetData()
	case interface{ GetShardedIndex() *ShardedHNSW }:
		sharded := h.GetShardedIndex()
		if sharded == nil || sharded.NumShards() == 0 {
			return nil
		}
		shard, ok := sharded.GetShardIndex(0).(interface{ GetData() *lbtypes.GraphData })
		if !ok || shard == nil {
			return nil
		}
		return shard.GetData()
	default:
		return nil
	}
}

// beginGraphAnalytics takes an analytics slot, then clones the live HNSW graph
// under a short dataset read lock. The returned snapshot must be released by
// the caller after the algorithm finishes.
func (s *VectorStore) beginGraphAnalytics(ctx context.Context, ds *Dataset) (*graphAnalyticsSnapshot, error) {
	release, err := s.acquireAnalyticsSlot(ctx)
	if err != nil {
		return nil, err
	}
	snapshot := &graphAnalyticsSnapshot{release: release}
	committed := false
	defer func() {
		if !committed {
			snapshot.done()
		}
	}()

	s.lockDatasetForRead(ds)
	index := ds.Index
	var cloned *lbtypes.GraphData
	if index != nil {
		if live := graphDataFromIndex(index); live != nil {
			cloned = live.CloneForSnapshot()
		}
	}
	s.unlockDatasetForRead(ds)

	if index == nil {
		return nil, errAnalyticsIndexUnavailable
	}
	if cloned == nil {
		return nil, errAnalyticsGraphUnsupported
	}
	snapshot.data = cloned
	limiter := s.getAnalyticsLimiter()
	limiter.snapshots.Add(1)
	committed = true
	limiter.notifySnapshot()
	return snapshot, nil
}

// snapshotAdjacencyForAnalytics copies the adjacency lists used by Traverse.
// Only the copied lists are read during traversal, so AddEdge can proceed
// while a long BFS is running.
func (gs *GraphStore) snapshotAdjacencyForAnalytics() *GraphStore {
	if gs == nil {
		return nil
	}
	gs.adjMu.RLock()
	forward := make([][]Edge, len(gs.adjList))
	for i, edges := range gs.adjList {
		forward[i] = append([]Edge(nil), edges...)
	}
	backward := make([][]Edge, len(gs.bwdAdjList))
	for i, edges := range gs.bwdAdjList {
		backward[i] = append([]Edge(nil), edges...)
	}
	gs.adjMu.RUnlock()

	return &GraphStore{adjList: forward, bwdAdjList: backward}
}

// beginGraphTraversal returns an isolated adjacency snapshot under a bounded
// analytics slot. A nil graph is valid (the caller reports an empty result).
func (s *VectorStore) beginGraphTraversal(ctx context.Context, ds *Dataset) (*graphAnalyticsSnapshot, error) {
	release, err := s.acquireAnalyticsSlot(ctx)
	if err != nil {
		return nil, err
	}
	snapshot := &graphAnalyticsSnapshot{release: release}
	committed := false
	defer func() {
		if !committed {
			snapshot.done()
		}
	}()

	s.lockDatasetForRead(ds)
	var cloned *GraphStore
	if ds.Graph != nil {
		cloned = ds.Graph.snapshotAdjacencyForAnalytics()
	}
	s.unlockDatasetForRead(ds)

	snapshot.graph = cloned
	limiter := s.getAnalyticsLimiter()
	limiter.snapshots.Add(1)
	committed = true
	limiter.notifySnapshot()
	return snapshot, nil
}

// analyticsSetupError maps internal setup failures to stable API status codes.
func analyticsSetupError(err error, unsupportedMessage string) error {
	switch {
	case errors.Is(err, errAnalyticsIndexUnavailable):
		return status.Error(codes.FailedPrecondition, "index not initialized")
	case errors.Is(err, errAnalyticsGraphUnsupported):
		return status.Error(codes.Unimplemented, unsupportedMessage)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	default:
		return status.Errorf(codes.Internal, "graph analytics setup failed: %v", err)
	}
}
