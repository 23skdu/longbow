package store

import (
	"context"
	"encoding/json"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// clearAnalyticsHook removes a test rendezvous hook from a store's limiter.
func clearAnalyticsHook(limiter *analyticsLimiter) {
	var nilHook *func()
	limiter.afterSnapshot.Store(nilHook)
}

// newAnalyticsTestStore builds a store with a dataset backed by an HNSW index.
func newAnalyticsTestStore(t *testing.T, name string) (*VectorStore, *Dataset, *lbtypes.GraphData) {
	t.Helper()
	s := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<30, 0, 0)
	t.Cleanup(func() { _ = s.Close() })

	ds := NewDataset(name, nil)
	cfg := DefaultArrowHNSWConfig()
	cfg.Dims = 2
	idx := NewArrowHNSW(ds, &cfg, nil)
	t.Cleanup(func() { _ = idx.Close() })
	ds.Index = idx
	s.UpdateDatasets(func(m map[string]*Dataset) { m[name] = ds })

	gd := idx.GetData()
	require.NotNil(t, gd)
	return s, ds, gd
}

// TestGraphAnalyticsSnapshotIsolation proves PageRank reads a stable clone:
// the live graph is rewritten after the snapshot is taken but before the
// algorithm starts, and the result still reflects the snapshotted topology.
func TestGraphAnalyticsSnapshotIsolation(t *testing.T) {
	const name = "analytics_snapshot_ds"
	s, _, gd := newAnalyticsTestStore(t, name)

	for id, neighbors := range map[uint32][]uint32{
		1: {2, 3, 4}, // snapshot star center
		2: {1}, 3: {1}, 4: {1},
	} {
		require.NoError(t, gd.SetNeighbors(id, neighbors))
	}

	limiter := s.getAnalyticsLimiter()
	snapshotTaken := make(chan struct{})
	mutationDone := make(chan struct{})
	hook := func() {
		close(snapshotTaken)
		<-mutationDone
	}
	limiter.afterSnapshot.Store(&hook)
	defer clearAnalyticsHook(limiter)

	resultCh := make(chan CentralityResult, 1)
	errCh := make(chan error, 1)
	go func() {
		body, err := json.Marshal(map[string]any{"dataset": name, "max_iterations": 30})
		if err != nil {
			errCh <- err
			return
		}
		stream := &mockFlightServer{}
		if err := s.handleCalculatePageRank(body, stream); err != nil {
			errCh <- err
			return
		}
		var result CentralityResult
		errCh <- json.Unmarshal(stream.results[0].Body, &result)
		resultCh <- result
	}()

	<-snapshotTaken
	// Mutate the live graph while analytics is stalled on its snapshot.
	for id, neighbors := range map[uint32][]uint32{
		1: {}, 2: {1, 3, 4}, 3: {2}, 4: {2},
	} {
		require.NoError(t, gd.SetNeighbors(id, neighbors))
	}
	close(mutationDone)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("PageRank did not finish")
	}
	result := <-resultCh
	require.Greater(t, result.Scores[1], result.Scores[2], "result must reflect the snapshotted star")
	require.Greater(t, result.Scores[1], result.Scores[3])
	require.Greater(t, result.Scores[1], result.Scores[4])

	// The live graph is now different: analytics on a new snapshot sees the
	// inverted star instead of the old one.
	liveResult, err := NewGraphAnalytics(func() *lbtypes.GraphData { return gd }).CalculatePageRank(context.Background(), DefaultPageRankConfig())
	require.NoError(t, err)
	require.Greater(t, liveResult.Scores[2], liveResult.Scores[1])
}

// TestGraphTraversalSnapshotIsolation verifies traverse-graph walks an
// adjacency clone so concurrent AddEdge calls cannot change the result.
func TestGraphTraversalSnapshotIsolation(t *testing.T) {
	const name = "traversal_snapshot_ds"
	s, ds, _ := newAnalyticsTestStore(t, name)
	ds.Graph = NewGraphStore()
	require.NoError(t, ds.Graph.AddEdge(Edge{Subject: 1, Predicate: "next", Object: 2, Weight: 1}))

	limiter := s.getAnalyticsLimiter()
	snapshotTaken := make(chan struct{})
	mutationDone := make(chan struct{})
	hook := func() {
		close(snapshotTaken)
		<-mutationDone
	}
	limiter.afterSnapshot.Store(&hook)
	defer clearAnalyticsHook(limiter)

	pathsCh := make(chan []Path, 1)
	errCh := make(chan error, 1)
	go func() {
		body, err := json.Marshal(map[string]any{"dataset": name, "start": 1, "max_hops": 3})
		if err != nil {
			errCh <- err
			return
		}
		stream := &mockFlightServer{}
		if err := s.handleTraverseGraph(body, stream); err != nil {
			errCh <- err
			return
		}
		var paths []Path
		errCh <- json.Unmarshal(stream.results[0].Body, &paths)
		pathsCh <- paths
	}()

	<-snapshotTaken
	require.NoError(t, ds.Graph.AddEdge(Edge{Subject: 2, Predicate: "next", Object: 3, Weight: 1}))
	close(mutationDone)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("traverse-graph did not finish")
	}
	paths := <-pathsCh
	require.Len(t, paths, 1, "only the snapshotted 1->2 edge is visible")
	require.Equal(t, VectorID(2), paths[0].Nodes[len(paths[0].Nodes)-1])
}

// TestGraphAnalyticsDoesNotBlockSearch holds an analytics operation after its
// snapshot (so it owns an analytics slot) and verifies a writer can take the
// dataset lock and a real vector search can complete without waiting for the
// analytics algorithm to release that slot.
func TestGraphAnalyticsDoesNotBlockSearch(t *testing.T) {
	const name = "analytics_search_ds"
	s, ds, gd := newAnalyticsTestStore(t, name)
	require.NoError(t, gd.SetNeighbors(1, []uint32{2}))

	limiter := s.getAnalyticsLimiter()
	analyticsRunning := make(chan struct{})
	releaseAnalytics := make(chan struct{})
	hook := func() {
		close(analyticsRunning)
		<-releaseAnalytics
	}
	limiter.afterSnapshot.Store(&hook)
	defer clearAnalyticsHook(limiter)

	analyticsDone := make(chan error, 1)
	go func() {
		body, err := json.Marshal(map[string]any{"dataset": name, "max_iterations": 10})
		if err != nil {
			analyticsDone <- err
			return
		}
		analyticsDone <- s.handleDetectCommunities(body, &mockFlightServer{})
	}()

	<-analyticsRunning

	// A writer acquiring ds.dataMu proves the analytics algorithm is no longer
	// holding the dataset read lock after its snapshot was taken.
	writerLocked := make(chan struct{})
	releaseWriter := make(chan struct{})
	go func() {
		ds.dataMu.Lock()
		close(writerLocked)
		<-releaseWriter
		ds.dataMu.Unlock()
	}()
	select {
	case <-writerLocked:
	case <-time.After(5 * time.Second):
		close(releaseAnalytics)
		t.Fatal("in-flight analytics still holds the dataset read lock")
	}
	close(releaseWriter)

	searchDone := make(chan error, 1)
	go func() {
		_, err := s.SearchDatasetVectors(context.Background(), name, []float32{0.1, 0.2}, 1, nil)
		searchDone <- err
	}()
	select {
	case err := <-searchDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		close(releaseAnalytics)
		t.Fatal("search was blocked by an in-flight analytics operation")
	}

	close(releaseAnalytics)
	select {
	case err := <-analyticsDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("analytics did not finish after release")
	}
}

// TestGraphAnalyticsConcurrencyIsBounded verifies the package-level semaphore
// admits at most maxConcurrentGraphAnalytics algorithms; additional requests
// wait for a slot instead of starting more expensive work.
func TestGraphAnalyticsConcurrencyIsBounded(t *testing.T) {
	const name = "analytics_bounded_ds"
	s, _, gd := newAnalyticsTestStore(t, name)
	require.NoError(t, gd.SetNeighbors(1, []uint32{2}))

	limiter := s.getAnalyticsLimiter()
	entered := make(chan struct{}, maxConcurrentGraphAnalytics)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()

	var starts atomic.Int64
	hook := func() {
		if starts.Add(1) <= int64(maxConcurrentGraphAnalytics) {
			entered <- struct{}{}
			<-release
		}
	}
	limiter.afterSnapshot.Store(&hook)
	defer clearAnalyticsHook(limiter)

	errCh := make(chan error, maxConcurrentGraphAnalytics+1)
	startAnalytics := func() {
		body, err := json.Marshal(map[string]any{"dataset": name, "max_iterations": 5})
		if err != nil {
			errCh <- err
			return
		}
		errCh <- s.handleDetectCommunities(body, &mockFlightServer{})
	}

	for i := 0; i < maxConcurrentGraphAnalytics; i++ {
		go startAnalytics()
	}
	for i := 0; i < maxConcurrentGraphAnalytics; i++ {
		<-entered
	}

	go startAnalytics()

	deadline := time.Now().Add(5 * time.Second)
	for limiter.waiters.Load() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("third analytics request was not queued")
		}
		runtime.Gosched()
	}
	require.Equal(t, int64(maxConcurrentGraphAnalytics), limiter.snapshots.Load(),
		"extra analytics must wait for a bounded slot")

	unblock()
	for i := 0; i < cap(errCh); i++ {
		select {
		case err := <-errCh:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("analytics did not finish after release")
		}
	}
}
