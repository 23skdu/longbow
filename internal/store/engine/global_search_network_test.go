package engine

import (
	"context"
	"encoding/json"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/23skdu/longbow/internal/mesh"
	qry "github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/cluster"
)

// These tests exercise the real scatter-gather path of GlobalSearchCoordinator
// over actual gRPC/Arrow Flight connections: DoGet out to each peer, record
// decoding, the hedged group wait loop, and the k-way merge of best-first
// streams. They complement the in-process unit tests by proving the merge
// contract holds once real (and therefore arbitrarily ordered/latency-varied)
// streams are involved.

// networkPeer is a real gRPC Flight server backed by its own VectorStore,
// standing in for one cluster member.
type networkPeer struct {
	store  *VectorStore
	addr   string
	server *grpc.Server
}

func (p *networkPeer) close() {
	p.server.Stop()
	_ = p.store.Close()
}

func newNetworkPeer(t *testing.T) *networkPeer {
	t.Helper()

	vs := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 64*1024*1024, 0, time.Minute)
	vs.StartIndexingWorkers(1)
	vs.StartIngestionWorkers(1)

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	flight.RegisterFlightServiceServer(srv, cluster.NewDataServer(vs))
	go func() { _ = srv.Serve(lis) }()

	p := &networkPeer{store: vs, addr: lis.Addr().String(), server: srv}
	t.Cleanup(p.close)
	return p
}

// waitPeerIndexed blocks until the peer's vector index reports every stored
// row searchable. (The shared waitDatasetIndexed helper additionally probes a
// BM25 index for a literal fixture term, which these vector-only peers have
// no reason to contain.)
func waitPeerIndexed(t *testing.T, vs *VectorStore, dataset string, want int) {
	t.Helper()
	require.Eventually(t, func() bool {
		ds, ok := vs.getDataset(dataset)
		return ok && ds.IndexLen() >= want
	}, 10*time.Second, 25*time.Millisecond, "dataset %s never reached %d indexed vectors", dataset, want)
}

// ingest stores a batch of 2-dim vectors under the given user IDs and blocks
// until the index reports them all searchable.
func (p *networkPeer) ingest(t *testing.T, dataset string, ids []uint64, vecs [][]float32) {
	t.Helper()
	require.Len(t, ids, len(vecs))

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "vector", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)},
	}, nil)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	idB := b.Field(0).(*array.Uint64Builder)
	vecB := b.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)

	for i, id := range ids {
		idB.Append(id)
		vecB.Append(true)
		valB.AppendValues(vecs[i], nil)
	}

	rec := b.NewRecordBatch()
	defer rec.Release()

	require.NoError(t, p.store.StoreRecordBatch(context.Background(), dataset, rec))
	waitPeerIndexed(t, p.store, dataset, len(ids))
}

func (p *networkPeer) member(id string, tags map[string]string) mesh.Member {
	return mesh.Member{ID: id, MetaAddr: p.addr, Tags: tags}
}

// doGetSearchViaGRPC performs the exact request GlobalSearch sends to a peer
// and returns the decoded results. It is the reference for what a single peer
// stream contributes, obtained independently of the coordinator's merge.
func doGetSearchViaGRPC(t *testing.T, addr string, req *qry.VectorSearchRequest) []SearchResult {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	client := flight.NewClientFromConn(conn, nil)
	defer func() { _ = client.Close() }()

	remote := *req
	remote.LocalOnly = true
	raw, err := json.Marshal(qry.TicketQuery{Search: &remote})
	require.NoError(t, err)

	stream, err := client.DoGet(ctx, &flight.Ticket{Ticket: raw})
	require.NoError(t, err)

	reader, err := flight.NewRecordReader(stream)
	require.NoError(t, err)
	defer reader.Release()

	sourceColIdx := -1
	if reader.Schema() != nil {
		for i, f := range reader.Schema().Fields() {
			if f.Name == "source" {
				sourceColIdx = i
				break
			}
		}
	}

	var out []SearchResult
	for reader.Next() {
		out = append(out, extractSearchResultsFromRecord(reader.RecordBatch(), sourceColIdx)...)
	}
	require.NoError(t, reader.Err())
	return out
}

// mergeReference concatenates the per-source best-first lists and returns the
// global best-first top-k — the contract GlobalSearch must reproduce.
func mergeReference(k int, lists ...[]SearchResult) []SearchResult {
	all := make([]SearchResult, 0, k*len(lists))
	for _, l := range lists {
		all = append(all, l...)
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].Score > all[j].Score })
	if len(all) > k {
		all = all[:k]
	}
	return all
}

// assertSameSearchResults checks the three properties of a correct
// scatter-gather result: identical ID multiset (nothing dropped or
// duplicated), best-first ordering, and — when scores are unambiguous — the
// exact global ranking.
func assertSameSearchResults(t *testing.T, got, want []SearchResult) {
	t.Helper()

	require.Len(t, got, len(want), "got %v, want %v", got, want)

	gotIDs := make(map[VectorID]int, len(got))
	for _, r := range got {
		gotIDs[r.ID]++
	}
	wantIDs := make(map[VectorID]int, len(want))
	for _, r := range want {
		wantIDs[r.ID]++
	}
	require.Equal(t, wantIDs, gotIDs, "peer/local results were dropped or duplicated: got %v, want %v", got, want)

	for i := 1; i < len(got); i++ {
		require.GreaterOrEqual(t, got[i-1].Score, got[i].Score,
			"results must be best-first (higher Score first); index %d: %v", i, got)
	}

	// Only demand an exact ID sequence when every score is distinct; ties may
	// legitimately resolve in either order.
	distinct := true
	seen := make(map[float32]bool, len(want))
	for _, r := range want {
		if seen[r.Score] {
			distinct = false
			break
		}
		seen[r.Score] = true
	}
	if distinct {
		for i := range want {
			require.Equal(t, want[i].ID, got[i].ID, "global ranking wrong at index %d", i)
		}
	}
}

// TestGlobalSearch_ScatterGather_RealGRPC runs GlobalSearch against two live
// gRPC peers plus a local stream and asserts the merged top-k is exactly the
// global best-first top-k of all three sources.
func TestGlobalSearch_ScatterGather_RealGRPC(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping network test in short mode")
	}

	peerA := newNetworkPeer(t)
	peerA.ingest(t, "global_net",
		[]uint64{100, 101, 102},
		[][]float32{{1.0, 0.0}, {0.9, 0.1}, {0.8, 0.2}})

	peerB := newNetworkPeer(t)
	peerB.ingest(t, "global_net",
		[]uint64{200, 201, 202},
		[][]float32{{0.0, 1.0}, {0.1, 0.9}, {0.2, 0.8}})

	coord := NewGlobalSearchCoordinator(zerolog.Nop(), NewFlightClientPool(DefaultFlightClientPoolConfig()))
	t.Cleanup(func() { _ = coord.Close() })

	req := &qry.VectorSearchRequest{
		Dataset: "global_net",
		Vector:  []float32{1.0, 0.0},
		K:       10,
	}
	local := []SearchResult{
		{ID: VectorID(300), Score: 0.55},
		{ID: VectorID(301), Score: 0.30},
	}
	peers := []mesh.Member{
		peerA.member("peerA", nil),
		peerB.member("peerB", nil),
	}

	// Reference streams, fetched over the same wire path but outside the
	// coordinator, so a merge bug cannot hide behind a matching bug.
	refA := doGetSearchViaGRPC(t, peerA.addr, req)
	refB := doGetSearchViaGRPC(t, peerB.addr, req)
	require.Len(t, refA, 3, "peer A must contribute its whole partition")
	require.Len(t, refB, 3, "peer B must contribute its whole partition")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	t.Run("FullMerge_NoDrops", func(t *testing.T) {
		// k exceeds the total candidate count: every local and peer result
		// must survive the gather.
		got, err := coord.GlobalSearch(ctx, local, req, peers)
		require.NoError(t, err)
		assertSameSearchResults(t, got, mergeReference(req.K, local, refA, refB))
		assert.Len(t, got, 8, "all 2 local + 3 + 3 peer results must be present")
	})

	t.Run("TopK_TruncatesToGlobalBest", func(t *testing.T) {
		smallK := *req
		smallK.K = 4
		got, err := coord.GlobalSearch(ctx, local, &smallK, peers)
		require.NoError(t, err)
		assertSameSearchResults(t, got, mergeReference(smallK.K, local, refA, refB))
		assert.Len(t, got, 4)
	})

	t.Run("NoPeers_ReturnsLocalUntouched", func(t *testing.T) {
		got, err := coord.GlobalSearch(ctx, local, req, nil)
		require.NoError(t, err)
		require.Equal(t, local, got)
	})

	// The winner of a hedged group posts its result and then cancels the
	// group context; the wait loop must never pick the cancellation over the
	// already-buffered winner. Repeat to give that interleaving every chance
	// to surface (it is timing dependent, so a single run proves little).
	t.Run("Repeated_NoIntermittentLoss", func(t *testing.T) {
		for i := 0; i < 25; i++ {
			got, err := coord.GlobalSearch(ctx, local, req, peers)
			require.NoError(t, err, "iteration %d", i)
			assertSameSearchResults(t, got, mergeReference(req.K, local, refA, refB))
		}
	})
}

// TestGlobalSearch_HedgedReplicas_NoDroppedWinner groups two peers under one
// shard tag so they are queried concurrently and the first success wins. The
// losing replica's context is cancelled at the same moment the winner's
// result becomes available, which is exactly the race that intermittently
// discarded a group's results.
func TestGlobalSearch_HedgedReplicas_NoDroppedWinner(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping network test in short mode")
	}

	// Two replicas of the same shard hold identical data.
	peerA := newNetworkPeer(t)
	peerA.ingest(t, "hedged_net", []uint64{10, 11}, [][]float32{{1.0, 0.0}, {0.0, 1.0}})

	peerB := newNetworkPeer(t)
	peerB.ingest(t, "hedged_net", []uint64{10, 11}, [][]float32{{1.0, 0.0}, {0.0, 1.0}})

	coord := NewGlobalSearchCoordinator(zerolog.Nop(), NewFlightClientPool(DefaultFlightClientPoolConfig()))
	t.Cleanup(func() { _ = coord.Close() })

	req := &qry.VectorSearchRequest{
		Dataset: "hedged_net",
		Vector:  []float32{1.0, 0.0},
		K:       10,
	}
	local := []SearchResult{{ID: VectorID(99), Score: 0.42}}
	peers := []mesh.Member{
		peerA.member("replica-a", map[string]string{"shard": "0"}),
		peerB.member("replica-b", map[string]string{"shard": "0"}),
	}

	// Reference from one replica; both hold identical data.
	ref := doGetSearchViaGRPC(t, peerA.addr, req)
	require.Len(t, ref, 2)
	want := mergeReference(req.K, local, ref)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for i := 0; i < 40; i++ {
		got, err := coord.GlobalSearch(ctx, local, req, peers)
		require.NoError(t, err, "iteration %d", i)
		assertSameSearchResults(t, got, want)
	}
}

// TestGlobalSearch_PartialPeerFailure_StillReturnsLocal covers the failure
// side of the wait loop: when every replica of a group fails, the group must
// drop out without discarding the local stream.
func TestGlobalSearch_PartialPeerFailure_StillReturnsLocal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping network test in short mode")
	}

	coord := NewGlobalSearchCoordinator(zerolog.Nop(), NewFlightClientPool(DefaultFlightClientPoolConfig()))
	t.Cleanup(func() { _ = coord.Close() })

	req := &qry.VectorSearchRequest{
		Dataset: "ghost_net",
		Vector:  []float32{1.0, 0.0},
		K:       5,
	}
	local := []SearchResult{
		{ID: VectorID(1), Score: 0.9},
		{ID: VectorID(2), Score: 0.4},
	}
	// Nothing is listening on these ports.
	deadA := mesh.Member{ID: "dead-a", MetaAddr: "127.0.0.1:1"}
	deadB := mesh.Member{ID: "dead-b", MetaAddr: "127.0.0.1:1"}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	got, err := coord.GlobalSearch(ctx, local, req, []mesh.Member{deadA, deadB})
	require.NoError(t, err, "peer failures must not fail the whole search")
	require.Equal(t, local, got, "local stream must survive total peer failure")
}
