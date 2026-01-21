package store

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/mesh"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type clientEntry struct {
	client  flight.Client
	lastUse time.Time
}

// GlobalSearchCoordinator handles scatter-gather logic
type GlobalSearchCoordinator struct {
	logger zerolog.Logger
	// clients: map[string]*clientEntry
	clients     sync.Map
	idleTimeout time.Duration
	stopCh      chan struct{}
}

//nolint:gocritic // Logger passed by value for simplicity
func NewGlobalSearchCoordinator(logger zerolog.Logger) *GlobalSearchCoordinator {
	c := &GlobalSearchCoordinator{
		logger:      logger,
		idleTimeout: 5 * time.Minute,
		stopCh:      make(chan struct{}),
	}
	go c.cleanupLoop()
	return c
}

// GlobalSearch performs scatter-gather search across the cluster
func (c *GlobalSearchCoordinator) GlobalSearch(ctx context.Context, localResults []SearchResult, req *query.VectorSearchRequest, peers []mesh.Member) ([]SearchResult, error) {
	start := time.Now()

	// If no peers, just return local
	if len(peers) == 0 {
		return localResults, nil
	}

	metrics.GlobalSearchFanoutSize.Observe(float64(len(peers)))

	// Streaming Merge with Replica Hedging
	// We treat local results as one stream.
	// For remote peers, we group them by "shard" tag (or ID if no tag).
	// For each group, we hedge: send to all replicas, accept FIRST success.

	// Group peers
	peerGroups := make(map[string][]mesh.Member)
	for i := range peers {
		p := peers[i]
		groupID := p.ID // Default to distinct if no shared tag
		if shard, ok := p.Tags["shard"]; ok {
			groupID = "shard:" + shard
		}
		peerGroups[groupID] = append(peerGroups[groupID], p)
	}

	numStreams := 1 + len(peerGroups)
	channels := make([]<-chan []SearchResult, numStreams)

	// 1. Local Stream
	localCh := make(chan []SearchResult, 1)
	localCh <- localResults
	close(localCh)
	channels[0] = localCh

	// 2. Peer Streams (Hedged)
	// We map each group to one output channel
	groupChs := make([]chan []SearchResult, len(peerGroups))

	// Request Body
	remoteReq := *req // Copy struct
	remoteReq.LocalOnly = true

	var wg sync.WaitGroup
	configTimeout := 30 * time.Second

	groupIdx := 0
	for _, members := range peerGroups {
		// Output channel for this group
		ch := make(chan []SearchResult, 1)
		groupChs[groupIdx] = ch
		channels[groupIdx+1] = ch
		groupIdx++

		wg.Add(1)
		go func(replicas []mesh.Member, outCh chan []SearchResult) {
			defer wg.Done()
			defer close(outCh)

			// Hedging:
			// Launch requests to ALL replicas concurrently.
			// First one to return success writes to outCh and cancels others.
			// If all fail, we write nothing (or log error).

			ctxHedge, cancelHedge := context.WithCancel(ctx)
			defer cancelHedge()

			resultHedge := make(chan []SearchResult, 1) // First winner
			failSignal := make(chan struct{}, len(replicas))
			var wgReplicas sync.WaitGroup

			subCtx, cancelTimeout := context.WithTimeout(ctxHedge, configTimeout)
			defer cancelTimeout()

			for i := range replicas {
				rp := replicas[i]
				wgReplicas.Add(1)
				go func(p mesh.Member) {
					defer wgReplicas.Done()

					client, err := c.getClient(p.MetaAddr)
					if err != nil {
						failSignal <- struct{}{}
						return
					}
					// Mark used
					if entry, ok := c.clients.Load(p.MetaAddr); ok {
						entry.(*clientEntry).lastUse = time.Now()
					}

					c.logger.Debug().Str("peer", p.ID).Str("addr", p.MetaAddr).Msg("Sending DoGet to peer")

					// DoGet with Search Ticket
					ticketQuery := query.TicketQuery{
						Search: &remoteReq,
					}
					ticketBytes, err := json.Marshal(ticketQuery)
					if err != nil {
						failSignal <- struct{}{}
						return
					}

					stream, err := client.DoGet(subCtx, &flight.Ticket{Ticket: ticketBytes})
					if err != nil {
						// Only log at Debug level for NotFound as it happens when Sharding skips a node
						if status.Code(err) == codes.NotFound {
							c.logger.Debug().Err(err).Str("peer", p.ID).Msg("Peer does not have dataset")
						} else {
							c.logger.Warn().Err(err).Str("peer", p.ID).Msg("DoGet failed")
						}
						failSignal <- struct{}{}
						return
					}

					reader, err := flight.NewRecordReader(stream)
					if err != nil {
						c.logger.Warn().Err(err).Str("peer", p.ID).Msg("NewRecordReader failed")
						failSignal <- struct{}{}
						return
					}
					defer reader.Release()

					var results []SearchResult
					for reader.Next() {
						rec := reader.RecordBatch()
						col0 := rec.Column(0)
						col1 := rec.Column(1)

						ids := col0.(*array.Uint64).Uint64Values()
						scores := col1.(*array.Float32).Float32Values()

						for k := 0; k < len(ids); k++ {
							results = append(results, SearchResult{
								ID:    VectorID(ids[k]),
								Score: scores[k],
							})
						}
					}
					if reader.Err() != nil {
						failSignal <- struct{}{}
						return
					}

					// Submit
					select {
					case resultHedge <- results:
						cancelHedge() // Cancel others
					case <-subCtx.Done():
					}
				}(rp)
			}

			// Goroutine to signal when all failed
			finishedAll := make(chan struct{})
			go func() {
				wgReplicas.Wait()
				close(finishedAll)
			}()

			// Wait for one success, all failure, or timeout
			failedCount := 0
			for {
				select {
				case res := <-resultHedge:
					outCh <- res
					return
				case <-failSignal:
					failedCount++
					if failedCount == len(replicas) {
						// All replicas in this group failed, return early
						metrics.GlobalSearchPartialFailures.Inc()
						return
					}
				case <-finishedAll:
					// Double check if we missed a result? theoretically shouldn't happen with resultHedge
					return
				case <-subCtx.Done():
					metrics.GlobalSearchPartialFailures.Inc()
					return
				}
			}
		}(members, ch)
	}

	// Wait for all groups to finish (they run independently)
	// Actually we don't need to wait here if we rely on channels closing?
	// But wg covers the Group goroutines.
	// We need to wait for them to ensure they launch.
	// Wait, the previous code didn't wait *here*. It launched a goroutine to wait.
	// But here I'm setting up channels. It's fine.

	// 3. Launch Merger
	// Merger runs concurrently and consumes from channels as they become available
	mergedCh := MergeSortedStreams(channels, req.K)

	// 4. Collect Final Results
	finalResults := make([]SearchResult, 0, req.K)
	for r := range mergedCh {
		finalResults = append(finalResults, r)
	}

	// Ensure peer goroutines finish (they should have closed their channels)
	// Actually we don't strictly need to wait for WG if we only want Top K and Merger closes early?
	// MergeSortedStreams drain logic: it closes output when it has K items or all inputs closed.
	// If it hits K, it closes output. But peer goroutines might still be running/blocked on write?
	// The peerChs are buffered (size 1). If peer writes 1 batch, it unblocks.
	// Peer goroutine then closes channel and exits.
	// So we don't leak goroutines even if we return early.
	// However, for cleanliness, we might want to ensure they are done or cancel context?
	// DoAction context `subCtx` will timeout anyway.

	// Wait for peers to cleanup if needed, but not strictly required for correctness of result
	// Let's run Wait in background to avoid blocking return if K is satisfied early?
	// But `mergedCh` only closes when K items yielded OR all sources exhausted.
	// If K satisfied, `MergeSortedStreams` loop yields K and returns (closing output).
	// But it does NOT close input channels. Peer goroutines close input channels.
	// The merger logic itself is:
	// "for h.Len() > 0 && count < k"
	// If count < k is hit, merger exits and closes `out`.
	// Peer goroutines are independent.
	go func() {
		wg.Wait()
	}()

	metrics.GlobalSearchDuration.Observe(time.Since(start).Seconds())
	return finalResults, nil
}

func (c *GlobalSearchCoordinator) getClient(addr string) (flight.Client, error) {
	if v, ok := c.clients.Load(addr); ok {
		entry := v.(*clientEntry)
		return entry.client, nil
	}

	// Dial new
	client, err := flight.NewClientWithMiddleware(addr, nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024*1024*10)),
	)
	if err != nil {
		return nil, err
	}

	c.clients.Store(addr, &clientEntry{
		client:  client,
		lastUse: time.Now(),
	})
	return client, nil
}

func (c *GlobalSearchCoordinator) cleanupLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.pruneVisible()
		}
	}
}

func (c *GlobalSearchCoordinator) pruneVisible() {
	c.clients.Range(func(key, value any) bool {
		entry := value.(*clientEntry)
		if time.Since(entry.lastUse) > c.idleTimeout {
			c.logger.Info().Str("addr", key.(string)).Msg("Pruning idle flight client")
			_ = entry.client.Close()
			c.clients.Delete(key)
		}
		return true
	})
}

func (c *GlobalSearchCoordinator) Close() error {
	close(c.stopCh)
	c.clients.Range(func(key, value any) bool {
		entry := value.(*clientEntry)
		_ = entry.client.Close()
		c.clients.Delete(key)
		return true
	})
	return nil
}
