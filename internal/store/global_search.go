package store

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/mesh"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/query"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/23skdu/longbow/internal/tracing"
	"github.com/23skdu/longbow/pkg/retry"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GlobalSearchCoordinator handles scatter-gather logic
type GlobalSearchCoordinator struct {
	logger      zerolog.Logger
	pool        *FlightClientPool
	retryPolicy retry.RetryPolicy
}

// NewGlobalSearchCoordinator creates a new GlobalSearchCoordinator with the provided logger and client pool.
func NewGlobalSearchCoordinator(logger zerolog.Logger, pool *FlightClientPool) *GlobalSearchCoordinator {
	return &GlobalSearchCoordinator{
		logger: logger,
		pool:   pool,
		retryPolicy: &retry.ExponentialBackoff{
			BaseDelay:      100 * time.Millisecond,
			MaxDelay:       2 * time.Second,
			Retries:        3,
			AttemptTimeout: 10 * time.Second,
		},
	}
}

// GlobalSearch performs scatter-gather search across the cluster
func (c *GlobalSearchCoordinator) GlobalSearch(ctx context.Context, localResults []SearchResult, req *query.VectorSearchRequest, peers []mesh.Member) ([]SearchResult, error) {
	start := time.Now()

	_, span := tracing.CreateSpan(ctx, "GlobalSearch")
	if span != nil {
		span.SetAttributes(
			"component", "distributed",
			"level", "coordination",
			"peers", fmt.Sprint(len(peers)),
		)
		defer span.End()
	}

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

	isHybrid := req.TextQuery != "" || (req.Alpha > 0 && req.Alpha < 1.0)

	// Request Body
	remoteReq := *req // Copy struct
	remoteReq.LocalOnly = true
	if isHybrid {
		remoteReq.RawHybrid = true
	}

	var wg sync.WaitGroup

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

			for i := range replicas {
				rp := replicas[i]
				wgReplicas.Add(1)
				go func(p mesh.Member) {
					defer wgReplicas.Done()
					defer func() {
						if r := recover(); r != nil {
							c.logger.Error().Interface("panic", r).Str("peer", p.ID).Msg("Recovered from panic in peer query")
						}
					}()

					err := retry.Do(ctxHedge, c.retryPolicy, func(subCtx context.Context) error {
						conn, err := c.pool.Get(subCtx, p.MetaAddr)
						if err != nil {
							return err
						}
						defer c.pool.Put(conn)
						client := conn.Client()

						c.logger.Debug().Str("peer", p.ID).Str("addr", p.MetaAddr).Msg("Sending DoGet to peer")

						// DoGet with Search Ticket
						ticketQuery := query.TicketQuery{
							Search: &remoteReq,
						}
						ticketBytes, err := json.Marshal(ticketQuery)
						if err != nil {
							return err
						}

						stream, err := client.DoGet(subCtx, &flight.Ticket{Ticket: ticketBytes})
						if err != nil {
							return err
						}

						reader, err := flight.NewRecordReader(stream)
						if err != nil {
							return err
						}
						defer reader.Release()

						var results []SearchResult
						sourceColIdx := -1
						if reader.Schema() != nil {
							for i, f := range reader.Schema().Fields() {
								if f.Name == "source" {
									sourceColIdx = i
									break
								}
							}
						}

						for reader.Next() {
							batchResults := extractSearchResultsFromRecord(reader.RecordBatch(), sourceColIdx)
							results = append(results, batchResults...)
						}
						if reader.Err() != nil {
							return reader.Err()
						}

						// Submit
						select {
						case resultHedge <- results:
							cancelHedge() // Cancel others
						case <-subCtx.Done():
						}
						return nil
					})

					if err != nil && ctxHedge.Err() == nil {
						// Only log and signal failure if the hedge context wasn't cancelled by a winner
						if status.Code(err) == codes.NotFound {
							c.logger.Debug().Err(err).Str("peer", p.ID).Msg("Peer does not have dataset after retries")
						} else {
							c.logger.Warn().Err(err).Str("peer", p.ID).Msg("DoGet failed after retries")
						}
						failSignal <- struct{}{}
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
				case <-ctxHedge.Done():
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

	// Wait for all peer requests to complete to ensure clean lifecycle
	go func() {
		wg.Wait()
	}()

	var finalResults []SearchResult

	if isHybrid {
		// Hybrid Mode: Drain all channels, separate by Source, then apply global RRF
		var allDense []SearchResult
		var allSparse []SearchResult

		for _, ch := range channels {
			for batch := range ch {
				for _, r := range batch {
					switch r.Source {
					case core.SourceDense:
						allDense = append(allDense, r)
					case core.SourceSparse:
						allSparse = append(allSparse, r)
					}
				}
			}
		}

		metrics.GlobalSearchFanoutSize.Observe(float64(len(allDense) + len(allSparse)))

		// Sort each list globally
		// Dense scores (e.g., Cosine/DotProduct) are descending. Distance (L2) is ascending.
		// RRF assumes sorted lists where index 0 is best.
		// We use a simple descending sort, assuming scores are higher-is-better for hybrid.
		sort.Slice(allDense, func(i, j int) bool { return allDense[i].Score > allDense[j].Score })
		sort.Slice(allSparse, func(i, j int) bool { return allSparse[i].Score > allSparse[j].Score })

		// Record payload size (number of elements fused globally)
		metrics.GlobalRRFPayloadBytes.Observe(float64(len(allDense) + len(allSparse)))

		// Apply Global Reciprocal Rank Fusion on the complete gathered lists
		rrfStart := time.Now()
		finalResults = ReciprocalRankFusion(req.Dataset, allDense, allSparse, 60, req.K, nil)
		metrics.GlobalRRFLatencySeconds.Observe(time.Since(rrfStart).Seconds())
	} else {
		// 3. Launch Merger for standard sorted streams
		finalResults = MergeSortedStreams(channels, req.K)
	}

	metrics.GlobalSearchDuration.Observe(time.Since(start).Seconds())
	return finalResults, nil
}

// Close releases any resources held by the coordinator.
func (c *GlobalSearchCoordinator) Close() error {
	// The pool is managed externally, so we don't need to close it here.
	return nil
}

// GlobalGeoSearch performs scatter-gather geospatial search across the cluster
func (c *GlobalSearchCoordinator) GlobalGeoSearch(ctx context.Context, localResults []SearchResult, req *core.GeoSearchRequest, peers []mesh.Member) ([]SearchResult, error) {
	start := time.Now()

	_, span := tracing.CreateSpan(ctx, "GlobalGeoSearch")
	if span != nil {
		span.SetAttributes(
			"component", "distributed",
			"level", "coordination",
			"peers", fmt.Sprint(len(peers)),
		)
		defer span.End()
	}

	if len(peers) == 0 {
		return localResults, nil
	}

	metrics.GlobalSearchFanoutSize.Observe(float64(len(peers)))

	peerGroups := make(map[string][]mesh.Member)
	for i := range peers {
		p := peers[i]
		groupID := p.ID
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
	groupChs := make([]chan []SearchResult, len(peerGroups))
	var wg sync.WaitGroup

	groupIdx := 0
	for _, members := range peerGroups {
		ch := make(chan []SearchResult, 1)
		groupChs[groupIdx] = ch
		channels[groupIdx+1] = ch
		groupIdx++

		wg.Add(1)
		go func(replicas []mesh.Member, outCh chan []SearchResult) {
			defer wg.Done()
			defer close(outCh)

			ctxHedge, cancelHedge := context.WithCancel(ctx)
			defer cancelHedge()

			resultHedge := make(chan []SearchResult, 1)
			failSignal := make(chan struct{}, len(replicas))
			var wgReplicas sync.WaitGroup

			for i := range replicas {
				rp := replicas[i]
				wgReplicas.Add(1)
				go func(p mesh.Member) {
					defer wgReplicas.Done()

					err := retry.Do(ctxHedge, c.retryPolicy, func(subCtx context.Context) error {
						conn, err := c.pool.Get(subCtx, p.MetaAddr)
						if err != nil {
							return err
						}
						defer c.pool.Put(conn)
						client := conn.Client()

						c.logger.Debug().Str("peer", p.ID).Str("addr", p.MetaAddr).Msg("Sending DoGet GeoSearch to peer")

						ticketQuery := core.TicketQuery{
							GeoSearch: req,
						}
						ticketBytes, err := json.Marshal(ticketQuery)
						if err != nil {
							return err
						}

						stream, err := client.DoGet(subCtx, &flight.Ticket{Ticket: ticketBytes})
						if err != nil {
							return err
						}

						reader, err := flight.NewRecordReader(stream)
						if err != nil {
							return err
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
									ID:    lbtypes.VectorID(ids[k]), // #nosec G115
									Score: scores[k],
								})
							}
						}
						if reader.Err() != nil {
							return reader.Err()
						}

						select {
						case resultHedge <- results:
							cancelHedge()
						case <-subCtx.Done():
						}
						return nil
					})

					if err != nil && ctxHedge.Err() == nil {
						c.logger.Warn().Err(err).Str("peer", p.ID).Msg("DoGet GeoSearch failed after retries")
						failSignal <- struct{}{}
					}
				}(rp)
			}

			finishedAll := make(chan struct{})
			go func() {
				wgReplicas.Wait()
				close(finishedAll)
			}()

			failedCount := 0
			for {
				select {
				case res := <-resultHedge:
					outCh <- res
					return
				case <-failSignal:
					failedCount++
					if failedCount == len(replicas) {
						metrics.GlobalSearchPartialFailures.Inc()
						return
					}
				case <-finishedAll:
					return
				case <-ctxHedge.Done():
					metrics.GlobalSearchPartialFailures.Inc()
					return
				}
			}
		}(members, ch)
	}

	go func() {
		wg.Wait()
	}()

	finalResults := MergeSortedStreams(channels, req.K)
	metrics.GlobalSearchDuration.Observe(time.Since(start).Seconds())
	return finalResults, nil
}

// GlobalTemporalSearch performs scatter-gather temporal search across the cluster
func (c *GlobalSearchCoordinator) GlobalTemporalSearch(ctx context.Context, localResults []SearchResult, req *core.TemporalSearchRequest, peers []mesh.Member) ([]SearchResult, error) {
	start := time.Now()

	_, span := tracing.CreateSpan(ctx, "GlobalTemporalSearch")
	if span != nil {
		span.SetAttributes(
			"component", "distributed",
			"level", "coordination",
			"peers", fmt.Sprint(len(peers)),
		)
		defer span.End()
	}

	if len(peers) == 0 {
		return localResults, nil
	}

	metrics.GlobalSearchFanoutSize.Observe(float64(len(peers)))

	peerGroups := make(map[string][]mesh.Member)
	for i := range peers {
		p := peers[i]
		groupID := p.ID
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
	groupChs := make([]chan []SearchResult, len(peerGroups))
	var wg sync.WaitGroup

	groupIdx := 0
	for _, members := range peerGroups {
		ch := make(chan []SearchResult, 1)
		groupChs[groupIdx] = ch
		channels[groupIdx+1] = ch
		groupIdx++

		wg.Add(1)
		go func(replicas []mesh.Member, outCh chan []SearchResult) {
			defer wg.Done()
			defer close(outCh)

			ctxHedge, cancelHedge := context.WithCancel(ctx)
			defer cancelHedge()

			resultHedge := make(chan []SearchResult, 1)
			failSignal := make(chan struct{}, len(replicas))
			var wgReplicas sync.WaitGroup

			for i := range replicas {
				rp := replicas[i]
				wgReplicas.Add(1)
				go func(p mesh.Member) {
					defer wgReplicas.Done()

					err := retry.Do(ctxHedge, c.retryPolicy, func(subCtx context.Context) error {
						conn, err := c.pool.Get(subCtx, p.MetaAddr)
						if err != nil {
							return err
						}
						defer c.pool.Put(conn)
						client := conn.Client()

						c.logger.Debug().Str("peer", p.ID).Str("addr", p.MetaAddr).Msg("Sending DoGet TemporalSearch to peer")

						ticketQuery := core.TicketQuery{
							TemporalSearch: req,
						}
						ticketBytes, err := json.Marshal(ticketQuery)
						if err != nil {
							return err
						}

						stream, err := client.DoGet(subCtx, &flight.Ticket{Ticket: ticketBytes})
						if err != nil {
							return err
						}

						reader, err := flight.NewRecordReader(stream)
						if err != nil {
							return err
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
									ID:    lbtypes.VectorID(ids[k]), // #nosec G115
									Score: scores[k],
								})
							}
						}
						if reader.Err() != nil {
							return reader.Err()
						}

						select {
						case resultHedge <- results:
							cancelHedge()
						case <-subCtx.Done():
						}
						return nil
					})

					if err != nil && ctxHedge.Err() == nil {
						c.logger.Warn().Err(err).Str("peer", p.ID).Msg("DoGet TemporalSearch failed after retries")
						failSignal <- struct{}{}
					}
				}(rp)
			}

			finishedAll := make(chan struct{})
			go func() {
				wgReplicas.Wait()
				close(finishedAll)
			}()

			failedCount := 0
			for {
				select {
				case res := <-resultHedge:
					outCh <- res
					return
				case <-failSignal:
					failedCount++
					if failedCount == len(replicas) {
						metrics.GlobalSearchPartialFailures.Inc()
						return
					}
				case <-finishedAll:
					return
				case <-ctxHedge.Done():
					metrics.GlobalSearchPartialFailures.Inc()
					return
				}
			}
		}(members, ch)
	}

	go func() {
		wg.Wait()
	}()

	finalResults := MergeSortedStreams(channels, req.K)
	metrics.GlobalSearchDuration.Observe(time.Since(start).Seconds())
	return finalResults, nil
}

func extractSearchResultsFromRecord(rec arrow.RecordBatch, sourceColIdx int) []SearchResult {
	if rec == nil || rec.NumRows() == 0 || rec.NumCols() < 2 {
		return nil
	}

	col0 := rec.Column(0)
	col1 := rec.Column(1)
	n := int(rec.NumRows())
	results := make([]SearchResult, 0, n)

	extractID := func(i int) uint64 {
		switch arr := col0.(type) {
		case *array.Uint64:
			return arr.Value(i)
		case *array.Uint32:
			return uint64(arr.Value(i))
		case *array.Int64:
			return uint64(arr.Value(i))
		case *array.Int32:
			return uint64(arr.Value(i))
		default:
			return 0
		}
	}

	extractScore := func(i int) float32 {
		switch arr := col1.(type) {
		case *array.Float32:
			return arr.Value(i)
		case *array.Float64:
			return float32(arr.Value(i))
		default:
			return 0
		}
	}

	var sourceCol *array.Uint8
	if sourceColIdx != -1 && sourceColIdx < int(rec.NumCols()) {
		if arr, ok := rec.Column(sourceColIdx).(*array.Uint8); ok {
			sourceCol = arr
		}
	}

	for k := 0; k < n; k++ {
		idVal := extractID(k)
		if idVal > math.MaxUint32 {
			idVal = math.MaxUint32
		}
		res := SearchResult{
			ID:    lbtypes.VectorID(idVal), // #nosec G115
			Score: extractScore(k),
		}
		if sourceCol != nil && k < sourceCol.Len() {
			res.Source = sourceCol.Value(k)
		}
		results = append(results, res)
	}
	return results
}
