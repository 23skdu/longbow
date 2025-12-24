package store

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/mesh"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type clientEntry struct {
	client  flight.Client
	lastUse time.Time
}

// GlobalSearchCoordinator handles scatter-gather logic
type GlobalSearchCoordinator struct {
	logger *zap.Logger
	mu     sync.Mutex
	// clients: map[string]*clientEntry
	clients     sync.Map
	idleTimeout time.Duration
	stopCh      chan struct{}
}

func NewGlobalSearchCoordinator(logger *zap.Logger) *GlobalSearchCoordinator {
	c := &GlobalSearchCoordinator{
		logger:      logger,
		idleTimeout: 5 * time.Minute,
		stopCh:      make(chan struct{}),
	}
	go c.cleanupLoop()
	return c
}

// GlobalSearch performs scatter-gather search across the cluster
func (c *GlobalSearchCoordinator) GlobalSearch(ctx context.Context, localResults []SearchResult, req VectorSearchRequest, peers []mesh.Member) ([]SearchResult, error) {
	start := time.Now()

	// If no peers, just return local
	if len(peers) == 0 {
		return localResults, nil
	}

	metrics.GlobalSearchFanoutSize.Observe(float64(len(peers)))

	resultsCh := make(chan []SearchResult, len(peers))
	errCh := make(chan error, len(peers))
	var wg sync.WaitGroup

	// Prepare remote request (LocalOnly = true to prevent recursion)
	remoteReq := req
	remoteReq.LocalOnly = true
	remoteReqBody, _ := json.Marshal(remoteReq) // Should not fail if original parsed ok

	for _, peer := range peers {
		wg.Add(1)
		go func(p mesh.Member) {
			defer wg.Done()

			// 1. Get Client
			client, err := c.getClient(p.MetaAddr)
			if err != nil {
				c.logger.Warn("Failed to dial peer", zap.String("peer", p.ID), zap.String("addr", p.MetaAddr), zap.Error(err))
				metrics.GlobalSearchPartialFailures.Inc()
				return
			}
			// Update last use time after retrieval
			if entry, ok := c.clients.Load(p.MetaAddr); ok {
				entry.(*clientEntry).lastUse = time.Now()
			}

			// 2. DoAction
			action := &flight.Action{
				Type: "VectorSearch",
				Body: remoteReqBody,
			}

			// Add timeout for remote calls
			subCtx, cancel := context.WithTimeout(ctx, 2*time.Second) // configurable?
			defer cancel()

			stream, err := client.DoAction(subCtx, action)
			if err != nil {
				c.logger.Warn("Remote search failed", zap.String("peer", p.ID), zap.Error(err))
				metrics.GlobalSearchPartialFailures.Inc()
				return
			}

			// 3. Read Response
			// Expecting single result with JSON body
			res, err := stream.Recv()
			if err != nil {
				c.logger.Warn("Remote search recv failed", zap.String("peer", p.ID), zap.Error(err))
				metrics.GlobalSearchPartialFailures.Inc()
				return
			}

			var resp VectorSearchResponse
			if err := json.Unmarshal(res.Body, &resp); err != nil {
				c.logger.Warn("Remote search parse failed", zap.String("peer", p.ID), zap.Error(err))
				metrics.GlobalSearchPartialFailures.Inc()
				return
			}

			// Convert to internal SearchResult
			results := make([]SearchResult, len(resp.IDs))
			for i := range resp.IDs {
				results[i] = SearchResult{
					ID:    VectorID(resp.IDs[i]),
					Score: resp.Scores[i],
				}
			}

			resultsCh <- results
		}(peer)
	}

	wg.Wait()
	close(resultsCh)
	close(errCh)

	// Gather results
	allResults := make([]SearchResult, 0, len(localResults)+len(peers)*req.K)
	allResults = append(allResults, localResults...) // Add local

	for remoteRes := range resultsCh {
		allResults = append(allResults, remoteRes...)
	}

	// Sort and Top K
	sort.Slice(allResults, func(i, j int) bool {
		// Descending score
		return allResults[i].Score > allResults[j].Score
	})

	if len(allResults) > req.K {
		allResults = allResults[:req.K]
	}

	metrics.GlobalSearchDuration.Observe(time.Since(start).Seconds())
	return allResults, nil
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
	c.clients.Range(func(key, value interface{}) bool {
		entry := value.(*clientEntry)
		if time.Since(entry.lastUse) > c.idleTimeout {
			c.logger.Info("Pruning idle flight client", zap.String("addr", key.(string)))
			_ = entry.client.Close()
			c.clients.Delete(key)
		}
		return true
	})
}

func (c *GlobalSearchCoordinator) Close() error {
	close(c.stopCh)
	c.clients.Range(func(key, value interface{}) bool {
		entry := value.(*clientEntry)
		_ = entry.client.Close()
		c.clients.Delete(key)
		return true
	})
	return nil
}
