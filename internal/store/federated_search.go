package store

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/core"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
)

// CollectionInfo contains metadata and statistics for a search collection.
type CollectionInfo struct {
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Tags        []string          `json:"tags"`
	Dimension   int               `json:"dimension"`
	VectorCount int               `json:"vector_count"`
	Metadata    map[string]string `json:"metadata"`
	Shards      []string          `json:"shards"`
	CreatedAt   int64             `json:"created_at"`
	UpdatedAt   int64             `json:"updated_at"`
}

// CollectionRegistry manages the registration and lookup of search collections and routing rules.
type CollectionRegistry struct {
	mu           sync.RWMutex
	collections  map[string]*CollectionInfo
	routingRules map[string]*RoutingRule
}

// RoutingRule defines how queries should be routed to collections based on tags.
type RoutingRule struct {
	Tag        string   `json:"tag"`
	Collection string   `json:"collection"`
	Priority   int      `json:"priority"`
	Conditions []string `json:"conditions"`
}

// NewCollectionRegistry creates a new empty collection registry.
func NewCollectionRegistry() *CollectionRegistry {
	return &CollectionRegistry{
		collections:  make(map[string]*CollectionInfo),
		routingRules: make(map[string]*RoutingRule),
	}
}

// RegisterCollection adds a new collection to the registry.
func (cr *CollectionRegistry) RegisterCollection(info *CollectionInfo) error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if info.Name == "" {
		return fmt.Errorf("collection name cannot be empty")
	}

	if _, exists := cr.collections[info.Name]; exists {
		return fmt.Errorf("collection %s already exists", info.Name)
	}

	cr.collections[info.Name] = info
	return nil
}

// GetCollection retrieves collection information by name.
func (cr *CollectionRegistry) GetCollection(name string) (*CollectionInfo, bool) {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	info, ok := cr.collections[name]
	return info, ok
}

// ListCollections returns a slice of all registered collections.
func (cr *CollectionRegistry) ListCollections() []*CollectionInfo {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	collections := make([]*CollectionInfo, 0, len(cr.collections))
	for _, info := range cr.collections {
		collections = append(collections, info)
	}
	return collections
}

// UpdateCollection updates the metadata for an existing collection.
func (cr *CollectionRegistry) UpdateCollection(name string, info *CollectionInfo) error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if _, exists := cr.collections[name]; !exists {
		return fmt.Errorf("collection %s not found", name)
	}

	cr.collections[name] = info
	return nil
}

// DeleteCollection removes a collection from the registry.
func (cr *CollectionRegistry) DeleteCollection(name string) error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if _, exists := cr.collections[name]; !exists {
		return fmt.Errorf("collection %s not found", name)
	}

	delete(cr.collections, name)
	return nil
}

// RegisterRoutingRule adds a new routing rule to the registry.
func (cr *CollectionRegistry) RegisterRoutingRule(rule *RoutingRule) error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	cr.routingRules[rule.Tag] = rule
	return nil
}

// GetRoutingRule retrieves a routing rule by its tag.
func (cr *CollectionRegistry) GetRoutingRule(tag string) (*RoutingRule, bool) {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	rule, ok := cr.routingRules[tag]
	return rule, ok
}

// ListRoutingRules returns all registered routing rules sorted by priority.
func (cr *CollectionRegistry) ListRoutingRules() []*RoutingRule {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	rules := make([]*RoutingRule, 0, len(cr.routingRules))
	for _, rule := range cr.routingRules {
		rules = append(rules, rule)
	}
	sort.Slice(rules, func(i, j int) bool {
		return rules[i].Priority < rules[j].Priority
	})
	return rules
}

// VectorSearcher is the interface for performing vector similarity search.
type VectorSearcher interface {
	Search(ctx context.Context, query []float32, k int, options SearchOptions) ([]lbtypes.SearchResult, error)
	GetDimension() int
}

// FederatedQueryRouter routes search queries to multiple collections and merges results.
type FederatedQueryRouter struct {
	mu           sync.RWMutex
	registry     *CollectionRegistry
	searchers    map[string]VectorSearcher
	defaultRRF   float64
	defaultLimit int
}

// NewFederatedQueryRouter creates a new federated query router with default settings.
func NewFederatedQueryRouter() *FederatedQueryRouter {
	return &FederatedQueryRouter{
		registry:     NewCollectionRegistry(),
		searchers:    make(map[string]VectorSearcher),
		defaultRRF:   0.6,
		defaultLimit: 100,
	}
}

// RegisterCollection registers a searcher and its metadata with the router.
func (fqr *FederatedQueryRouter) RegisterCollection(name string, searcher VectorSearcher, info *CollectionInfo) error {
	fqr.mu.Lock()
	defer fqr.mu.Unlock()

	fqr.searchers[name] = searcher

	info.Dimension = searcher.GetDimension()
	return fqr.registry.RegisterCollection(info)
}

// Search performs a parallel search across specified collections and merges results using RRF.
func (fqr *FederatedQueryRouter) Search(ctx context.Context, query []float32, collections []string, k int) ([]lbtypes.SearchResult, error) {
	fqr.mu.RLock()
	defer fqr.mu.RUnlock()

	resultChan := make(chan partialResult, len(collections))
	var wg sync.WaitGroup

	for _, collection := range collections {
		wg.Add(1)
		go func(col string) {
			defer wg.Done()
			searcher, ok := fqr.searchers[col]
			if !ok {
				resultChan <- partialResult{collection: col, err: fmt.Errorf("collection %s not found", col)}
				return
			}

			results, err := searcher.Search(ctx, query, k, SearchOptions{ExactK: true})
			resultChan <- partialResult{collection: col, results: results, err: err}
		}(collection)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	var allResults []partialResult
	for r := range resultChan {
		if r.err != nil {
			continue
		}
		allResults = append(allResults, r)
	}

	merged := fqr.mergeResultsRRF(allResults, k)
	return merged, nil
}

// SearchByTags routes a search query to collections matching the specified tags.
func (fqr *FederatedQueryRouter) SearchByTags(ctx context.Context, query []float32, tags []string, k int) ([]lbtypes.SearchResult, error) {
	fqr.mu.RLock()
	collections := fqr.getCollectionsForTags(tags)
	fqr.mu.RUnlock()

	return fqr.Search(ctx, query, collections, k)
}

func (fqr *FederatedQueryRouter) getCollectionsForTags(tags []string) []string {
	rules := fqr.registry.ListRoutingRules()

	collectionSet := make(map[string]bool)
	for _, tag := range tags {
		for _, rule := range rules {
			if rule.Tag == tag {
				collectionSet[rule.Collection] = true
			}
		}
	}

	if len(collectionSet) == 0 {
		for name := range fqr.searchers {
			collectionSet[name] = true
		}
	}

	collections := make([]string, 0, len(collectionSet))
	for c := range collectionSet {
		collections = append(collections, c)
	}
	return collections
}

type partialResult struct {
	collection string
	results    []lbtypes.SearchResult
	err        error
}

func (fqr *FederatedQueryRouter) mergeResultsRRF(partials []partialResult, k int) []lbtypes.SearchResult {
	type scoredResult struct {
		id         lbtypes.VectorID
		score      float64
		collection string
		distance   float32
	}

	var allScored []scoredResult

	for _, p := range partials {
		for i, r := range p.results {
			rank := i + 1
			rrfScore := 1.0 / (fqr.defaultRRF * float64(rank))
			allScored = append(allScored, scoredResult{
				id:         r.ID,
				score:      rrfScore,
				collection: p.collection,
				distance:   r.Distance,
			})
		}
	}

	scoreMap := make(map[lbtypes.VectorID]*scoredResult)
	for i := range allScored {
		s := &allScored[i]
		if existing, ok := scoreMap[s.id]; ok {
			existing.score += s.score
		} else {
			scoreMap[s.id] = s
		}
	}

	uniqueScored := make([]scoredResult, 0, len(scoreMap))
	for _, s := range scoreMap {
		uniqueScored = append(uniqueScored, *s)
	}

	sort.Slice(uniqueScored, func(i, j int) bool {
		return uniqueScored[i].score > uniqueScored[j].score
	})

	limit := k
	if limit > len(uniqueScored) {
		limit = len(uniqueScored)
	}

	results := make([]lbtypes.SearchResult, limit)
	for i := 0; i < limit; i++ {
		metaBytes, _ := core.EncodeMetadata(map[string]interface{}{
			"collection": uniqueScored[i].collection,
		})
		results[i] = lbtypes.SearchResult{
			ID:       uniqueScored[i].id,
			Distance: uniqueScored[i].distance,
			Score:    float32(uniqueScored[i].score),
			Metadata: metaBytes,
		}
	}

	return results
}

// SetRRFFactor updates the Reciprocal Rank Fusion factor.
func (fqr *FederatedQueryRouter) SetRRFFactor(rraf float64) {
	fqr.mu.Lock()
	defer fqr.mu.Unlock()
	fqr.defaultRRF = rraf
}

// GetRRFFactor returns the current Reciprocal Rank Fusion factor.
func (fqr *FederatedQueryRouter) GetRRFFactor() float64 {
	fqr.mu.RLock()
	defer fqr.mu.RUnlock()
	return fqr.defaultRRF
}

// GetCollectionCount returns the number of registered collections.
func (fqr *FederatedQueryRouter) GetCollectionCount() int {
	fqr.mu.RLock()
	defer fqr.mu.RUnlock()
	return len(fqr.searchers)
}

// FederatedBenchmark provides tools for benchmarking federated search performance.
type FederatedBenchmark struct {
	router *FederatedQueryRouter
}

// NewFederatedBenchmark creates a new benchmark tool for the given router.
func NewFederatedBenchmark(router *FederatedQueryRouter) *FederatedBenchmark {
	return &FederatedBenchmark{router: router}
}

// BenchmarkResult holds the performance metrics for a search benchmark.
type BenchmarkResult struct {
	Collection         string  `json:"collection"`
	QueryCount         int     `json:"query_count"`
	AvgLatencyMs       float64 `json:"avg_latency_ms"`
	AvgResultsPerQuery float64 `json:"avg_results_per_query"`
	P50LatencyMs       float64 `json:"p50_latency_ms"`
	P99LatencyMs       float64 `json:"p99_latency_ms"`
	ThroughputQPS      float64 `json:"throughput_qps"`
}

// RunSingleCollection benchmarks search performance for a single collection.
func (fb *FederatedBenchmark) RunSingleCollection(ctx context.Context, collection string, queries [][]float32, k int) (*BenchmarkResult, error) {
	latencies := make([]float64, len(queries))
	resultCounts := make([]int, len(queries))

	for i, query := range queries {
		start := now()
		results, err := fb.router.Search(ctx, query, []string{collection}, k)
		latencies[i] = float64(now().Sub(start).Milliseconds())
		if err != nil {
			return nil, err
		}
		resultCounts[i] = len(results)
	}

	return &BenchmarkResult{
		Collection:         collection,
		QueryCount:         len(queries),
		AvgLatencyMs:       avg(latencies),
		AvgResultsPerQuery: avgFloat64(float64s(resultCounts)),
		P50LatencyMs:       percentile(latencies, 0.5),
		P99LatencyMs:       percentile(latencies, 0.99),
		ThroughputQPS:      float64(len(queries)) / (sum(latencies) / 1000),
	}, nil
}

// RunFederated benchmarks search performance across multiple collections.
func (fb *FederatedBenchmark) RunFederated(ctx context.Context, collections []string, queries [][]float32, k int) (*BenchmarkResult, error) {
	latencies := make([]float64, len(queries))
	resultCounts := make([]int, len(queries))

	for i, query := range queries {
		start := now()
		results, err := fb.router.Search(ctx, query, collections, k)
		latencies[i] = float64(now().Sub(start).Milliseconds())
		if err != nil {
			return nil, err
		}
		resultCounts[i] = len(results)
	}

	return &BenchmarkResult{
		Collection:         "federated",
		QueryCount:         len(queries),
		AvgLatencyMs:       avg(latencies),
		AvgResultsPerQuery: avgFloat64(float64s(resultCounts)),
		P50LatencyMs:       percentile(latencies, 0.5),
		P99LatencyMs:       percentile(latencies, 0.99),
		ThroughputQPS:      float64(len(queries)) / (sum(latencies) / 1000),
	}, nil
}

// CompareSingleVsFederated runs benchmarks for both single and federated search for comparison.
func (fb *FederatedBenchmark) CompareSingleVsFederated(ctx context.Context, collections []string, queries [][]float32, k int) (map[string]*BenchmarkResult, error) {
	federatedResult, err := fb.RunFederated(ctx, collections, queries, k)
	if err != nil {
		return nil, err
	}

	singleResults := make(map[string]*BenchmarkResult)
	for _, collection := range collections {
		result, err := fb.RunSingleCollection(ctx, collection, queries, k)
		if err != nil {
			return nil, err
		}
		singleResults[collection] = result
	}

	singleResults["federated"] = federatedResult
	return singleResults, nil
}

func now() time.Time {
	return time.Now()
}

func avg(slice []float64) float64 {
	if len(slice) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range slice {
		sum += v
	}
	return sum / float64(len(slice))
}

func sum(slice []float64) float64 {
	sum := 0.0
	for _, v := range slice {
		sum += v
	}
	return sum
}

func percentile(slice []float64, p float64) float64 {
	if len(slice) == 0 {
		return 0
	}
	sorted := make([]float64, len(slice))
	copy(sorted, slice)
	sort.Float64s(sorted)
	index := int(float64(len(sorted)) * p)
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func float64s(ints []int) []float64 {
	floats := make([]float64, len(ints))
	for i, v := range ints {
		floats[i] = float64(v)
	}
	return floats
}

func avgFloat64(slice []float64) float64 {
	return avg(slice)
}
