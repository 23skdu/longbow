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

type CollectionRegistry struct {
	mu           sync.RWMutex
	collections  map[string]*CollectionInfo
	routingRules map[string]*RoutingRule
}

type RoutingRule struct {
	Tag        string   `json:"tag"`
	Collection string   `json:"collection"`
	Priority   int      `json:"priority"`
	Conditions []string `json:"conditions"`
}

func NewCollectionRegistry() *CollectionRegistry {
	return &CollectionRegistry{
		collections:  make(map[string]*CollectionInfo),
		routingRules: make(map[string]*RoutingRule),
	}
}

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

func (cr *CollectionRegistry) GetCollection(name string) (*CollectionInfo, bool) {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	info, ok := cr.collections[name]
	return info, ok
}

func (cr *CollectionRegistry) ListCollections() []*CollectionInfo {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	collections := make([]*CollectionInfo, 0, len(cr.collections))
	for _, info := range cr.collections {
		collections = append(collections, info)
	}
	return collections
}

func (cr *CollectionRegistry) UpdateCollection(name string, info *CollectionInfo) error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if _, exists := cr.collections[name]; !exists {
		return fmt.Errorf("collection %s not found", name)
	}

	cr.collections[name] = info
	return nil
}

func (cr *CollectionRegistry) DeleteCollection(name string) error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if _, exists := cr.collections[name]; !exists {
		return fmt.Errorf("collection %s not found", name)
	}

	delete(cr.collections, name)
	return nil
}

func (cr *CollectionRegistry) RegisterRoutingRule(rule *RoutingRule) error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	cr.routingRules[rule.Tag] = rule
	return nil
}

func (cr *CollectionRegistry) GetRoutingRule(tag string) (*RoutingRule, bool) {
	cr.mu.RLock()
	defer cr.mu.RUnlock()
	rule, ok := cr.routingRules[tag]
	return rule, ok
}

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

type VectorSearcher interface {
	Search(ctx context.Context, query []float32, k int, options SearchOptions) ([]lbtypes.SearchResult, error)
	GetDimension() int
}

type FederatedQueryRouter struct {
	mu           sync.RWMutex
	registry     *CollectionRegistry
	searchers    map[string]VectorSearcher
	defaultRRF   float64
	defaultLimit int
}

func NewFederatedQueryRouter() *FederatedQueryRouter {
	return &FederatedQueryRouter{
		registry:     NewCollectionRegistry(),
		searchers:    make(map[string]VectorSearcher),
		defaultRRF:   0.6,
		defaultLimit: 100,
	}
}

func (fqr *FederatedQueryRouter) RegisterCollection(name string, searcher VectorSearcher, info *CollectionInfo) error {
	fqr.mu.Lock()
	defer fqr.mu.Unlock()

	fqr.searchers[name] = searcher

	info.Dimension = searcher.GetDimension()
	return fqr.registry.RegisterCollection(info)
}

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

func (fqr *FederatedQueryRouter) SetRRFFactor(rraf float64) {
	fqr.mu.Lock()
	defer fqr.mu.Unlock()
	fqr.defaultRRF = rraf
}

func (fqr *FederatedQueryRouter) GetRRFFactor() float64 {
	fqr.mu.RLock()
	defer fqr.mu.RUnlock()
	return fqr.defaultRRF
}

func (fqr *FederatedQueryRouter) GetCollectionCount() int {
	fqr.mu.RLock()
	defer fqr.mu.RUnlock()
	return len(fqr.searchers)
}

type FederatedBenchmark struct {
	router *FederatedQueryRouter
}

func NewFederatedBenchmark(router *FederatedQueryRouter) *FederatedBenchmark {
	return &FederatedBenchmark{router: router}
}

type BenchmarkResult struct {
	Collection         string  `json:"collection"`
	QueryCount         int     `json:"query_count"`
	AvgLatencyMs       float64 `json:"avg_latency_ms"`
	AvgResultsPerQuery float64 `json:"avg_results_per_query"`
	P50LatencyMs       float64 `json:"p50_latency_ms"`
	P99LatencyMs       float64 `json:"p99_latency_ms"`
	ThroughputQPS      float64 `json:"throughput_qps"`
}

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
