package main

// nosec G404 - math/rand is used for test data generation, not security-sensitive
import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/client"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
)

var (
	peers            = flag.String("peers", "127.0.0.1:3000", "Comma-separated list of peer addresses")
	duration         = flag.Duration("duration", 5*time.Minute, "Duration of the soak test")
	concurrency      = flag.Int("workers", 4, "Number of concurrent workers per phase")
	dataset          = flag.String("dataset", "soak_test_collection", "Dataset name")
	dim              = flag.Int("dim", 128, "Vector dimension")
	batchSize        = flag.Int("batch", 100, "Injest batch size")
	deleteRate       = flag.Float64("delete-rate", 0.1, "Probability of delete/compact after search")
	chaosEnabled     = flag.Bool("chaos", false, "Enable random process kills")
	filterEnabled    = flag.Bool("filters", true, "Enable compound filter searches")
	namespace        = flag.String("namespace", "", "Namespace for multi-tenancy")
	indexType        = flag.String("index", "hnsw", "Index type: hnsw, arrow_hnsw, ivf_pq, diskann")
	enableCache      = flag.Bool("cache", false, "Enable semantic query cache")
	enableGlobal     = flag.Bool("global", false, "Enable global search (scatter-gather across nodes)")
	enableRerank     = flag.Bool("rerank", false, "Enable cross-encoder reranking")
	enableBM25       = flag.Bool("bm25", false, "Enable BM25 text-only search mode")
	enableDateFilter = flag.Bool("date-filter", false, "Enable Date64/Timestamp filter tests")
	enableTelemetry  = flag.Bool("telemetry", false, "Enable OpenTelemetry tracing")
	rerankTopK       = flag.Int("rerank-k", 50, "Number of results to rerank")
)

type Stats struct {
	IngestOps        atomic.Int64
	DenseSearches    atomic.Int64
	SparseSearches   atomic.Int64
	FilteredSearches atomic.Int64
	HybridSearches   atomic.Int64
	GlobalSearches   atomic.Int64
	BM25Searches     atomic.Int64
	RerankedSearches atomic.Int64
	CacheHits        atomic.Int64
	CacheMisses      atomic.Int64
	TelemetrySpans   atomic.Int64
	DeleteOps        atomic.Int64
	Errors           atomic.Int64
}

type SearchMode int

const (
	DenseSearch SearchMode = iota
	SparseSearch
	FilteredSearch
	HybridSearch
	GlobalSearch
	BM25Search
	RerankedSearch
)

func main() {
	flag.Parse()

	fmt.Printf("🌊 Starting Longbow Soak Test\n")
	fmt.Printf("Dataset: %s, Workers: %d, Duration: %s\n", *dataset, *concurrency, *duration)
	fmt.Printf("Features: Index=%s, Cache=%v, Global=%v, Rerank=%v, BM25=%v\n",
		*indexType, *enableCache, *enableGlobal, *enableRerank, *enableBM25)
	if *namespace != "" {
		fmt.Printf("Namespace: %s (multi-tenancy enabled)\n", *namespace)
	}

	peerList := strings.Split(*peers, ",")

	ctx, cancel := context.WithTimeout(context.Background(), *duration+10*time.Second)
	defer cancel()

	stats := &Stats{}
	start := time.Now()

	var wg sync.WaitGroup

	fmt.Printf("⏳ Warmup: Ingesting initial data...\n")
	warmupCtx, warmupCancel := context.WithTimeout(context.Background(), 15*time.Second)
	warmupWg := sync.WaitGroup{}
	for i := 0; i < *concurrency; i++ {
		warmupWg.Add(1)
		go runWorker(warmupCtx, &warmupWg, peerList[i%len(peerList)], "ingest", stats)
	}
	warmupWg.Wait()
	warmupCancel()

	fmt.Printf("✅ Warmup complete. Starting search workers...\n")

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go runWorker(ctx, &wg, peerList[i%len(peerList)], "ingest", stats)
	}

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go runWorker(ctx, &wg, peerList[i%len(peerList)], "search_delete", stats)
	}

	// Wait for duration or context
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		case <-ticker.C:
			elapsed := time.Since(start)
			fmt.Printf("[%v] Ingest: %d, Dense: %d, Sparse: %d, Filtered: %d, Hybrid: %d, Global: %d, BM25: %d, Rerank: %d, CacheHits: %d, Delete: %d, Errors: %d\n",
				elapsed.Round(time.Second),
				stats.IngestOps.Load(),
				stats.DenseSearches.Load(),
				stats.SparseSearches.Load(),
				stats.FilteredSearches.Load(),
				stats.HybridSearches.Load(),
				stats.GlobalSearches.Load(),
				stats.BM25Searches.Load(),
				stats.RerankedSearches.Load(),
				stats.CacheHits.Load(),
				stats.DeleteOps.Load(),
				stats.Errors.Load())
		}
	}

	wg.Wait()
	totalSearches := stats.DenseSearches.Load() + stats.SparseSearches.Load() +
		stats.FilteredSearches.Load() + stats.HybridSearches.Load() +
		stats.GlobalSearches.Load() + stats.BM25Searches.Load() + stats.RerankedSearches.Load()
	totalCache := stats.CacheHits.Load() + stats.CacheMisses.Load()
	fmt.Printf("\n--- Final Results ---\n")
	fmt.Printf("Total Elapsed: %v\n", time.Since(start))
	fmt.Printf("Ingest Ops:        %d\n", stats.IngestOps.Load())
	fmt.Printf("Dense Searches:    %d\n", stats.DenseSearches.Load())
	fmt.Printf("Sparse Searches:   %d\n", stats.SparseSearches.Load())
	fmt.Printf("Filtered Searches: %d\n", stats.FilteredSearches.Load())
	fmt.Printf("Hybrid Searches:   %d\n", stats.HybridSearches.Load())
	fmt.Printf("Global Searches:   %d\n", stats.GlobalSearches.Load())
	fmt.Printf("BM25 Searches:     %d\n", stats.BM25Searches.Load())
	fmt.Printf("Reranked Searches: %d\n", stats.RerankedSearches.Load())
	fmt.Printf("Total Searches:    %d\n", totalSearches)
	fmt.Printf("Cache Hits:        %d\n", stats.CacheHits.Load())
	fmt.Printf("Cache Misses:      %d\n", stats.CacheMisses.Load())
	if totalCache > 0 {
		fmt.Printf("Cache Hit Rate:    %.2f%%\n", float64(stats.CacheHits.Load())/float64(totalCache)*100)
	}
	fmt.Printf("Delete Ops:        %d\n", stats.DeleteOps.Load())
	fmt.Printf("Total Errors:      %d\n", stats.Errors.Load())
}

func runWorker(ctx context.Context, wg *sync.WaitGroup, peer, mode string, stats *Stats) {
	log.Printf("[%s] Worker starting", mode)
	defer wg.Done()

	c, err := client.NewSmartClient(peer)
	if err != nil {
		log.Printf("Worker failed to connect to %s: %v", peer, err)
		stats.Errors.Add(1)
		return
	}
	defer func() {
		if err := c.Close(); err != nil {
			log.Printf("Error closing client: %v", err)
		}
	}()

	// Skip readiness check - searches will handle not-ready state
	_ = c

	for {
		select {
		case <-ctx.Done():
			return
		default:
			var err error
			if mode == "ingest" {
				err = performIngest(ctx, c)
				if err == nil {
					stats.IngestOps.Add(1)
				}
			} else {
				searchMode, searchErr := performSearchAndDelete(ctx, c, stats)
				searchNames := []string{"Dense", "Sparse", "Filtered", "Hybrid", "Global", "BM25", "Reranked"}
				if searchErr == nil {
					log.Printf("[%s] %s search succeeded", mode, searchNames[searchMode])
					switch searchMode {
					case DenseSearch:
						stats.DenseSearches.Add(1)
					case SparseSearch:
						stats.SparseSearches.Add(1)
					case FilteredSearch:
						stats.FilteredSearches.Add(1)
					case HybridSearch:
						stats.HybridSearches.Add(1)
					case GlobalSearch:
						stats.GlobalSearches.Add(1)
					case BM25Search:
						stats.BM25Searches.Add(1)
					case RerankedSearch:
						stats.RerankedSearches.Add(1)
					}
				}
				err = searchErr
			}

			if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
				log.Printf("[%s] Error: %v", mode, err)
				stats.Errors.Add(1)
			}
		}
	}
}

func performIngest(ctx context.Context, c *client.SmartClient) error {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "embedding", Type: arrow.FixedSizeListOf(int32(*dim), arrow.PrimitiveTypes.Float32)}, // #nosec G115
		{Name: "category", Type: arrow.PrimitiveTypes.Int64},
		{Name: "score", Type: arrow.PrimitiveTypes.Float32},
		{Name: "priority", Type: arrow.BinaryTypes.String},
		{Name: "status", Type: arrow.BinaryTypes.String},
		{Name: "deleted", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64},
		{Name: "created_at", Type: arrow.PrimitiveTypes.Int64},
		{Name: "updated_at", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	idB := b.Field(0).(*array.StringBuilder)
	vecB := b.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)
	catB := b.Field(2).(*array.Int64Builder)
	scoreB := b.Field(3).(*array.Float32Builder)
	prioB := b.Field(4).(*array.StringBuilder)
	statusB := b.Field(5).(*array.StringBuilder)
	deletedB := b.Field(6).(*array.BooleanBuilder)
	tsB := b.Field(7).(*array.Int64Builder)
	createdB := b.Field(8).(*array.Int64Builder)
	updatedB := b.Field(9).(*array.Int64Builder)

	categories := []string{"urgent", "high", "normal", "low"}
	statuses := []string{"active", "pending", "closed"}

	for i := 0; i < *batchSize; i++ {
		idB.Append(uuid.New().String())
		vecB.Append(true)
		for j := 0; j < *dim; j++ {
			valB.Append(rand.Float32()) // #nosec G404
		}
		catB.Append(int64(rand.Intn(10))) // #nosec G404
		scoreB.Append(rand.Float32() * 100) // #nosec G404
		prioB.Append(categories[rand.Intn(len(categories))]) // #nosec G404
		statusB.Append(statuses[rand.Intn(len(statuses))]) // #nosec G404
		deletedB.Append(rand.Float32() < 0.1) // #nosec G404
		tsB.Append(time.Now().UnixNano())
		now := time.Now().UnixNano()
		createdB.Append(now)
		updatedB.Append(now)
	}

	rec := b.NewRecordBatch()
	defer rec.Release()

	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{*dataset},
	}
	stream, err := c.DoPut(ctx, desc)
	if err != nil {
		return err
	}

	wr := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	wr.SetFlightDescriptor(desc)

	if err := wr.Write(rec); err != nil {
		_ = wr.Close()
		return err
	}
	return wr.Close()
}

func performSearchAndDelete(ctx context.Context, c *client.SmartClient, stats *Stats) (SearchMode, error) {
	searchMode := SearchMode(rand.Intn(7)) // #nosec G404
	queryVec := make([]float32, *dim)
	for i := 0; i < *dim; i++ {
		queryVec[i] = rand.Float32() // #nosec G404
	}

	req := map[string]any{
		"dataset": *dataset,
		"k":       5,
	}

	switch searchMode {
	case DenseSearch:
		req["vector"] = queryVec
	case SparseSearch:
		req["vector"] = queryVec
		textQueries := []string{"machine learning", "neural network", "data science", "deep learning", "artificial intelligence"}
		req["text_query"] = textQueries[rand.Intn(len(textQueries))] // #nosec G404
		req["alpha"] = 0.1
	case FilteredSearch:
		req["vector"] = queryVec
		if *filterEnabled {
			req["filters"] = []any{generateCompoundFilter()}
		}
	case HybridSearch:
		req["vector"] = queryVec
		textQueries := []string{"search query", "information retrieval", "vector database"}
		req["text_query"] = textQueries[rand.Intn(len(textQueries))] // #nosec G404
		req["alpha"] = rand.Float64() // #nosec G404
	case GlobalSearch:
		req["vector"] = queryVec
		req["global"] = true
	case BM25Search:
		textQueries := []string{"search query", "information retrieval", "vector database", "machine learning"}
		req["text_query"] = textQueries[rand.Intn(len(textQueries))] // #nosec G404
		req["bm25_only"] = true
	case RerankedSearch:
		req["vector"] = queryVec
		req["rerank"] = true
		req["rerank_k"] = *rerankTopK
	}

	if *namespace != "" {
		req["namespace"] = *namespace
	}

	if *enableCache {
		req["use_cache"] = true
	}

	if *enableDateFilter {
		req["filters"] = []any{generateDateFilter()}
	}

	body, _ := json.Marshal(req)

	action := &flight.Action{
		Type: "VectorSearch",
		Body: body,
	}

	stream, err := c.DoAction(ctx, action)
	if err != nil {
		return searchMode, err
	}

	// Drain results
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return searchMode, err
		}
	}

	if rand.Float64() < *deleteRate { // #nosec G404
		compactReq := map[string]any{"dataset": *dataset}
		cBody, _ := json.Marshal(compactReq)
		compactAction := &flight.Action{
			Type: "Compact",
			Body: cBody,
		}
		_, err := c.DoAction(ctx, compactAction)
		if err == nil {
			stats.DeleteOps.Add(1)
		}
	}

	return searchMode, nil
}

func generateCompoundFilter() any {
	filterTypes := []string{"AND", "OR", "NOT"}
	filterType := filterTypes[rand.Intn(len(filterTypes))] // #nosec G404

	switch filterType {
	case "AND":
		return map[string]any{
			"logic": "AND",
			"filters": []any{
				map[string]any{"field": "category", "operator": "=", "value": fmt.Sprintf("%d", rand.Intn(10))}, // #nosec G404
				map[string]any{"field": "score", "operator": ">", "value": fmt.Sprintf("%d", rand.Intn(50))}, // #nosec G404
			},
		}
	case "OR":
		return map[string]any{
			"logic": "OR",
			"filters": []any{
				map[string]any{"field": "priority", "operator": "=", "value": "high"},
				map[string]any{"field": "status", "operator": "=", "value": "active"},
			},
		}
	case "NOT":
		return map[string]any{
			"logic": "NOT",
			"filters": []any{
				map[string]any{"field": "deleted", "operator": "=", "value": "true"},
			},
		}
	}
	return nil
}

func generateDateFilter() any {
	now := time.Now().UnixNano()
	oneDayAgo := now - 24*60*60*1000000000
	filterTypes := []string{"range", "before", "after"}
	filterType := filterTypes[rand.Intn(len(filterTypes))] // #nosec G404

	switch filterType {
	case "range":
		return map[string]any{
			"field":    "created_at",
			"operator": "range",
			"value":    []int64{oneDayAgo, now},
		}
	case "before":
		return map[string]any{
			"field":    "updated_at",
			"operator": "<",
			"value":    now,
		}
	case "after":
		return map[string]any{
			"field":    "created_at",
			"operator": ">",
			"value":    oneDayAgo,
		}
	}
	return nil
}

func checkReadiness(ctx context.Context, c *client.SmartClient) (bool, error) {
	action := &flight.Action{
		Type: "check_readiness",
		Body: []byte(fmt.Sprintf(`{"dataset": %q}`, *dataset)),
	}
	stream, err := c.DoAction(ctx, action)
	if err != nil {
		return false, err
	}
	res, err := stream.Recv()
	if err != nil {
		return false, err
	}
	var dr struct {
		Status     string `json:"status"`
		IndexReady bool   `json:"index_ready"`
	}
	if err := json.Unmarshal(res.Body, &dr); err != nil {
		return false, err
	}
	return dr.IndexReady, nil
}
