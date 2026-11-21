package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/23skdu/longbow/client"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/metadata"
)

func runSearchSuite(
	sc *client.SmartClient,
	dataset string,
	dim int,
	dtype string,
	scale int,
	queries int,
	workers int,
	tqBits int,
	seed int64,
	searchTimeout time.Duration,
	modes []string,
	shuffleModes bool,
) ([]BenchmarkResult, map[string][][]searchHit, string) {
	var mmDatasetToDrop string
	for _, m := range modes {
		if m == "MultiModal" {
			mmDataset := dataset + "_multimodal"
			if err := ensureMultiModalDataset(context.Background(), sc, mmDataset); err != nil {
				log.Printf("Warning: MultiModal setup failed for %s: %v\n", mmDataset, err)
			} else {
				mmDatasetToDrop = mmDataset
			}
			break
		}
	}

	// R12a: randomize mode order using the run seed if requested to break order coupling.
	if shuffleModes && len(modes) > 1 {
		r := rand.New(rand.NewSource(seed)) // #nosec G404 -- deterministic benchmark order shuffle
		r.Shuffle(len(modes), func(i, j int) {
			modes[i], modes[j] = modes[j], modes[i]
		})
	}

	// R10: a per-mode budget. The previous single 5-minute context covered all
	// modes, so each mode inherited what the ones before it left behind and the
	// last modes silently truncated.
	//
	// Item 31: every hit is kept, keyed by mode and by the stable global query
	// index, so the run can be hashed into one digest and compared against the
	// digest of another run of the same seed. Queries that failed or were
	// truncated leave a nil slot, which the digest records as "-" rather than
	// silently shortening.
	var results []BenchmarkResult
	searchHitsByMode := make(map[string][][]searchHit, len(modes))
	modeOrders := append([]string(nil), modes...)
	for modeIdx, mode := range modes {
		searchCtx, searchCancel := context.WithTimeout(context.Background(), searchTimeout)
		searchHitsByMode[mode] = make([][]searchHit, queries)

		log.Printf("[SEARCH][%s] Running %d queries with %d workers...\n", mode, queries, workers)
		start := time.Now()

		var latencies []float64
		var failed, truncated int64
		var mu sync.Mutex
		var wg sync.WaitGroup

		queriesPerWorker := queries / workers
		if queriesPerWorker == 0 {
			queriesPerWorker = 1
			workers = queries
		}

		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				state := NewReusableSearchState(dim)
				localLatencies := make([]float64, 0, queriesPerWorker)

				numToRun := queriesPerWorker
				if workerID == workers-1 {
					numToRun = queries - (queriesPerWorker * (workers - 1))
				}

				for i := 0; i < numToRun; i++ {
					// Stable global index: derived from the worker's slice
					// offset, not from a counter, so the query a given index
					// denotes does not depend on goroutine scheduling.
					globalIdx := workerID*queriesPerWorker + i
					// Stop issuing once this mode's own budget is spent;
					// every later query would fail instantly and be
					// reported as a fast miss.
					if searchCtx.Err() != nil {
						mu.Lock()
						truncated++
						mu.Unlock()
						continue
					}
					qStart := time.Now()
					hits, err := executeSearch(searchCtx, sc, dataset, dim, dtype, mode, state, globalIdx, scale)
					if err != nil {
						mu.Lock()
						if errors.Is(searchCtx.Err(), context.DeadlineExceeded) {
							truncated++
						} else {
							failed++
						}
						mu.Unlock()
						logDetailedError(fmt.Sprintf("[%s][Worker %d] Query %d", mode, workerID, i), err, sc)
						continue
					}
					mu.Lock()
					if globalIdx >= 0 && globalIdx < len(searchHitsByMode[mode]) {
						searchHitsByMode[mode][globalIdx] = hits
					}
					mu.Unlock()
					localLatencies = append(localLatencies, time.Since(qStart).Seconds()*1000)
				}

				mu.Lock()
				latencies = append(latencies, localLatencies...)
				mu.Unlock()
			}(w)
		}
		wg.Wait()

		// Read the deadline before cancelling, so a mode that ran out of budget
		// is labelled as truncated rather than merely slow.
		deadlineExceeded := errors.Is(searchCtx.Err(), context.DeadlineExceeded)
		searchCancel()

		duration := time.Since(start).Seconds()
		requested := int64(queries)
		completed := int64(len(latencies))

		mean, p50, p95, p99 := 0.0, 0.0, 0.0, 0.0
		if len(latencies) > 0 {
			// Mean before sorting, so it is the arithmetic mean and not an
			// artefact of the ordering. Recorded alongside the percentiles because
			// it is the only latency figure that pairs with QPS exactly: throughput
			// is completed/duration and mean latency is sum/completed, so
			// QPS x mean == workers by construction. See the concurrency invariant
			// in scripts/unified_benchmark.py, which used P50 and so fired on any
			// right-skewed latency distribution.
			var sum float64
			for _, l := range latencies {
				sum += l
			}
			mean = sum / float64(len(latencies))
			sort.Float64s(latencies)
			p50 = latencies[len(latencies)/2]
			p95 = latencies[int(float64(len(latencies))*0.95)]
			p99 = latencies[int(float64(len(latencies))*0.99)]
		}

		results = append(results, BenchmarkResult{
			Name:            "Search_" + mode,
			DurationSeconds: duration,
			Throughput:      float64(len(latencies)) / duration,
			ThroughputUnit:  "queries/s",
			Rows:            int64(len(latencies)),
			LatenciesMs:     latencies,
			MeanLatencyMs:   mean,
			P50LatencyMs:    p50,
			P95LatencyMs:    p95,
			P99LatencyMs:    p99,
			TqBits:          tqBits,

			ContextDeadlineExceeded: deadlineExceeded,
			QueriesRequested:        requested,
			QueriesFailed:           failed,
			QueriesTruncated:        truncated,
			Seed:                    seed,
			ModeIndex:               modeIdx,
			ModeCount:               len(modes),
			ModeOrders:              modeOrders,
		})
		note := ""
		if deadlineExceeded {
			note = fmt.Sprintf(" [TRUNCATED: budget %s exhausted, %d/%d queries ran; QPS is a floor not a measurement]",
				searchTimeout, completed, requested)
		} else if truncated > 0 {
			note = fmt.Sprintf(" [%d queries truncated]", truncated)
		}
		log.Printf("[SEARCH][%s] Completed %d/%d queries in %.4fs (%.2f QPS, mean: %.3fms, P50: %.2fms, P95: %.2fms, P99: %.2fms)%s\n",
			mode, completed, requested, duration, float64(completed)/duration, mean, p50, p95, p99, note)
	}

	return results, searchHitsByMode, mmDatasetToDrop
}

// executeSearch performs search by setting JSON ticket in DoGet or invoking Flight DoAction.
func executeSearch(ctx context.Context, sc *client.SmartClient, dataset string, dim int, dtype string, mode string, state *ReusableSearchState, queryIdx, corpusSize int) ([]searchHit, error) {
	switch mode {
	case "AdvancedText":
		return executeAdvancedTextAction(ctx, sc, dataset, 10)
	case "MultiModal":
		return executeMultiModalAction(ctx, sc, dataset+"_multimodal", 10)
	}

	var ticketBytes []byte
	switch mode {
	case "Recommend", "Geo", "GeoBox", "Temporal", "TemporalRange", "TemporalWindow", "ByID":
		ticketBytes = state.BuildSpecialTicket(dataset, mode, queryIdx, corpusSize)
	case "Range", "GeoHybrid", "Federated":
		ticketBytes = state.BuildVectorSpecialTicket(dataset, dim, dtype, mode, 10, queryIdx)
	default:
		ticketBytes = state.BuildSearchTicket(dataset, dim, dtype, mode, 10, queryIdx)
	}

	if mode == "GlobalGraphRAG" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-longbow-global", "true")
	}

	return executeDoGet(ctx, sc, ticketBytes)
}

func executeAdvancedTextAction(ctx context.Context, sc *client.SmartClient, dataset string, k int) ([]searchHit, error) {
	body, err := json.Marshal(map[string]any{
		"dataset":       dataset,
		"query":         "sku42 AND electronics",
		"k":             k,
		"facet_fields":  []string{"category"},
		"snippet_chars": 120,
	})
	if err != nil {
		return nil, err
	}
	stream, err := sc.DoAction(ctx, &flight.Action{Type: "AdvancedTextSearch", Body: body})
	if err != nil {
		return nil, err
	}
	res, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	var parsed struct {
		Hits []struct {
			VectorID uint32  `json:"vector_id"`
			Score    float64 `json:"score"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(res.Body, &parsed); err != nil {
		return nil, err
	}
	hits := make([]searchHit, 0, len(parsed.Hits))
	for _, h := range parsed.Hits {
		hits = append(hits, searchHit{ID: uint64(h.VectorID), Score: float32(h.Score)})
	}
	return hits, nil
}

func ensureMultiModalDataset(ctx context.Context, sc *client.SmartClient, mmDataset string) error {
	items := []map[string]any{
		{"id": "mm_0", "modality": "text", "caption": "electronics laptop benchmark vector"},
		{"id": "mm_1", "modality": "text", "caption": "electronics sensor audio stream"},
		{"id": "mm_2", "modality": "text", "caption": "clothing jacket outdoor winter"},
		{"id": "mm_3", "modality": "text", "caption": "home kitchen appliance smart"},
		{"id": "mm_4", "modality": "text", "caption": "books science fiction edition"},
	}
	body, err := json.Marshal(map[string]any{
		"dataset": mmDataset,
		"items":   items,
	})
	if err != nil {
		return err
	}
	stream, err := sc.DoAction(ctx, &flight.Action{Type: "MultiModalIngest", Body: body})
	if err != nil {
		return err
	}
	if _, err := stream.Recv(); err != nil {
		return err
	}
	waitForIndexingComplete(ctx, sc, mmDataset, 30*time.Second)
	return nil
}

func executeMultiModalAction(ctx context.Context, sc *client.SmartClient, mmDataset string, k int) ([]searchHit, error) {
	body, err := json.Marshal(map[string]any{
		"dataset":    mmDataset,
		"modality":   "text",
		"text_query": "electronics vector benchmark",
		"k":          k,
	})
	if err != nil {
		return nil, err
	}
	stream, err := sc.DoAction(ctx, &flight.Action{Type: "MultiModalSearch", Body: body})
	if err != nil {
		return nil, err
	}
	res, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	var parsed struct {
		Results []struct {
			ID       uint64  `json:"ID"`
			Score    float32 `json:"Score"`
			Distance float32 `json:"Distance"`
		} `json:"results"`
	}
	if err := json.Unmarshal(res.Body, &parsed); err != nil {
		return nil, err
	}
	hits := make([]searchHit, 0, len(parsed.Results))
	for _, r := range parsed.Results {
		scVal := r.Score
		if scVal == 0 {
			scVal = r.Distance
		}
		hits = append(hits, searchHit{ID: r.ID, Score: scVal})
	}
	return hits, nil
}

// executeDoGet performs the actual Flight DoGet call, drains the stream and
// returns what it contained. It used to discard the record outright, which
// left no way to tell whether two runs of the same seed had returned the same
// answers (roadmap Item 31).
func executeDoGet(ctx context.Context, sc *client.SmartClient, ticket []byte) ([]searchHit, error) {
	stream, err := sc.DoGet(ctx, ticket)
	if err != nil {
		return nil, err
	}
	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		return nil, err
	}
	var hits []searchHit
	// reader does not have Release
	for reader.Next() {
		hits = append(hits, collectSearchHits(reader.RecordBatch())...)
	}
	return hits, reader.Err()
}

// collectSearchHits pulls the (id, score) pair out of one response record.
// Schemas differ per mode, so the columns are matched by name first and by
// type second. A record without a recognisable pair contributes nothing: a
// digest taken over misread columns would look like a reproducibility failure
// and would be worse than a gap in the digest.
func collectSearchHits(rec arrow.RecordBatch) []searchHit {
	if rec == nil || rec.NumRows() == 0 {
		return nil
	}
	idCol, scoreCol := -1, -1
	for i := 0; i < int(rec.NumCols()); i++ {
		switch strings.ToLower(rec.Schema().Field(i).Name) {
		case "id", "vector_id", "doc_id", "docid":
			if idCol == -1 {
				idCol = i
			}
		case "score", "distance", "similarity", "value":
			if scoreCol == -1 {
				scoreCol = i
			}
		}
	}
	if idCol == -1 || scoreCol == -1 {
		for i := 0; i < int(rec.NumCols()); i++ {
			if idCol == -1 && isIntegerColumn(rec.Column(i)) {
				idCol = i
				continue
			}
			if scoreCol == -1 && isFloatColumn(rec.Column(i)) {
				scoreCol = i
			}
		}
	}
	if idCol == -1 || scoreCol == -1 || idCol == scoreCol {
		return nil
	}

	n := int(rec.NumRows())
	ids := columnUint64s(rec.Column(idCol))
	scores := columnFloat32s(rec.Column(scoreCol))
	if ids == nil || scores == nil || len(ids) < n || len(scores) < n {
		return nil
	}
	hits := make([]searchHit, n)
	for i := 0; i < n; i++ {
		hits[i] = searchHit{ID: ids[i], Score: scores[i]}
	}
	return hits
}

func isIntegerColumn(col arrow.Array) bool {
	switch col.(type) {
	case *array.Uint8, *array.Uint16, *array.Uint32, *array.Uint64,
		*array.Int8, *array.Int16, *array.Int32, *array.Int64:
		return true
	}
	return false
}

func isFloatColumn(col arrow.Array) bool {
	switch col.(type) {
	case *array.Float16, *array.Float32, *array.Float64:
		return true
	}
	return false
}

// columnUint64s and columnFloat32s read a column as the pair of types the
// digest needs. Anything unrecognised (or any null) returns nil, which drops
// the record rather than guessing - a digest over substituted values would be
// worse than a gap.
func columnUint64s(col arrow.Array) []uint64 {
	n := int(col.Len())
	out := make([]uint64, n)
	switch c := col.(type) {
	case *array.Uint64:
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				return nil
			}
			out[i] = c.Value(i)
		}
	case *array.Uint32:
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				return nil
			}
			out[i] = uint64(c.Value(i))
		}
	case *array.Int64:
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				return nil
			}
			out[i] = uint64(c.Value(i)) // #nosec G115 -- negative ids cannot occur here
		}
	case *array.Int32:
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				return nil
			}
			out[i] = uint64(c.Value(i)) // #nosec G115
		}
	default:
		return nil
	}
	return out
}

func columnFloat32s(col arrow.Array) []float32 {
	n := int(col.Len())
	out := make([]float32, n)
	switch c := col.(type) {
	case *array.Float32:
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				return nil
			}
			out[i] = c.Value(i)
		}
	case *array.Float64:
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				return nil
			}
			out[i] = float32(c.Value(i)) // #nosec G414 -- narrowing the score for the digest only
		}
	case *array.Float16:
		for i := 0; i < n; i++ {
			if c.IsNull(i) {
				return nil
			}
			out[i] = c.Value(i).Float32()
		}
	default:
		return nil
	}
	return out
}
