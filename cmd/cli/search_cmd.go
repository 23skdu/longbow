package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
)

func runSearch(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("search", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	mode := fs.String("mode", "dense", "Search mode: dense, sparse, filtered, hybrid, by-id, range")
	vector := fs.String("vector", "", "Query vector as comma-separated floats")
	idStr := fs.String("id", "", "Seed document ID for by-id search")
	textQuery := fs.String("text", "", "Text query for sparse/hybrid search")
	alpha := fs.Float64("alpha", 0.5, "Alpha for hybrid search (0=sparse, 1=dense)")
	threshold := fs.Float64("threshold", 0.5, "Distance threshold for range search")
	k := fs.Int("k", 10, "Number of results (or max_results for range search)")
	rrfK := fs.Int("rrf-k", 60, "RRF constant k for hybrid search")
	graphAlpha := fs.Float64("graph-alpha", 0, "GraphRAG blending alpha")
	graphDepth := fs.Int("graph-depth", 0, "GraphRAG traversal depth")
	rawHybrid := fs.Bool("raw-hybrid", false, "Return un-re-ranked raw hybrid scores")
	filters := fs.String("filters", "", "JSON filter expression (inline or file path)")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *dataset == "" {
		fmt.Fprintf(os.Stderr, "Usage: longbow-cli search -dataset <name> -mode <dense|sparse|filtered|hybrid|by-id|range> [options]\n")
		os.Exit(1)
	}

	ticketBytes, err := buildSearchTicket(*mode, *dataset, *vector, *textQuery, *idStr, *filters, *k, *rrfK, *graphDepth, *alpha, *threshold, *graphAlpha, *rawHybrid)
	if err != nil {
		log.Fatalf("%v", err)
	}

	totalRows, elapsed, err := executeTicketDoGet(ctx, *uri, ticketBytes)
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}
	fmt.Printf("\nFound %d results in %v\n", totalRows, elapsed)
}

func buildSearchTicket(mode, dataset, vector, textQuery, idStr, filtersStr string, k, rrfK, graphDepth int, alpha, threshold, graphAlpha float64, rawHybrid bool) ([]byte, error) {
	req := map[string]interface{}{
		"dataset": dataset,
		"k":       k,
	}

	if filtersStr != "" {
		filterExpr, err := parseFilterExpression(filtersStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse filters: %w", err)
		}
		req["filters"] = filterExpr
	}

	switch strings.ToLower(mode) {
	case "dense":
		if vector == "" {
			return nil, fmt.Errorf("dense search requires -vector flag")
		}
		req["vector"] = parseFloats(vector)
		return json.Marshal(map[string]interface{}{"search": req})

	case "sparse":
		if textQuery == "" {
			return nil, fmt.Errorf("sparse search requires -text flag")
		}
		req["text_query"] = textQuery
		req["alpha"] = 0.0
		return json.Marshal(map[string]interface{}{"search": req})

	case "filtered":
		if vector == "" {
			return nil, fmt.Errorf("filtered search requires -vector flag")
		}
		req["vector"] = parseFloats(vector)
		return json.Marshal(map[string]interface{}{"search": req})

	case "hybrid":
		if vector == "" && textQuery == "" {
			return nil, fmt.Errorf("hybrid search requires -vector and/or -text flag")
		}
		if vector != "" {
			req["vector"] = parseFloats(vector)
		}
		req["alpha"] = alpha
		if textQuery != "" {
			req["text_query"] = textQuery
		}
		if rrfK > 0 {
			req["rrf_k"] = rrfK
		}
		if graphAlpha > 0 {
			req["graph_alpha"] = graphAlpha
		}
		if graphDepth > 0 {
			req["graph_depth"] = graphDepth
		}
		if rawHybrid {
			req["raw_hybrid"] = true
		}
		return json.Marshal(map[string]interface{}{"search": req})

	case "by-id", "by_id":
		if idStr == "" {
			return nil, fmt.Errorf("by-id search requires -id flag")
		}
		byIDReq := map[string]interface{}{
			"dataset": dataset,
			"id":      idStr,
			"k":       k,
		}
		if f, ok := req["filters"]; ok {
			byIDReq["filters"] = f
		}
		return json.Marshal(map[string]interface{}{"search_by_id": byIDReq})

	case "range":
		if vector == "" {
			return nil, fmt.Errorf("range search requires -vector flag")
		}
		rangeReq := map[string]interface{}{
			"dataset":     dataset,
			"vector":      parseFloats(vector),
			"threshold":   threshold,
			"max_results": k,
		}
		if f, ok := req["filters"]; ok {
			rangeReq["filters"] = f
		}
		return json.Marshal(map[string]interface{}{"range_search": rangeReq})

	default:
		return nil, fmt.Errorf("unknown search mode: %s", mode)
	}
}

func executeTicketDoGet(ctx context.Context, uri string, ticketBytes []byte) (int64, time.Duration, error) {
	sc := mustGetClient(uri)
	defer sc.Close()

	start := time.Now()
	stream, err := sc.DoGet(ctx, ticketBytes)
	if err != nil {
		return 0, 0, err
	}

	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read results: %w", err)
	}
	defer reader.Release()

	var totalRows int64
	for reader.Next() {
		rec := reader.RecordBatch()
		totalRows += rec.NumRows()
		printResults(rec)
	}
	if err := reader.Err(); err != nil {
		return totalRows, time.Since(start), err
	}
	return totalRows, time.Since(start), nil
}

func executeActionStream(ctx context.Context, uri, actionType string, bodyObj any) error {
	sc := mustGetClient(uri)
	defer sc.Close()

	actionBody, err := json.Marshal(bodyObj)
	if err != nil {
		return err
	}
	stream, err := sc.DoAction(ctx, &flight.Action{Type: actionType, Body: actionBody})
	if err != nil {
		return err
	}
	for {
		res, err := stream.Recv()
		if err != nil {
			break
		}
		fmt.Printf("%s\n", string(res.Body))
	}
	return nil
}

func splitCSV(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func parseFloats(s string) []float32 {
	parts := strings.Split(s, ",")
	result := make([]float32, len(parts))
	for i, p := range parts {
		var f float64
		_, _ = fmt.Sscanf(strings.TrimSpace(p), "%f", &f)
		result[i] = float32(f)
	}
	return result
}

func parseFilterExpression(s string) (interface{}, error) {
	data, err := os.ReadFile(filepath.Clean(s))
	if err != nil {
		return json.RawMessage(s), nil
	}

	var filter interface{}
	if err := json.Unmarshal(data, &filter); err != nil {
		return nil, err
	}
	return filter, nil
}

func printResults(rec arrow.RecordBatch) {
	for i := int64(0); i < rec.NumRows(); i++ {
		for j := int64(0); j < rec.NumCols(); j++ {
			col := rec.Column(int(j))
			if j > 0 {
				fmt.Print(", ")
			}
			fmt.Printf("%s=%v", rec.Schema().Field(int(j)).Name, extractValue(col, i))
		}
		fmt.Println()
	}
}

func extractValue(col arrow.Array, idx int64) interface{} {
	if col.IsNull(int(idx)) {
		return nil
	}
	switch col.DataType().ID() {
	case arrow.INT64:
		return col.(*array.Int64).Value(int(idx))
	case arrow.FLOAT32:
		return col.(*array.Float32).Value(int(idx))
	case arrow.FLOAT64:
		return col.(*array.Float64).Value(int(idx))
	case arrow.STRING:
		return col.(*array.String).Value(int(idx))
	default:
		return fmt.Sprintf("<%s>", col.DataType().Name())
	}
}

func runGeoSearch(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("geo-search", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	searchType := fs.String("type", "radius", "Geo search type: radius, box, hybrid")
	lat := fs.Float64("lat", 0, "Center latitude")
	lon := fs.Float64("lon", 0, "Center longitude")
	radius := fs.Float64("radius", 1.0, "Search radius in km")
	boxStr := fs.String("box", "", "Bounding box for box search: min_lat,min_lon,max_lat,max_lon")
	vectorStr := fs.String("vector", "", "Query vector (comma-separated floats) for hybrid geo search")
	k := fs.Int("k", 10, "Number of results")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *dataset == "" {
		log.Fatal("Dataset name is required")
	}

	ticketBytes, err := buildGeoSearchTicket(*searchType, *dataset, *boxStr, *vectorStr, *lat, *lon, *radius, *k)
	if err != nil {
		log.Fatalf("Geo-Search failed: %v", err)
	}
	if _, _, err := executeTicketDoGet(ctx, *uri, ticketBytes); err != nil {
		log.Fatalf("Geo-Search failed: %v", err)
	}
}

func buildGeoSearchTicket(searchType, dataset, boxStr, vectorStr string, lat, lon, radius float64, k int) ([]byte, error) {
	st := strings.ToLower(strings.TrimSpace(searchType))
	if st == "" {
		st = "radius"
	}
	req := map[string]interface{}{
		"dataset":     dataset,
		"k":           k,
		"search_type": st,
	}
	switch st {
	case "radius":
		req["center"] = map[string]float64{"lat": lat, "lon": lon}
		req["radius_km"] = radius
	case "box":
		parts := splitCSV(boxStr)
		if len(parts) != 4 {
			return nil, fmt.Errorf("box search requires -box min_lat,min_lon,max_lat,max_lon")
		}
		coords := make([]float64, 4)
		for i, p := range parts {
			v, err := strconv.ParseFloat(p, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid box coordinate %q: %w", p, err)
			}
			coords[i] = v
		}
		req["box"] = map[string]float64{
			"min_lat": coords[0],
			"min_lon": coords[1],
			"max_lat": coords[2],
			"max_lon": coords[3],
		}
	case "hybrid":
		if vectorStr == "" {
			return nil, fmt.Errorf("geo hybrid search requires -vector flag")
		}
		req["center"] = map[string]float64{"lat": lat, "lon": lon}
		req["radius_km"] = radius
		req["query_vector"] = parseFloats(vectorStr)
	default:
		return nil, fmt.Errorf("unknown geo search type: %s", searchType)
	}
	return json.Marshal(map[string]interface{}{"geo_search": req})
}

func runRecommend(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("recommend", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	seeds := fs.String("seeds", "", "Comma-separated seed IDs")
	k := fs.Int("k", 10, "Number of results")
	alpha := fs.Float64("alpha", 0.5, "Hybrid blend alpha")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *dataset == "" || *seeds == "" {
		log.Fatal("Dataset and seeds are required")
	}

	req := map[string]interface{}{
		"dataset":  *dataset,
		"seed_ids": splitCSV(*seeds),
		"k":        *k,
		"alpha":    *alpha,
	}
	ticketBytes, _ := json.Marshal(map[string]interface{}{"recommend": req})
	if _, _, err := executeTicketDoGet(ctx, *uri, ticketBytes); err != nil {
		log.Fatalf("Recommend failed: %v", err)
	}
}

func runTemporalSearch(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("temporal-search", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	searchType := fs.String("type", "as_of", "Search type: as_of, range, sliding_window, sliding_window_time")
	ts := fs.Int64("ts", 0, "Timestamp for as_of (unix nanos)")
	start := fs.Int64("start", 0, "Start time (unix nanos)")
	end := fs.Int64("end", 0, "End time (unix nanos)")
	windowSize := fs.Int("window-size", 100, "Window size for sliding_window")
	duration := fs.String("duration", "5m", "Window duration for sliding_window_time")
	k := fs.Int("k", 10, "Number of results")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *dataset == "" {
		log.Fatal("Dataset name is required")
	}

	req, err := buildTemporalSearchBody(*searchType, *dataset, *duration, *ts, *start, *end, *windowSize, *k)
	if err != nil {
		log.Fatalf("Temporal search failed: %v", err)
	}
	if err := executeActionStream(ctx, *uri, "TemporalSearch", req); err != nil {
		log.Fatalf("Temporal search failed: %v", err)
	}
}

func buildTemporalSearchBody(searchType, dataset, durStr string, ts, start, end int64, windowSize, k int) (map[string]interface{}, error) {
	st := strings.ToLower(strings.TrimSpace(searchType))
	if st == "window" {
		st = "sliding_window"
	}
	switch st {
	case "as_of", "range", "sliding_window", "sliding_window_time":
	default:
		return nil, fmt.Errorf("unknown temporal search type: %s", searchType)
	}
	req := map[string]interface{}{
		"dataset":     dataset,
		"search_type": st,
		"timestamp":   ts,
		"start_time":  start,
		"end_time":    end,
		"window_size": windowSize,
		"k":           k,
	}
	if st == "sliding_window_time" && durStr != "" {
		d, err := time.ParseDuration(durStr)
		if err != nil {
			return nil, fmt.Errorf("invalid duration %q: %w", durStr, err)
		}
		req["duration"] = int64(d)
	}
	return req, nil
}

func runRangeSearch(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("range-search", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	vector := fs.String("vector", "", "Query vector as comma-separated floats (required)")
	threshold := fs.Float64("threshold", 0.5, "Distance threshold")
	k := fs.Int("k", 100, "Max results")
	filters := fs.String("filters", "", "JSON filter expression (inline or file path)")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *dataset == "" {
		log.Fatal("Dataset name is required")
	}
	ticketBytes, err := buildSearchTicket("range", *dataset, *vector, "", "", *filters, *k, 0, 0, 0, *threshold, 0, false)
	if err != nil {
		log.Fatalf("Range search failed: %v", err)
	}
	totalRows, elapsed, err := executeTicketDoGet(ctx, *uri, ticketBytes)
	if err != nil {
		log.Fatalf("Range search failed: %v", err)
	}
	fmt.Printf("\nFound %d results in %v\n", totalRows, elapsed)
}

func runFederatedSearch(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("federated-search", flag.ExitOnError)
	collections := fs.String("collections", "", "Comma-separated collection names")
	tags := fs.String("tags", "", "Comma-separated routing tags")
	vector := fs.String("vector", "", "Query vector as comma-separated floats (required)")
	k := fs.Int("k", 10, "Number of results")
	rrfK := fs.Int("rrf-k", 60, "RRF constant k")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	ticketBytes, err := buildFederatedSearchTicket(*collections, *tags, *vector, *k, *rrfK)
	if err != nil {
		log.Fatalf("Federated search failed: %v", err)
	}
	totalRows, elapsed, err := executeTicketDoGet(ctx, *uri, ticketBytes)
	if err != nil {
		log.Fatalf("Federated search failed: %v", err)
	}
	fmt.Printf("\nFound %d results in %v\n", totalRows, elapsed)
}

func buildFederatedSearchTicket(collectionsCSV, tagsCSV, vectorStr string, k, rrfK int) ([]byte, error) {
	if vectorStr == "" {
		return nil, fmt.Errorf("federated search requires -vector flag")
	}
	req := map[string]interface{}{
		"collections": splitCSV(collectionsCSV),
		"tags":        splitCSV(tagsCSV),
		"vector":      parseFloats(vectorStr),
		"k":           k,
		"rrf_k":       rrfK,
	}
	return json.Marshal(map[string]interface{}{"federated_search": req})
}

func runTextSearch(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("text-search", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	query := fs.String("query", "", "Full-text query (supports AND/OR/NOT, \"phrase\", term~1)")
	facets := fs.String("facets", "", "Comma-separated facet fields")
	highlightTag := fs.String("highlight-tag", "<mark>", "Highlight tag")
	fuzzyMaxDist := fs.Int("fuzzy", 1, "Maximum Levenshtein edit distance for fuzzy matching")
	k := fs.Int("k", 10, "Number of results")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *dataset == "" || *query == "" {
		log.Fatal("Dataset and query are required")
	}
	req := buildAdvancedTextRequest(*dataset, *query, *facets, *highlightTag, *k, *fuzzyMaxDist)
	if err := executeActionStream(ctx, *uri, "AdvancedTextSearch", req); err != nil {
		log.Fatalf("Text search failed: %v", err)
	}
}

func buildAdvancedTextRequest(dataset, query, facetFieldsCSV, highlightTag string, k, fuzzyMaxDist int) map[string]interface{} {
	req := map[string]interface{}{
		"dataset":        dataset,
		"query":          query,
		"k":              k,
		"fuzzy_max_dist": fuzzyMaxDist,
		"highlight_tag":  highlightTag,
	}
	if fields := splitCSV(facetFieldsCSV); len(fields) > 0 {
		req["facet_fields"] = fields
	}
	return req
}

func runMultiModalSearch(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("multimodal-search", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	modality := fs.String("modality", "text", "Target modality: text, image, audio")
	textQuery := fs.String("text", "", "Text query")
	filePath := fs.String("file", "", "Optional image/audio file path to base64-encode")
	mimeType := fs.String("mime", "", "MIME type (e.g. image/png, audio/wav)")
	k := fs.Int("k", 10, "Number of results")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *dataset == "" {
		log.Fatal("Dataset name is required")
	}
	var imgB64, audB64 string
	if *filePath != "" {
		raw, err := os.ReadFile(filepath.Clean(*filePath))
		if err != nil {
			log.Fatalf("Failed to read media file: %v", err)
		}
		b64 := base64.StdEncoding.EncodeToString(raw)
		if strings.EqualFold(*modality, "audio") {
			audB64 = b64
		} else {
			imgB64 = b64
		}
	}
	req, err := buildMultiModalRequest(*dataset, *modality, *textQuery, imgB64, audB64, *mimeType, *k)
	if err != nil {
		log.Fatalf("MultiModal search failed: %v", err)
	}
	if err := executeActionStream(ctx, *uri, "MultiModalSearch", req); err != nil {
		log.Fatalf("MultiModal search failed: %v", err)
	}
}

func buildMultiModalRequest(dataset, modality, textQuery, imageB64, audioB64, mimeType string, k int) (map[string]interface{}, error) {
	m := strings.ToLower(strings.TrimSpace(modality))
	if m != "" && m != "text" && m != "image" && m != "audio" {
		return nil, fmt.Errorf("unknown modality: %s", modality)
	}
	if textQuery == "" && imageB64 == "" && audioB64 == "" {
		return nil, fmt.Errorf("multimodal search requires -text or -file")
	}
	req := map[string]interface{}{
		"dataset":  dataset,
		"modality": m,
		"k":        k,
	}
	if textQuery != "" {
		req["text_query"] = textQuery
	}
	if imageB64 != "" {
		req["image_base64"] = imageB64
	}
	if audioB64 != "" {
		req["audio_base64"] = audioB64
	}
	if mimeType != "" {
		req["mime_type"] = mimeType
	}
	return req, nil
}

func runExplainSearch(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("explain-search", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	vector := fs.String("vector", "", "Query vector as comma-separated floats")
	textQuery := fs.String("text", "", "Optional text query")
	k := fs.Int("k", 5, "Number of results")
	alpha := fs.Float64("alpha", 1.0, "Hybrid alpha")
	graphAlpha := fs.Float64("graph-alpha", 0.0, "Graph alpha")
	maxHops := fs.Int("max-hops", 32, "Maximum HNSW hops to record")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *dataset == "" || *vector == "" {
		log.Fatal("Dataset and vector are required")
	}
	req := map[string]interface{}{
		"dataset":     *dataset,
		"vector":      parseFloats(*vector),
		"text_query":  *textQuery,
		"k":           *k,
		"alpha":       *alpha,
		"graph_alpha": *graphAlpha,
		"max_hops":    *maxHops,
	}
	if err := executeActionStream(ctx, *uri, "ExplainSearch", req); err != nil {
		log.Fatalf("ExplainSearch failed: %v", err)
	}
}

func runRerank(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("rerank", flag.ExitOnError)
	strategy := fs.String("strategy", "heuristic", "Rerank strategy: heuristic, cross_encoder, mmr")
	query := fs.String("query", "", "Query text")
	resultsJSON := fs.String("results", "", "JSON array of search results")
	topK := fs.Int("top-k", 5, "Top-K results to keep")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	var results []interface{}
	if *resultsJSON != "" {
		if err := json.Unmarshal([]byte(*resultsJSON), &results); err != nil {
			log.Fatalf("Invalid results JSON: %v", err)
		}
	}
	req := map[string]interface{}{
		"strategy": *strategy,
		"query":    *query,
		"results":  results,
		"top_k":    *topK,
	}
	if err := executeActionStream(ctx, *uri, "Rerank", req); err != nil {
		log.Fatalf("Rerank failed: %v", err)
	}
}

func runRewriteQuery(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("rewrite-query", flag.ExitOnError)
	query := fs.String("query", "", "Query string to rewrite (required)")
	mode := fs.String("mode", "synonyms", "Rewrite mode: synonyms, expand, hyde")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *query == "" {
		log.Fatal("Query is required")
	}
	req := map[string]interface{}{
		"query": *query,
		"mode":  *mode,
	}
	if err := executeActionStream(ctx, *uri, "RewriteQuery", req); err != nil {
		log.Fatalf("RewriteQuery failed: %v", err)
	}
}
