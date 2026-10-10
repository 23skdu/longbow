package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
)

func runSearch(ctx context.Context, args []string) {
	fs := flag.NewFlagSet("search", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	mode := fs.String("mode", "dense", "Search mode: dense, sparse, filtered, hybrid")
	vector := fs.String("vector", "", "Query vector as comma-separated floats")
	textQuery := fs.String("text", "", "Text query for sparse/hybrid search")
	alpha := fs.Float64("alpha", 0.5, "Alpha for hybrid search (0=sparse, 1=dense)")
	k := fs.Int("k", 10, "Number of results")
	filters := fs.String("filters", "", "JSON filter expression (inline or file path)")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *dataset == "" {
		fmt.Fprintf(os.Stderr, "Usage: longbow-cli search -dataset <name> -mode <dense|sparse|filtered|hybrid> [options]\n")
		os.Exit(1)
	}

	sc := mustGetClient(*uri)
	defer sc.Close()

	req := map[string]interface{}{
		"dataset": *dataset,
		"k":       *k,
	}

	switch strings.ToLower(*mode) {
	case "dense":
		if *vector == "" {
			log.Fatal("Dense search requires -vector flag")
		}
		req["vector"] = parseFloats(*vector)

	case "sparse":
		if *textQuery == "" {
			log.Fatal("Sparse search requires -text flag")
		}
		req["text_query"] = *textQuery
		req["alpha"] = 0.0

	case "filtered":
		if *vector == "" {
			log.Fatal("Filtered search requires -vector flag")
		}
		req["vector"] = parseFloats(*vector)
		if *filters != "" {
			filterExpr, err := parseFilterExpression(*filters)
			if err != nil {
				log.Fatalf("Failed to parse filters: %v", err)
			}
			req["filters"] = filterExpr
		}

	case "hybrid":
		if *vector == "" {
			log.Fatal("Hybrid search requires -vector flag")
		}
		req["vector"] = parseFloats(*vector)
		req["alpha"] = *alpha
		if *textQuery != "" {
			req["text_query"] = *textQuery
		}

	default:
		log.Fatalf("Unknown search mode: %s", *mode)
	}

	ticketBytes, _ := json.Marshal(map[string]interface{}{"search": req})

	start := time.Now()
	stream, err := sc.DoGet(ctx, ticketBytes)
	if err != nil {
		log.Fatalf("Search failed: %v", err)
	}

	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		log.Fatalf("Failed to read results: %v", err)
	}
	defer reader.Release()

	var totalRows int64
	for reader.Next() {
		rec := reader.Record()
		totalRows += rec.NumRows()
		printResults(rec)
	}

	if err := reader.Err(); err != nil {
		log.Fatalf("Error reading results: %v", err)
	}

	fmt.Printf("\nFound %d results in %v\n", totalRows, time.Since(start))
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

func printResults(rec arrow.Record) {
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
	lat := fs.Float64("lat", 0, "Center latitude")
	lon := fs.Float64("lon", 0, "Center longitude")
	radius := fs.Float64("radius", 1.0, "Search radius in km")
	k := fs.Int("k", 10, "Number of results")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *dataset == "" {
		log.Fatal("Dataset name is required")
	}

	sc := mustGetClient(*uri)
	defer sc.Close()

	req := map[string]interface{}{
		"dataset":     *dataset,
		"k":           *k,
		"center":      map[string]float64{"lat": *lat, "lon": *lon},
		"radius_km":   *radius,
		"search_type": "radius",
	}

	ticketBytes, _ := json.Marshal(map[string]interface{}{"geo_search": req})
	stream, err := sc.DoGet(ctx, ticketBytes)
	if err != nil {
		log.Fatalf("Geo-Search failed: %v", err)
	}

	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		log.Fatalf("Failed to read results: %v", err)
	}
	defer reader.Release()

	for reader.Next() {
		printResults(reader.Record())
	}
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

	sc := mustGetClient(*uri)
	defer sc.Close()

	req := map[string]interface{}{
		"dataset":  *dataset,
		"seed_ids": strings.Split(*seeds, ","),
		"k":        *k,
		"alpha":    *alpha,
	}

	ticketBytes, _ := json.Marshal(map[string]interface{}{"recommend": req})
	stream, err := sc.DoGet(ctx, ticketBytes)
	if err != nil {
		log.Fatalf("Recommend failed: %v", err)
	}

	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		log.Fatalf("Failed to read results: %v", err)
	}
	defer reader.Release()

	for reader.Next() {
		printResults(reader.Record())
	}
}

func runTemporalSearch(_ context.Context, args []string) {
	fs := flag.NewFlagSet("temporal-search", flag.ExitOnError)
	dataset := fs.String("dataset", "", "Dataset name (required)")
	searchType := fs.String("type", "as_of", "Search type: as_of, range, window")
	ts := fs.Int64("ts", 0, "Timestamp for as_of")
	start := fs.Int64("start", 0, "Start time")
	end := fs.Int64("end", 0, "End time")
	k := fs.Int("k", 10, "Number of results")
	uri := fs.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
	_ = fs.Parse(args)

	if *dataset == "" {
		log.Fatal("Dataset name is required")
	}

	sc := mustGetClient(*uri)
	defer sc.Close()

	req := map[string]interface{}{
		"dataset":     *dataset,
		"search_type": *searchType,
		"timestamp":   *ts,
		"start_time":  *start,
		"end_time":    *end,
		"k":           *k,
	}
	actionBody, _ := json.Marshal(req)
	action := &flight.Action{Type: "TemporalSearch", Body: actionBody}
	stream, err := sc.DoAction(context.Background(), action)
	if err != nil {
		log.Fatalf("Temporal search failed: %v", err)
	}

	for {
		res, err := stream.Recv()
		if err != nil {
			break
		}
		fmt.Printf("%s\n", string(res.Body))
	}
}
