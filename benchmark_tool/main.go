package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/23skdu/longbow/client"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type BenchmarkResult struct {
	Name             string    `json:"name"`
	DurationSeconds  float64   `json:"duration_seconds"`
	Throughput       float64   `json:"throughput"`
	ThroughputUnit   string    `json:"throughput_unit"`
	ThroughputMBs    float64   `json:"throughput_mbs"`
	Rows             int64     `json:"rows"`
	BytesProcessed   int64     `json:"bytes_processed"`
	LatenciesMs      []float64 `json:"latencies_ms,omitempty"`
	P50LatencyMs     float64   `json:"p50_latency_ms,omitempty"`
	P95LatencyMs     float64   `json:"p95_latency_ms,omitempty"`
	P99LatencyMs     float64   `json:"p99_latency_ms,omitempty"`
	IndexingDuration float64   `json:"indexing_duration_seconds,omitempty"`
}

func main() {
	uri := flag.String("uri", "127.0.0.1:3000", "Data plane address (host:port)")
	dim := flag.Int("dim", 128, "Vector dimension")
	scale := flag.Int("scale", 1000, "Vector count")
	dtype := flag.String("dtype", "float32", "Data type (float32, int32, etc)")
	dataset := flag.String("dataset", "bench_go", "Target dataset name")
	queries := flag.Int("queries", 1000, "Number of search queries")
	outputJson := flag.String("json", "", "Save stats as JSON file")
	flag.Parse()

	log.Printf("Starting Go Benchmark: Dataset=%s, Scale=%d, Dim=%d, Type=%s\n", *dataset, *scale, *dim, *dtype)

	sc, err := client.NewSmartClient(*uri)
	if err != nil {
		log.Fatalf("Failed to connect SmartClient: %v", err)
	}
	defer sc.Close()

	var results []BenchmarkResult

	// 1. Ingest/DoPut
	log.Println("[PUT] Generating vectors and uploading...")
	record, schema, err := generateRecord(*scale, *dim, *dtype)
	if err != nil {
		log.Fatalf("Generation failed: %v", err)
	}
	defer record.Release()

	putCtx, putCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	start := time.Now()
	if err := uploadBatch(putCtx, sc, *dataset, record, schema); err != nil {
		putCancel()
		log.Fatalf("DoPut failed: %v", err)
	}
	putCancel()
	duration := time.Since(start).Seconds()

	var bytesPerElement int64 = 4
	switch *dtype {
	case "int8":
		bytesPerElement = 1
	case "int16":
		bytesPerElement = 2
	case "int32", "uint32", "float32":
		bytesPerElement = 4
	case "int64", "uint64", "float64", "complex64":
		bytesPerElement = 8
	case "complex128":
		bytesPerElement = 16
	case "turboquant":
		bytesPerElement = 1
	}

	var totalBytes int64
	if *dtype == "turboquant" {
		totalBytes = int64(*scale) * (int64(*dim)*3/8 + 1)
	} else {
		totalBytes = int64(*scale) * int64(*dim) * bytesPerElement
	}

	results = append(results, BenchmarkResult{
		Name:            "DoPut",
		DurationSeconds: duration,
		Throughput:      float64(*scale) / duration,
		ThroughputUnit:  "vec/s",
		ThroughputMBs:   (float64(totalBytes) / (1024 * 1024)) / duration,
		Rows:            int64(*scale),
		BytesProcessed:  totalBytes,
	})
	log.Printf("[PUT] Completed in %.4fs (%.2f vec/s, %.2f MB/s)\n", duration, float64(*scale)/duration, (float64(totalBytes)/(1024*1024))/duration)

	log.Println("Waiting for background indexing to complete...")
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 660*time.Second)
	indexingStart := time.Now()
	readyStatus := waitForIndexingComplete(waitCtx, sc, *dataset, 600*time.Second)
	waitCancel()
	indexingSeconds := time.Since(indexingStart).Seconds()
	log.Printf("Indexing complete in %.4fs (status: %s).", indexingSeconds, readyStatus)

	// 2. DoGet
	log.Println("[GET] Downloading to verify scan...")
	getCtx, getCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	start = time.Now()
	rowsRead, err := downloadBatch(getCtx, sc, *dataset)
	getCancel()
	if err != nil {
		log.Fatalf("DoGet failed: %v", err)
	}
	duration = time.Since(start).Seconds()
	totalBytesGet := rowsRead * int64(*dim) * bytesPerElement

	results = append(results, BenchmarkResult{
		Name:            "DoGet",
		DurationSeconds: duration,
		Throughput:      float64(rowsRead) / duration,
		ThroughputUnit:  "vec/s",
		ThroughputMBs:   (float64(totalBytesGet) / (1024 * 1024)) / duration,
		Rows:            rowsRead,
		BytesProcessed:  totalBytesGet,
	})
	log.Printf("[GET] Completed in %.4fs (%.2f vec/s, %.2f MB/s)\n", duration, float64(rowsRead)/duration, (float64(totalBytesGet)/(1024*1024))/duration)

	// 3. Search
	modes := []string{"Dense", "Hybrid", "Filtered", "ByID"}
	searchCtx, searchCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer searchCancel()
	for _, mode := range modes {
		log.Printf("[SEARCH][%s] Running queries...\n", mode)
		start = time.Now()
		var latencies []float64
		for i := 0; i < *queries; i++ {
			qStart := time.Now()
			if err := executeSearch(searchCtx, sc, *dataset, *dim, mode); err != nil {
				log.Printf("[%s] Query %d failed: %v\n", mode, i, err)
				continue
			}
			latencies = append(latencies, time.Since(qStart).Seconds()*1000)
		}
		duration = time.Since(start).Seconds()

		p50, p95, p99 := 0.0, 0.0, 0.0
		if len(latencies) > 0 {
			sort.Float64s(latencies)
			p50 = latencies[len(latencies)/2]
			p95 = latencies[int(float64(len(latencies))*0.95)]
			p99 = latencies[int(float64(len(latencies))*0.99)]
		}

		results = append(results, BenchmarkResult{
			Name:            "Search_" + mode,
			DurationSeconds: duration,
			Throughput:      float64(*queries) / duration,
			ThroughputUnit:  "queries/s",
			Rows:            int64(*queries),
			LatenciesMs:     latencies,
			P50LatencyMs:    p50,
			P95LatencyMs:    p95,
			P99LatencyMs:    p99,
		})
		log.Printf("[SEARCH][%s] Completed %d queries in %.4fs (%.2f QPS, P50: %.2fms, P95: %.2fms, P99: %.2fms)\n", mode, *queries, duration, float64(*queries)/duration, p50, p95, p99)
	}

	// 4. Print Summary
	fmt.Printf("\n%s\n", "BENCHMARK SUITE SUMMARY")
	fmt.Printf("%-20s | %-18s | %-18s | %-10s | %-10s | %-10s | %-10s\n", "Name", "Throughput (vec/s)", "Throughput (MB/s)", "Rows", "P50(ms)", "P95(ms)", "P99(ms)")
	for _, r := range results {
		fmt.Printf("%-20s | %-18.2f | %-18.2f | %-10d | %-10.2f | %-10.2f | %-10.2f\n", r.Name, r.Throughput, r.ThroughputMBs, r.Rows, r.P50LatencyMs, r.P95LatencyMs, r.P99LatencyMs)
	}

	if *outputJson != "" {
		f, err := os.Create(*outputJson)
		if err != nil {
			log.Fatalf("Failed to create JSON: %v", err)
		}
		defer f.Close()
		json.NewEncoder(f).Encode(results)
		log.Printf("Results saved to %s\n", *outputJson)
	}
}

func uploadBatch(ctx context.Context, sc *client.SmartClient, dataset string, record arrow.Record, schema *arrow.Schema) error {
	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{dataset},
	}
	stream, err := sc.DoPut(ctx, desc)
	if err != nil {
		return err
	}

	writer := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	writer.SetFlightDescriptor(desc)

	if err := writer.Write(record); err != nil {
		writer.Close()
		return err
	}

	if err := writer.Close(); err != nil {
		return err
	}

	if err := stream.CloseSend(); err != nil {
		return err
	}

	_, _ = stream.Recv()

	return nil
}

// downloadBatch performs a DoGet download and returns count.
// Retries up to 30s if dataset is empty (persistence worker populates ds.Records async).
func downloadBatch(ctx context.Context, sc *client.SmartClient, dataset string) (int64, error) {
	deadline := time.Now().Add(30 * time.Second)
	for {
		reqBytes, _ := json.Marshal(map[string]string{"name": dataset})
		stream, err := sc.DoGet(ctx, reqBytes)
		if err != nil {
			return 0, err
		}

		reader, err := flight.NewRecordReader(stream)
		if err != nil {
			return 0, err
		}

		var total int64
		for reader.Next() {
			total += reader.Record().NumRows()
		}
		err = reader.Err()
		reader.Release()

		if total > 0 || time.Now().After(deadline) {
			return total, err
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// executeSearch performs search by setting JSON ticket in DoGet
func executeSearch(ctx context.Context, sc *client.SmartClient, dataset string, dim int, mode string) error {
	// Generate random query vector (always use float32 slice in request payload JSON)
	vector := make([]float32, dim)
	for i := range vector {
		vector[i] = rand.Float32()
	}

	req := map[string]interface{}{
		"dataset": dataset,
		"k":       10,
	}

	switch mode {
	case "Dense":
		req["vector"] = vector
	case "Hybrid":
		req["vector"] = vector
		req["text_query"] = "benchmark search term"
		req["alpha"] = 0.5
	case "Filtered":
		req["vector"] = vector
		req["filters"] = []map[string]interface{}{
			{
				"field":    "id",
				"operator": ">",
				"value":    "10",
			},
		}
	case "ByID":
		// SearchByID requires an existing ID. We use ID "0" from our ingest.
		req["id"] = "0"
		ticketBytes, _ := json.Marshal(map[string]interface{}{"search_by_id": req})
		stream, err := sc.DoGet(ctx, ticketBytes)
		if err != nil {
			return err
		}
		reader, err := flight.NewRecordReader(stream)
		if err != nil {
			return err
		}
		defer reader.Release()
		for reader.Next() {
			_ = reader.Record()
		}
		return reader.Err()
	}

	ticketBytes, _ := json.Marshal(map[string]interface{}{"search": req})
	stream, err := sc.DoGet(ctx, ticketBytes)
	if err != nil {
		return err
	}

	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		return err
	}
	defer reader.Release()

	for reader.Next() {
		_ = reader.Record() // Consume
	}
	return reader.Err()
}

// waitForIndexingComplete polls until the dataset is indexed.
// The passed ctx is used for the initial action; polling uses an independent Background context.
func waitForIndexingComplete(ctx context.Context, sc *client.SmartClient, dataset string, timeout time.Duration) string {
	actionBody, _ := json.Marshal(map[string]string{"dataset": dataset})
	action := &flight.Action{Type: "wait-for-indexing", Body: actionBody}
	stream, err := sc.DoAction(ctx, action)
	if err == nil {
		for {
			if result, err := stream.Recv(); err != nil {
				break
			} else {
				body := result.Body
				if len(body) > 0 {
					var status map[string]interface{}
					if err := json.Unmarshal(body, &status); err == nil {
						if s, ok := status["status"].(string); ok {
							return s
						}
					}
				}
			}
		}
		return "complete"
	}
	log.Printf("wait-for-indexing action failed, polling check_readiness: %v", err)

	pollCtx, pollCancel := context.WithTimeout(context.Background(), timeout)
	defer pollCancel()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		checkBody, _ := json.Marshal(map[string]string{"dataset": dataset})
		checkAction := &flight.Action{Type: "check_readiness", Body: checkBody}
		checkStream, err := sc.DoAction(pollCtx, checkAction)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		for {
			if result, err := checkStream.Recv(); err != nil {
				break
			} else {
				body := result.Body
				if len(body) > 0 {
					var status map[string]interface{}
					if err := json.Unmarshal(body, &status); err == nil {
						if s, ok := status["status"].(string); ok {
							if s == "READY" {
								return s
							}
							if reason, ok := status["reason"].(string); ok {
								log.Printf("  Still indexing... (%s)", reason)
							}
						}
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("WARNING: Timeout (%v) waiting for indexing to complete for dataset %s", timeout, dataset)
	return "timeout"
}

// generateRecord is a multi-type arrow table builder
func generateRecord(count int, dim int, dtype string) (arrow.Record, *arrow.Schema, error) {
	pool := memory.NewGoAllocator()
	var dt arrow.DataType

	switch dtype {
	case "float32", "turboquant":
		dt = arrow.PrimitiveTypes.Float32
	case "float64":
		dt = arrow.PrimitiveTypes.Float64
	case "int32":
		dt = arrow.PrimitiveTypes.Int32
	case "int16":
		dt = arrow.PrimitiveTypes.Int16
	case "int8":
		dt = arrow.PrimitiveTypes.Int8
	case "uint32":
		dt = arrow.PrimitiveTypes.Uint32
	case "complex64":
		dt = arrow.PrimitiveTypes.Float32
	case "complex128":
		dt = arrow.PrimitiveTypes.Float64
	default:
		return nil, nil, fmt.Errorf("unsupported dtype: %s", dtype)
	}

	listLen := int32(dim)
	var meta arrow.Metadata
	if dtype == "complex64" || dtype == "complex128" || dtype == "turboquant" {
		if dtype == "complex64" || dtype == "complex128" {
			listLen = int32(2 * dim)
		}
		meta = arrow.NewMetadata([]string{"longbow.vector_type"}, []string{dtype})
	}

	var vecField arrow.Field
	if meta.Len() > 0 {
		vecField = arrow.Field{Name: "vector", Type: arrow.FixedSizeListOf(listLen, dt), Metadata: meta}
	} else {
		vecField = arrow.Field{Name: "vector", Type: arrow.FixedSizeListOf(listLen, dt)}
	}

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			vecField,
			{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_ns},
		},
		nil,
	)

	// 1. Build IDs
	idBldr := array.NewInt64Builder(pool)
	defer idBldr.Release()
	idBldr.Reserve(count)
	for i := 0; i < count; i++ {
		idBldr.Append(int64(i))
	}
	idArr := idBldr.NewArray()
	defer idArr.Release()

	// 2. Build Vectors
	listBldr := array.NewFixedSizeListBuilder(pool, listLen, dt)
	defer listBldr.Release()
	listBldr.Reserve(count)

	dimensionMultiplier := 1
	if dtype == "complex64" || dtype == "complex128" {
		dimensionMultiplier = 2
	}

	switch dtype {
	case "float32", "complex64", "turboquant":
		vb := listBldr.ValueBuilder().(*array.Float32Builder)
		stride := dim * dimensionMultiplier
		vb.Reserve(count * stride)
		vals := make([]float32, count*stride)
		for i := range vals {
			vals[i] = rand.Float32()
		}
		for i := 0; i < count; i++ {
			listBldr.Append(true)
			vb.AppendValues(vals[i*stride:(i+1)*stride], nil)
		}
	case "float64", "complex128":
		vb := listBldr.ValueBuilder().(*array.Float64Builder)
		stride := dim * dimensionMultiplier
		vb.Reserve(count * stride)
		vals := make([]float64, count*stride)
		for i := range vals {
			vals[i] = rand.Float64()
		}
		for i := 0; i < count; i++ {
			listBldr.Append(true)
			vb.AppendValues(vals[i*stride:(i+1)*stride], nil)
		}
	case "int32":
		vb := listBldr.ValueBuilder().(*array.Int32Builder)
		vb.Reserve(count * dim)
		vals := make([]int32, count*dim)
		for i := range vals {
			vals[i] = int32(rand.Intn(1000))
		}
		for i := 0; i < count; i++ {
			listBldr.Append(true)
			vb.AppendValues(vals[i*dim:(i+1)*dim], nil)
		}
	case "int16":
		vb := listBldr.ValueBuilder().(*array.Int16Builder)
		vb.Reserve(count * dim)
		vals := make([]int16, count*dim)
		for i := range vals {
			vals[i] = int16(rand.Intn(1000))
		}
		for i := 0; i < count; i++ {
			listBldr.Append(true)
			vb.AppendValues(vals[i*dim:(i+1)*dim], nil)
		}
	case "int8":
		vb := listBldr.ValueBuilder().(*array.Int8Builder)
		vb.Reserve(count * dim)
		vals := make([]int8, count*dim)
		for i := range vals {
			vals[i] = int8(rand.Intn(127))
		}
		for i := 0; i < count; i++ {
			listBldr.Append(true)
			vb.AppendValues(vals[i*dim:(i+1)*dim], nil)
		}
	case "uint32":
		vb := listBldr.ValueBuilder().(*array.Uint32Builder)
		vb.Reserve(count * dim)
		vals := make([]uint32, count*dim)
		for i := range vals {
			vals[i] = uint32(rand.Intn(1000))
		}
		for i := 0; i < count; i++ {
			listBldr.Append(true)
			vb.AppendValues(vals[i*dim:(i+1)*dim], nil)
		}
	}

	vecArr := listBldr.NewArray()
	defer vecArr.Release()

	// 3. Build Timestamp
	tsBldr := array.NewTimestampBuilder(pool, arrow.FixedWidthTypes.Timestamp_ns.(*arrow.TimestampType))
	defer tsBldr.Release()
	tsBldr.Reserve(count)
	now := arrow.Timestamp(time.Now().UnixNano())
	for i := 0; i < count; i++ {
		tsBldr.Append(now)
	}
	tsArr := tsBldr.NewArray()
	defer tsArr.Release()

	return array.NewRecordBatch(schema, []arrow.Array{idArr, vecArr, tsArr}, int64(count)), schema, nil
}
