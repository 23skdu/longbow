package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/23skdu/longbow/client"
	"github.com/23skdu/longbow/pkg/safe"
	"github.com/23skdu/longbow/pkg/version"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/metadata"
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
	MeanLatencyMs    float64   `json:"mean_latency_ms,omitempty"`
	P50LatencyMs     float64   `json:"p50_latency_ms,omitempty"`
	P95LatencyMs     float64   `json:"p95_latency_ms,omitempty"`
	P99LatencyMs     float64   `json:"p99_latency_ms,omitempty"`
	IndexingDuration float64   `json:"indexing_duration_seconds,omitempty"`
	TqBits           int       `json:"tq_bits,omitempty"`

	// R10: a mode whose context deadline expired was truncated, so its QPS is a
	// floor, not a measurement. Recorded explicitly so it cannot be read as a
	// low number and treated as a regression.
	ContextDeadlineExceeded bool  `json:"context_deadline_exceeded,omitempty"`
	QueriesRequested        int64 `json:"queries_requested,omitempty"`
	QueriesFailed           int64 `json:"queries_failed,omitempty"`
	QueriesTruncated        int64 `json:"queries_truncated,omitempty"`

	// R12: a baseline collected from a different mode list, or with a mode in a
	// different position, is not comparable - the modes before this one decide
	// its cache and GC state.
	Seed int64 `json:"seed,omitempty"`

	ModeIndex  int      `json:"mode_index,omitempty"`
	ModeCount  int      `json:"mode_count,omitempty"`
	ModeOrders []string `json:"mode_order,omitempty"`
}

func main() {
	// Handle version flags early
	if len(os.Args) > 1 {
		for _, arg := range os.Args[1:] {
			if arg == "--version" || arg == "-v" {
				version.Print()
				return
			}
		}
	}

	uri := flag.String("uri", "127.0.0.1:3000", "Data plane address (host:port or unix:///path/to/socket)")
	dim := flag.Int("dim", 128, "Vector dimension (up to 3072)")
	scale := flag.Int("scale", 1000, "Vector count")
	dtype := flag.String("dtype", "float32", "Data type (float32, int32, etc, including turboquant)")
	dataset := flag.String("dataset", "bench_go", "Target dataset name")
	queries := flag.Int("queries", 1000, "Number of search queries")
	outputJson := flag.String("json", "", "Save stats as JSON file")
	tqBits := flag.Int("tq-bits", 4, "TurboQuant bit depth (2, 4, 8)")
	seed := flag.Int64("seed", defaultSeed,
		"Seed for corpus generation, query vectors and ByID ids. Repeatability is a precondition for a percentage regression gate: with the corpus and the queries both drawn from the clock, two runs of the same binary differ by more than most regressions anyone would gate on. Recorded in the output JSON.")
	workers := flag.Int("workers", 1, "Number of concurrent search workers")
	drop := flag.Bool("drop", false, "Drop dataset after benchmark")
	fbin := flag.String("fbin", "", "Read vectors from Arrow IPC binary file or true .fbin instead of generating them")
	outputArrow := flag.String("output-arrow", "", "Save generated vectors to Arrow IPC file and exit")
	outputFbin := flag.String("output-fbin", "", "Save generated vectors to .fbin file and exit")
	mode := flag.String("mode", "vec", "Benchmark mode (vec, kv, cluster)")
	searchModes := flag.String("search-modes", "all", "Comma-separated search modes to run (dense, hybrid, sparse, filtered, byid, graphrag, geo, temporal, learned_index)")
	searchTimeout := flag.Duration("search-timeout", 5*time.Minute,
		"Budget for each search mode. This is per mode, not shared across them: a single budget covering all modes meant the last mode inherited whatever was left and started silently truncating, which reads as a low QPS rather than as a truncated run.")
	temporalAsOf := flag.Int64("temporal-asof-nanos", defaultTemporalAsOf,
		"Fixed as-of timestamp for Temporal searches, in Unix nanoseconds. It must be fixed: the server keys its temporal result cache on (timestamp, k), so a per-query clock reading guarantees 100% miss rate, an LRU insert per query, and a mode that measures the cache-miss path while appearing to measure search.")
	shuffleModes := flag.Bool("shuffle-modes", false,
		"Randomize search mode execution order using seed (roadmap R12a) to eliminate mode-order coupling.")
	reset := flag.Bool("reset", false, "Reset dataset in-place before running the benchmark")
	flag.Parse()

	if *drop {
		defer func() {
			sc, err := client.NewSmartClient(*uri)
			if err != nil {
				log.Printf("Failed to create client for drop: %v\n", err)
				return
			}
			defer sc.Close()
			if err := dropDataset(context.Background(), sc, *dataset); err != nil {
				log.Printf("Failed to drop dataset %s: %v\n", *dataset, err)
			} else {
				log.Printf("Dataset %s dropped successfully\n", *dataset)
			}
		}()
	}

	if *dim > 3072 {
		log.Printf("WARNING: Dimension %d exceeds recommended 3072 limit. Proceeding anyway.\n", *dim)
	}

	log.Printf("Starting Go Benchmark: Mode=%s, Dataset=%s, Scale=%d, Dim=%d, Type=%s\n", *mode, *dataset, *scale, *dim, *dtype)

	sc, err := client.NewSmartClient(*uri)
	if err != nil {
		log.Fatalf("Failed to connect SmartClient: %v", err)
	}
	defer sc.Close()

	if *reset {
		log.Printf("Performing in-place reset for dataset %s before benchmark...\n", *dataset)
		if err := resetDataset(context.Background(), sc, *dataset); err != nil {
			log.Printf("In-place reset status/info: %v (this is normal if dataset was not already present)\n", err)
		} else {
			log.Printf("In-place reset for dataset %s completed successfully.\n", *dataset)
		}
	}

	var results []BenchmarkResult

	var totalUploaded int
	var start time.Time

	if *fbin != "" {
		f, err := os.Open(*fbin)
		if err != nil {
			log.Fatalf("Failed to open input file: %v", err)
		}
		defer f.Close()

		// Peek first 6 bytes to check for ARROW1
		magic := make([]byte, 6)
		_, _ = f.ReadAt(magic, 0)

		if string(magic) == "ARROW1" {
			log.Printf("[PUT] Ingesting from Arrow IPC file: %s\n", *fbin)
			reader, err := ipc.NewFileReader(f)
			if err != nil {
				log.Fatalf("Failed to create IPC reader: %v", err)
			}

			numRecords := reader.NumRecords()
			totalUploaded = 0
			start = time.Now()

			var uploader *StreamUploader
			for i := 0; i < numRecords; i++ {
				record, err := reader.Record(i)
				if err != nil {
					log.Fatalf("Failed to read record %d: %v", i, err)
				}
				record.Retain()

				if uploader == nil {
					uploader, err = newStreamUploader(sc, *dataset, record.Schema())
					if err != nil {
						log.Fatalf("Failed to init uploader: %v", err)
					}
				}

				if err := uploader.Write(record); err != nil {
					log.Fatalf("DoPut write failed at record %d: %v", i, err)
				}

				totalUploaded += int(record.NumRows())
				record.Release()

				if totalUploaded%50000 == 0 || i == numRecords-1 {
					log.Printf("  Progress: %d vectors uploaded\n", totalUploaded)
				}
			}
			if uploader != nil {
				if err := uploader.Close(); err != nil {
					log.Fatalf("Failed to close uploader: %v", err)
				}
			}
		} else {
			// Try as true .fbin (4B count, 4B dim, then floats)
			log.Printf("[PUT] Ingesting from true .fbin file: %s\n", *fbin)
			var countVal, dimVal uint32
			if err := binary.Read(f, binary.LittleEndian, &countVal); err != nil {
				log.Fatalf("Failed to read fbin count: %v", err)
			}
			if err := binary.Read(f, binary.LittleEndian, &dimVal); err != nil {
				log.Fatalf("Failed to read fbin dim: %v", err)
			}

			log.Printf("  fbin: count=%d, dim=%d\n", countVal, dimVal)
			*scale = int(countVal)
			*dim = int(dimVal)

			// Build schema for fbin (minimal schema)
			fbinSchema := arrow.NewSchema(
				[]arrow.Field{
					{Name: "id", Type: arrow.BinaryTypes.String},
					{Name: "vector", Type: arrow.FixedSizeListOf(int32(dimVal), arrow.PrimitiveTypes.Float32)},
				},
				nil,
			)

			totalUploaded = 0
			start = time.Now()

			chunkSize := 10000
			var uploader *StreamUploader
			for totalUploaded < int(countVal) {
				currentChunk := chunkSize
				if totalUploaded+currentChunk > int(countVal) {
					currentChunk = int(countVal) - totalUploaded
				}

				// Generate IDs
				pool := memory.NewGoAllocator()
				idBldr := array.NewStringBuilder(pool)
				vecBldr := array.NewFixedSizeListBuilder(pool, int32(dimVal), arrow.PrimitiveTypes.Float32)
				valBldr := vecBldr.ValueBuilder().(*array.Float32Builder)

				for i := 0; i < currentChunk; i++ {
					idBldr.Append(fmt.Sprintf("%d", totalUploaded+i))
					vecBldr.Append(true)

					row := make([]float32, dimVal)
					if err := binary.Read(f, binary.LittleEndian, &row); err != nil {
						log.Fatalf("Failed to read fbin data at %d: %v", totalUploaded+i, err)
					}
					valBldr.AppendValues(row, nil)
				}

				idArr := idBldr.NewArray()
				vecArr := vecBldr.NewArray()
				record := array.NewRecordBatch(fbinSchema, []arrow.Array{idArr, vecArr}, int64(currentChunk))

				if uploader == nil {
					uploader, err = newStreamUploader(sc, *dataset, fbinSchema)
					if err != nil {
						log.Fatalf("Failed to init uploader for fbin: %v", err)
					}
				}

				if err := uploader.Write(record); err != nil {
					log.Fatalf("DoPut write failed at chunk starting %d: %v", totalUploaded, err)
				}

				totalUploaded += currentChunk
				record.Release()
				idArr.Release()
				vecArr.Release()
				idBldr.Release()
				vecBldr.Release()

				if totalUploaded%50000 == 0 || totalUploaded == int(countVal) {
					log.Printf("  Progress: %d/%d vectors uploaded\n", totalUploaded, countVal)
				}
			}
			if uploader != nil {
				if err := uploader.Close(); err != nil {
					log.Fatalf("Failed to close uploader: %v", err)
				}
			}
		}
		*scale = totalUploaded
	} else {
		chunkSize := 10000
		if *scale < chunkSize {
			chunkSize = *scale
		}
		numChunks := (*scale + chunkSize - 1) / chunkSize
		numWorkers := runtime.GOMAXPROCS(0)
		if numWorkers < 1 {
			numWorkers = 1
		}
		log.Printf("[PUT] Generating vectors across %d parallel workers (chunk size: %d, chunks: %d)...\n", numWorkers, chunkSize, numChunks)

		type chunkJob struct {
			index  int
			offset int
			count  int
		}
		type chunkOutput struct {
			index  int
			rec    arrow.Record
			schema *arrow.Schema
			err    error
		}

		sem := make(chan struct{}, numWorkers*2)
		chunkChans := make([]chan chunkOutput, numChunks)
		for i := range chunkChans {
			chunkChans[i] = make(chan chunkOutput, 1)
		}

		jobs := make(chan chunkJob, numChunks)
		for idx := 0; idx < numChunks; idx++ {
			offset := idx * chunkSize
			count := chunkSize
			if offset+count > *scale {
				count = *scale - offset
			}
			jobs <- chunkJob{index: idx, offset: offset, count: count}
		}
		close(jobs)

		for w := 0; w < numWorkers; w++ {
			// Derived from the fixed seed and the worker index, not the clock
			// (R16, H7). Each worker still gets a distinct stream so the chunks
			// are not identical to one another.
			workerSeed := *seed + int64(w)*10007
			go func(seed int64) {
				rng := rand.New(rand.NewSource(seed)) // #nosec G404 -- non-cryptographic PRNG for benchmark
				for job := range jobs {
					sem <- struct{}{}
					rec, schema, err := generateRecordBatch(rng, job.offset, job.count, *dim, *dtype, *tqBits)
					chunkChans[job.index] <- chunkOutput{index: job.index, rec: rec, schema: schema, err: err}
				}
			}(workerSeed)
		}

		// Check for generation-only mode (saving to file) — must pre-generate
		if *outputArrow != "" || *outputFbin != "" {
			var genSchema *arrow.Schema
			preGenerated := make([]arrow.Record, 0, numChunks)

			for idx := 0; idx < numChunks; idx++ {
				out := <-chunkChans[idx]
				<-sem
				if out.err != nil {
					log.Fatalf("Pre-generation failed at chunk %d: %v", idx, out.err)
				}
				preGenerated = append(preGenerated, out.rec)
				if genSchema == nil {
					genSchema = out.schema
				}
			}

			if *outputArrow != "" {
				log.Printf("[GEN] Saving %d vectors to Arrow IPC: %s\n", *scale, *outputArrow)
				f, err := os.Create(*outputArrow)
				if err != nil {
					log.Fatalf("Failed to create output file: %v", err)
				}
				writer, err := ipc.NewFileWriter(f, ipc.WithSchema(genSchema))
				if err != nil {
					log.Fatalf("Failed to create IPC writer: %v", err)
				}
				for _, rec := range preGenerated {
					if err := writer.Write(rec); err != nil {
						log.Fatalf("Failed to write record: %v", err)
					}
				}
				if err := writer.Close(); err != nil {
					log.Printf("Warning: failed to close IPC writer: %v", err)
				}
				if err := f.Close(); err != nil {
					log.Printf("Warning: failed to close output file: %v", err)
				}
			} else {
				log.Printf("[GEN] Saving %d vectors to .fbin: %s\n", *scale, *outputFbin)
				f, err := os.Create(*outputFbin)
				if err != nil {
					log.Fatalf("Failed to create output file: %v", err)
				}
				if _, err := safe.Int64ToUint32(int64(*scale)); err != nil {
					log.Fatalf("scale %d out of uint32 range", *scale)
				}
				if _, err := safe.Int64ToUint32(int64(*dim)); err != nil {
					log.Fatalf("dim %d out of uint32 range", *dim)
				}
				if err := binary.Write(f, binary.LittleEndian, uint32(*scale)); err != nil { // #nosec G115
					log.Fatalf("Failed to write .fbin header (count): %v", err)
				}
				if err := binary.Write(f, binary.LittleEndian, uint32(*dim)); err != nil { // #nosec G115
					log.Fatalf("Failed to write .fbin header (dim): %v", err)
				}
				for _, rec := range preGenerated {
					vecArr := rec.Column(1).(*array.FixedSizeList)
					floatArr := vecArr.ListValues().(*array.Float32)
					if err := binary.Write(f, binary.LittleEndian, floatArr.Float32Values()); err != nil {
						log.Fatalf("Failed to write .fbin vector data: %v", err)
					}
				}
				if err := f.Close(); err != nil {
					log.Printf("Warning: failed to close output file: %v", err)
				}
			}

			for _, rec := range preGenerated {
				rec.Release()
			}
			log.Printf("[GEN] Saved %d vectors. Exiting.\n", *scale)
			os.Exit(0)
		}

		log.Printf("[PUT] Uploading %d chunks (parallel streaming pipeline)...\n", numChunks)
		totalUploaded = 0
		start = time.Now()
		var uploader *StreamUploader

		for idx := 0; idx < numChunks; idx++ {
			out := <-chunkChans[idx]
			<-sem
			if out.err != nil {
				log.Fatalf("Record generation failed at chunk %d: %v", idx, out.err)
			}
			rec := out.rec

			if uploader == nil {
				uploader, err = newStreamUploader(sc, *dataset, out.schema)
				if err != nil {
					rec.Release()
					log.Fatalf("Failed to init uploader for generated records: %v", err)
				}
			}

			if err := uploader.Write(rec); err != nil {
				rec.Release()
				log.Fatalf("DoPut write failed at chunk %d: %v", idx, err)
			}
			totalUploaded += int(rec.NumRows())
			rec.Release()

			if totalUploaded%50000 == 0 || totalUploaded == *scale {
				log.Printf("  Progress: %d/%d vectors uploaded\n", totalUploaded, *scale)
			}
		}
		if uploader != nil {
			if err := uploader.Close(); err != nil {
				log.Fatalf("Failed to close uploader: %v", err)
			}
		}
	}
	duration := time.Since(start).Seconds()

	var bytesPerElement int64 = 4
	switch *dtype {
	case "int8", "uint8":
		bytesPerElement = 1
	case "int16", "uint16", "float16":
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
		totalBytes = int64(*scale) * (int64(*dim) * int64(*tqBits) / 8)
		if totalBytes == 0 {
			totalBytes = int64(*scale) // minimal estimate
		}
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
		TqBits:          *tqBits,
	})
	log.Printf("[PUT] Completed in %.4fs (%.2f vec/s, %.2f MB/s)\n", duration, float64(*scale)/duration, (float64(totalBytes)/(1024*1024))/duration)

	log.Println("Waiting for background indexing to complete...")
	indexingTimeout := 3600 * time.Second
	if v := os.Getenv("LONGBOW_BENCH_HNSW_TIMEOUT"); v != "" {
		if t, err := strconv.ParseInt(v, 10, 64); err == nil && t > 0 {
			indexingTimeout = time.Duration(t) * time.Second
		}
	} else if *scale >= 50000 && (*dtype == "complex128" || *dtype == "float64" || *dtype == "int64" || *dtype == "uint64") {
		indexingTimeout = 14400 * time.Second
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), indexingTimeout)
	indexingStart := time.Now()
	readyStatus := waitForIndexingComplete(waitCtx, sc, *dataset, indexingTimeout)
	waitCancel()
	if readyStatus == "ResourceExhausted" {
		log.Fatalf("FATAL: Benchmark aborted for dataset %s: ResourceExhausted (admission blocked by memory limit)", *dataset)
	}
	indexingSeconds := time.Since(indexingStart).Seconds()
	for i := range results {
		if results[i].Name == "DoPut" {
			results[i].IndexingDuration = indexingSeconds
		}
	}
	results = append(results, BenchmarkResult{
		Name:             "Indexing",
		DurationSeconds:  indexingSeconds,
		Throughput:       float64(*scale) / indexingSeconds,
		ThroughputUnit:   "vec/s",
		Rows:             int64(*scale),
		IndexingDuration: indexingSeconds,
	})
	log.Printf("Indexing complete in %.4fs (status: %s, %.2f vec/s).\n", indexingSeconds, readyStatus, float64(*scale)/indexingSeconds)
	logLoadHints(sc)

	// 2. DoGet
	log.Println("[GET] Downloading to verify scan...")
	getTimeout := 5 * time.Minute
	if *dtype == "complex128" || *dtype == "float64" {
		getTimeout = 30 * time.Minute
	}
	getCtx, getCancel := context.WithTimeout(context.Background(), getTimeout)
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
		TqBits:          *tqBits,
	})
	log.Printf("[GET] Completed in %.4fs (%.2f vec/s, %.2f MB/s)\n", duration, float64(rowsRead)/duration, (float64(totalBytesGet)/(1024*1024))/duration)

	// 3. Search
	allModes := []string{"Dense", "Hybrid", "Filtered", "FilteredBool", "FilteredString", "Sparse", "ByID", "GraphRAG", "GlobalGraphRAG", "Recommend", "Geo", "Temporal", "LearnedIndex"}
	var modes []string
	if *searchModes == "all" {
		modes = allModes
	} else {
		selected := strings.Split(*searchModes, ",")
		for _, m := range selected {
			m = strings.TrimSpace(m)
			// Normalize: strip underscores and lowercase for fuzzy matching
			norm := strings.ReplaceAll(strings.ToLower(m), "_", "")
			for _, am := range allModes {
				if norm == strings.ToLower(am) {
					modes = append(modes, am)
					break
				}
			}
		}
	}
	if len(modes) == 0 && *searchModes != "" {
		log.Printf("Warning: No valid search modes found for %s, skipping search phase\n", *searchModes)
	}
	// R11: one fixed as-of timestamp for the whole run.
	TemporalAsOfNanos = *temporalAsOf
	RunSeed = *seed

	// R12a: randomize mode order using the run seed if requested to break order coupling.
	if *shuffleModes && len(modes) > 1 {
		r := rand.New(rand.NewSource(*seed)) // #nosec G404 -- deterministic benchmark order shuffle
		r.Shuffle(len(modes), func(i, j int) {
			modes[i], modes[j] = modes[j], modes[i]
		})
	}

	// R10: a per-mode budget. The previous single 5-minute context covered all
	// modes, so each mode inherited what the ones before it left behind and the
	// last modes silently truncated.
	modeOrders := append([]string(nil), modes...)
	for modeIdx, mode := range modes {
		searchCtx, searchCancel := context.WithTimeout(context.Background(), *searchTimeout)

		log.Printf("[SEARCH][%s] Running %d queries with %d workers...\n", mode, *queries, *workers)
		start = time.Now()

		var latencies []float64
		var failed, truncated int64
		var mu sync.Mutex
		var wg sync.WaitGroup

		queriesPerWorker := *queries / *workers
		if queriesPerWorker == 0 {
			queriesPerWorker = 1
			*workers = *queries
		}

		for w := 0; w < *workers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				state := NewReusableSearchState(*dim)
				localLatencies := make([]float64, 0, queriesPerWorker)

				numToRun := queriesPerWorker
				if workerID == *workers-1 {
					numToRun = *queries - (queriesPerWorker * (*workers - 1))
				}

				for i := 0; i < numToRun; i++ {
					// Stable global index: derived from the worker's slice
					// offset, not from a counter, so the query a given index
					// denotes does not depend on goroutine scheduling.
					globalIdx := workerID*queriesPerWorker + i
					corpusN := *scale
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
					if err := executeSearch(searchCtx, sc, *dataset, *dim, *dtype, mode, state, globalIdx, corpusN); err != nil {
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

		duration = time.Since(start).Seconds()
		requested := int64(*queries)
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
			TqBits:          *tqBits,

			ContextDeadlineExceeded: deadlineExceeded,
			QueriesRequested:        requested,
			QueriesFailed:           failed,
			QueriesTruncated:        truncated,
			Seed:                    *seed,
			ModeIndex:               modeIdx,
			ModeCount:               len(modes),
			ModeOrders:              modeOrders,
		})
		note := ""
		if deadlineExceeded {
			note = fmt.Sprintf(" [TRUNCATED: budget %s exhausted, %d/%d queries ran; QPS is a floor not a measurement]",
				*searchTimeout, completed, requested)
		} else if truncated > 0 {
			note = fmt.Sprintf(" [%d queries truncated]", truncated)
		}
		log.Printf("[SEARCH][%s] Completed %d/%d queries in %.4fs (%.2f QPS, mean: %.3fms, P50: %.2fms, P95: %.2fms, P99: %.2fms)%s\n",
			mode, completed, requested, duration, float64(completed)/duration, mean, p50, p95, p99, note)
	}

	// 4. Print Summary
	fmt.Printf("\n%s\n", "BENCHMARK SUITE SUMMARY")
	fmt.Printf("%-20s | %-18s | %-18s | %-10s | %-10s | %-10s | %-10s | %-10s\n", "Name", "Throughput (vec/s)", "Throughput (MB/s)", "Rows", "mean(ms)", "P50(ms)", "P95(ms)", "P99(ms)")
	for _, r := range results {
		fmt.Printf("%-20s | %-18.2f | %-18.2f | %-10d | %-10.3f | %-10.2f | %-10.2f | %-10.2f\n", r.Name, r.Throughput, r.ThroughputMBs, r.Rows, r.MeanLatencyMs, r.P50LatencyMs, r.P95LatencyMs, r.P99LatencyMs)
	}

	if *outputJson != "" {
		f, err := os.Create(*outputJson)
		if err != nil {
			log.Fatalf("Failed to create JSON: %v", err)
		}
		defer f.Close()
		_ = json.NewEncoder(f).Encode(results)
		log.Printf("Results saved to %s\n", *outputJson)
	}
}

type StreamUploader struct {
	sc     *client.SmartClient
	stream flight.FlightService_DoPutClient
	writer *flight.Writer
	cancel context.CancelFunc
}

func newStreamUploader(sc *client.SmartClient, dataset string, schema *arrow.Schema) (*StreamUploader, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Hour)
	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{dataset},
	}
	stream, err := sc.DoPut(ctx, desc)
	if err != nil {
		cancel()
		return nil, err
	}

	writer := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	writer.SetFlightDescriptor(desc)

	return &StreamUploader{
		sc:     sc,
		stream: stream,
		writer: writer,
		cancel: cancel,
	}, nil
}

func (u *StreamUploader) Write(record arrow.Record) error {
	return u.writer.Write(record)
}

func (u *StreamUploader) Close() error {
	defer u.cancel()
	if err := u.writer.Close(); err != nil {
		return err
	}
	if err := u.stream.CloseSend(); err != nil {
		return err
	}
	if _, err := u.stream.Recv(); err != nil && err != io.EOF {
		return err
	}
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
		// reader does not have Release

		if total > 0 || time.Now().After(deadline) {
			return total, err
		}
		time.Sleep(500 * time.Millisecond)
	}
}

type ReusableSearchState struct {
	vector []float32
	buf    []byte
}

func NewReusableSearchState(maxDim int) *ReusableSearchState {
	return &ReusableSearchState{
		vector: make([]float32, maxDim*2), // 2*dim for complex types
		buf:    make([]byte, 0, 1024*64),  // 64KB should fit most vectors
	}
}

// corpusValueRange is the half-open interval the generated corpus occupies for a
// given dtype. It is the single source of truth for that interval: the corpus
// generator draws from it and BuildSearchTicket draws its queries from it too.
//
// The two must agree because a query has to live in the same numeric domain as
// the vectors it is compared against. The server narrows a float32 query into
// the dataset's integer type with a rounding cast that clamps at the type's
// limits, so a query drawn from [0,1) against an int8 corpus spanning [0,127)
// arrives as 128 zeros and the search degenerates into "nearest node to the
// origin" - the same answer for every query, measured as if it were a search.
// That is what made the 100k matrix report int8 at 1162 QPS and uint8 at 3974
// QPS while both are one byte wide; see docs/roadmap.md item 1 (R32).
func corpusValueRange(dtype string) (lo, hi float32) {
	switch dtype {
	case "int8":
		return 0, 127
	case "uint8":
		return 0, 255
	case "int16", "uint16", "int32", "uint32", "int64", "uint64":
		return 0, 1000
	default:
		// float16, float32, float64, complex64, complex128, turboquant.
		return 0, 1
	}
}

// corpusValueBound is the exclusive upper bound of corpusValueRange, as an int,
// for the integer element types whose corpus is drawn with rng.Intn.
func corpusValueBound(dtype string) int {
	_, hi := corpusValueRange(dtype)
	return int(hi)
}

// BuildSearchTicket builds a search ticket for one query.
//
// queryIdx makes the query vector a deterministic function of (mode, query
// index) rather than of the global math/rand, which Go seeds randomly per
// process (R16, H7). Two runs of the same binary therefore issue byte-identical
// queries, which is the precondition for comparing two runs at all.
func (s *ReusableSearchState) BuildSearchTicket(dataset string, dim int, dtype string, mode string, k int, queryIdx int) []byte {
	s.buf = s.buf[:0]
	s.buf = append(s.buf, `{"search":{"dataset":"`...)
	s.buf = append(s.buf, dataset...)
	s.buf = append(s.buf, `","k":`...)
	s.buf = append(s.buf, fmt.Sprintf("%d", k)...)

	queryLen := dim
	if dtype == "complex64" || dtype == "complex128" {
		queryLen = dim * 2
	}

	// Randomize vector in-place
	// One stream per (mode, query index): reproducible, and independent enough
	// that neighbouring queries are not near-duplicates.
	qLo, qHi := corpusValueRange(dtype)
	qRng := rand.New(rand.NewSource(RunSeed + int64(queryIdx)*2654435761 + int64(len(mode))*97)) // #nosec G404 -- benchmark data
	span := float64(qHi - qLo)
	for i := 0; i < queryLen; i++ {
		s.vector[i] = qLo + float32(qRng.Float64()*span) // #nosec G404 -- benchmark data
	}

	switch mode {
	case "Dense":
		s.buf = append(s.buf, `,"vector":[`...)
		for i := 0; i < queryLen; i++ {
			if i > 0 {
				s.buf = append(s.buf, ',')
			}
			s.buf = fmt.Appendf(s.buf, "%g", s.vector[i])
		}
		s.buf = append(s.buf, ']')
	case "Hybrid":
		s.buf = append(s.buf, `,"vector":[`...)
		for i := 0; i < queryLen; i++ {
			if i > 0 {
				s.buf = append(s.buf, ',')
			}
			s.buf = fmt.Appendf(s.buf, "%g", s.vector[i])
		}
		s.buf = append(s.buf, `],"text_query":"benchmark search term","alpha":0.5`...)
	case "Filtered", "FilteredBool", "FilteredString":
		s.buf = append(s.buf, `,"vector":[`...)
		for i := 0; i < queryLen; i++ {
			if i > 0 {
				s.buf = append(s.buf, ',')
			}
			s.buf = fmt.Appendf(s.buf, "%g", s.vector[i])
		}
		s.buf = append(s.buf, `],"filters":[`...)
		switch mode {
		case "Filtered":
			s.buf = append(s.buf, `{"field":"id","operator":">","value":"10"}`...)
		case "FilteredBool":
			s.buf = append(s.buf, `{"field":"active","operator":"==","value":"true"}`...)
		default:
			s.buf = append(s.buf, `{"field":"category","operator":"==","value":"electronics"}`...)
		}
		s.buf = append(s.buf, ']')
	case "GraphRAG", "GlobalGraphRAG":
		s.buf = append(s.buf, `,"vector":[`...)
		for i := 0; i < queryLen; i++ {
			if i > 0 {
				s.buf = append(s.buf, ',')
			}
			s.buf = fmt.Appendf(s.buf, "%g", s.vector[i])
		}
		s.buf = append(s.buf, `],"graph_alpha":0.5`...)
	case "Sparse":
		s.buf = append(s.buf, `,"text_query":"benchmark search term","alpha":0.0`...)
	case "LearnedIndex":
		s.buf = append(s.buf, `,"vector":[`...)
		for i := 0; i < queryLen; i++ {
			if i > 0 {
				s.buf = append(s.buf, ',')
			}
			s.buf = fmt.Appendf(s.buf, "%g", s.vector[i])
		}
		s.buf = append(s.buf, `],"enable_learned_index":true`...)
	}

	s.buf = append(s.buf, `}}`...)
	return s.buf
}

// defaultTemporalAsOf is 2100-01-01T00:00:00Z in Unix nanoseconds. as-of search
// is inclusive of everything at or before the timestamp, so a fixed far-future
// value is both deterministic and non-empty regardless of when the corpus was
// generated. Fixed-value determinism matters more here than semantic realism: the
// mode exists to be compared against a baseline, not to model a point in time.
// defaultSeed is the fixed seed for corpus, queries and ByID ids (R16, H7).
// Any value works as long as it does not change between runs; 42 is arbitrary and
// fixed. The previous behaviour seeded from time.Now().UnixNano() per chunk, so
// no two runs saw the same corpus and neither index shape nor ingest throughput
// was comparable across runs - which is where the 40%+ per-mode variance came
// from.
const defaultSeed int64 = 42

// RunSeed is the seed for this run, set once from -seed. Package-level so the
// few helpers that build tickets or single records do not each need it threaded
// through; it is written once during flag parsing and read-only afterwards.
var RunSeed = defaultSeed

const defaultTemporalAsOf int64 = 4102444800000000000

// TemporalAsOfNanos is the as-of timestamp used by every Temporal ticket in this
// run. Set once from -temporal-asof-nanos; deliberately not read from the clock
// per query.
var TemporalAsOfNanos = defaultTemporalAsOf

func (s *ReusableSearchState) BuildSpecialTicket(dataset string, mode string, queryIdx int, corpusSize int) []byte {
	s.buf = s.buf[:0]
	switch mode {
	case "Recommend":
		s.buf = append(s.buf, `{"recommend":{"dataset":"`...)
		s.buf = append(s.buf, dataset...)
		// Use first 3 IDs from dataset as seeds (IDs are generated as "0", "1", "2", etc.)
		s.buf = append(s.buf, `","k":10,"seed_ids":["0","1","2"],"max_hops":3,"decay":0.8,"alpha":0.5}}`...)
	case "Geo":
		s.buf = append(s.buf, `{"geo_search":{"dataset":"`...)
		s.buf = append(s.buf, dataset...)
		s.buf = append(s.buf, `","k":10,"center":{"lat":40.7128,"lon":-74.0060},"radius_km":50.0,"search_type":"radius"}}`...)
	case "Temporal":
		s.buf = append(s.buf, `{"temporal_search":{"dataset":"`...)
		s.buf = append(s.buf, dataset...)
		s.buf = append(s.buf, `","k":10,"search_type":"as_of","timestamp":`...)
		// Fixed, not time.Now(): the server keys its temporal result cache on
		// (timestamp, k), so a per-query clock reading guarantees a miss and an
		// LRU insert for every single query. See -temporal-asof-nanos.
		s.buf = append(s.buf, fmt.Sprintf("%d", TemporalAsOfNanos)...)
		s.buf = append(s.buf, `}}`...)
	case "ByID":
		// R16, H6: this was hardcoded to id "0", so every query hit one
		// permanently hot node and measured the mode's cache hit rather than
		// its search - a 7x swing between runs. Derived from the query index
		// and wrapped into the corpus, so consecutive queries touch different
		// ids and the mode is reproducible.
		byID := queryIdx
		if corpusSize > 0 {
			byID = queryIdx % corpusSize
		}
		s.buf = append(s.buf, `{"search_by_id":{"dataset":"`...)
		s.buf = append(s.buf, dataset...)
		s.buf = append(s.buf, `","k":10,"id":"`...)
		s.buf = append(s.buf, fmt.Sprintf("%d", byID)...)
		s.buf = append(s.buf, `"}}`...)
	}
	return s.buf
}

// executeSearch performs search by setting JSON ticket in DoGet
func executeSearch(ctx context.Context, sc *client.SmartClient, dataset string, dim int, dtype string, mode string, state *ReusableSearchState, queryIdx, corpusSize int) error {
	var ticketBytes []byte
	if mode == "Recommend" || mode == "Geo" || mode == "Temporal" || mode == "ByID" {
		ticketBytes = state.BuildSpecialTicket(dataset, mode, queryIdx, corpusSize)
	} else {
		ticketBytes = state.BuildSearchTicket(dataset, dim, dtype, mode, 10, queryIdx)
	}

	if mode == "GlobalGraphRAG" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-longbow-global", "true")
	}

	return executeDoGet(ctx, sc, ticketBytes)
}

// executeDoGet performs the actual Flight DoGet call and drains the stream
func executeDoGet(ctx context.Context, sc *client.SmartClient, ticket []byte) error {
	stream, err := sc.DoGet(ctx, ticket)
	if err != nil {
		return err
	}
	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		return err
	}
	// reader does not have Release
	for reader.Next() {
		_ = reader.Record()
	}
	return reader.Err()
}

// waitForIndexingComplete polls until the dataset is indexed.
// Uses polling approach with check_readiness for reliability.
func waitForIndexingComplete(ctx context.Context, sc *client.SmartClient, dataset string, timeout time.Duration) string {
	pollCtx, pollCancel := context.WithTimeout(context.Background(), timeout)
	defer pollCancel()

	checkBody := []byte(`{"dataset":"` + dataset + `"}`)
	checkAction := &flight.Action{Type: "check_readiness", Body: checkBody}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	consecutiveErrors := 0
	consecutiveExhausted := 0

	for {
		select {
		case <-ctx.Done():
			return "cancelled"
		case <-pollCtx.Done():
			log.Printf("WARNING: Timeout (%v) waiting for indexing to complete for dataset %s", timeout, dataset)
			return "timeout"
		case <-ticker.C:
			checkStream, err := sc.DoAction(pollCtx, checkAction)
			if err != nil {
				if strings.Contains(err.Error(), "NotFound") {
					continue
				}
				if strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "Unavailable") {
					consecutiveErrors++
					if consecutiveErrors > 10 {
						log.Printf("  Readiness check failed 10 times consecutively, server likely dead: %v", err)
						return "server_dead"
					}
				}
				log.Printf("  Readiness check failed: %v", err)
				continue
			}
			consecutiveErrors = 0

			result, err := checkStream.Recv()
			if err != nil {
				continue
			}

			var status map[string]interface{}
			if err := json.Unmarshal(result.Body, &status); err == nil {
				if s, ok := status["status"].(string); ok {
					if s == "READY" {
						return s
					}
					if s == "RESOURCE_EXHAUSTED" || s == "EXHAUSTED" {
						log.Printf("ERROR: Dataset %s readiness blocked: ResourceExhausted (%v)", dataset, status["reason"])
						return "ResourceExhausted"
					}
					if reason, ok := status["reason"].(string); ok {
						if strings.Contains(reason, "ResourceExhausted") || strings.Contains(reason, "exceeds limit") {
							consecutiveExhausted++
							if consecutiveExhausted >= 2 {
								log.Printf("ERROR: Dataset %s admission blocked by memory limit: %s", dataset, reason)
								return "ResourceExhausted"
							}
						} else {
							consecutiveExhausted = 0
						}
						log.Printf("  Still indexing %s... (%s)", dataset, reason)
					}
				}
			}
		}
	}
}

// generateRecord is a multi-type arrow table builder (backward compatible wrapper)
func generateRecord(count int, dim int, dtype string, tqBits int) (arrow.Record, *arrow.Schema, error) {
	// R16: deterministic, unlike the time.Now() seed it replaces.
	rng := rand.New(rand.NewSource(RunSeed)) // #nosec G404 -- benchmark data, not crypto
	return generateRecordBatch(rng, 0, count, dim, dtype, tqBits)
}

// generateRecordBatch builds an Arrow record batch using an isolated PRNG and base offset for lock-free parallel generation
func generateRecordBatch(rng *rand.Rand, offset int, count int, dim int, dtype string, tqBits int) (arrow.Record, *arrow.Schema, error) {
	pool := memory.NewGoAllocator()
	var dt arrow.DataType

	switch dtype {
	case "float32", "turboquant":
		dt = arrow.PrimitiveTypes.Float32
	case "float64":
		dt = arrow.PrimitiveTypes.Float64
	case "float16":
		dt = arrow.FixedWidthTypes.Float16
	case "int32":
		dt = arrow.PrimitiveTypes.Int32
	case "int16":
		dt = arrow.PrimitiveTypes.Int16
	case "int8":
		dt = arrow.PrimitiveTypes.Int8
	case "uint32":
		dt = arrow.PrimitiveTypes.Uint32
	case "uint16":
		dt = arrow.PrimitiveTypes.Uint16
	case "uint8":
		dt = arrow.PrimitiveTypes.Uint8
	case "int64":
		dt = arrow.PrimitiveTypes.Int64
	case "uint64":
		dt = arrow.PrimitiveTypes.Uint64
	case "complex64":
		dt = arrow.PrimitiveTypes.Float32
	case "complex128":
		dt = arrow.PrimitiveTypes.Float64
	default:
		return nil, nil, fmt.Errorf("unsupported dtype: %s", dtype)
	}

	listLen := int32(dim) // #nosec G115
	if dtype == "complex64" || dtype == "complex128" {
		listLen = int32(2 * dim) // #nosec G115
	}
	var meta arrow.Metadata
	if dtype == "turboquant" {
		meta = arrow.NewMetadata([]string{"longbow.vector_type", "longbow.turboquant_bits"}, []string{dtype, strconv.Itoa(tqBits)})
	} else {
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
			{Name: "id", Type: arrow.BinaryTypes.String},
			vecField,
			{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_ns},
			{Name: "geo_point", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float64)},
			{Name: "active", Type: arrow.FixedWidthTypes.Boolean},
			{Name: "category", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	// 1. Build IDs using strconv.Itoa (fast zero-reflection string formatting)
	idBldr := array.NewStringBuilder(pool)
	defer idBldr.Release()
	idBldr.Reserve(count)
	for i := 0; i < count; i++ {
		idBldr.Append(strconv.Itoa(offset + i))
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
			vals[i] = rng.Float32() // #nosec G404
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
			vals[i] = rng.Float64() // #nosec G404
		}
		for i := 0; i < count; i++ {
			listBldr.Append(true)
			vb.AppendValues(vals[i*stride:(i+1)*stride], nil)
		}
	case "float16":
		vb := listBldr.ValueBuilder().(*array.Float16Builder)
		vb.Reserve(count * dim)
		vals := make([]float16.Num, count*dim)
		for i := range vals {
			vals[i] = float16.New(rng.Float32()) // #nosec G404
		}
		for i := 0; i < count; i++ {
			listBldr.Append(true)
			vb.AppendValues(vals[i*dim:(i+1)*dim], nil)
		}
	case "int32":
		vb := listBldr.ValueBuilder().(*array.Int32Builder)
		vb.Reserve(count * dim)
		vals := make([]int32, count*dim)
		for i := range vals {
			vals[i] = int32(rng.Intn(corpusValueBound("int32"))) // #nosec G115,G404 -- bound comes from corpusValueRange
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
			vals[i] = int16(rng.Intn(corpusValueBound("int16"))) // #nosec G115,G404 -- bound comes from corpusValueRange
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
			vals[i] = int8(rng.Intn(corpusValueBound("int8"))) // #nosec G115,G404 -- bound comes from corpusValueRange
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
			vals[i] = uint32(rng.Intn(corpusValueBound("uint32"))) // #nosec G115,G404 -- bound comes from corpusValueRange
		}
		for i := 0; i < count; i++ {
			listBldr.Append(true)
			vb.AppendValues(vals[i*dim:(i+1)*dim], nil)
		}
	case "uint16":
		vb := listBldr.ValueBuilder().(*array.Uint16Builder)
		vb.Reserve(count * dim)
		vals := make([]uint16, count*dim)
		for i := range vals {
			vals[i] = uint16(rng.Intn(corpusValueBound("uint16"))) // #nosec G115,G404 -- bound comes from corpusValueRange
		}
		for i := 0; i < count; i++ {
			listBldr.Append(true)
			vb.AppendValues(vals[i*dim:(i+1)*dim], nil)
		}
	case "uint8":
		vb := listBldr.ValueBuilder().(*array.Uint8Builder)
		vb.Reserve(count * dim)
		vals := make([]uint8, count*dim)
		for i := range vals {
			vals[i] = uint8(rng.Intn(corpusValueBound("uint8"))) // #nosec G115,G404 -- bound comes from corpusValueRange
		}
		for i := 0; i < count; i++ {
			listBldr.Append(true)
			vb.AppendValues(vals[i*dim:(i+1)*dim], nil)
		}
	case "int64":
		vb := listBldr.ValueBuilder().(*array.Int64Builder)
		vb.Reserve(count * dim)
		vals := make([]int64, count*dim)
		for i := range vals {
			vals[i] = int64(rng.Intn(corpusValueBound("int64"))) // #nosec G404 -- bound comes from corpusValueRange
		}
		for i := 0; i < count; i++ {
			listBldr.Append(true)
			vb.AppendValues(vals[i*dim:(i+1)*dim], nil)
		}
	case "uint64":
		vb := listBldr.ValueBuilder().(*array.Uint64Builder)
		vb.Reserve(count * dim)
		vals := make([]uint64, count*dim)
		for i := range vals {
			vals[i] = uint64(rng.Intn(corpusValueBound("uint64"))) // #nosec G115,G404 -- bound comes from corpusValueRange
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

	// 4. Build Geo Point
	geoBldr := array.NewFixedSizeListBuilder(pool, 2, arrow.PrimitiveTypes.Float64)
	defer geoBldr.Release()
	geoBldr.Reserve(count)
	geoValBldr := geoBldr.ValueBuilder().(*array.Float64Builder)
	geoValBldr.Reserve(count * 2)
	for i := 0; i < count; i++ {
		geoBldr.Append(true)
		geoValBldr.Append(40.7128 + rng.Float64()*0.1)  // #nosec G404 -- non-cryptographic use for benchmark data
		geoValBldr.Append(-74.0060 + rng.Float64()*0.1) // #nosec G404 -- non-cryptographic use for benchmark data
	}
	geoArr := geoBldr.NewArray()
	defer geoArr.Release()

	// 5. Build Active (Boolean)
	boolBldr := array.NewBooleanBuilder(pool)
	defer boolBldr.Release()
	boolBldr.Reserve(count)
	for i := 0; i < count; i++ {
		boolBldr.Append(rng.Float32() > 0.5) // #nosec G404
	}
	activeArr := boolBldr.NewArray()
	defer activeArr.Release()

	// 6. Build Category (String)
	strBldr := array.NewStringBuilder(pool)
	defer strBldr.Release()
	strBldr.Reserve(count)
	categories := []string{"electronics", "clothing", "home", "books"}
	for i := 0; i < count; i++ {
		strBldr.Append(categories[rng.Intn(len(categories))]) // #nosec G404
	}
	catArr := strBldr.NewArray()
	defer catArr.Release()

	return array.NewRecordBatch(schema, []arrow.Array{idArr, vecArr, tsArr, geoArr, activeArr, catArr}, int64(count)), schema, nil
}

func dropDataset(ctx context.Context, sc *client.SmartClient, dataset string) error {
	req := struct {
		Dataset string `json:"dataset"`
	}{Dataset: dataset}
	body, _ := json.Marshal(req)

	action := &flight.Action{
		Type: "drop",
		Body: body,
	}

	stream, err := sc.DoAction(ctx, action)
	if err != nil {
		return err
	}

	_, err = stream.Recv()
	return err
}

func resetDataset(ctx context.Context, sc *client.SmartClient, dataset string) error {
	req := struct {
		Name string `json:"name"`
	}{Name: dataset}
	body, _ := json.Marshal(req)

	action := &flight.Action{
		Type: "ResetDataset",
		Body: body,
	}

	stream, err := sc.DoAction(ctx, action)
	if err != nil {
		return err
	}

	_, err = stream.Recv()
	return err
}

func logLoadHints(sc *client.SmartClient) {
	// Trigger a GetFlightInfo to refresh hints
	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{"_health"},
	}
	// We use a short timeout for this background refresh
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := sc.GetFlightInfo(ctx, desc)
	if err != nil {
		// Log error but don't fail, hints might still be available from previous calls
		log.Printf("[LOAD-REFRESH-ERROR] %v\n", err)
	}

	hints := sc.GetLastLoadHints()
	if hints != nil {
		log.Printf("[LOAD] CPU: %d%%, Mem: %d%%, Queue: %d, Health: %d%%\n",
			hints.CPULoad, hints.MemLoad, hints.QueueDepth, hints.Health)
	}
}

func logDetailedError(cmd string, err error, sc *client.SmartClient) {
	log.Printf("[ERROR] %s failed: %v\n", cmd, err)
	hints := sc.GetLastLoadHints()
	if hints != nil {
		log.Printf("[LOAD-AT-FAILURE] CPU: %d%%, Mem: %d%%, Queue: %d, Health: %d%%\n",
			hints.CPULoad, hints.MemLoad, hints.QueueDepth, hints.Health)
	}
}
