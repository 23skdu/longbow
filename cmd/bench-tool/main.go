package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/23skdu/longbow/client"
	"github.com/23skdu/longbow/pkg/version"
)

// newBenchClient builds a client that authenticates with the always-on RBAC
// layer. The key is read from the environment rather than a CLI flag so it
// cannot leak through process listings or benchmark provenance artifacts.
func newBenchClient(uri string) (*client.SmartClient, error) {
	return client.NewSmartClientWithConfig(client.ClientConfig{
		Addr:   uri,
		APIKey: os.Getenv("LONGBOW_API_KEY"),
	})
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
	seedFlagDefault := defaultSeed
	seedSource := "default"
	if v := os.Getenv("LONGBOW_BENCH_SEED"); v != "" {
		if s, err := strconv.ParseInt(v, 10, 64); err == nil {
			seedFlagDefault = s
			seedSource = "LONGBOW_BENCH_SEED"
		} else {
			log.Printf("Warning: LONGBOW_BENCH_SEED=%q is not an integer, using default seed %d\n", v, defaultSeed) // #nosec G706 -- %q quotes and escapes control characters
		}
	}
	seed := flag.Int64("seed", seedFlagDefault,
		fmt.Sprintf("Seed for corpus generation, query vectors and ByID ids. Repeatability is a precondition for a percentage regression gate: with the corpus and the queries both drawn from the clock, two runs of the same binary differ by more than most regressions anyone would gate on. Defaults to $LONGBOW_BENCH_SEED (%s) when it is set. Recorded in the output JSON.", seedSource))
	workers := flag.Int("workers", 1, "Number of concurrent search workers")
	drop := flag.Bool("drop", false, "Drop dataset after benchmark")
	fbin := flag.String("fbin", "", "Read vectors from Arrow IPC binary file or true .fbin instead of generating them")
	outputArrow := flag.String("output-arrow", "", "Save generated vectors to Arrow IPC file and exit")
	outputFbin := flag.String("output-fbin", "", "Save generated vectors to .fbin file and exit")
	mode := flag.String("mode", "vec", "Benchmark mode (vec, kv, cluster)")
	searchModes := flag.String("search-modes", "all", "Comma-separated search modes to run (all, dense, hybrid, sparse, filtered, filteredbool, filteredstring, byid, range, advancedtext, graphrag, globalgraphrag, recommend, geo, geobox, geohybrid, temporal, temporalrange, temporalwindow, multimodal, federated, learnedindex)")
	searchTimeout := flag.Duration("search-timeout", 5*time.Minute,
		"Budget for each search mode. This is per mode, not shared across them: a single budget covering all modes meant the last mode inherited whatever was left and started silently truncating, which reads as a low QPS rather than as a truncated run.")
	temporalAsOf := flag.Int64("temporal-asof-nanos", defaultTemporalAsOf,
		"Fixed as-of timestamp for Temporal searches, in Unix nanoseconds. It must be fixed: the server keys its temporal result cache on (timestamp, k), so a per-query clock reading guarantees 100% miss rate, an LRU insert per query, and a mode that measures the cache-miss path while appearing to measure search.")
	shuffleModes := flag.Bool("shuffle-modes", false,
		"Randomize search mode execution order using seed (roadmap R12a) to eliminate mode-order coupling.")
	resultsOut := flag.String("results-out", "",
		"Write every search hit (mode -> query -> id/score) to this JSON file. Item 31: two runs with the same seed must agree here bit for bit; this is what the digest in -json is computed over.")
	gpuProfileOut := flag.String("gpu-profile-out", "",
		"Write the server's per-kernel GPU time breakdown to this JSON file (roadmap Item 34). Empty skips the call; a server without GPU profiling answers an empty, unsupported profile rather than an error.")
	reset := flag.Bool("reset", false, "Reset dataset in-place before running the benchmark")
	flag.Parse()

	if *drop {
		defer func() {
			sc, err := newBenchClient(*uri)
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

	sc, err := newBenchClient(*uri)
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

	// R11: fixed seed and timestamp for deterministic corpus & queries.
	TemporalAsOfNanos = *temporalAsOf
	RunSeed = *seed

	var start time.Time
	var corpusDigest string
	if *fbin != "" {
		start = ingestFromFile(sc, *fbin, *dataset, scale, dim)
	} else {
		start, corpusDigest = ingestGeneratedCorpus(sc, *dataset, *scale, *dim, *dtype, *tqBits, *seed, *outputArrow, *outputFbin)
	}
	duration := time.Since(start).Seconds()
	bytesPerElement := computeBytesPerElement(*dtype)

	var totalBytes int64
	if *dtype == "turboquant" {
		totalBytes = int64(*scale) * (int64(*dim) * int64(*tqBits) / 8)
		if totalBytes == 0 {
			totalBytes = int64(*scale) // minimal estimate
		}
	} else {
		totalBytes = int64(*scale) * int64(*dim) * bytesPerElement
	}

	var results []BenchmarkResult
	results = append(results, BenchmarkResult{
		Name:            "DoPut",
		CorpusDigest:    corpusDigest,
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
	modes := resolveSearchModes(*searchModes)
	if len(modes) == 0 && *searchModes != "" {
		log.Printf("Warning: No valid search modes found for %s, skipping search phase\n", *searchModes)
	}
	searchResults, searchHitsByMode, mmDatasetToDrop := runSearchSuite(
		sc, *dataset, *dim, *dtype, *scale, *queries, *workers, *tqBits, *seed, *searchTimeout, modes, *shuffleModes,
	)
	if *drop && mmDatasetToDrop != "" {
		defer func(dsName string) {
			dsc, derr := newBenchClient(*uri)
			if derr == nil {
				defer dsc.Close()
				_ = dropDataset(context.Background(), dsc, dsName)
			}
		}(mmDatasetToDrop)
	}
	results = append(results, searchResults...)

	// 4 & 5. Digest, artifacts, and summary table
	writeSummaryAndArtifacts(sc, results, searchHitsByMode, modes, *dataset, *dtype, *dim, *queries, *seed, *gpuProfileOut, *resultsOut, *outputJson)
}
