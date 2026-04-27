package main

// nosec G404 - math/rand is used for benchmark test data, not security-sensitive
import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/client"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"golang.org/x/sys/unix"
)

func main() {
	var (
		mode        = flag.String("mode", "write", "Benchmark mode: 'write', 'read', 'mixed', 'vec'")
		dir         = flag.String("dir", "./bench_data", "Directory to store benchmark files")
		fileSizeMB  = flag.Int("size", 1024, "Total file size in MB for read test / target size for write")
		blockSize   = flag.Int("block", 4096, "Block size in bytes (simulating vector size + header)")
		concurrency = flag.Int("workers", 1, "Number of concurrent workers")
		duration    = flag.Duration("duration", 10*time.Second, "Duration to run the test")
		doSync      = flag.Bool("sync", false, "Perform fsync after every write (write mode only)")
		
		// Vector benchmark mode flags
		uri         = flag.String("uri", "grpc://127.0.0.1:3000", "Longbow server URI")
		dim         = flag.Int("dim", 128, "Vector dimension")
		dtype       = flag.String("dtype", "float32", "Data type")
		tqBits      = flag.Int("tq-bits", 4, "Turboquant bits")
		scale       = flag.Int("scale", 1000, "Number of vectors")
		queries     = flag.Int("queries", 1000, "Number of search queries")
		dataset     = flag.String("dataset", "benchmark", "Dataset name")
		jsonFile    = flag.String("json", "", "Output JSON file")
		searchModes = flag.String("search-modes", "dense,hybrid,sparse,filtered,byid", "Search modes")
	)
	flag.Parse()

	if *mode == "vec" {
		runVectorBenchmark(*uri, *dim, *dtype, *tqBits, *scale, *queries, *dataset, *jsonFile, *searchModes)
		return
	}

	if err := os.MkdirAll(*dir, 0750); err != nil {
		panic(err)
	}

	fmt.Printf("Starting I/O Benchmark\n")
	fmt.Printf("Mode: %s, Dir: %s, Size: %dMB, Block: %d, Workers: %d, Sync: %v\n", *mode, *dir, *fileSizeMB, *blockSize, *concurrency, *doSync)

	switch *mode {
	case "write":
		runWriteBenchmark(*dir, *fileSizeMB, *blockSize, *concurrency, *duration, *doSync)
	case "read":
		runReadBenchmark(*dir, *fileSizeMB, *blockSize, *concurrency, *duration)
	case "mixed":
		// Simple mixed: 50/50 split of workers
		half := *concurrency / 2
		if half < 1 {
			half = 1
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			runWriteBenchmark(*dir, *fileSizeMB/2, *blockSize, half, *duration, *doSync)
		}()
		go func() {
			defer wg.Done()
			// Ensure we have something to read first
			prepFile(*dir, *fileSizeMB/2, *blockSize)
			runReadBenchmark(*dir, *fileSizeMB/2, *blockSize, half, *duration)
		}()
		wg.Wait()
	case "mmap":
		runMmapBenchmark(*dir, *fileSizeMB, *blockSize, *concurrency, *duration, true) // true = random
	case "scan":
		runMmapBenchmark(*dir, *fileSizeMB, *blockSize, *concurrency, *duration, false) // false = sequential
	default:
		fmt.Println("Invalid mode. Use write, read, mixed, mmap, or scan.")
		os.Exit(1)
	}
}

func runMmapBenchmark(dir string, sizeMB int, blockSize int, workers int, duration time.Duration, random bool) {
	modeStr := "Random Mmap"
	if !random {
		modeStr = "Sequential Mmap Scan"
	}
	fmt.Printf("\n--- %s Benchmark ---\n", modeStr)

	filename := filepath.Join(dir, "bench_read_master.dat")
	fileSize := prepFile(dir, sizeMB, blockSize)
	defer os.Remove(filename)

	f, err := os.Open(filepath.Clean(filename))
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Mmap the file
	data, err := unix.Mmap(int(f.Fd()), 0, int(fileSize), unix.PROT_READ, unix.MAP_SHARED) // #nosec G115
	if err != nil {
		panic(fmt.Sprintf("mmap failed: %v", err))
	}
	defer unix.Munmap(data)

	// Advise
	if random {
		_ = unix.Madvise(data, unix.MADV_RANDOM) // nosec G104
	} else {
		_ = unix.Madvise(data, unix.MADV_SEQUENTIAL) // nosec G104
	}

	var wg sync.WaitGroup
	var totalOps uint64
	var totalBytes uint64

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	numBlocks := int(fileSize) / blockSize

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// For sequential scan, we iterate
			// For random, we jump

			localOffset := 0
			if !random {
				// Partition for workers? Or just have them all scan the whole thing?
				// Typically parallel scan means they scan disjoint ranges.
				// Let's divide the file.
				partSize := numBlocks / workers
				localOffset = id * partSize * blockSize
			}

			for {
				select {
				case <-ctx.Done():
					return
				default:
					var offset int
					if random {
						blockIdx := rand.Intn(numBlocks) // #nosec G404
						offset = blockIdx * blockSize
					} else {
						offset = localOffset
						localOffset += blockSize
						if localOffset >= int(fileSize) {
							localOffset = 0 // Wrap around
						}
					}

					// Access memory
					// We sum byte to ensure page fault / memory access actually happens (basic check)
					// or just copy it. HNSW does copy/read usually.
					// Let's copy to a small buffer to simulate "ExtractVector"
					end := offset + blockSize
					if end > len(data) {
						end = len(data)
					}
					// Volatile read
					_ = data[offset]
					// Simulate slight work
					sum := byte(0)
					for k := offset; k < end; k += 64 { // Skip stride to be faster but touch pages
						sum += data[k]
					}

					atomic.AddUint64(&totalOps, 1)
					atomic.AddUint64(&totalBytes, uint64(blockSize)) // #nosec G115
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	printStats("Mmap "+modeStr, elapsed, totalOps, totalBytes)
}

func runWriteBenchmark(dir string, sizeMB int, blockSize int, workers int, duration time.Duration, doSync bool) {
	fmt.Println("\n--- Write Benchmark (Sequential Append) ---")

	// Pre-generate a data block to avoid measuring generation time
	data := make([]byte, blockSize)
	rand.Read(data) // #nosec G404

	var wg sync.WaitGroup
	var totalOps uint64
	var totalBytes uint64

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			filename := filepath.Join(dir, fmt.Sprintf("bench_write_%d.dat", id))
			f, err := os.Create(filepath.Clean(filename))
			if err != nil {
				fmt.Printf("Worker %d error creating file: %v\n", id, err)
				return
			}
			defer f.Close()
			defer os.Remove(filename) // Cleanup

			// Buffered writer to simulate WAL buffering (optional, but typical)
			w := bufio.NewWriterSize(f, 64*1024)

			for {
				select {
				case <-ctx.Done():
					_ = w.Flush() // nosec G104
					return
				default:
					n, err := w.Write(data)
					if err != nil {
						fmt.Printf("Write error: %v\n", err)
						return
					}
					if doSync {
						_ = w.Flush() // nosec G104
						_ = f.Sync()  // nosec G104
					}
					atomic.AddUint64(&totalOps, 1)
					atomic.AddUint64(&totalBytes, uint64(n)) // #nosec G115
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	printStats("Write", elapsed, totalOps, totalBytes)
}

func runReadBenchmark(dir string, sizeMB int, blockSize int, workers int, duration time.Duration) {
	fmt.Println("\n--- Read Benchmark (Random Seek) ---")

	filename := filepath.Join(dir, "bench_read_master.dat")
	// Ensure file exists
	fileSize := prepFile(dir, sizeMB, blockSize)
	defer os.Remove(filename)

	var wg sync.WaitGroup
	var totalOps uint64
	var totalBytes uint64

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			f, err := os.Open(filepath.Clean(filename))
			if err != nil {
				fmt.Printf("Worker %d error opening file: %v\n", id, err)
				return
			}
			defer f.Close()

			buf := make([]byte, blockSize)
			numBlocks := fileSize / int64(blockSize)

			for {
				select {
				case <-ctx.Done():
					return
				default:
					// Random seek
					blockIdx := rand.Int63n(numBlocks) // #nosec G404
					offset := blockIdx * int64(blockSize)

					_, err := f.ReadAt(buf, offset)
					if err != nil && err != io.EOF {
						fmt.Printf("Read error: %v\n", err)
						return
					}
					atomic.AddUint64(&totalOps, 1)
					atomic.AddUint64(&totalBytes, uint64(blockSize)) // #nosec G115
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	printStats("Read", elapsed, totalOps, totalBytes)
}

func prepFile(dir string, sizeMB int, blockSize int) int64 {
	filename := filepath.Join(dir, "bench_read_master.dat")
	info, err := os.Stat(filename)
	targetSize := int64(sizeMB) * 1024 * 1024

	if err == nil && info.Size() >= targetSize {
		// File exists and is big enough
		return info.Size()
	}

	fmt.Printf("Preparing %dMB read file...\n", sizeMB)
	f, err := os.Create(filepath.Clean(filename))
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Fill with random-ish data
	chunk := make([]byte, 1024*1024) // 1MB chunk
	rand.Read(chunk) // #nosec G404

	written := int64(0)
	for written < targetSize {
		n, err := f.Write(chunk)
		if err != nil {
			panic(err)
		}
		written += int64(n)
	}
	return written
}

func printStats(op string, elapsed time.Duration, ops, bytes uint64) {
	iops := float64(ops) / elapsed.Seconds()
	mbps := float64(bytes) / 1024 / 1024 / elapsed.Seconds()
	avgLat := elapsed.Seconds() / float64(ops) * 1000 // ms

	fmt.Printf("%s Results:\n", op)
	fmt.Printf("  Duration: %.2fs\n", elapsed.Seconds())
	fmt.Printf("  Total Ops: %d\n", ops)
	fmt.Printf("  Throughput: %.2f MB/s\n", mbps)
	fmt.Printf("  IOPS: %.2f\n", iops)
	fmt.Printf("  Avg Latency: %.4f ms/op\n", avgLat)
}

type BenchmarkResult struct {
	Dim             int     `json:"dim"`
	Dtype           string  `json:"dtype"`
	Count           int     `json:"count"`
	IngestVecPerSec float64 `json:"ingest_vec_per_sec"`
	IngestP50Ms     float64 `json:"ingest_p50_ms"`
	DenseQPS        float64 `json:"dense_qps"`
	DenseP50Ms      float64 `json:"dense_p50_ms"`
	DenseP95Ms      float64 `json:"dense_p95_ms"`
	DenseP99Ms      float64 `json:"dense_p99_ms"`
	HybridQPS       float64 `json:"hybrid_qps"`
	HybridP50Ms     float64 `json:"hybrid_p50_ms"`
	HybridP95Ms     float64 `json:"hybrid_p95_ms"`
	HybridP99Ms     float64 `json:"hybrid_p99_ms"`
	SparseQPS       float64 `json:"sparse_qps"`
	SparseP50Ms     float64 `json:"sparse_p50_ms"`
	SparseP95Ms     float64 `json:"sparse_p95_ms"`
	SparseP99Ms     float64 `json:"sparse_p99_ms"`
	FilteredQPS     float64 `json:"filtered_qps"`
	FilteredP50Ms   float64 `json:"filtered_p50_ms"`
	FilteredP95Ms  float64 `json:"filtered_p95_ms"`
	FilteredP99Ms  float64 `json:"filtered_p99_ms"`
	ByIDQPS         float64 `json:"byid_qps"`
	ByIDP50Ms       float64 `json:"byid_p50_ms"`
	ByIDP95Ms       float64 `json:"byid_p95_ms"`
	ByIDP99Ms       float64 `json:"byid_p99_ms"`
}

func runVectorBenchmark(uri string, dim int, dtype string, tqBits, scale, queries int, dataset, jsonFile, searchModes string) {
	ctx := context.Background()
	result := &BenchmarkResult{
		Dim:   dim,
		Dtype: dtype,
		Count: scale,
	}

	sc, err := client.NewSmartClient(uri)
	if err != nil {
		log.Fatalf("Failed to connect to %s: %v", uri, err)
	}
	defer sc.Close()

	fmt.Printf("Vector Benchmark (dim=%d, dtype=%s, count=%d)\n", dim, dtype, scale)

	vecs := make([]float32, scale*dim)
	rnd := rand.New(rand.NewSource(42)) // #nosec G404
	for i := range vecs {
		vecs[i] = rnd.Float32()
	}

	fmt.Printf("Ingesting %d vectors...\n", scale)
	ingestStart := time.Now()

	mem := memory.NewGoAllocator()
	sch := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)}, // #nosec G115
	}, nil)

	idBuilder := array.NewInt64Builder(mem)
	defer idBuilder.Release()
	listBuilder := array.NewFixedSizeListBuilder(mem, int32(dim), arrow.PrimitiveTypes.Float32) // #nosec G115
	defer listBuilder.Release()
	vecBuilder := listBuilder.ValueBuilder().(*array.Float32Builder)

	idBuilder.Reserve(scale)
	listBuilder.Reserve(scale)
	vecBuilder.Reserve(scale * dim)

	for i := 0; i < scale; i++ {
		idBuilder.Append(int64(i))
		listBuilder.Append(true)
	}
	vecBuilder.AppendValues(vecs, nil)

	idArr := idBuilder.NewArray()
	defer idArr.Release()
	vecArr := listBuilder.NewArray()
	defer vecArr.Release()

	rec := array.NewRecordBatch(sch, []arrow.Array{idArr, vecArr}, int64(scale))
	defer rec.Release()

	if err := uploadData(ctx, sc, dataset, rec, sch); err != nil {
		log.Fatalf("Ingest failed: %v", err)
	}

	ingestDur := time.Since(ingestStart)
	result.IngestVecPerSec = float64(scale) / ingestDur.Seconds()
	result.IngestP50Ms = ingestDur.Seconds() * 1000
	fmt.Printf("Ingested %d vectors in %.2fs (%.0f vec/s)\n", scale, ingestDur.Seconds(), result.IngestVecPerSec)

	modes := strings.Split(searchModes, ",")
	var wg sync.WaitGroup
	var mu sync.Mutex
	concurrency := 10
	queriesPerWorker := queries / concurrency

	for _, mode := range modes {
		mode = strings.TrimSpace(mode)
		wg.Add(1)
		go func(m string) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(time.Now().UnixNano())) // #nosec G404
			latencies := make([]float64, 0, queriesPerWorker*concurrency)

			for w := 0; w < concurrency; w++ {
				for i := 0; i < queriesPerWorker; i++ {
					query := make([]float32, dim)
					sum := 0.0
					for j := 0; j < dim; j++ {
						v := rnd.Float32()
						query[j] = v
						sum += float64(v * v)
					}
					norm := math.Sqrt(sum)
					if norm > 0 {
						for j := 0; j < dim; j++ {
							query[j] /= float32(norm)
						}
					}

					start := time.Now()
					var runErr error

					req := map[string]interface{}{
						"dataset": dataset,
						"vector":  query,
						"k":       10,
						"mode":    m,
					}
					ticketBytes, _ := json.Marshal(map[string]interface{}{"search": req})

					stream, runErr := sc.DoGet(ctx, ticketBytes)
					if runErr == nil {
						reader, err := flight.NewRecordReader(stream)
						if err == nil {
							for reader.Next() {
								reader.Record()
							}
							reader.Release()
						}
					}

					latencyMs := time.Since(start).Seconds() * 1000
					if runErr == nil {
						mu.Lock()
						latencies = append(latencies, latencyMs)
						mu.Unlock()
					}
				}
			}

			if len(latencies) == 0 {
				return
			}
			sorted := make([]float64, len(latencies))
			copy(sorted, latencies)
			sort.Float64s(sorted)

			p50 := sorted[int(0.5*float64(len(sorted)))]
			p95 := sorted[int(0.95*float64(len(sorted)))]
			p99 := sorted[int(0.99*float64(len(sorted)))]

			avgLatency := 0.0
			for _, l := range latencies {
				avgLatency += l
			}
			avgLatency /= float64(len(latencies))
			qps := 1000.0 / avgLatency

			mu.Lock()
			switch m {
			case "dense":
				result.DenseQPS, result.DenseP50Ms, result.DenseP95Ms, result.DenseP99Ms = qps, p50, p95, p99
			case "hybrid":
				result.HybridQPS, result.HybridP50Ms, result.HybridP95Ms, result.HybridP99Ms = qps, p50, p95, p99
			case "sparse":
				result.SparseQPS, result.SparseP50Ms, result.SparseP95Ms, result.SparseP99Ms = qps, p50, p95, p99
			case "filtered":
				result.FilteredQPS, result.FilteredP50Ms, result.FilteredP95Ms, result.FilteredP99Ms = qps, p50, p95, p99
			case "byid":
				result.ByIDQPS, result.ByIDP50Ms, result.ByIDP95Ms, result.ByIDP99Ms = qps, p50, p95, p99
			}
			mu.Unlock()
		}(mode)
	}
	wg.Wait()

	fmt.Printf("\nResults:\n")
	fmt.Printf("  Dense:    %8.0f QPS (p50=%.2fms, p95=%.2fms, p99=%.2fms)\n", result.DenseQPS, result.DenseP50Ms, result.DenseP95Ms, result.DenseP99Ms)
	fmt.Printf("  Hybrid:  %8.0f QPS (p50=%.2fms, p95=%.2fms, p99=%.2fms)\n", result.HybridQPS, result.HybridP50Ms, result.HybridP95Ms, result.HybridP99Ms)
	fmt.Printf("  Sparse:  %8.0f QPS (p50=%.2fms, p95=%.2fms, p99=%.2fms)\n", result.SparseQPS, result.SparseP50Ms, result.SparseP95Ms, result.SparseP99Ms)
	fmt.Printf("  Filtered:%8.0f QPS (p50=%.2fms, p95=%.2fms, p99=%.2fms)\n", result.FilteredQPS, result.FilteredP50Ms, result.FilteredP95Ms, result.FilteredP99Ms)
	fmt.Printf("  ByID:    %8.0f QPS (p50=%.2fms, p95=%.2fms, p99=%.2fms)\n", result.ByIDQPS, result.ByIDP50Ms, result.ByIDP95Ms, result.ByIDP99Ms)

	if jsonFile != "" {
data, _ := json.MarshalIndent(result, "", "  ")
	if err := os.WriteFile(jsonFile, data, 0600); err != nil { // #nosec G306
		fmt.Printf("Warning: failed to write JSON: %v\n", err)
	} else {
		fmt.Printf("Results written to %s\n", jsonFile)
	}
	}
}

func uploadData(ctx context.Context, sc *client.SmartClient, dataset string, rec arrow.Record, sch *arrow.Schema) error {
	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{dataset},
	}

	stream, err := sc.DoPut(ctx, desc)
	if err != nil {
		return err
	}

	writer := flight.NewRecordWriter(stream)
	writer.SetFlightDescriptor(desc)

	if err := writer.Write(rec); err != nil {
		_ = writer.Close()
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
