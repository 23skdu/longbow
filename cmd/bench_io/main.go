package main

// nosec G404 - math/rand is used for benchmark test data, not security-sensitive
import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

func main() {
	var (
		mode        = flag.String("mode", "write", "Benchmark mode: 'write', 'read', 'mixed'")
		dir         = flag.String("dir", "./bench_data", "Directory to store benchmark files")
		fileSizeMB  = flag.Int("size", 1024, "Total file size in MB for read test / target size for write")
		blockSize   = flag.Int("block", 4096, "Block size in bytes (simulating vector size + header)")
		concurrency = flag.Int("workers", 1, "Number of concurrent workers")
		duration    = flag.Duration("duration", 10*time.Second, "Duration to run the test")
		doSync      = flag.Bool("sync", false, "Perform fsync after every write (write mode only)")
	)
	flag.Parse()

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

	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Mmap the file
	data, err := unix.Mmap(int(f.Fd()), 0, int(fileSize), unix.PROT_READ, unix.MAP_SHARED)
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
						blockIdx := rand.Intn(numBlocks)
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
					atomic.AddUint64(&totalBytes, uint64(blockSize))
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
	rand.Read(data)

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
			f, err := os.Create(filename)
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
					atomic.AddUint64(&totalBytes, uint64(n))
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
			f, err := os.Open(filename)
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
					blockIdx := rand.Int63n(numBlocks)
					offset := blockIdx * int64(blockSize)

					_, err := f.ReadAt(buf, offset)
					if err != nil && err != io.EOF {
						fmt.Printf("Read error: %v\n", err)
						return
					}
					atomic.AddUint64(&totalOps, 1)
					atomic.AddUint64(&totalBytes, uint64(blockSize))
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
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Fill with random-ish data
	chunk := make([]byte, 1024*1024) // 1MB chunk
	rand.Read(chunk)

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
