package main

import (
	"context"
	"flag"
	"fmt"
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
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
)

var (
	peers       = flag.String("peers", "127.0.0.1:3000", "Comma-separated list of peer addresses (e.g., 127.0.0.1:3000,127.0.0.1:3010)")
	duration    = flag.Duration("duration", 10*time.Second, "Duration of the benchmark")
	concurrency = flag.Int("concurrency", 1, "Number of concurrent workers")
	mode        = flag.String("mode", "ingest", "Benchmark mode: 'ingest' or 'search'")
	batchSize   = flag.Int("batch-size", 1000, "Number of records per batch (ingest mode)")
	dim         = flag.Int("dim", 128, "Vector dimension")
)

func main() {
	flag.Parse()

	peerList := strings.Split(*peers, ",")
	if len(peerList) == 0 {
		log.Fatal("No peers specified")
	}

	fmt.Printf("Starting benchmark:\n")
	fmt.Printf("  Mode:        %s\n", *mode)
	fmt.Printf("  Peers:       %v\n", peerList)
	fmt.Printf("  Concurrency: %d\n", *concurrency)
	fmt.Printf("  Duration:    %s\n", *duration)
	fmt.Printf("  Batch Size:  %d\n", *batchSize)
	fmt.Printf("  Dimension:   %d\n", *dim)

	ctx, cancel := context.WithTimeout(context.Background(), *duration+5*time.Second)
	defer cancel()

	var ops atomic.Int64
	var errors atomic.Int64
	var latency sumLatency

	start := time.Now()
	var wg sync.WaitGroup

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Round-robin or random peer selection for initial connection
			initialPeer := peerList[id%len(peerList)]
			c, err := client.NewSmartClient(initialPeer)
			if err != nil {
				log.Printf("Worker %d failed to connect: %v", id, err)
				errors.Add(1)
				return
			}
			defer c.Close()

			endTime := start.Add(*duration)

			for time.Now().Before(endTime) {
				t0 := time.Now()
				var err error

				if *mode == "ingest" {
					err = runIngest(ctx, c)
				} else if *mode == "search" {
					err = runSearch(ctx, c)
				} else {
					log.Printf("Unknown mode: %s", *mode)
					return
				}

				dur := time.Since(t0)
				latency.Record(dur)

				if err != nil {
					errors.Add(1)
					// log.Printf("Error: %v", err) // Verbose
				} else {
					ops.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()
	printResults(time.Since(start), ops.Load(), errors.Load(), &latency)
}

// runIngest generates a random batch and sends it via DoPut
func runIngest(ctx context.Context, c *client.SmartClient) error {
	// Generate Schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.BinaryTypes.String},
			{Name: "embedding", Type: arrow.FixedSizeListOf(int32(*dim), arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	// Build Record
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	idBuilder := b.Field(0).(*array.StringBuilder)
	vecBuilder := b.Field(1).(*array.FixedSizeListBuilder)
	valBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < *batchSize; i++ {
		idBuilder.Append(uuid.New().String())
		vecBuilder.Append(true)
		for j := 0; j < *dim; j++ {
			valBuilder.Append(rand.Float32())
		}
	}

	rec := b.NewRecordBatch()
	defer rec.Release()

	// DoPut
	stream, err := c.DoPut(ctx, &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{"bench_collection"},
	})
	if err != nil {
		return err
	}

	// Send Schema first (if needed by implementation, standard Flight does)
	// But arrow-go Flight Writer handles this usually.

	wr := flight.NewRecordWriter(stream)
	defer wr.Close()

	if err := wr.Write(rec); err != nil {
		return err
	}

	return wr.Close()
}

// runSearch performs a DoGet/VectorSearch
func runSearch(ctx context.Context, c *client.SmartClient) error {
	// Simulate simple DoGet for now
	// For vector search, we'd typically pass a serialized query in the Ticket
	ticket := []byte(`{"query": "test", "k": 10}`)
	stream, err := c.DoGet(ctx, ticket)
	if err != nil {
		return err
	}

	// Drain stream
	r, err := flight.NewRecordReader(stream)
	if err != nil {
		return err
	}
	defer r.Release()

	for r.Next() {
		// Just consume
	}
	return r.Err()
}

// Latency tracking
type sumLatency struct {
	totalNs atomic.Int64
	count   atomic.Int64
	maxNs   atomic.Int64
}

func (l *sumLatency) Record(d time.Duration) {
	ns := d.Nanoseconds()
	l.totalNs.Add(ns)
	l.count.Add(1)

	// Simple spin-loop max update
	for {
		current := l.maxNs.Load()
		if ns <= current {
			break
		}
		if l.maxNs.CompareAndSwap(current, ns) {
			break
		}
	}
}

func printResults(d time.Duration, ops int64, errs int64, l *sumLatency) {
	seconds := d.Seconds()
	throughput := float64(ops) / seconds

	var avgLatency time.Duration
	if count := l.count.Load(); count > 0 {
		avgLatency = time.Duration(l.totalNs.Load() / count)
	}
	maxLatency := time.Duration(l.maxNs.Load())

	fmt.Println("\n--- Results ---")
	fmt.Printf("Elapsed:     %.2fs\n", seconds)
	fmt.Printf("Total Ops:   %d\n", ops)
	fmt.Printf("Errors:      %d\n", errs)
	fmt.Printf("Throughput:  %.2f ops/sec\n", throughput)
	fmt.Printf("Avg Latency: %v\n", avgLatency)
	fmt.Printf("Max Latency: %v\n", maxLatency)
}
