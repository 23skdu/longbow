package main

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
			defer func() { _ = c.Close() }()

			endTime := start.Add(*duration)

			for time.Now().Before(endTime) {
				t0 := time.Now()
				var err error

				switch *mode {
				case "ingest":
					err = runIngest(ctx, c)
				case "search":
					err = runSearch(ctx, c)
				default:
					log.Printf("Unknown mode: %s", *mode)
					return
				}

				dur := time.Since(t0)
				latency.Record(dur)

				if err != nil {
					if errors.Load() == 0 {
						log.Printf("First Error: %v", err)
					}
					errors.Add(1)
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
	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{"bench_collection"},
	}
	stream, err := c.DoPut(ctx, desc)
	if err != nil {
		return err
	}

	wr := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	wr.SetFlightDescriptor(desc)
	defer func() { _ = wr.Close() }()

	if err := wr.Write(rec); err != nil {
		return err
	}

	return wr.Close()
}

// runSearch performs a VectorSearch DoAction
func runSearch(ctx context.Context, c *client.SmartClient) error {
	// Generate random query vector
	queryVec := make([]float32, *dim)
	for i := 0; i < *dim; i++ {
		queryVec[i] = rand.Float32()
	}

	req := map[string]any{
		"dataset": "bench_collection",
		"vector":  queryVec,
		"k":       10,
	}
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}

	action := &flight.Action{
		Type: "VectorSearch",
		Body: body,
	}

	stream, err := c.DoAction(ctx, action)
	if err != nil {
		return err
	}

	// Drain results
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
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

func printResults(d time.Duration, ops, errs int64, l *sumLatency) {
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
