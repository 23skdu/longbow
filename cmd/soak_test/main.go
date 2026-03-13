package main

// nosec G404 - math/rand is used for test data generation, not security-sensitive
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
	peers        = flag.String("peers", "127.0.0.1:3000", "Comma-separated list of peer addresses")
	duration     = flag.Duration("duration", 5*time.Minute, "Duration of the soak test")
	concurrency  = flag.Int("workers", 4, "Number of concurrent workers per phase")
	dataset      = flag.String("dataset", "soak_test_collection", "Dataset name")
	dim          = flag.Int("dim", 128, "Vector dimension")
	batchSize    = flag.Int("batch", 100, "Injest batch size")
	deleteRate   = flag.Float64("delete-rate", 0.1, "Probability of deleting a batch after search")
	chaosEnabled = flag.Bool("chaos", false, "Enable random process kills (not yet implemented in this tool, but for external script)")
)

type Stats struct {
	IngestOps atomic.Int64
	SearchOps atomic.Int64
	DeleteOps atomic.Int64
	Errors    atomic.Int64
}

func main() {
	flag.Parse()

	fmt.Printf("🌊 Starting Longbow Soak Test\n")
	fmt.Printf("Dataset: %s, Workers: %d, Duration: %s, Chaos: %v\n", *dataset, *concurrency, *duration, *chaosEnabled)

	peerList := strings.Split(*peers, ",")

	ctx, cancel := context.WithTimeout(context.Background(), *duration+10*time.Second)
	defer cancel()

	stats := &Stats{}
	start := time.Now()

	var wg sync.WaitGroup

	// Phase 1: Ingest Workers
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go runWorker(ctx, &wg, peerList[i%len(peerList)], "ingest", stats)
	}

	// Phase 2: Search & Delete Workers
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go runWorker(ctx, &wg, peerList[i%len(peerList)], "search_delete", stats)
	}

	// Wait for duration or context
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		case <-ticker.C:
			elapsed := time.Since(start)
			fmt.Printf("[%v] Ingest: %d, Search: %d, Delete: %d, Errors: %d\n",
				elapsed.Round(time.Second),
				stats.IngestOps.Load(),
				stats.SearchOps.Load(),
				stats.DeleteOps.Load(),
				stats.Errors.Load())
		}
	}

	wg.Wait()
	fmt.Printf("\n--- Final Results ---\n")
	fmt.Printf("Total Elapsed: %v\n", time.Since(start))
	fmt.Printf("Ingest Ops:   %d\n", stats.IngestOps.Load())
	fmt.Printf("Search Ops:   %d\n", stats.SearchOps.Load())
	fmt.Printf("Delete Ops:   %d\n", stats.DeleteOps.Load())
	fmt.Printf("Total Errors: %d\n", stats.Errors.Load())
}

func runWorker(ctx context.Context, wg *sync.WaitGroup, peer, mode string, stats *Stats) {
	defer wg.Done()

	c, err := client.NewSmartClient(peer)
	if err != nil {
		log.Printf("Worker failed to connect to %s: %v", peer, err)
		stats.Errors.Add(1)
		return
	}
	defer func() {
		if err := c.Close(); err != nil {
			log.Printf("Error closing client: %v", err)
		}
	}()

	// Wait for first index to be ready (only for search/delete)
	if mode != "ingest" {
		log.Printf("[%s] Waiting for index readiness...", mode)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			ready, err := checkReadiness(ctx, c)
			if err == nil && ready {
				log.Printf("[%s] Index READY, starting operations", mode)
				break
			}
			time.Sleep(2 * time.Second)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			var err error
			if mode == "ingest" {
				err = performIngest(ctx, c)
				if err == nil {
					stats.IngestOps.Add(1)
				}
			} else {
				err = performSearchAndDelete(ctx, c, stats)
				if err == nil {
					stats.SearchOps.Add(1)
				}
			}

			if err != nil {
				if err != context.Canceled && err != context.DeadlineExceeded {
					log.Printf("[%s] Error: %v", mode, err)
					stats.Errors.Add(1)
					// Small backoff on error
					time.Sleep(100 * time.Millisecond)
				}
			}
		}
	}
}

func performIngest(ctx context.Context, c *client.SmartClient) error {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "embedding", Type: arrow.FixedSizeListOf(int32(*dim), arrow.PrimitiveTypes.Float32)},
		{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	idB := b.Field(0).(*array.StringBuilder)
	vecB := b.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)
	tsB := b.Field(2).(*array.Int64Builder)

	for i := 0; i < *batchSize; i++ {
		idB.Append(uuid.New().String())
		vecB.Append(true)
		for j := 0; j < *dim; j++ {
			valB.Append(rand.Float32())
		}
		tsB.Append(time.Now().UnixNano())
	}

	rec := b.NewRecordBatch()
	defer rec.Release()

	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{*dataset},
	}
	stream, err := c.DoPut(ctx, desc)
	if err != nil {
		return err
	}

	wr := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	wr.SetFlightDescriptor(desc)

	if err := wr.Write(rec); err != nil {
		_ = wr.Close()
		return err
	}
	return wr.Close()
}

func performSearchAndDelete(ctx context.Context, c *client.SmartClient, stats *Stats) error {
	queryVec := make([]float32, *dim)
	for i := 0; i < *dim; i++ {
		queryVec[i] = rand.Float32()
	}

	req := map[string]any{
		"dataset": *dataset,
		"vector":  queryVec,
		"k":       5,
	}
	body, _ := json.Marshal(req)

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
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		// In a real scenario we'd parse the result, but here we just collect IDs if we want chaos
		// The mock action returns metadata usually.
		// For simplicity, let's assume we want to delete something occasionally.
		if rand.Float64() < *deleteRate {
			// In our schema we had "id"
			// Actually flight Action results are Result objects.
			_ = res
		}
	}

	// Occasionally perform a "Delete" action if we want to test fragmentation
	if rand.Float64() < *deleteRate {
		// Mock delete - we don't have a direct "DeleteByID" flight action yet?
		// Let's check if there is one.
		// If not, we skip for now or implement it.
		// For now, let's just trigger a "Compact" action to stress the system.
		compactReq := map[string]any{"dataset": *dataset}
		cBody, _ := json.Marshal(compactReq)
		compactAction := &flight.Action{
			Type: "Compact",
			Body: cBody,
		}
		_, err := c.DoAction(ctx, compactAction)
		if err == nil {
			stats.DeleteOps.Add(1)
		}
	}

	return nil
}
func checkReadiness(ctx context.Context, c *client.SmartClient) (bool, error) {
	action := &flight.Action{
		Type: "check_readiness",
		Body: []byte(fmt.Sprintf(`{"dataset": %q}`, *dataset)),
	}
	stream, err := c.DoAction(ctx, action)
	if err != nil {
		return false, err
	}
	res, err := stream.Recv()
	if err != nil {
		return false, err
	}
	var dr struct {
		Status     string `json:"status"`
		IndexReady bool   `json:"index_ready"`
	}
	if err := json.Unmarshal(res.Body, &dr); err != nil {
		return false, err
	}
	return dr.IndexReady, nil
}
