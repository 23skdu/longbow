package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/23skdu/longbow/client"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// StreamUploader represents a stream uploader value.
type StreamUploader struct {
	sc     *client.SmartClient
	stream flight.FlightService_DoPutClient
	writer *flight.Writer
	cancel context.CancelFunc
}

func newStreamUploader(sc *client.SmartClient, dataset string, schema *arrow.Schema) (*StreamUploader, error) {
	ctx, cancel := context.WithTimeout(sc.WithAuthContext(context.Background()), 3*time.Hour)
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

// Write runs the write operation.
func (u *StreamUploader) Write(record arrow.RecordBatch) error {
	return u.writer.Write(record)
}

// Close runs the close operation.
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
	ctx = sc.WithAuthContext(ctx)
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
			total += reader.RecordBatch().NumRows()
		}
		err = reader.Err()
		// reader does not have Release

		if total > 0 || time.Now().After(deadline) {
			return total, err
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func waitForIndexingComplete(ctx context.Context, sc *client.SmartClient, dataset string, timeout time.Duration) string {
	pollCtx, pollCancel := context.WithTimeout(sc.WithAuthContext(context.Background()), timeout)
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

func ingestFromFile(sc *client.SmartClient, fbinPath, dataset string, scale, dim *int) time.Time {
	f, err := os.Open(fbinPath) // #nosec G304 -- CLI operator flag
	if err != nil {
		log.Fatalf("Failed to open input file: %v", err)
	}
	defer f.Close()

	// Peek first 6 bytes to check for ARROW1
	magic := make([]byte, 6)
	_, _ = f.ReadAt(magic, 0)

	if string(magic) == "ARROW1" {
		log.Printf("[PUT] Ingesting from Arrow IPC file: %s\n", fbinPath)
		reader, err := ipc.NewFileReader(f)
		if err != nil {
			log.Fatalf("Failed to create IPC reader: %v", err)
		}

		numRecords := reader.NumRecords()
		totalUploaded := 0
		start := time.Now()

		var uploader *StreamUploader
		for i := 0; i < numRecords; i++ {
			record, err := reader.RecordBatch(i)
			if err != nil {
				log.Fatalf("Failed to read record %d: %v", i, err)
			}
			record.Retain()

			if uploader == nil {
				uploader, err = newStreamUploader(sc, dataset, record.Schema())
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
		*scale = totalUploaded
		return start
	}

	// Try as true .fbin (4B count, 4B dim, then floats)
	log.Printf("[PUT] Ingesting from true .fbin file: %s\n", fbinPath)
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

	totalUploaded := 0
	start := time.Now()

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
			uploader, err = newStreamUploader(sc, dataset, fbinSchema)
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
	*scale = totalUploaded
	return start
}

func ingestGeneratedCorpus(
	sc *client.SmartClient,
	dataset string,
	scale, dim int,
	dtype string,
	tqBits int,
	seed int64,
	outputArrow, outputFbin string,
) (time.Time, string) {
	chunkSize := 10000
	if scale < chunkSize {
		chunkSize = scale
	}
	numChunks := (scale + chunkSize - 1) / chunkSize
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

	sem := make(chan struct{}, numWorkers*2)
	chunkChans := make([]chan generatedChunkOutput, numChunks)
	for i := range chunkChans {
		chunkChans[i] = make(chan generatedChunkOutput, 1)
	}

	jobs := make(chan chunkJob, numChunks)
	for idx := 0; idx < numChunks; idx++ {
		offset := idx * chunkSize
		count := chunkSize
		if offset+count > scale {
			count = scale - offset
		}
		jobs <- chunkJob{index: idx, offset: offset, count: count}
	}
	close(jobs)

	for w := 0; w < numWorkers; w++ {
		// Item 31: the RNG stream is derived from the chunk index, not
		// from the worker that drew it.
		go func() {
			for job := range jobs {
				sem <- struct{}{}
				rng := rand.New(rand.NewSource(seed + int64(job.index)*10007)) // #nosec G404 -- non-cryptographic PRNG for benchmark
				rec, schema, err := generateRecordBatch(rng, job.offset, job.count, dim, dtype, tqBits)
				chunkChans[job.index] <- generatedChunkOutput{index: job.index, rec: rec, schema: schema, err: err}
			}
		}()
	}

	// Check for generation-only mode (saving to file) — must pre-generate
	if outputArrow != "" || outputFbin != "" {
		saveGeneratedCorpusToFiles(scale, dim, outputArrow, outputFbin, numChunks, chunkChans, sem)
		os.Exit(0)
	}

	// Item 31: hashed in upload order as the records go out, so the run
	// records which corpus it built rather than only which seed it asked for.
	corpusHash := sha256.New()

	log.Printf("[PUT] Uploading %d chunks (parallel streaming pipeline)...\n", numChunks)
	totalUploaded := 0
	start := time.Now()
	var uploader *StreamUploader

	for idx := 0; idx < numChunks; idx++ {
		out := <-chunkChans[idx]
		<-sem
		if out.err != nil {
			log.Fatalf("Record generation failed at chunk %d: %v", idx, out.err)
		}
		rec := out.rec

		if uploader == nil {
			var err error
			uploader, err = newStreamUploader(sc, dataset, out.schema)
			if err != nil {
				rec.Release()
				log.Fatalf("Failed to init uploader for generated records: %v", err)
			}
		}

		if err := hashRecord(corpusHash, rec); err != nil {
			rec.Release()
			log.Fatalf("Failed to hash chunk %d: %v", idx, err)
		}
		if err := uploader.Write(rec); err != nil {
			rec.Release()
			log.Fatalf("DoPut write failed at chunk %d: %v", idx, err)
		}
		totalUploaded += int(rec.NumRows())
		rec.Release()

		if totalUploaded%50000 == 0 || totalUploaded == scale {
			log.Printf("  Progress: %d/%d vectors uploaded\n", totalUploaded, scale)
		}
	}
	if uploader != nil {
		if err := uploader.Close(); err != nil {
			log.Fatalf("Failed to close uploader: %v", err)
		}
	}
	return start, hex.EncodeToString(corpusHash.Sum(nil))
}

func computeBytesPerElement(dtype string) int64 {
	switch dtype {
	case "int8", "uint8":
		return 1
	case "int16", "uint16", "float16":
		return 2
	case "int32", "uint32", "float32":
		return 4
	case "int64", "uint64", "float64", "complex64":
		return 8
	case "complex128":
		return 16
	case "turboquant":
		return 1
	default:
		return 4
	}
}

func dropDataset(ctx context.Context, sc *client.SmartClient, dataset string) error {
	ctx = sc.WithAuthContext(ctx)
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
	ctx = sc.WithAuthContext(ctx)
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
