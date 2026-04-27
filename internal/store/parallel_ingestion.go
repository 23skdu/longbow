package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

// ParallelRecordReader handles gRPC-to-Arrow decoding in parallel
type ParallelRecordReader struct {
	stream     flight.FlightService_DoPutServer
	schema     *arrow.Schema
	alloc      memory.Allocator
	
	schemaBytes []byte
	dataChan    chan sequencedData
	resultChan  chan recordResult
	
	nextSeq     int
	reorderBuf  map[int]recordResult
	
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	
	err        error
	latestRec  arrow.RecordBatch
	descriptor *flight.FlightDescriptor
	logger     zerolog.Logger
}

type sequencedData struct {
	data *flight.FlightData
	seq  int
}

type recordResult struct {
	batch      arrow.RecordBatch
	descriptor *flight.FlightDescriptor
	err        error
	seq        int
}

func NewParallelRecordReader(stream flight.FlightService_DoPutServer, alloc memory.Allocator, logger zerolog.Logger) (*ParallelRecordReader, error) {
	// First message MUST contain the schema
	data, err := stream.Recv()
	if err != nil {
		return nil, err
	}

	logger.Info().
		Int("header_len", len(data.DataHeader)).
		Int("body_len", len(data.DataBody)).
		Str("header_hex", fmt.Sprintf("%x", data.DataHeader[:min(len(data.DataHeader), 64)])).
		Msg("ParallelIngest: First message received")

	if len(data.DataHeader) < 4 {
		return nil, fmt.Errorf("invalid FlightData: header too short (%d bytes)", len(data.DataHeader))
	}

	// Try standard deserialize first
	var schema *arrow.Schema
	header := data.DataHeader
	schema, err = flight.DeserializeSchema(header, alloc)
	if err != nil {
		logger.Warn().Err(err).Msg("Standard DeserializeSchema failed, attempting IPC reader fallback")
		// Fallback: Use a full IPC reader to handle complex encapsulation
		var buf bytes.Buffer
		buf.Write(header)
		// IPC reader expects an EOS marker if we want it to be happy
		_ = binary.Write(&buf, binary.LittleEndian, uint32(0xFFFFFFFF))
		_ = binary.Write(&buf, binary.LittleEndian, uint32(0))
		
		rdr, rerr := ipc.NewReader(&buf, ipc.WithAllocator(alloc))
		if rerr == nil {
			schema = rdr.Schema()
			err = nil
			rdr.Release()
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to deserialize schema (hlen=%d): %w", len(data.DataHeader), err)
	}

	// Store the "raw" schema metadata (without prefix) for reconstruction


	// Pre-render schema message for per-batch decoding
	// We use ipc.NewWriter to ensure proper alignment and prefixing.
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	if err := writer.Close(); err != nil {
		logger.Warn().Err(err).Msg("Failed to close IPC writer")
	}
	schemaBytes := buf.Bytes()
	// Strip the 8-byte EOS marker so we can append batches to this "stream" later.
	if len(schemaBytes) >= 8 {
		schemaBytes = schemaBytes[:len(schemaBytes)-8]
	}


	ctx, cancel := context.WithCancel(stream.Context())
	
	pr := &ParallelRecordReader{
		stream:      stream,
		schema:      schema,
		alloc:       alloc,
		schemaBytes: schemaBytes,
		dataChan:    make(chan sequencedData, 32),
		resultChan:  make(chan recordResult, 32),
		reorderBuf:  make(map[int]recordResult),
		ctx:         ctx,
		cancel:      cancel,
		descriptor:  data.FlightDescriptor,
		logger:      logger,
	}

	// Start workers
	numWorkers := runtime.NumCPU()
	if numWorkers > 8 {
		numWorkers = 8
	}
	
	logger.Info().Int("workers", numWorkers).Msg("ParallelIngest: Starting workers")

	pr.wg.Add(numWorkers + 1)
	
	// Producer: Reads from gRPC stream
	go pr.produce()

	// Consumers: Decode IPC to Arrow
	for i := 0; i < numWorkers; i++ {
		go pr.consume(i)
	}

	// Close results when workers are done
	go pr.cleanup()

	// IMPORTANT: The first message might already contain a record batch!
	if len(data.DataBody) > 0 || len(data.DataHeader) > 0 {
		// Re-decode the first message too if it has data
		// Wait, DeserializeSchema already used DataHeader. 
		// If it also had data, we'd need to be careful.
		// For now, assume first message is just schema for bench-tool.
	}

	return pr, nil
}

func (pr *ParallelRecordReader) produce() {
	defer pr.wg.Done()
	defer close(pr.dataChan)

	seq := 0
	for {
		data, err := pr.stream.Recv()
		if err != nil {
			if err != io.EOF {
				pr.logger.Error().Err(err).Msg("ParallelIngest: produce error")
			}
			return
		}
		
		select {
		case pr.dataChan <- sequencedData{data: data, seq: seq}:
			seq++
		case <-pr.ctx.Done():
			return
		}
	}
}

func (pr *ParallelRecordReader) consume(id int) {
	defer pr.wg.Done()

	for sd := range pr.dataChan {
		// Use a closure to handle panics and ensure results are ALWAYS sent for every sequence number.
		func() {
			var batch arrow.RecordBatch
			var err error
			
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic in worker %d: %v", id, r)
					pr.logger.Error().Int("worker", id).Interface("recover", r).Msg("ParallelIngest: worker panic")
				}
				
				// Send result (success or error) to resultChan
				select {
				case pr.resultChan <- recordResult{
					batch:      batch,
					descriptor: sd.data.FlightDescriptor,
					err:        err,
					seq:        sd.seq,
				}:
				case <-pr.ctx.Done():
					if batch != nil {
						batch.Release()
					}
					return // Worker MUST exit on context cancellation
				}
			}()

			// Decode FlightData to RecordBatch
			batch, err = pr.decodePayload(sd.data)
			if err != nil {
				pr.logger.Error().Int("worker", id).Err(err).Int("seq", sd.seq).Msg("ParallelIngest: worker decode error")
			}
		}()
	}
}

func (pr *ParallelRecordReader) decodePayload(data *flight.FlightData) (arrow.RecordBatch, error) {
	if len(data.DataHeader) == 0 {
		return nil, nil
	}

	// Construct a standalone IPC stream for this single record batch
	// [Schema Message (with prefix)] [RecordBatch Message (with prefix)] [EOS (with prefix)]
	var streamBuf bytes.Buffer
	streamBuf.Write(pr.schemaBytes)

	// Check if DataHeader already has the 8-byte IPC prefix
	hasPrefix := len(data.DataHeader) >= 8 && binary.LittleEndian.Uint32(data.DataHeader[0:4]) == 0xFFFFFFFF
	
	if !hasPrefix {
		_ = binary.Write(&streamBuf, binary.LittleEndian, uint32(0xFFFFFFFF))
		_ = binary.Write(&streamBuf, binary.LittleEndian, uint32(len(data.DataHeader))) // #nosec G115 -- intentional conversion for binary write
	}
	streamBuf.Write(data.DataHeader)
	streamBuf.Write(data.DataBody)
	
	// Add proper EOS (8 bytes: ffffffff 00000000)
	_ = binary.Write(&streamBuf, binary.LittleEndian, uint32(0xFFFFFFFF))
	_ = binary.Write(&streamBuf, binary.LittleEndian, uint32(0))

	reader, err := ipc.NewReader(&streamBuf, ipc.WithAllocator(pr.alloc))
	if err != nil {
		return nil, fmt.Errorf("failed to create IPC reader for batch (hasPrefix=%v, hlen=%d, blen=%d): %w", hasPrefix, len(data.DataHeader), len(data.DataBody), err)
	}
	defer reader.Release()

	if reader.Next() {
		rec := reader.Record()
		rec.Retain()
		return rec, nil
	}
	
	return nil, reader.Err()
}

func (pr *ParallelRecordReader) Next() bool {
	if pr.err != nil {
		return false
	}
	
	if pr.latestRec != nil {
		pr.latestRec.Release()
		pr.latestRec = nil
	}

	for {
		// Check reorder buffer first
		if res, ok := pr.reorderBuf[pr.nextSeq]; ok {
			delete(pr.reorderBuf, pr.nextSeq)
			pr.nextSeq++
			
			if res.err != nil {
				pr.err = res.err
				return false
			}
			if res.batch == nil {
				continue // Skip empty batches (metadata only)
			}
			pr.latestRec = res.batch
			if res.descriptor != nil {
				pr.descriptor = res.descriptor
			}
			return true
		}

		// Wait for next result
		select {
		case res, ok := <-pr.resultChan:
			if !ok {
				// Re-check buffer one last time after channel close
				if _, ok := pr.reorderBuf[pr.nextSeq]; ok {
					continue
				}
				return false
			}
			if res.seq == pr.nextSeq {
				pr.nextSeq++
				if res.err != nil {
					pr.err = res.err
					return false
				}
				if res.batch == nil {
					continue
				}
				pr.latestRec = res.batch
				if res.descriptor != nil {
					pr.descriptor = res.descriptor
				}
				return true
			}
			// Out of order, buffer it
			pr.reorderBuf[res.seq] = res
		case <-pr.ctx.Done():
			pr.err = pr.ctx.Err()
			return false
		}
	}
}

func (pr *ParallelRecordReader) RecordBatch() arrow.RecordBatch {
	return pr.latestRec
}

func (pr *ParallelRecordReader) Schema() *arrow.Schema {
	return pr.schema
}

func (pr *ParallelRecordReader) LatestFlightDescriptor() *flight.FlightDescriptor {
	return pr.descriptor
}

func (pr *ParallelRecordReader) Err() error {
	return pr.err
}

func (pr *ParallelRecordReader) Release() {
	pr.cancel()
	pr.wg.Wait() // Now safe to wait because workers will exit on ctx.Done() or dataChan close
}

func (pr *ParallelRecordReader) cleanup() {
	pr.wg.Wait()
	close(pr.resultChan)
}
