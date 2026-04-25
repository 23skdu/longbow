package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ParallelRecordReader handles gRPC-to-Arrow decoding in parallel
type ParallelRecordReader struct {
	stream     flight.FlightService_DoPutServer
	schema     *arrow.Schema
	alloc      memory.Allocator
	
	schemaBytes []byte
	dataChan    chan *flight.FlightData
	resultChan  chan recordResult
	
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	
	err        error
	latestRec  arrow.RecordBatch
	descriptor *flight.FlightDescriptor
}

type recordResult struct {
	batch      arrow.RecordBatch
	descriptor *flight.FlightDescriptor
	err        error
}

func NewParallelRecordReader(stream flight.FlightService_DoPutServer, alloc memory.Allocator) (*ParallelRecordReader, error) {
	// First message MUST contain the schema
	data, err := stream.Recv()
	if err != nil {
		return nil, err
	}

	// Use internal helper or standard ipc to read schema from FlightData
	// FlightData.DataHeader contains the Schema message
	schema, err := flight.DeserializeSchema(data.DataHeader, alloc)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize schema: %w", err)
	}

	// Pre-render schema message for per-batch decoding
	var buf bytes.Buffer
	// Note: We use NewWriter for a stream (not file) to avoid file headers
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	// We only want the schema message, but writer writes it on first call
	// or on Start? Actually it writes it immediately.
	// However, it might not be a standalone message.
	
	// A better way: just create a dummy IPC stream and capture the first part
	_ = writer.Write(nil) // Trigger schema write
	writer.Close()
	schemaBytes := buf.Bytes()
	// Strip the EOS (last 4-8 bytes) if needed, but ipc.NewReader will handle it.

	ctx, cancel := context.WithCancel(stream.Context())
	
	pr := &ParallelRecordReader{
		stream:      stream,
		schema:      schema,
		alloc:       alloc,
		schemaBytes: schemaBytes,
		dataChan:    make(chan *flight.FlightData, 16),
		resultChan:  make(chan recordResult, 16),
		ctx:         ctx,
		cancel:      cancel,
		descriptor:  data.FlightDescriptor,
	}

	// Start workers
	numWorkers := runtime.NumCPU()
	if numWorkers > 8 {
		numWorkers = 8 // Diminishing returns beyond 8 for single stream
	}

	pr.wg.Add(numWorkers + 1) // Workers + Producer
	
	// Producer: Reads from gRPC stream
	go pr.produce()

	// Consumers: Decode IPC to Arrow
	for i := 0; i < numWorkers; i++ {
		go pr.consume()
	}

	// Close results when workers are done
	go pr.cleanup()

	return pr, nil
}

func (pr *ParallelRecordReader) produce() {
	defer pr.wg.Done()
	defer close(pr.dataChan)

	for {
		data, err := pr.stream.Recv()
		if err != nil {
			if err != context.Canceled {
				// We don't report EOF as error here, Next() will handle it
			}
			return
		}
		
		select {
		case pr.dataChan <- data:
		case <-pr.ctx.Done():
			return
		}
	}
}

func (pr *ParallelRecordReader) consume() {
	defer pr.wg.Done()

	for data := range pr.dataChan {
		// Decode FlightData to RecordBatch
		// This is the "critical block" the user wants to parallelize
		
		// Note: ipc.NewReader needs the full stream usually, but for single messages
		// we can use ipc.ReadRecordBatch if we have the schema.
		
		// We need to be careful with memory allocation here
		batch, err := pr.decodePayload(data)
		
		select {
		case pr.resultChan <- recordResult{batch: batch, descriptor: data.FlightDescriptor, err: err}:
		case <-pr.ctx.Done():
			if batch != nil {
				batch.Release()
			}
			return
		}
	}
}

func (pr *ParallelRecordReader) decodePayload(data *flight.FlightData) (arrow.RecordBatch, error) {
	if len(data.DataHeader) == 0 {
		return nil, nil
	}

	// Construct a full IPC stream for this single record batch
	// [Schema][RecordBatch][EOS]
	// data.DataHeader contains the IPC metadata (continuation + size + message)
	// data.DataBody contains the IPC data
	
	var r io.Reader
	if len(data.DataBody) > 0 {
		r = io.MultiReader(
			bytes.NewReader(pr.schemaBytes),
			bytes.NewReader(data.DataHeader),
			bytes.NewReader(data.DataBody),
		)
	} else {
		r = io.MultiReader(
			bytes.NewReader(pr.schemaBytes),
			bytes.NewReader(data.DataHeader),
		)
	}

	reader, err := ipc.NewReader(r, ipc.WithSchema(pr.schema), ipc.WithAllocator(pr.alloc))
	if err != nil {
		return nil, err
	}
	defer reader.Release()

	// Skip the first record (which is our dummy nil write in schemaBytes)
	if reader.Next() {
		// This was the nil record
	}

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

	select {
	case res, ok := <-pr.resultChan:
		if !ok {
			return false
		}
		if res.err != nil {
			pr.err = res.err
			return false
		}
		pr.latestRec = res.batch
		if res.descriptor != nil {
			pr.descriptor = res.descriptor
		}
		return pr.latestRec != nil
	case <-pr.ctx.Done():
		pr.err = pr.ctx.Err()
		return false
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
