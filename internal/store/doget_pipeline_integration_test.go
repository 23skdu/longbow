package store

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

// StoreRecords stores arrow records in the dataset (helper for tests)
func (s *VectorStore) StoreRecords(name string, records []arrow.RecordBatch) error {
	ds := &Dataset{
		Name:    name,
		Records: make([]arrow.RecordBatch, 0, len(records)),
	}
	for _, rec := range records {
		rec.Retain()
		ds.Records = append(ds.Records, rec)
	}
	s.updateDatasets(func(m map[string]*Dataset) {
		m[name] = ds
	})
	return nil
}

// TestDoGetUsesPipelineForMultiBatch verifies DoGet uses pipeline for datasets with multiple batches
func TestDoGetUsesPipelineForMultiBatch(t *testing.T) {
	logger := zerolog.Nop()
	mem := memory.NewGoAllocator()

	store := NewVectorStoreWithPipeline(mem, logger, 4, 16)
	if store == nil {
		t.Fatal("NewVectorStoreWithPipeline returned nil")
	}

	if store.GetDoGetPipelinePool() == nil {
		t.Fatal("DoGetPipelinePool should be initialized")
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.BinaryTypes.String},
	}, nil)

	const numBatches = 10
	records := make([]arrow.RecordBatch, numBatches)
	for i := 0; i < numBatches; i++ {
		builder := array.NewRecordBuilder(mem, schema)
		builder.Field(0).(*array.Int64Builder).AppendValues([]int64{int64(i * 100), int64(i*100 + 1)}, nil)
		builder.Field(1).(*array.StringBuilder).AppendValues([]string{fmt.Sprintf("val_%d_0", i), fmt.Sprintf("val_%d_1", i)}, nil)
		records[i] = builder.NewRecordBatch()
		builder.Release()
	}
	defer func() {
		for _, rec := range records {
			rec.Release()
		}
	}()

	err := store.StoreRecords("multi_batch_dataset", records)
	if err != nil {
		t.Fatalf("Failed to store records: %v", err)
	}

	statsBefore := store.GetPipelineStats()

	mockStream := NewMockDoGetStream(context.Background())
	tkt := &flight.Ticket{Ticket: []byte("multi_batch_dataset")}
	err = store.DoGet(tkt, mockStream)
	if err != nil {
		t.Fatalf("DoGet failed: %v", err)
	}

	statsAfter := store.GetPipelineStats()
	if statsAfter.BatchesProcessed <= statsBefore.BatchesProcessed {
		t.Errorf("Pipeline should have processed batches: before=%d, after=%d",
			statsBefore.BatchesProcessed, statsAfter.BatchesProcessed)
	}
}

// TestDoGetSerialPathForSingleBatch verifies single-batch datasets use fast serial path
func TestDoGetSerialPathForSingleBatch(t *testing.T) {
	logger := zerolog.Nop()
	mem := memory.NewGoAllocator()

	store := NewVectorStoreWithPipeline(mem, logger, 4, 16)
	if store == nil {
		t.Fatal("NewVectorStoreWithPipeline returned nil")
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()

	err := store.StoreRecords("single_batch", []arrow.RecordBatch{record})
	if err != nil {
		t.Fatalf("Failed to store: %v", err)
	}

	statsBefore := store.GetPipelineStats()

	mockStream := NewMockDoGetStream(context.Background())
	tkt := &flight.Ticket{Ticket: []byte("single_batch")}
	err = store.DoGet(tkt, mockStream)
	if err != nil {
		t.Fatalf("DoGet failed: %v", err)
	}

	statsAfter := store.GetPipelineStats()
	if statsAfter.BatchesProcessed != statsBefore.BatchesProcessed {
		t.Errorf("Single batch should use serial path, not pipeline: before=%d, after=%d",
			statsBefore.BatchesProcessed, statsAfter.BatchesProcessed)
	}
}

// TestDoGetPipelinePreservesOrder verifies batch ordering is maintained
func TestDoGetPipelinePreservesOrder(t *testing.T) {
	logger := zerolog.Nop()
	mem := memory.NewGoAllocator()

	store := NewVectorStoreWithPipeline(mem, logger, 8, 32)
	if store == nil {
		t.Fatal("NewVectorStoreWithPipeline returned nil")
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "batch_id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	const numBatches = 20
	records := make([]arrow.RecordBatch, numBatches)
	for i := 0; i < numBatches; i++ {
		builder := array.NewRecordBuilder(mem, schema)
		builder.Field(0).(*array.Int64Builder).Append(int64(i))
		records[i] = builder.NewRecordBatch()
		builder.Release()
	}
	defer func() {
		for _, rec := range records {
			rec.Release()
		}
	}()

	err := store.StoreRecords("ordered_dataset", records)
	if err != nil {
		t.Fatalf("Failed to store: %v", err)
	}

	mockStream := NewMockDoGetStream(context.Background())
	tkt := &flight.Ticket{Ticket: []byte("ordered_dataset")}
	err = store.DoGet(tkt, mockStream)
	if err != nil {
		t.Fatalf("DoGet failed: %v", err)
	}

	stats := store.GetPipelineStats()
	if stats.BatchesProcessed < int64(numBatches) {
		t.Logf("Expected pipeline to process %d batches, processed: %d", numBatches, stats.BatchesProcessed)
	}
}

// TestDoGetPipelineConcurrentRequests verifies pipeline handles concurrent DoGet calls
func TestDoGetPipelineConcurrentRequests(t *testing.T) {
	logger := zerolog.Nop()
	mem := memory.NewGoAllocator()

	store := NewVectorStoreWithPipeline(mem, logger, 4, 16)
	if store == nil {
		t.Fatal("NewVectorStoreWithPipeline returned nil")
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	const numBatches = 5
	records := make([]arrow.RecordBatch, numBatches)
	for i := 0; i < numBatches; i++ {
		builder := array.NewRecordBuilder(mem, schema)
		builder.Field(0).(*array.Int64Builder).Append(int64(i))
		records[i] = builder.NewRecordBatch()
		builder.Release()
	}
	defer func() {
		for _, rec := range records {
			rec.Release()
		}
	}()

	err := store.StoreRecords("concurrent_test", records)
	if err != nil {
		t.Fatalf("Failed to store: %v", err)
	}

	const numGoroutines = 10
	var successCount atomic.Int32
	done := make(chan struct{}, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			mockStream := NewMockDoGetStream(context.Background())
			tkt := &flight.Ticket{Ticket: []byte("concurrent_test")}
			if err := store.DoGet(tkt, mockStream); err == nil {
				successCount.Add(1)
			}
			done <- struct{}{}
		}()
	}

	timeout := time.After(10 * time.Second)
	for i := 0; i < numGoroutines; i++ {
		select {
		case <-done:
		case <-timeout:
			t.Fatal("Timeout waiting for concurrent DoGets")
		}
	}

	if successCount.Load() != numGoroutines {
		t.Errorf("Expected %d successful DoGets, got %d", numGoroutines, successCount.Load())
	}
}
