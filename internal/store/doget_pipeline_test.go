
package store

import (
"context"
"errors"
"sync"
"sync/atomic"
"testing"
"time"

"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/array"
"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestDoGetPipelineCreation tests pipeline initialization
func TestDoGetPipelineCreation(t *testing.T) {
tests := []struct {
name        string
workers     int
bufferSize  int
wantWorkers int
}{
{"default workers", 0, 0, 4},        // 0 means use default
{"explicit workers", 8, 16, 8},
{"minimum workers", 1, 2, 1},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
pipeline := NewDoGetPipeline(tt.workers, tt.bufferSize)
if pipeline == nil {
t.Fatal("NewDoGetPipeline returned nil")
}
if pipeline.NumWorkers() != tt.wantWorkers {
t.Errorf("NumWorkers() = %d, want %d", pipeline.NumWorkers(), tt.wantWorkers)
}
pipeline.Stop()
})
}
}

// TestDoGetPipelineInOrderProcessing verifies batches are returned in order
func TestDoGetPipelineInOrderProcessing(t *testing.T) {
pipeline := NewDoGetPipeline(4, 16)
defer pipeline.Stop()

ctx := context.Background()
mem := memory.NewGoAllocator()

// Create test batches with sequence numbers
numBatches := 20
batches := make([]arrow.RecordBatch, numBatches)
schema := arrow.NewSchema([]arrow.Field{
{Name: "seq", Type: arrow.PrimitiveTypes.Int64},
}, nil)

for i := 0; i < numBatches; i++ {
builder := array.NewInt64Builder(mem)
builder.Append(int64(i))
arr := builder.NewArray()
batches[i] = array.NewRecordBatch(schema, []arrow.Array{arr}, 1)
builder.Release()
}
defer func() {
for _, b := range batches {
b.Release()
}
}()

// Submit all batches
resultCh, errCh := pipeline.Process(ctx, batches, func(ctx context.Context, rec arrow.RecordBatch) (arrow.RecordBatch, error) {
// Simulate variable processing time
time.Sleep(time.Duration(rec.Column(0).(*array.Int64).Value(0)%5) * time.Millisecond)
rec.Retain()
return rec, nil
})

// Collect results and verify order
results := make([]int64, 0, numBatches)
for result := range resultCh {
val := result.Record.Column(0).(*array.Int64).Value(0)
results = append(results, val)
result.Record.Release()
}

// Check for errors
select {
case err := <-errCh:
if err != nil {
t.Fatalf("unexpected error: %v", err)
}
default:
}

if len(results) != numBatches {
t.Fatalf("got %d results, want %d", len(results), numBatches)
}

for i, seq := range results {
if seq != int64(i) {
t.Errorf("result[%d] = %d, want %d (out of order)", i, seq, i)
}
}
}

// TestDoGetPipelineErrorPropagation tests error handling
func TestDoGetPipelineErrorPropagation(t *testing.T) {
pipeline := NewDoGetPipeline(4, 16)
defer pipeline.Stop()

ctx := context.Background()
mem := memory.NewGoAllocator()

schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
}, nil)

batches := make([]arrow.RecordBatch, 10)
for i := 0; i < 10; i++ {
builder := array.NewInt64Builder(mem)
builder.Append(int64(i))
arr := builder.NewArray()
batches[i] = array.NewRecordBatch(schema, []arrow.Array{arr}, 1)
builder.Release()
}
defer func() {
for _, b := range batches {
b.Release()
}
}()

expectedErr := errors.New("filter failed at batch 5")

resultCh, errCh := pipeline.Process(ctx, batches, func(ctx context.Context, rec arrow.RecordBatch) (arrow.RecordBatch, error) {
val := rec.Column(0).(*array.Int64).Value(0)
if val == 5 {
return nil, expectedErr
}
rec.Retain()
return rec, nil
})

// Drain results
for result := range resultCh {
result.Record.Release()
}

// Check error was propagated
err := <-errCh
if err == nil {
t.Fatal("expected error but got nil")
}
if !errors.Is(err, expectedErr) {
t.Errorf("got error %v, want %v", err, expectedErr)
}
}

// TestDoGetPipelineContextCancellation tests graceful shutdown on cancel
func TestDoGetPipelineContextCancellation(t *testing.T) {
pipeline := NewDoGetPipeline(4, 16)
defer pipeline.Stop()

ctx, cancel := context.WithCancel(context.Background())
mem := memory.NewGoAllocator()

schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
}, nil)

batches := make([]arrow.RecordBatch, 100)
for i := 0; i < 100; i++ {
builder := array.NewInt64Builder(mem)
builder.Append(int64(i))
arr := builder.NewArray()
batches[i] = array.NewRecordBatch(schema, []arrow.Array{arr}, 1)
builder.Release()
}
defer func() {
for _, b := range batches {
b.Release()
}
}()

var processedCount atomic.Int64

resultCh, errCh := pipeline.Process(ctx, batches, func(ctx context.Context, rec arrow.RecordBatch) (arrow.RecordBatch, error) {
time.Sleep(10 * time.Millisecond)
processedCount.Add(1)
rec.Retain()
return rec, nil
})

// Cancel after processing a few
time.Sleep(50 * time.Millisecond)
cancel()

// Drain channels
for result := range resultCh {
result.Record.Release()
}
<-errCh

// Should have processed fewer than all batches
processed := processedCount.Load()
if processed >= 100 {
t.Errorf("expected early termination, but processed %d batches", processed)
}
}

// TestDoGetPipelineEmptyInput tests handling of empty batch slice
func TestDoGetPipelineEmptyInput(t *testing.T) {
pipeline := NewDoGetPipeline(4, 16)
defer pipeline.Stop()

ctx := context.Background()
var batches []arrow.RecordBatch

resultCh, errCh := pipeline.Process(ctx, batches, func(ctx context.Context, rec arrow.RecordBatch) (arrow.RecordBatch, error) {
return rec, nil
})

count := 0
for range resultCh {
count++
}

if count != 0 {
t.Errorf("expected 0 results for empty input, got %d", count)
}

if err := <-errCh; err != nil {
t.Errorf("unexpected error: %v", err)
}
}

// TestDoGetPipelineStats tests statistics tracking
func TestDoGetPipelineStats(t *testing.T) {
pipeline := NewDoGetPipeline(4, 16)
defer pipeline.Stop()

ctx := context.Background()
mem := memory.NewGoAllocator()

schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
}, nil)

numBatches := 10
batches := make([]arrow.RecordBatch, numBatches)
for i := 0; i < numBatches; i++ {
builder := array.NewInt64Builder(mem)
builder.Append(int64(i))
arr := builder.NewArray()
batches[i] = array.NewRecordBatch(schema, []arrow.Array{arr}, 1)
builder.Release()
}
defer func() {
for _, b := range batches {
b.Release()
}
}()

resultCh, errCh := pipeline.Process(ctx, batches, func(ctx context.Context, rec arrow.RecordBatch) (arrow.RecordBatch, error) {
rec.Retain()
return rec, nil
})

for result := range resultCh {
result.Record.Release()
}
<-errCh

stats := pipeline.Stats()
if stats.BatchesProcessed != int64(numBatches) {
t.Errorf("BatchesProcessed = %d, want %d", stats.BatchesProcessed, numBatches)
}
}

// TestDoGetPipelineConcurrentProcesses tests multiple concurrent Process calls
func TestDoGetPipelineConcurrentProcesses(t *testing.T) {
pipeline := NewDoGetPipeline(4, 16)
defer pipeline.Stop()

ctx := context.Background()
mem := memory.NewGoAllocator()

schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
}, nil)

createBatches := func(start, count int) []arrow.RecordBatch {
batches := make([]arrow.RecordBatch, count)
for i := 0; i < count; i++ {
builder := array.NewInt64Builder(mem)
builder.Append(int64(start + i))
arr := builder.NewArray()
batches[i] = array.NewRecordBatch(schema, []arrow.Array{arr}, 1)
builder.Release()
}
return batches
}

var wg sync.WaitGroup
numProcs := 5
batchesPerProc := 10

for p := 0; p < numProcs; p++ {
wg.Add(1)
go func(procID int) {
defer wg.Done()
batches := createBatches(procID*100, batchesPerProc)
defer func() {
for _, b := range batches {
b.Release()
}
}()

resultCh, errCh := pipeline.Process(ctx, batches, func(ctx context.Context, rec arrow.RecordBatch) (arrow.RecordBatch, error) {
rec.Retain()
return rec, nil
})

count := 0
for result := range resultCh {
result.Record.Release()
count++
}

if err := <-errCh; err != nil {
t.Errorf("proc %d: unexpected error: %v", procID, err)
}

if count != batchesPerProc {
t.Errorf("proc %d: got %d results, want %d", procID, count, batchesPerProc)
}
}(p)
}

wg.Wait()
}

// BenchmarkDoGetPipelineVsSerial compares pipelined vs serial processing
func BenchmarkDoGetPipelineVsSerial(b *testing.B) {
mem := memory.NewGoAllocator()
schema := arrow.NewSchema([]arrow.Field{
{Name: "data", Type: arrow.BinaryTypes.String},
}, nil)

numBatches := 50
batches := make([]arrow.RecordBatch, numBatches)
for i := 0; i < numBatches; i++ {
builder := array.NewStringBuilder(mem)
for j := 0; j < 1000; j++ {
builder.Append("test data string for processing")
}
arr := builder.NewArray()
batches[i] = array.NewRecordBatch(schema, []arrow.Array{arr}, 1000)
builder.Release()
}
defer func() {
for _, batch := range batches {
batch.Release()
}
}()

filterFn := func(ctx context.Context, rec arrow.RecordBatch) (arrow.RecordBatch, error) {
// Simulate filter work
time.Sleep(100 * time.Microsecond)
rec.Retain()
return rec, nil
}

writeFn := func(_ arrow.RecordBatch) {
// Simulate write work
time.Sleep(50 * time.Microsecond)
}

b.Run("Serial", func(b *testing.B) {
for i := 0; i < b.N; i++ {
ctx := context.Background()
for _, batch := range batches {
filtered, _ := filterFn(ctx, batch)
writeFn(filtered)
filtered.Release()
}
}
})

b.Run("Pipelined", func(b *testing.B) {
pipeline := NewDoGetPipeline(4, 16)
defer pipeline.Stop()

for i := 0; i < b.N; i++ {
ctx := context.Background()
resultCh, errCh := pipeline.Process(ctx, batches, filterFn)

for result := range resultCh {
writeFn(result.Record)
result.Record.Release()
}
<-errCh
}
})
}

// BenchmarkDoGetPipelineThroughput measures throughput
func BenchmarkDoGetPipelineThroughput(b *testing.B) {
pipeline := NewDoGetPipeline(4, 32)
defer pipeline.Stop()

mem := memory.NewGoAllocator()
schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
}, nil)

numBatches := 100
batches := make([]arrow.RecordBatch, numBatches)
for i := 0; i < numBatches; i++ {
builder := array.NewInt64Builder(mem)
for j := 0; j < 10000; j++ {
builder.Append(int64(j))
}
arr := builder.NewArray()
batches[i] = array.NewRecordBatch(schema, []arrow.Array{arr}, 10000)
builder.Release()
}
defer func() {
for _, batch := range batches {
batch.Release()
}
}()

b.ResetTimer()
for i := 0; i < b.N; i++ {
ctx := context.Background()
resultCh, errCh := pipeline.Process(ctx, batches, func(ctx context.Context, rec arrow.RecordBatch) (arrow.RecordBatch, error) {
rec.Retain()
return rec, nil
})

for result := range resultCh {
result.Record.Release()
}
<-errCh
}
b.ReportMetric(float64(numBatches*b.N)/b.Elapsed().Seconds(), "batches/sec")
}
