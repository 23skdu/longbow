package store

import (
"context"
"fmt"
"log/slog"
"os"
"testing"
"time"

"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/array"
"github.com/apache/arrow-go/v18/arrow/flight"
"github.com/apache/arrow-go/v18/arrow/ipc"
"github.com/apache/arrow-go/v18/arrow/memory"
"github.com/stretchr/testify/assert"
"google.golang.org/grpc"
"google.golang.org/grpc/credentials/insecure"
)

// TestAsyncIndexingQA verifies that indexing happens asynchronously using a real flight client
func TestAsyncIndexingQA(t *testing.T) {
// Setup server with async indexing enabled
vs, _, dialer := setupServer(t)

ctx := context.Background()
client, err := flight.NewClientWithMiddleware(
"passthrough:///bufnet",
nil,
nil,
grpc.WithContextDialer(dialer),
grpc.WithTransportCredentials(insecure.NewCredentials()),
)
if err != nil {
t.Fatalf("Failed to create client: %v", err)
}
defer func() { _ = client.Close() }()

// Create a record
schema := arrow.NewSchema(
[]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int32},
{Name: "vector", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)},
},
nil,
)
mem := memory.NewGoAllocator()
b := array.NewRecordBuilder(mem, schema)
defer b.Release()

b.Field(0).(*array.Int32Builder).AppendValues([]int32{1}, nil)
vb := b.Field(1).(*array.FixedSizeListBuilder)
vvb := vb.ValueBuilder().(*array.Float32Builder)
vb.Append(true)
vvb.AppendValues([]float32{0.1, 0.2}, nil)

rec := b.NewRecordBatch()
defer rec.Release()

// DoPut
stream, err := client.DoPut(ctx)
if err != nil {
t.Fatalf("DoPut failed: %v", err)
}
w := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
w.SetFlightDescriptor(&flight.FlightDescriptor{Path: []string{"async_test"}})

start := time.Now()
if err := w.Write(rec); err != nil {
t.Fatalf("Write failed: %v", err)
}
_ = w.Close()
if err := stream.CloseSend(); err != nil {
t.Fatalf("CloseSend failed: %v", err)
}
_, _ = stream.Recv()
duration := time.Since(start)
t.Logf("DoPut completed in %v", duration)

// Verify index is eventually updated
assert.Eventually(t, func() bool {
ds, ok := vs.vectors.Get("async_test")
if !ok || ds.Index == nil {
return false
}
// Check internal locations slice of HNSWIndex
ds.Index.mu.RLock()
defer ds.Index.mu.RUnlock()
return len(ds.Index.locations) > 0
}, 2*time.Second, 100*time.Millisecond, "Index should be updated asynchronously")
}

// TestWALSizeLimitQA verifies that exceeding WAL size triggers snapshot
func TestWALSizeLimitQA(t *testing.T) {
tmpDir := t.TempDir()
mem := memory.NewGoAllocator()
logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
// Set small WAL limit (e.g., 100 bytes)
store := NewVectorStore(mem, logger, 1024*1024, 100, 0)
err := store.InitPersistence(tmpDir, 0)
assert.NoError(t, err)

// Create a record larger than 100 bytes compatible with writeParquet
// writeParquet expects: ID (int32), Vector (FixedSizeList<float32>)
vecLen := 50 // 50 * 4 bytes = 200 bytes > 100 bytes limit
schema := arrow.NewSchema(
[]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int32},
{Name: "vector", Type: arrow.FixedSizeListOf(int32(vecLen), arrow.PrimitiveTypes.Float32)},
},
nil,
)
b := array.NewRecordBuilder(mem, schema)
defer b.Release()

// Append ID
b.Field(0).(*array.Int32Builder).Append(1)

// Append Vector
vb := b.Field(1).(*array.FixedSizeListBuilder)
vvb := vb.ValueBuilder().(*array.Float32Builder)
vb.Append(true)

floats := make([]float32, vecLen)
for i := range floats {
floats[i] = 0.5
}
vvb.AppendValues(floats, nil)

rec := b.NewRecordBatch()
defer rec.Release()

// Manually populate memory so Snapshot has data to write
rec.Retain()
store.vectors.Set("wal_limit_test", &Dataset{
Records: []arrow.RecordBatch{rec},
lastAccess: time.Now().UnixNano(),
})

// Write through WAL batcher and flush to ensure on-disk
err = store.walBatcher.Write(rec, "wal_limit_test")
assert.NoError(t, err)

// Stop batcher to force flush to disk
err = store.walBatcher.Stop()
assert.NoError(t, err)

// Trigger check manually
store.checkWALSize()

// Verify snapshot file exists
assert.Eventually(t, func() bool {
path := fmt.Sprintf("%s/snapshots/wal_limit_test.parquet", tmpDir)
_, err := os.Stat(path)
return err == nil
}, 2*time.Second, 100*time.Millisecond, "Snapshot should be created when WAL limit exceeded")
}
