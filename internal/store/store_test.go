package store_test

import (
"context"
"encoding/json"
"fmt"
"log/slog"
"net"
"os"
"testing"
"time"

"github.com/apache/arrow/go/v18/arrow"
"github.com/apache/arrow/go/v18/arrow/array"
"github.com/apache/arrow/go/v18/arrow/flight"
"github.com/apache/arrow/go/v18/arrow/ipc"
"github.com/apache/arrow/go/v18/arrow/memory"
"github.com/23skdu/longbow/internal/store"
"google.golang.org/grpc"
"google.golang.org/grpc/credentials/insecure"
"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

var lis *bufconn.Listener

func init() {
lis = bufconn.Listen(bufSize)
}

func bufDialer(ctx context.Context, address string) (net.Conn, error) {
return lis.Dial()
}


func setupServer(t *testing.T) (*store.VectorStore, string, func(context.Context, string) (net.Conn, error)) {
lis := bufconn.Listen(bufSize)

// Create temp dir for persistence
tmpDir, err := os.MkdirTemp("", "longbow_test_*")
if err != nil {
t.Fatalf("Failed to create temp dir: %v", err)
}

mem := memory.NewGoAllocator()
logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
vs := store.NewVectorStore(mem, logger, 1024*1024*100) // 100MB limit

// Init persistence
if err := vs.InitPersistence(tmpDir, 0); err != nil {
t.Fatalf("Failed to init persistence: %v", err)
}

s := grpc.NewServer()
flight.RegisterFlightServiceServer(s, vs)

go func() {
if err := s.Serve(lis); err != nil {
// Server closed
_ = err
}
}()

dialer := func(ctx context.Context, address string) (net.Conn, error) {
return lis.Dial()
}

t.Cleanup(func() {
s.Stop()
lis.Close()
os.RemoveAll(tmpDir)
})

return vs, tmpDir, dialer
}

func TestDoPutAndDoGet(t *testing.T) {
_, _, dialer := setupServer(t)

ctx := context.Background()
client, err := flight.NewClientWithMiddleware(
"passthrough",
nil,
nil,
grpc.WithContextDialer(dialer),
grpc.WithTransportCredentials(insecure.NewCredentials()),
)
if err != nil {
t.Fatalf("Failed to create client: %v", err)
}
defer client.Close()

// 1. Create Data
schema := arrow.NewSchema(
[]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
{Name: "val", Type: arrow.PrimitiveTypes.Float64},
},
nil,
)

mem := memory.NewGoAllocator()
b := array.NewRecordBuilder(mem, schema)
defer b.Release()

b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
b.Field(1).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 3.3}, nil)
rec := b.NewRecord()
defer rec.Release()

// 2. DoPut
stream, err := client.DoPut(ctx)
if err != nil {
t.Fatalf("DoPut failed: %v", err)
}

desc := &flight.FlightDescriptor{
Type: flight.DescriptorPATH,
Path: []string{"test_dataset"},
}

w := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
w.SetFlightDescriptor(desc)

if err := w.Write(rec); err != nil {
t.Fatalf("Write failed: %v", err)
}
w.Close()
if err := stream.CloseSend(); err != nil { t.Fatalf("CloseSend failed: %v", err) }

// Wait for server to process
_, err = stream.Recv()
if err != nil {
	// Expected EOF
	_ = err
}

// 3. DoGet
ticketBytes, _ := json.Marshal(map[string]interface{}{"name": "test_dataset"})
ticket := &flight.Ticket{Ticket: ticketBytes}
rStream, err := client.DoGet(ctx, ticket)
if err != nil {
t.Fatalf("DoGet failed: %v", err)
}

r, err := flight.NewRecordReader(rStream)
if err != nil {
t.Fatalf("NewRecordReader failed: %v", err)
}
defer r.Release()

count := 0
for r.Next() {
count += int(r.Record().NumRows())
}
if r.Err() != nil {
t.Fatalf("Reader error: %v", r.Err())
}

if count != 3 {
t.Errorf("Expected 3 rows, got %d", count)
}
}


func TestSchemaValidation(t *testing.T) {
_, _, dialer := setupServer(t)
ctx := context.Background()
client, err := flight.NewClientWithMiddleware(
"passthrough",
nil,
nil,
grpc.WithContextDialer(dialer),
grpc.WithTransportCredentials(insecure.NewCredentials()),
)
if err != nil {
t.Fatalf("Failed to create client: %v", err)
}
defer client.Close()

// Schema A
schemaA := arrow.NewSchema([]arrow.Field{{Name: "col1", Type: arrow.PrimitiveTypes.Int64}}, nil)
// Schema B
schemaB := arrow.NewSchema([]arrow.Field{{Name: "col1", Type: arrow.PrimitiveTypes.Float64}}, nil)

mem := memory.NewGoAllocator()

// Upload Schema A
bA := array.NewRecordBuilder(mem, schemaA)
defer bA.Release()

bA.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
recA := bA.NewRecord()
defer recA.Release()

streamA, err := client.DoPut(ctx)
	if err != nil { t.Fatalf("DoPut A failed: %v", err) }
wA := flight.NewRecordWriter(streamA, ipc.WithSchema(schemaA))
wA.SetFlightDescriptor(&flight.FlightDescriptor{Path: []string{"schema_test"}})

if err := wA.Write(recA); err != nil {
t.Fatalf("Write A failed: %v", err)
}
wA.Close()
if err := streamA.CloseSend(); err != nil { t.Fatalf("CloseSend A failed: %v", err) }
_, err = streamA.Recv() // Wait for completion
if err != nil && err.Error() != "EOF" {
	t.Logf("Stream A recv: %v", err)
}

// Upload Schema B (Should Fail)
bB := array.NewRecordBuilder(mem, schemaB)
defer bB.Release()

// Safe type assertion
fb, ok := bB.Field(0).(*array.Float64Builder)
if !ok {
t.Fatalf("Expected Float64Builder, got %T", bB.Field(0))
}
fb.AppendValues([]float64{1.1}, nil)

recB := bB.NewRecord()
defer recB.Release()

streamB, _ := client.DoPut(ctx)
wB := flight.NewRecordWriter(streamB, ipc.WithSchema(schemaB))
wB.SetFlightDescriptor(&flight.FlightDescriptor{Path: []string{"schema_test"}})

if err := wB.Write(recB); err != nil {
// Write might fail immediately if server rejects schema
// t.Logf("Write B failed as expected: %v", err)
}
wB.Close()
if err := streamB.CloseSend(); err != nil { t.Fatalf("CloseSend B failed: %v", err) }

_, err = streamB.Recv()
if err == nil {
t.Fatal("Expected error due to schema mismatch, got nil")
}
}

func TestPersistence(t *testing.T) {
vs, tmpDir, dialer := setupServer(t)
ctx := context.Background()
client, err := flight.NewClientWithMiddleware(
"passthrough",
nil,
nil,
grpc.WithContextDialer(dialer),
grpc.WithTransportCredentials(insecure.NewCredentials()),
)
if err != nil {
t.Fatalf("Failed to create client: %v", err)
}
defer client.Close()

// Write Data
schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
mem := memory.NewGoAllocator()
b := array.NewRecordBuilder(mem, schema)
defer b.Release()

if b.Field(0) == nil {
t.Fatal("Field 0 builder is nil")
}
ib, ok := b.Field(0).(*array.Int64Builder)
if !ok {
t.Fatalf("Field 0 is not Int64Builder, got %T", b.Field(0))
}
ib.AppendValues([]int64{100}, nil)

rec := b.NewRecord()
defer rec.Release()

stream, err := client.DoPut(ctx)
	if err != nil { t.Fatalf("DoPut failed: %v", err) }
w := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
w.SetFlightDescriptor(&flight.FlightDescriptor{Path: []string{"persist_test"}})

if err := w.Write(rec); err != nil {
t.Fatalf("Write failed: %v", err)
}
w.Close()
if err := stream.CloseSend(); err != nil { t.Fatalf("CloseSend failed: %v", err) }
_, _ = stream.Recv()

// Force Snapshot
if err := vs.Snapshot(); err != nil {
t.Fatalf("Snapshot failed: %v", err)
}

// Verify file exists
path := fmt.Sprintf("%s/snapshots/persist_test.arrow", tmpDir)

// Wait briefly for file system
time.Sleep(100 * time.Millisecond)

if _, err := os.Stat(path); os.IsNotExist(err) {
t.Fatal("Snapshot file not created")
}
}
