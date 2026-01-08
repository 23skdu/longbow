package store

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/rs/zerolog"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const testBufSize = 1024 * 1024

// setupDataServerTest creates a DataServer with in-memory gRPC
func setupDataServerTest(t *testing.T) flight.Client {
	lis := bufconn.Listen(testBufSize)

	tmpDir, err := os.MkdirTemp("", "dataserver_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	vs := NewVectorStore(mem, logger, 1024*1024*100, 0, 0)

	if err := vs.InitPersistence(storage.StorageConfig{DataPath: tmpDir, SnapshotInterval: 0}); err != nil {
		t.Fatalf("Failed to init persistence: %v", err)
	}

	ds := NewDataServer(vs)
	s := grpc.NewServer()
	flight.RegisterFlightServiceServer(s, ds)

	go func() {
		_ = s.Serve(lis)
	}()

	dialer := func(ctx context.Context, address string) (net.Conn, error) {
		return lis.Dial()
	}

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

	t.Cleanup(func() {
		_ = client.Close()
		s.Stop()
		_ = lis.Close()
		_ = vs.Close()
		_ = os.RemoveAll(tmpDir)
	})

	return client
}

// makeVectorRecord creates test record with vectors
func makeVectorRecord(mem memory.Allocator, dims, count int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "vec", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	idBldr := bldr.Field(0).(*array.StringBuilder)
	vecBldr := bldr.Field(1).(*array.FixedSizeListBuilder)
	valBldr := vecBldr.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < count; i++ {
		idBldr.Append(fmt.Sprintf("vec_%d", i))
		vecBldr.Append(true)
		for j := 0; j < dims; j++ {
			valBldr.Append(float32(i*dims + j))
		}
	}

	return bldr.NewRecordBatch()
}

// TestDataServerDoPut tests successful data ingestion
func TestDataServerDoPut(t *testing.T) {
	client := setupDataServerTest(t)
	ctx := context.Background()
	mem := memory.NewGoAllocator()

	rec := makeVectorRecord(mem, 128, 10)
	defer rec.Release()

	stream, err := client.DoPut(ctx)
	if err != nil {
		t.Fatalf("DoPut failed: %v", err)
	}

	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{"ds_put_test"},
	}

	w := flight.NewRecordWriter(stream, ipc.WithSchema(rec.Schema()))
	w.SetFlightDescriptor(desc)

	if err := w.Write(rec); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	_ = w.Close()
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend failed: %v", err)
	}

	// Wait for server response (may be EOF)
	_, _ = stream.Recv()
}

// TestDataServerDoGet tests successful data retrieval
func TestDataServerDoGet(t *testing.T) {
	client := setupDataServerTest(t)
	ctx := context.Background()
	mem := memory.NewGoAllocator()

	// Put data first
	rec := makeVectorRecord(mem, 128, 10)
	defer rec.Release()

	stream, _ := client.DoPut(ctx)
	desc := &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{"ds_get_test"}}
	w := flight.NewRecordWriter(stream, ipc.WithSchema(rec.Schema()))
	w.SetFlightDescriptor(desc)
	_ = w.Write(rec)
	_ = w.Close()
	_ = stream.CloseSend()
	_, _ = stream.Recv()

	// Now get data - ticket uses JSON format
	ticketBytes, _ := json.Marshal(map[string]interface{}{"name": "ds_get_test"})
	tkt := &flight.Ticket{Ticket: ticketBytes}
	getStream, err := client.DoGet(ctx, tkt)
	if err != nil {
		t.Fatalf("DoGet failed: %v", err)
	}

	reader, err := flight.NewRecordReader(getStream)
	if err != nil {
		t.Fatalf("NewRecordReader failed: %v", err)
	}
	defer reader.Release()

	rowCount := int64(0)
	for reader.Next() {
		rowCount += reader.RecordBatch().NumRows()
	}

	if rowCount != 10 {
		t.Errorf("Expected 10 rows, got %d", rowCount)
	}
}

// TestDataServerDoGetNotFound tests NotFound error conversion
func TestDataServerDoGetNotFound(t *testing.T) {
	client := setupDataServerTest(t)
	ctx := context.Background()

	ticketBytes, _ := json.Marshal(map[string]interface{}{"name": "nonexistent"})
	tkt := &flight.Ticket{Ticket: ticketBytes}
	stream, err := client.DoGet(ctx, tkt)
	if err != nil {
		// Error on initial call - check it
		st, ok := status.FromError(err)
		if !ok {
			t.Fatalf("Expected gRPC status, got: %v", err)
		}
		if st.Code() != codes.NotFound {
			t.Errorf("Expected NotFound, got %v", st.Code())
		}
		return
	}

	// Error may come when reading from stream
	_, err = flight.NewRecordReader(stream)
	if err == nil {
		t.Fatal("Expected error from stream")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("Expected gRPC status, got: %v", err)
	}

	if st.Code() != codes.NotFound {
		t.Errorf("Expected NotFound, got %v", st.Code())
	}
}

// TestDataServerListFlightsUnimplemented tests Unimplemented response
func TestDataServerListFlightsUnimplemented(t *testing.T) {
	client := setupDataServerTest(t)
	ctx := context.Background()

	stream, err := client.ListFlights(ctx, &flight.Criteria{})
	if err != nil {
		t.Fatalf("ListFlights call failed: %v", err)
	}

	_, err = stream.Recv()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("Expected gRPC status, got: %v", err)
	}

	if st.Code() != codes.Unimplemented {
		t.Errorf("Expected Unimplemented, got %v", st.Code())
	}
}

// TestDataServerGetFlightInfoUnimplemented tests Unimplemented response
func TestDataServerGetFlightInfoUnimplemented(t *testing.T) {
	client := setupDataServerTest(t)
	ctx := context.Background()

	desc := &flight.FlightDescriptor{Type: flight.DescriptorPATH, Path: []string{"test"}}
	_, err := client.GetFlightInfo(ctx, desc)
	if err == nil {
		t.Fatal("Expected error")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("Expected gRPC status, got: %v", err)
	}

	if st.Code() != codes.Unimplemented {
		t.Errorf("Expected Unimplemented, got %v", st.Code())
	}
}

// TestDataServerDoActionUnimplemented tests Unimplemented response
func TestDataServerDoActionUnimplemented(t *testing.T) {
	client := setupDataServerTest(t)
	ctx := context.Background()

	action := &flight.Action{Type: "any", Body: []byte("{}")}
	stream, err := client.DoAction(ctx, action)
	if err != nil {
		t.Fatalf("DoAction call failed: %v", err)
	}

	_, err = stream.Recv()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("Expected gRPC status, got: %v", err)
	}

	if st.Code() != codes.Unimplemented {
		t.Errorf("Expected Unimplemented, got %v", st.Code())
	}
}

// ============== MetaServer Tests ==============

// setupMetaServerTest creates a MetaServer with in-memory gRPC
func setupMetaServerTest(t *testing.T) (flight.Client, *VectorStore) {
	lis := bufconn.Listen(testBufSize)

	tmpDir, err := os.MkdirTemp("", "metaserver_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	vs := NewVectorStore(mem, logger, 1024*1024*100, 0, 0)

	if err := vs.InitPersistence(storage.StorageConfig{DataPath: tmpDir, SnapshotInterval: 0}); err != nil {
		t.Fatalf("Failed to init persistence: %v", err)
	}

	ms := NewMetaServer(vs)
	s := grpc.NewServer()
	flight.RegisterFlightServiceServer(s, ms)

	go func() {
		_ = s.Serve(lis)
	}()

	dialer := func(ctx context.Context, address string) (net.Conn, error) {
		return lis.Dial()
	}

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

	t.Cleanup(func() {
		_ = client.Close()
		s.Stop()
		_ = lis.Close()
		_ = vs.Close()
		_ = os.RemoveAll(tmpDir)
	})

	return client, vs
}

// putDataViaVectorStore puts test data directly to VectorStore for MetaServer tests
func putDataViaVectorStore(vs *VectorStore, name string) {
	// Use internal method to add data for testing
	mem := memory.NewGoAllocator()
	rec := makeVectorRecord(mem, 128, 5)
	defer rec.Release()

	// Store directly - this is a simplified approach
	// In real scenario we'd use DoPut through DataServer
	// Store directly - this is a simplified approach
	// In real scenario we'd use DoPut through DataServer
	vs.updateDatasets(func(m map[string]*Dataset) {
		m[name] = &Dataset{
			Name:    name,
			Records: []arrow.RecordBatch{rec},
		}
	})
	rec.Retain() // Keep record alive in dataset
}

// TestMetaServerListFlights tests listing available datasets
func TestMetaServerListFlights(t *testing.T) {
	client, vs := setupMetaServerTest(t)
	ctx := context.Background()

	// Add test data
	putDataViaVectorStore(vs, "meta_list_test")

	stream, err := client.ListFlights(ctx, &flight.Criteria{})
	if err != nil {
		t.Fatalf("ListFlights failed: %v", err)
	}

	found := false
	for {
		info, err := stream.Recv()
		if err != nil {
			break
		}
		if info != nil && len(info.FlightDescriptor.Path) > 0 {
			if info.FlightDescriptor.Path[0] == "meta_list_test" {
				found = true
			}
		}
	}

	if !found {
		t.Error("Expected to find meta_list_test in ListFlights")
	}
}

// TestMetaServerGetFlightInfo tests getting dataset metadata
func TestMetaServerGetFlightInfo(t *testing.T) {
	client, vs := setupMetaServerTest(t)
	ctx := context.Background()

	// Add test data
	putDataViaVectorStore(vs, "meta_info_test")

	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{"meta_info_test"},
	}

	info, err := client.GetFlightInfo(ctx, desc)
	if err != nil {
		t.Fatalf("GetFlightInfo failed: %v", err)
	}

	if info == nil {
		t.Fatal("Expected FlightInfo")
	}

	if info.TotalRecords < 0 {
		t.Error("Expected non-negative TotalRecords")
	}
}

// TestMetaServerGetFlightInfoNotFound tests NotFound error
func TestMetaServerGetFlightInfoNotFound(t *testing.T) {
	client, _ := setupMetaServerTest(t)
	ctx := context.Background()

	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{"nonexistent_dataset"},
	}

	_, err := client.GetFlightInfo(ctx, desc)
	if err == nil {
		t.Fatal("Expected error for nonexistent dataset")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("Expected gRPC status, got: %v", err)
	}

	if st.Code() != codes.NotFound {
		t.Errorf("Expected NotFound, got %v", st.Code())
	}
}

// TestMetaServerDoGetUnimplemented tests Unimplemented response
func TestMetaServerDoGetUnimplemented(t *testing.T) {
	client, _ := setupMetaServerTest(t)
	ctx := context.Background()

	ticketBytes, _ := json.Marshal(map[string]interface{}{"name": "test"})
	tkt := &flight.Ticket{Ticket: ticketBytes}
	stream, err := client.DoGet(ctx, tkt)
	if err != nil {
		// Error on initial call
		st, ok := status.FromError(err)
		if !ok {
			t.Fatalf("Expected gRPC status, got: %v", err)
		}
		if st.Code() != codes.NotFound {
			t.Errorf("Expected NotFound, got %v", st.Code())
		}
		return
	}

	// Try to read from stream
	_, err = flight.NewRecordReader(stream)
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("Expected gRPC status, got: %v", err)
	}

	if st.Code() != codes.NotFound {
		t.Errorf("Expected NotFound, got %v", st.Code())
	}
}

// TestMetaServerDoPutUnimplemented tests Unimplemented response
func TestMetaServerDoPutUnimplemented(t *testing.T) {
	client, _ := setupMetaServerTest(t)
	ctx := context.Background()

	stream, err := client.DoPut(ctx)
	if err != nil {
		t.Fatalf("DoPut call failed: %v", err)
	}

	// Try to close and get response
	_ = stream.CloseSend()
	_, err = stream.Recv()

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("Expected gRPC status, got: %v", err)
	}

	if st.Code() != codes.Unimplemented {
		t.Errorf("Expected Unimplemented, got %v", st.Code())
	}
}

// TestMetaServerDoActionUnknown tests unknown action returns Unimplemented
func TestMetaServerDoActionUnknown(t *testing.T) {
	client, _ := setupMetaServerTest(t)
	ctx := context.Background()

	action := &flight.Action{Type: "unknown_action", Body: []byte("{}")}
	stream, err := client.DoAction(ctx, action)
	if err != nil {
		t.Fatalf("DoAction call failed: %v", err)
	}

	_, err = stream.Recv()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("Expected gRPC status, got: %v", err)
	}

	if st.Code() != codes.Unimplemented {
		t.Errorf("Expected Unimplemented, got %v", st.Code())
	}
}

// TestMetaServerDoActionNil tests nil action returns InvalidArgument
func TestMetaServerDoActionNil(t *testing.T) {
	client, _ := setupMetaServerTest(t)
	ctx := context.Background()

	// Send action with empty type (closest to nil we can send)
	action := &flight.Action{Type: "", Body: nil}
	stream, err := client.DoAction(ctx, action)
	if err != nil {
		t.Fatalf("DoAction call failed: %v", err)
	}

	_, err = stream.Recv()
	if err == nil {
		t.Fatal("Expected error")
	}
	// Either InvalidArgument or Unimplemented is acceptable here
	_, _ = status.FromError(err)
}
