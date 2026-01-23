package store

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/23skdu/longbow/internal/mesh"
	qry "github.com/23skdu/longbow/internal/query"
)

// TestDoGetSearch_Integration verifies the end-to-end flow of searching via DoGet
func TestDoGetSearch_Integration(t *testing.T) {
	// 1. Start Server
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	defer func() { _ = lis.Close() }()

	logger := zerolog.Nop()
	// NewVectorStore args: allocator, logger, maxMemory, walBytes(unused), checkInterval(unused)
	store := NewVectorStore(memory.NewGoAllocator(), logger, 1024*1024, 0, time.Minute)
	defer func() { _ = store.Close() }()
	store.StartIndexingWorkers(1) // Start 1 worker for test

	// Create and Populate Dataset
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
			{Name: "vector", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	// Add 3 vectors: [1,0] (ID 10), [0,1] (ID 20), [1,1] (ID 30)
	idB := b.Field(0).(*array.Uint64Builder)
	vecB := b.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)

	// Rec 1
	idB.Append(10)
	vecB.Append(true)
	valB.AppendValues([]float32{1.0, 0.0}, nil)

	// Rec 2
	idB.Append(20)
	vecB.Append(true)
	valB.AppendValues([]float32{0.0, 1.0}, nil)

	// Rec 3
	idB.Append(30)
	vecB.Append(true)
	valB.AppendValues([]float32{1.0, 1.0}, nil)

	rec := b.NewRecordBatch()
	defer rec.Release()

	err = store.StoreRecordBatch(context.Background(), "test_ds", rec)
	require.NoError(t, err)

	// Start Flight Server (gRPC)
	// We use DataServer which wraps VectorStore and provides Flight service methods
	dataServer := NewDataServer(store)
	grpcServer := grpc.NewServer()

	// Register directly
	flight.RegisterFlightServiceServer(grpcServer, dataServer)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	// Wait for start
	time.Sleep(100 * time.Millisecond)

	// Wait for indexing to complete
	ds, err := store.GetDataset("test_ds")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return ds.IndexLen() == 3
	}, 5*time.Second, 100*time.Millisecond, "Index should reach 3 records")

	// 2. Setup Client (GlobalSearchCoordinator)
	coord := NewGlobalSearchCoordinator(logger)
	defer func() { _ = coord.Close() }()

	// 3. Execute Search
	// Query: [1, 0] (Should match ID 10 best)
	req := &qry.VectorSearchRequest{
		Dataset: "test_ds",
		Vector:  []float32{1.0, 0.0},
		K:       2,
	}

	// Pseudo-remote peer pointing to our server
	peer := mesh.Member{
		ID:       "peer1",
		MetaAddr: addr,
	}

	// Verify LocalOnly=false triggers remote call logic
	results, err := coord.GlobalSearch(context.Background(), nil, req, []mesh.Member{peer})
	require.NoError(t, err)

	// 4. Assert Results
	// Should get ID 10 then ID 30
	assert.Len(t, results, 2)
	assert.Equal(t, uint64(10), uint64(results[0].ID))

	fmt.Printf("Results: %+v\n", results)
}
