package sync

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestSyncWorker_Replication(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()

	// 1. Setup Leader Store
	leaderDir := t.TempDir()
	leaderStore := store.NewVectorStore(mem, logger, 1<<30, 1<<30, 0)
	require.NoError(t, leaderStore.InitPersistence(store.StorageConfig{
		DataPath:         leaderDir,
		SnapshotInterval: time.Hour,
	}))
	defer func() { _ = leaderStore.Close() }()

	// 2. Start Leader gRPC Server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	leaderAddr := lis.Addr().String()

	server := grpc.NewServer()
	dataServer := store.NewDataServer(leaderStore)
	flight.RegisterFlightServiceServer(server, dataServer)

	go func() { _ = server.Serve(lis) }()
	defer server.Stop()

	// 3. Setup Follower Store
	followerDir := t.TempDir()
	followerStore := store.NewVectorStore(mem, logger, 1<<30, 1<<30, 0)
	require.NoError(t, followerStore.InitPersistence(store.StorageConfig{
		DataPath:         followerDir,
		SnapshotInterval: time.Hour,
	}))
	defer func() { _ = followerStore.Close() }()

	// 4. Create Record on Leader
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "val", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.Uint32Builder).Append(1)
	b.Field(1).(*array.Int64Builder).Append(100)
	listB := b.Field(2).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float32Builder)
	listB.Append(true)
	valB.AppendValues([]float32{1.0, 2.0, 3.0, 4.0}, nil)
	rec := b.NewRecordBatch()

	// We use DoPut simulation or direct WAL write on leader
	// DoPut is better as it uses the batcher correctly
	// But for test simplicity, let's use a mock stream to leaderStore.DoPut?
	// Or just use a real client to DoPut to leader!

	conn, err := grpc.NewClient(leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	client := flight.NewFlightServiceClient(conn)

	putStream, err := client.DoPut(context.Background())
	require.NoError(t, err)

	writer := flight.NewRecordWriter(putStream, ipc.WithSchema(schema))
	writer.SetFlightDescriptor(&flight.FlightDescriptor{Path: []string{"ds1"}})
	err = writer.Write(rec)
	require.NoError(t, err)
	_ = writer.Close()
	require.NoError(t, putStream.CloseSend())
	_, err = putStream.Recv() // wait for ack
	require.Equal(t, io.EOF, err)

	// Wait for WAL flush on leader
	time.Sleep(100 * time.Millisecond)

	// 5. Start SyncWorker on Follower
	worker := NewSyncWorker(followerStore, logger)
	worker.AddPeer(leaderAddr)
	worker.Start()
	defer worker.Stop()

	// 6. Verify Replication
	// Worker should poll and find the record
	require.Eventually(t, func() bool {
		ds, err := followerStore.GetDataset("ds1")
		if err != nil {
			return false
		}
		return ds.IndexLen() > 0
	}, 5*time.Second, 100*time.Millisecond, "Record should be replicated to follower")

	// Verify data
	ds, _ := followerStore.GetDataset("ds1")
	assert.Equal(t, 1, ds.IndexLen())

	// Verify Merkle Roots Match
	followerRoot := followerStore.MerkleRoot("ds1")
	leaderRoot := leaderStore.MerkleRoot("ds1")
	assert.Equal(t, leaderRoot, followerRoot, "Merkle roots should match after sync")
}

// Fixed import error in sync_worker.go by adding bytes and sync.
// Also fixing sync_worker_test.go imports and grpc.Dial.
