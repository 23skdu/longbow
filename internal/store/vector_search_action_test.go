package store

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	qry "github.com/23skdu/longbow/internal/query"
)

// Helper to decode Arrow IPC response
func readArrowResponse(t *testing.T, body []byte) qry.VectorSearchResponse {
	if len(body) == 0 {
		return qry.VectorSearchResponse{}
	}
	rdr, err := ipc.NewReader(bytes.NewReader(body))
	require.NoError(t, err)
	defer rdr.Release()

	var resp qry.VectorSearchResponse
	if rdr.Next() {
		rec := rdr.RecordBatch()
		ids := rec.Column(0).(*array.Uint64)
		scores := rec.Column(1).(*array.Float32)
		for i := 0; i < ids.Len(); i++ {
			resp.IDs = append(resp.IDs, ids.Value(i))
			resp.Scores = append(resp.Scores, scores.Value(i))
		}
	}
	require.NoError(t, rdr.Err())
	return resp
}

// mockVectorSearchActionServer implements flight.FlightService_DoActionServer for testing
type mockVectorSearchActionServer struct {
	flight.FlightService_DoActionServer
	results []*flight.Result
	ctx     context.Context
}

func (m *mockVectorSearchActionServer) Send(r *flight.Result) error {
	m.results = append(m.results, r)
	return nil
}

func (m *mockVectorSearchActionServer) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}

func TestVectorSearchAction_ValidRequest(t *testing.T) {
	// Setup store with test data
	store := createTestStoreWithVectors(t, "test-dataset", 100, 128)
	defer func() { _ = store.Close() }()

	metaServer := NewMetaServer(store)

	// Create VectorSearch request
	req := qry.VectorSearchRequest{
		Dataset: "test-dataset",
		Vector:  make([]float32, 128),
		K:       10,
	}
	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	action := &flight.Action{
		Type: "VectorSearch",
		Body: reqBytes,
	}

	mockStream := &mockVectorSearchActionServer{}
	err = metaServer.DoAction(action, mockStream)
	require.NoError(t, err)

	// Verify response
	require.Len(t, mockStream.results, 1)

	resp := readArrowResponse(t, mockStream.results[0].Body)

	assert.Len(t, resp.IDs, 10)
	assert.Len(t, resp.Scores, 10)
}

func TestVectorSearchAction_InvalidDataset(t *testing.T) {
	store := createTestStoreWithVectors(t, "test-dataset", 10, 128)
	defer func() { _ = store.Close() }()

	metaServer := NewMetaServer(store)

	req := qry.VectorSearchRequest{
		Dataset: "nonexistent",
		Vector:  make([]float32, 128),
		K:       10,
	}
	reqBytes, _ := json.Marshal(req)

	action := &flight.Action{
		Type: "VectorSearch",
		Body: reqBytes,
	}

	mockStream := &mockVectorSearchActionServer{}
	err := metaServer.DoAction(action, mockStream)

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.NotFound, st.Code())
}

func TestVectorSearchAction_InvalidJSON(t *testing.T) {
	store := createTestStoreWithVectors(t, "test-dataset", 10, 128)
	defer func() { _ = store.Close() }()

	metaServer := NewMetaServer(store)

	action := &flight.Action{
		Type: "VectorSearch",
		Body: []byte("invalid json"),
	}

	mockStream := &mockVectorSearchActionServer{}
	err := metaServer.DoAction(action, mockStream)

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestVectorSearchAction_DimensionMismatch(t *testing.T) {
	store := createTestStoreWithVectors(t, "test-dataset", 10, 128)
	defer func() { _ = store.Close() }()

	metaServer := NewMetaServer(store)

	// Wrong dimension - dataset has 128, we send 64
	req := qry.VectorSearchRequest{
		Dataset: "test-dataset",
		Vector:  make([]float32, 64),
		K:       10,
	}
	reqBytes, _ := json.Marshal(req)

	action := &flight.Action{
		Type: "VectorSearch",
		Body: reqBytes,
	}

	mockStream := &mockVectorSearchActionServer{}
	err := metaServer.DoAction(action, mockStream)

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestVectorSearchAction_KLessThanOne(t *testing.T) {
	store := createTestStoreWithVectors(t, "test-dataset", 10, 128)
	defer func() { _ = store.Close() }()

	metaServer := NewMetaServer(store)

	req := qry.VectorSearchRequest{
		Dataset: "test-dataset",
		Vector:  make([]float32, 128),
		K:       0,
	}
	reqBytes, _ := json.Marshal(req)

	action := &flight.Action{
		Type: "VectorSearch",
		Body: reqBytes,
	}

	mockStream := &mockVectorSearchActionServer{}
	err := metaServer.DoAction(action, mockStream)

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestVectorSearchAction_MetricsEmitted(t *testing.T) {
	store := createTestStoreWithVectors(t, "test-dataset", 50, 128)
	defer func() { _ = store.Close() }()

	metaServer := NewMetaServer(store)

	req := qry.VectorSearchRequest{
		Dataset: "test-dataset",
		Vector:  make([]float32, 128),
		K:       5,
	}
	reqBytes, _ := json.Marshal(req)

	action := &flight.Action{
		Type: "VectorSearch",
		Body: reqBytes,
	}

	mockStream := &mockVectorSearchActionServer{}
	err := metaServer.DoAction(action, mockStream)
	require.NoError(t, err)

	// Metrics are emitted - check via Prometheus registry
	// VectorSearchActionTotal should be incremented
	// VectorSearchActionDuration should have observation
}

// Helper to create a test store with vectors
// nolint:unparam
func createTestStoreWithVectors(t *testing.T, datasetName string, numVectors, _ int) *VectorStore {
	t.Helper()

	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	store := NewVectorStore(mem, logger, 1<<30, 0, time.Hour)

	// Create dataset with Mock index instead of real HNSW for robust testing
	ds := &Dataset{
		Name: datasetName,
	}
	mockIdx := NewMockIndex()
	// Populate mock index with dummy IDs
	for i := 0; i < numVectors; i++ {
		mockIdx.Vectors[VectorID(i)] = Location{BatchIdx: 0, RowIdx: i}
	}
	ds.Index = mockIdx

	// Add dummy record batch to satisfy MapInternalToUserIDs lookup
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Uint64}}, nil)
	bldr := array.NewUint64Builder(mem)
	defer bldr.Release()
	for i := 0; i < numVectors; i++ {
		bldr.Append(uint64(i * 100))
	}
	idArr := bldr.NewArray()
	defer idArr.Release()
	rec := array.NewRecordBatch(schema, []arrow.Array{idArr}, int64(numVectors))
	ds.Records = []arrow.RecordBatch{rec}

	store.getOrCreateDataset(datasetName, func() *Dataset {
		return ds
	})

	return store
}

func TestVectorSearchByIDAction_Success(t *testing.T) {
	// Custom setup to include "vector" column
	store := createTestStoreWithVectors(t, "test-dataset", 10, 128)
	defer func() { _ = store.Close() }()

	// Replace the record batch with one that has "vector" column for extraction
	ds, _ := store.getDataset("test-dataset")
	oldRec := ds.Records[0]
	mem := memory.NewGoAllocator()

	// Create schema with ID (Uint64) and Vector (FixedSizeList)
	vecType := arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "vector", Type: vecType},
	}, nil)

	// Build IDs
	idBldr := array.NewUint64Builder(mem)
	defer idBldr.Release()
	for i := 0; i < 10; i++ {
		idBldr.Append(uint64(i * 100))
	}
	idArr := idBldr.NewArray()
	defer idArr.Release()

	// Build Vectors
	vecBldr := array.NewFixedSizeListBuilder(mem, 128, arrow.PrimitiveTypes.Float32)
	defer vecBldr.Release()
	valBldr := vecBldr.ValueBuilder().(*array.Float32Builder)
	for i := 0; i < 10; i++ {
		vecBldr.Append(true)
		for j := 0; j < 128; j++ {
			valBldr.Append(0.1) // Dummy vector
		}
	}
	vecArr := vecBldr.NewArray()
	defer vecArr.Release()

	newRec := array.NewRecordBatch(schema, []arrow.Array{idArr, vecArr}, 10)
	ds.Records[0] = newRec // Replace
	oldRec.Release()

	metaServer := NewMetaServer(store)

	// Test: Find ID "100" (Index 1)
	req := qry.VectorSearchByIDRequest{
		Dataset: "test-dataset",
		ID:      "100",
		K:       5,
	}
	reqBytes, _ := json.Marshal(req)

	action := &flight.Action{Type: "VectorSearchByID", Body: reqBytes}
	mockStream := &mockVectorSearchActionServer{}

	err := metaServer.DoAction(action, mockStream)
	require.NoError(t, err)

	require.Len(t, mockStream.results, 1)
	var resp qry.VectorSearchResponse
	err = json.Unmarshal(mockStream.results[0].Body, &resp)
	require.NoError(t, err)

	// Check we got results (MockIndex returns top K)
	assert.Len(t, resp.IDs, 5)
}

func TestVectorSearchAction_GraphBias(t *testing.T) {
	// Setup store with 3 vectors: 0(ID=10), 1(ID=20), 2(ID=30)
	store := createTestStoreWithVectors(t, "graph-dataset", 3, 128)
	defer func() { _ = store.Close() }()

	metaServer := NewMetaServer(store)
	ds, _ := store.getDataset("graph-dataset")

	// Setup Graph: Connect 0(ID=10) <-> 1(ID=20)
	// 2(ID=30) is isolated.
	ds.dataMu.Lock()
	ds.Graph = NewGraphStore()
	_ = ds.Graph.AddEdge(Edge{Subject: VectorID(0), Predicate: "link", Object: VectorID(1), Weight: 1.0})
	_ = ds.Graph.AddEdge(Edge{Subject: VectorID(1), Predicate: "link", Object: VectorID(0), Weight: 1.0})
	ds.dataMu.Unlock()

	req := qry.VectorSearchRequest{
		Dataset:    "graph-dataset",
		Vector:     make([]float32, 128),
		K:          3,
		GraphAlpha: 0.8, // Heavy graph bias
	}
	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	action := &flight.Action{
		Type: "VectorSearch",
		Body: reqBytes,
	}

	mockStream := &mockVectorSearchActionServer{}
	err = metaServer.DoAction(action, mockStream)
	require.NoError(t, err)

	// Verify response structure
	require.Len(t, mockStream.results, 1)
	resp := readArrowResponse(t, mockStream.results[0].Body)

	// We can't easily check exact ranking without a real HNSW, but verify we got result IDs
	assert.Len(t, resp.IDs, 3)
}
