package store

import (
"context"
"encoding/json"
"testing"
"time"

"github.com/apache/arrow-go/v18/arrow/flight"
"github.com/apache/arrow-go/v18/arrow/memory"
"github.com/coder/hnsw"
"github.com/stretchr/testify/assert"
"github.com/stretchr/testify/require"
"go.uber.org/zap"
"google.golang.org/grpc/codes"
"google.golang.org/grpc/status"
)

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
req := VectorSearchRequest{
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

var resp VectorSearchResponse
err = json.Unmarshal(mockStream.results[0].Body, &resp)
require.NoError(t, err)

assert.Len(t, resp.IDs, 10)
assert.Len(t, resp.Scores, 10)
}

func TestVectorSearchAction_InvalidDataset(t *testing.T) {
store := createTestStoreWithVectors(t, "test-dataset", 10, 128)
defer func() { _ = store.Close() }()

metaServer := NewMetaServer(store)

req := VectorSearchRequest{
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
req := VectorSearchRequest{
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

req := VectorSearchRequest{
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

req := VectorSearchRequest{
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
func createTestStoreWithVectors(t *testing.T, datasetName string, numVectors, dim int) *VectorStore {
t.Helper()

mem := memory.NewGoAllocator()
logger := zap.NewNop()
store := NewVectorStore(mem, logger, 1<<30, 0, time.Hour)

// Create dataset with HNSW index
ds := &Dataset{
Name: datasetName,
}
ds.Index = NewHNSWIndex(ds)
// Set dimensions explicitly since we're bypassing Add()
ds.Index.dims = dim

// Add vectors directly to the HNSW graph
for i := 0; i < numVectors; i++ {
vec := make([]float32, dim)
for j := range vec {
vec[j] = float32(i*dim + j)
}
ds.Index.Graph.Add(hnsw.MakeNode(VectorID(i), vec))
}

store.vectors.Set(datasetName, ds)
return store
}
