package hnsw

import (
	"context"
	"testing"

	"io"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/mock"
)

// MockVectorIndex is a mock implementation of VectorIndex interface
type MockVectorIndex struct {
	mock.Mock
}

// AddByLocation runs the add by location operation.
func (m *MockVectorIndex) AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	args := m.Called(ctx, batchIdx, rowIdx)
	return args.Get(0).(uint32), args.Error(1)
}

// AddByRecord runs the add by record operation.
func (m *MockVectorIndex) AddByRecord(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	args := m.Called(ctx, rec, rowIdx, batchIdx)
	return args.Get(0).(uint32), args.Error(1)
}

// Search runs the search operation.
func (m *MockVectorIndex) Search(ctx context.Context, qv any, k int, filter any) ([]types.Candidate, error) {
	args := m.Called(ctx, qv, k, filter)
	return args.Get(0).([]types.Candidate), args.Error(1)
}

// SearchVectors runs the search vectors operation.
func (m *MockVectorIndex) SearchVectors(ctx context.Context, q any, k int, filters []core.Filter, options any) ([]SearchResult, error) {
	args := m.Called(ctx, q, k, filters, options)
	return args.Get(0).([]SearchResult), args.Error(1)
}

// SearchVectorsWithBitmap runs the search vectors with bitmap operation.
func (m *MockVectorIndex) SearchVectorsWithBitmap(ctx context.Context, q any, k int, filter *roaring.Bitmap, options any) ([]SearchResult, error) {
	args := m.Called(ctx, q, k, filter, options)
	return args.Get(0).([]SearchResult), args.Error(1)
}

// IsSharded reports whether the condition holds.
func (m *MockVectorIndex) IsSharded() bool {
	args := m.Called()
	return args.Bool(0)
}

// Size runs the size operation.
func (m *MockVectorIndex) Size() int {
	args := m.Called()
	return args.Int(0)
}

// Len runs the len operation.
func (m *MockVectorIndex) Len() int {
	args := m.Called()
	return args.Int(0)
}

// GetEntryPoint returns the entry point.
func (m *MockVectorIndex) GetEntryPoint() uint32 {
	args := m.Called()
	return args.Get(0).(uint32)
}

// GetLocation returns the location.
func (m *MockVectorIndex) GetLocation(id uint32) (any, bool) {
	args := m.Called(id)
	return args.Get(0), args.Bool(1)
}

// GetVectorID returns the vector id.
func (m *MockVectorIndex) GetVectorID(loc any) (uint32, bool) {
	args := m.Called(loc)
	return args.Get(0).(uint32), args.Bool(1)
}

// GetDimension returns the dimension.
func (m *MockVectorIndex) GetDimension() uint32 {
	args := m.Called()
	return args.Get(0).(uint32)
}

// SetIndexedColumns sets the indexed columns.
func (m *MockVectorIndex) SetIndexedColumns(cols []string) {
	m.Called(cols)
}

// GetRawNeighbors returns the raw neighbors.
func (m *MockVectorIndex) GetRawNeighbors(id uint32) ([]uint32, error) {
	args := m.Called(id)
	return args.Get(0).([]uint32), args.Error(1)
}

// GetNeighbors returns the neighbors.
func (m *MockVectorIndex) GetNeighbors(ctx context.Context, id uint32, k int) ([]types.SearchResult, error) {
	args := m.Called(ctx, id, k)
	return args.Get(0).([]types.SearchResult), args.Error(1)
}

// PreWarm runs the pre warm operation.
func (m *MockVectorIndex) PreWarm(targetSize int) {
	m.Called(targetSize)
}

// Warmup runs the warmup operation.
func (m *MockVectorIndex) Warmup() int {
	args := m.Called()
	return args.Int(0)
}

// EstimateMemory runs the estimate memory operation.
func (m *MockVectorIndex) EstimateMemory() int64 {
	args := m.Called()
	return args.Get(0).(int64)
}

// TrainPQ runs the train pq operation.
func (m *MockVectorIndex) TrainPQ(vectors [][]float32) error {
	args := m.Called(vectors)
	return args.Error(0)
}

// GetPQEncoder returns the pq encoder.
func (m *MockVectorIndex) GetPQEncoder() *pq.PQEncoder {
	args := m.Called()
	return args.Get(0).(*pq.PQEncoder)
}

// Close runs the close operation.
func (m *MockVectorIndex) Close() error {
	args := m.Called()
	return args.Error(0)
}

// AddBatch runs the add batch operation.
func (m *MockVectorIndex) AddBatch(ctx context.Context, recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	args := m.Called(ctx, recs, rowIdxs, batchIdxs)
	return args.Get(0).([]uint32), args.Error(1)
}

// DeleteBatch runs the delete batch operation.
func (m *MockVectorIndex) DeleteBatch(ctx context.Context, ids []uint32) error {
	args := m.Called(ctx, ids)
	return args.Error(0)
}

// ExportState runs the export state operation.
func (m *MockVectorIndex) ExportState() ([]byte, error) {
	args := m.Called()
	return args.Get(0).([]byte), args.Error(1)
}

// ImportState runs the import state operation.
func (m *MockVectorIndex) ImportState(data []byte) error {
	args := m.Called(data)
	return args.Error(0)
}

// ExportGraph runs the export graph operation.
func (m *MockVectorIndex) ExportGraph(w io.Writer) error {
	args := m.Called(w)
	return args.Error(0)
}

// ImportGraph runs the import graph operation.
func (m *MockVectorIndex) ImportGraph(r io.Reader) error {
	args := m.Called(r)
	return args.Error(0)
}

// ExportDelta runs the export delta operation.
func (m *MockVectorIndex) ExportDelta(fromVersion uint64) (*types.DeltaSync, error) {
	args := m.Called(fromVersion)
	return args.Get(0).(*types.DeltaSync), args.Error(1)
}

// ApplyDelta runs the apply delta operation.
func (m *MockVectorIndex) ApplyDelta(delta *types.DeltaSync) error {
	args := m.Called(delta)
	return args.Error(0)
}

// RelocateToOffHeap runs the relocate to off heap operation.
func (m *MockVectorIndex) RelocateToOffHeap() error {
	args := m.Called()
	return args.Error(0)
}

// ReleaseMonolithicChunk runs the release monolithic chunk operation.
func (m *MockVectorIndex) ReleaseMonolithicChunk(cID int) error {
	args := m.Called(cID)
	return args.Error(0)
}

// RemapLocations runs the remap locations operation.
func (m *MockVectorIndex) RemapLocations(ctx context.Context, mapping map[uint32]any) error {
	args := m.Called(ctx, mapping)
	return args.Error(0)
}

// GetGPUIndex returns the gpu index.
func (m *MockVectorIndex) GetGPUIndex() any {
	args := m.Called()
	return args.Get(0)
}

// SetParallelSearchConfig sets the parallel search config.
func (m *MockVectorIndex) SetParallelSearchConfig(cfg types.ParallelSearchConfig) {
	m.Called(cfg)
}

// GetParallelSearchConfig returns the parallel search config.
func (m *MockVectorIndex) GetParallelSearchConfig() types.ParallelSearchConfig {
	args := m.Called()
	return args.Get(0).(types.ParallelSearchConfig)
}

// SearchVectorsInRange runs the search vectors in range operation.
func (m *MockVectorIndex) SearchVectorsInRange(ctx context.Context, q any, threshold float32, filters []core.Filter, options any) ([]SearchResult, error) {
	args := m.Called(ctx, q, threshold, filters, options)
	return args.Get(0).([]SearchResult), args.Error(1)
}

// GetIndexType returns the index type.
func (m *MockVectorIndex) GetIndexType() string {
	args := m.Called()
	return args.String(0)
}

// Additional interface requirements?
// Note: Some methods in interfaces.go used types.VectorIndexer aliases.
// GetPQEncoder returns *pq.PQEncoder which is internal. We skip precise mocking unless needed.

// TestShardedHNSW_WithRefactor checks if we can inject a custom factory.
func TestShardedHNSW_WithRefactor(t *testing.T) {
	mockIndex := new(MockVectorIndex)

	config := ShardedHNSWConfig{
		NumShards: 2,
		Dimension: 128,
		IndexFactory: func(shardIdx int) VectorIndex {
			return mockIndex
		},
	}

	s := NewShardedHNSW(config, nil).(*ShardedHNSW)
	if len(s.shards) != 2 {
		t.Errorf("expected 2 shards, got %d", len(s.shards))
	}

	for i, shard := range s.shards {
		if shard.index != VectorIndex(mockIndex) {
			t.Errorf("shard %d index is not the mock index", i)
		}
	}
}
