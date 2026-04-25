package store

import (
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/mock"
	"io"
)

// MockVectorIndex is a mock implementation of VectorIndex interface
type MockVectorIndex struct {
	mock.Mock
}

func (m *MockVectorIndex) AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	args := m.Called(ctx, batchIdx, rowIdx)
	return args.Get(0).(uint32), args.Error(1)
}

func (m *MockVectorIndex) AddByRecord(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	args := m.Called(ctx, rec, rowIdx, batchIdx)
	return args.Get(0).(uint32), args.Error(1)
}

func (m *MockVectorIndex) Search(ctx context.Context, qv any, k int, filter any) ([]types.Candidate, error) {
	args := m.Called(ctx, qv, k, filter)
	return args.Get(0).([]types.Candidate), args.Error(1)
}

func (m *MockVectorIndex) SearchVectors(ctx context.Context, q any, k int, filters []query.Filter, options any) ([]SearchResult, error) {
	args := m.Called(ctx, q, k, filters, options)
	return args.Get(0).([]SearchResult), args.Error(1)
}

func (m *MockVectorIndex) SearchVectorsWithBitmap(ctx context.Context, q any, k int, filter *roaring.Bitmap, options any) ([]SearchResult, error) {
	args := m.Called(ctx, q, k, filter, options)
	return args.Get(0).([]SearchResult), args.Error(1)
}

func (m *MockVectorIndex) IsSharded() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockVectorIndex) Size() int {
	args := m.Called()
	return args.Int(0)
}

func (m *MockVectorIndex) Len() int {
	args := m.Called()
	return args.Int(0)
}

func (m *MockVectorIndex) GetEntryPoint() uint32 {
	args := m.Called()
	return args.Get(0).(uint32)
}

func (m *MockVectorIndex) GetLocation(id uint32) (any, bool) {
	args := m.Called(id)
	return args.Get(0), args.Bool(1)
}

func (m *MockVectorIndex) GetVectorID(loc any) (uint32, bool) {
	args := m.Called(loc)
	return args.Get(0).(uint32), args.Bool(1)
}

func (m *MockVectorIndex) GetDimension() uint32 {
	args := m.Called()
	return args.Get(0).(uint32)
}

func (m *MockVectorIndex) SetIndexedColumns(cols []string) {
	m.Called(cols)
}

func (m *MockVectorIndex) GetRawNeighbors(id uint32) ([]uint32, error) {
	args := m.Called(id)
	return args.Get(0).([]uint32), args.Error(1)
}

func (m *MockVectorIndex) GetNeighbors(ctx context.Context, id uint32, k int) ([]types.SearchResult, error) {
	args := m.Called(ctx, id, k)
	return args.Get(0).([]types.SearchResult), args.Error(1)
}

func (m *MockVectorIndex) PreWarm(targetSize int) {
	m.Called(targetSize)
}

func (m *MockVectorIndex) Warmup() int {
	args := m.Called()
	return args.Int(0)
}

func (m *MockVectorIndex) EstimateMemory() int64 {
	args := m.Called()
	return args.Get(0).(int64)
}

func (m *MockVectorIndex) TrainPQ(vectors [][]float32) error {
	args := m.Called(vectors)
	return args.Error(0)
}

func (m *MockVectorIndex) GetPQEncoder() *pq.PQEncoder {
	args := m.Called()
	return args.Get(0).(*pq.PQEncoder)
}

func (m *MockVectorIndex) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockVectorIndex) AddBatch(ctx context.Context, recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	args := m.Called(ctx, recs, rowIdxs, batchIdxs)
	return args.Get(0).([]uint32), args.Error(1)
}

func (m *MockVectorIndex) DeleteBatch(ctx context.Context, ids []uint32) error {
	args := m.Called(ctx, ids)
	return args.Error(0)
}

func (m *MockVectorIndex) ExportState() ([]byte, error) {
	args := m.Called()
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockVectorIndex) ImportState(data []byte) error {
	args := m.Called(data)
	return args.Error(0)
}

func (m *MockVectorIndex) ExportGraph(w io.Writer) error {
	args := m.Called(w)
	return args.Error(0)
}

func (m *MockVectorIndex) ImportGraph(r io.Reader) error {
	args := m.Called(r)
	return args.Error(0)
}

func (m *MockVectorIndex) ExportDelta(fromVersion uint64) (*types.DeltaSync, error) {
	args := m.Called(fromVersion)
	return args.Get(0).(*types.DeltaSync), args.Error(1)
}

func (m *MockVectorIndex) ApplyDelta(delta *types.DeltaSync) error {
	args := m.Called(delta)
	return args.Error(0)
}

func (m *MockVectorIndex) SetParallelSearchConfig(cfg types.ParallelSearchConfig) {
	m.Called(cfg)
}

func (m *MockVectorIndex) GetParallelSearchConfig() types.ParallelSearchConfig {
	args := m.Called()
	return args.Get(0).(types.ParallelSearchConfig)
}

func (m *MockVectorIndex) RemapLocations(ctx context.Context, mapping map[uint32]any) error {
	args := m.Called(ctx, mapping)
	return args.Error(0)
}

func (m *MockVectorIndex) SearchVectorsInRange(ctx context.Context, q any, threshold float32, filters []query.Filter, options any) ([]SearchResult, error) {
	args := m.Called(ctx, q, threshold, filters, options)
	return args.Get(0).([]SearchResult), args.Error(1)
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

	s := NewShardedHNSW(config, nil)
	if len(s.shards) != 2 {
		t.Errorf("expected 2 shards, got %d", len(s.shards))
	}

	for i, shard := range s.shards {
		if shard.index != VectorIndex(mockIndex) {
			t.Errorf("shard %d index is not the mock index", i)
		}
	}
}
