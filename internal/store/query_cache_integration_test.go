package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockQueryCacheActionServer implements FlightService_DoActionServer for testing
type mockQueryCacheActionServer struct {
	flight.FlightService_DoActionServer
	ctx     context.Context
	sent    []*flight.Result
	sendErr error
}

func (m *mockQueryCacheActionServer) Send(r *flight.Result) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.sent = append(m.sent, r)
	return nil
}

func (m *mockQueryCacheActionServer) Context() context.Context {
	return m.ctx
}

func TestVectorStore_QueryCache_Integration(t *testing.T) {
	mem := memory.NewGoAllocator()
	store := NewVectorStore(mem, zerolog.Nop(), 1024*1024*1024, 1024*1024*100, 1*time.Hour)
	defer store.Close()

	// 1. Create a dataset with some data
	dsName := "cache_test_ds"
	rec := createCacheTestRecordBatch(t, 100)
	defer rec.Release()
	err := store.StoreRecordBatch(context.Background(), dsName, rec)
	require.NoError(t, err)

	// Wait for indexing
	// Wait for dataset creation and indexing
	require.Eventually(t, func() bool {
		ds, ok := store.getDataset(dsName)
		if !ok {
			t.Logf("Dataset %s not found yet", dsName)
			return false
		}
		indexLen := ds.IndexLen()
		t.Logf("Dataset found. IndexLen: %d", indexLen)
		return indexLen > 0
	}, 10*time.Second, 500*time.Millisecond)

	// 2. Prepare HybridSearch Action
	req := map[string]interface{}{
		"dataset": dsName,
		"vector":  []float32{0.1, 0.2}, // 2 dims? verify createTestRecordBatch dims. usually 128.
		// Wait, createTestRecordBatch uses 128 dims. Let's make a zero vector of 128 dims.
		"k":          5,
		"text_query": "test",
		"alpha":      0.5,
	}
	// Correction: createTestRecordBatch creates 128 dim vectors.
	vec := make([]float32, 128)
	req["vector"] = vec

	body, _ := json.Marshal(req)
	action := &flight.Action{
		Type: "HybridSearch",
		Body: body,
	}

	// 3. First Call - Should be MISS (slower, executes search)
	// We can't easily measure time diff in micro-test without sleep, but we can check if results match
	// and if subsequent calls return identical results without error.
	// To verify HIT, we might need to inspect metrics or internal state.
	// Store exposes queryCache? It is private field.
	// We can check metrics: "query_cache_hits_total", "query_cache_misses_total"
	// But metrics are global. We can reset or check diff.

	mockStream := &mockQueryCacheActionServer{ctx: context.Background()}
	start := time.Now()
	err = store.DoAction(action, mockStream)
	require.NoError(t, err)
	duration1 := time.Since(start)
	require.Len(t, mockStream.sent, 1)

	// Decode result 1
	var res1 []SearchResult
	json.Unmarshal(mockStream.sent[0].Body, &res1)
	assert.NotEmpty(t, res1)

	// 4. Second Call - Should be HIT (faster)
	mockStream2 := &mockQueryCacheActionServer{ctx: context.Background()}
	start = time.Now()
	err = store.DoAction(action, mockStream2)
	require.NoError(t, err)
	duration2 := time.Since(start)

	var res2 []SearchResult
	json.Unmarshal(mockStream2.sent[0].Body, &res2)
	assert.Equal(t, res1, res2, "Results should be identical")

	t.Logf("First call: %v, Second call: %v", duration1, duration2)
	// Can't strictly assert duration2 < duration1 in CI due to noise, but usually true.

	// 5. Verify Metrics (optional/advanced)
	// If we had access to metrics registry...

	// 6. Test Expiry
	// We configured global cache but TTL is 60s in NewVectorStore.
	// We can't wait 60s.
	// We can try to manually expire or overwrite?
	// Can't access cache directly.
}

func createCacheTestRecordBatch(t *testing.T, rows int) arrow.RecordBatch {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	idB := b.Field(0).(*array.Int64Builder)
	vecB := b.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < rows; i++ {
		idB.Append(int64(i))
		vecB.Append(true)
		for j := 0; j < 128; j++ {
			valB.Append(float32(0.1))
		}
	}

	return b.NewRecordBatch()
}
