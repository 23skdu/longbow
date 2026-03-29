package store

import (
	"context"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func FuzzRecommend_EdgeCases(f *testing.F) {
	f.Add(float32(0.5), 2, float32(0.5), 10)
	f.Add(float32(0.0), 1, float32(0.0), 5)
	f.Add(float32(1.0), 3, float32(1.0), 20)
	f.Add(float32(0.5), 0, float32(0.5), 1)
	f.Add(float32(0.5), 10, float32(2.0), 100)

	f.Fuzz(func(t *testing.T, alpha float32, maxHops int, decay float32, k int) {
		mem := memory.NewGoAllocator()
		logger := zerolog.Nop()
		vs := NewVectorStore(mem, logger, 1024*1024, 0, 0)
		defer func() { _ = vs.Close() }()
		ctx := context.Background()

		datasetName := "fuzz_test"
		dims := 2

		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
		}, nil)

		b := array.NewRecordBuilder(mem, schema)
		defer b.Release()

		vectors := [][]float32{{1, 0}, {1.1, 0.1}, {0, 1}, {-1, -1}, {2, 2}}
		b.Field(0).(*array.Uint64Builder).AppendValues([]uint64{10, 11, 20, 30, 40}, nil)
		vb := b.Field(1).(*array.FixedSizeListBuilder)
		vvb := vb.ValueBuilder().(*array.Float32Builder)

		for _, v := range vectors {
			vb.Append(true)
			vvb.AppendValues(v, nil)
		}

		rec := b.NewRecordBatch()
		defer rec.Release()

		err := vs.applyReplayBatch(datasetName, rec, 1, time.Now().UnixNano())
		if err != nil {
			return
		}

		time.Sleep(100 * time.Millisecond)

		ds, err := vs.GetDataset(datasetName)
		if err != nil {
			return
		}

		ds.dataMu.Lock()
		if ds.Graph == nil {
			ds.Graph = NewGraphStore()
		}
		_ = ds.Graph.AddEdge(Edge{Subject: 0, Predicate: "related", Object: 1, Weight: 1.0})
		_ = ds.Graph.AddEdge(Edge{Subject: 1, Predicate: "related", Object: 2, Weight: 1.0})
		_ = ds.Graph.AddEdge(Edge{Subject: 2, Predicate: "related", Object: 3, Weight: 1.0})
		_ = ds.Graph.AddEdge(Edge{Subject: 3, Predicate: "related", Object: 4, Weight: 1.0})
		ds.dataMu.Unlock()

		if k <= 0 {
			k = 1
		}
		if k > 100 {
			k = 100
		}

		req := &query.RecommendRequest{
			Dataset: datasetName,
			SeedIDs: []string{"10"},
			K:       k,
			Alpha:   alpha,
			MaxHops: maxHops,
			Decay:   decay,
		}

		results, err := vs.Recommend(ctx, req)
		if err != nil {
			if len(req.SeedIDs) == 0 {
				assert.Error(t, err)
				return
			}
			t.Logf("Recommend returned error for alpha=%f, maxHops=%d, decay=%f, k=%d: %v", alpha, maxHops, decay, k, err)
			return
		}

		assert.NotNil(t, results)
		if k > len(vectors) {
			assert.LessOrEqual(t, len(results), len(vectors))
		} else {
			assert.LessOrEqual(t, len(results), k)
		}

		for _, r := range results {
			assert.GreaterOrEqual(t, uint64(r.ID), uint64(0))
		}
	})
}

func FuzzRecommend_AlphaRange(f *testing.F) {
	f.Add(float32(0.0))
	f.Add(float32(0.25))
	f.Add(float32(0.5))
	f.Add(float32(0.75))
	f.Add(float32(1.0))
	f.Add(float32(-0.1))
	f.Add(float32(1.5))
	f.Add(float32(2.0))

	f.Fuzz(func(t *testing.T, alpha float32) {
		mem := memory.NewGoAllocator()
		logger := zerolog.Nop()
		vs := NewVectorStore(mem, logger, 1024*1024, 0, 0)
		defer func() { _ = vs.Close() }()
		ctx := context.Background()

		datasetName := "alpha_fuzz_test"
		dims := 2

		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
		}, nil)

		b := array.NewRecordBuilder(mem, schema)
		defer b.Release()

		vectors := [][]float32{{1, 0}, {0, 1}, {-1, 0}, {0, -1}}
		b.Field(0).(*array.Uint64Builder).AppendValues([]uint64{1, 2, 3, 4}, nil)
		vb := b.Field(1).(*array.FixedSizeListBuilder)
		vvb := vb.ValueBuilder().(*array.Float32Builder)

		for _, v := range vectors {
			vb.Append(true)
			vvb.AppendValues(v, nil)
		}

		rec := b.NewRecordBatch()
		defer rec.Release()

		err := vs.applyReplayBatch(datasetName, rec, 1, time.Now().UnixNano())
		if err != nil {
			return
		}

		time.Sleep(100 * time.Millisecond)

		ds, err := vs.GetDataset(datasetName)
		if err != nil {
			return
		}

		ds.dataMu.Lock()
		if ds.Graph == nil {
			ds.Graph = NewGraphStore()
		}
		_ = ds.Graph.AddEdge(Edge{Subject: 0, Predicate: "related", Object: 1, Weight: 1.0})
		_ = ds.Graph.AddEdge(Edge{Subject: 1, Predicate: "related", Object: 2, Weight: 1.0})
		_ = ds.Graph.AddEdge(Edge{Subject: 2, Predicate: "related", Object: 3, Weight: 1.0})
		ds.dataMu.Unlock()

		req := &query.RecommendRequest{
			Dataset: datasetName,
			SeedIDs: []string{"1"},
			K:       2,
			Alpha:   alpha,
			MaxHops: 2,
			Decay:   0.5,
		}

		results, err := vs.Recommend(ctx, req)
		if err != nil {
			t.Logf("Recommend error with alpha=%f: %v", alpha, err)
			return
		}

		assert.NotNil(t, results)
		assert.LessOrEqual(t, len(results), 2)
	})
}

func FuzzRecommend_DecayRange(f *testing.F) {
	f.Add(float32(0.0))
	f.Add(float32(0.1))
	f.Add(float32(0.5))
	f.Add(float32(1.0))
	f.Add(float32(-0.1))
	f.Add(float32(1.5))

	f.Fuzz(func(t *testing.T, decay float32) {
		mem := memory.NewGoAllocator()
		logger := zerolog.Nop()
		vs := NewVectorStore(mem, logger, 1024*1024, 0, 0)
		defer func() { _ = vs.Close() }()
		ctx := context.Background()

		datasetName := "decay_fuzz_test"
		dims := 2

		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
		}, nil)

		b := array.NewRecordBuilder(mem, schema)
		defer b.Release()

		vectors := [][]float32{{1, 0}, {2, 0}, {3, 0}, {4, 0}, {5, 0}}
		b.Field(0).(*array.Uint64Builder).AppendValues([]uint64{1, 2, 3, 4, 5}, nil)
		vb := b.Field(1).(*array.FixedSizeListBuilder)
		vvb := vb.ValueBuilder().(*array.Float32Builder)

		for _, v := range vectors {
			vb.Append(true)
			vvb.AppendValues(v, nil)
		}

		rec := b.NewRecordBatch()
		defer rec.Release()

		err := vs.applyReplayBatch(datasetName, rec, 1, time.Now().UnixNano())
		if err != nil {
			return
		}

		time.Sleep(100 * time.Millisecond)

		ds, err := vs.GetDataset(datasetName)
		if err != nil {
			return
		}

		ds.dataMu.Lock()
		if ds.Graph == nil {
			ds.Graph = NewGraphStore()
		}
		_ = ds.Graph.AddEdge(Edge{Subject: 0, Predicate: "related", Object: 1, Weight: 1.0})
		_ = ds.Graph.AddEdge(Edge{Subject: 1, Predicate: "related", Object: 2, Weight: 1.0})
		_ = ds.Graph.AddEdge(Edge{Subject: 2, Predicate: "related", Object: 3, Weight: 1.0})
		_ = ds.Graph.AddEdge(Edge{Subject: 3, Predicate: "related", Object: 4, Weight: 1.0})
		ds.dataMu.Unlock()

		req := &query.RecommendRequest{
			Dataset: datasetName,
			SeedIDs: []string{"1"},
			K:       3,
			Alpha:   0.5,
			MaxHops: 3,
			Decay:   decay,
		}

		results, err := vs.Recommend(ctx, req)
		if err != nil {
			t.Logf("Recommend error with decay=%f: %v", decay, err)
			return
		}

		assert.NotNil(t, results)
		assert.LessOrEqual(t, len(results), 3)
	})
}

func FuzzRecommend_MaxHopsRange(f *testing.F) {
	f.Add(0)
	f.Add(1)
	f.Add(2)
	f.Add(5)
	f.Add(10)
	f.Add(-1)
	f.Add(100)

	f.Fuzz(func(t *testing.T, maxHops int) {
		mem := memory.NewGoAllocator()
		logger := zerolog.Nop()
		vs := NewVectorStore(mem, logger, 1024*1024, 0, 0)
		defer func() { _ = vs.Close() }()
		ctx := context.Background()

		datasetName := "hops_fuzz_test"
		dims := 2

		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
		}, nil)

		b := array.NewRecordBuilder(mem, schema)
		defer b.Release()

		vectors := [][]float32{{1, 0}, {1, 1}, {0, 1}, {-1, 1}, {-1, 0}}
		b.Field(0).(*array.Uint64Builder).AppendValues([]uint64{10, 11, 12, 13, 14}, nil)
		vb := b.Field(1).(*array.FixedSizeListBuilder)
		vvb := vb.ValueBuilder().(*array.Float32Builder)

		for _, v := range vectors {
			vb.Append(true)
			vvb.AppendValues(v, nil)
		}

		rec := b.NewRecordBatch()
		defer rec.Release()

		err := vs.applyReplayBatch(datasetName, rec, 1, time.Now().UnixNano())
		if err != nil {
			return
		}

		time.Sleep(100 * time.Millisecond)

		ds, err := vs.GetDataset(datasetName)
		if err != nil {
			return
		}

		ds.dataMu.Lock()
		if ds.Graph == nil {
			ds.Graph = NewGraphStore()
		}
		_ = ds.Graph.AddEdge(Edge{Subject: 0, Predicate: "related", Object: 1, Weight: 1.0})
		_ = ds.Graph.AddEdge(Edge{Subject: 1, Predicate: "related", Object: 2, Weight: 1.0})
		_ = ds.Graph.AddEdge(Edge{Subject: 2, Predicate: "related", Object: 3, Weight: 1.0})
		_ = ds.Graph.AddEdge(Edge{Subject: 3, Predicate: "related", Object: 4, Weight: 1.0})
		ds.dataMu.Unlock()

		req := &query.RecommendRequest{
			Dataset: datasetName,
			SeedIDs: []string{"10"},
			K:       3,
			Alpha:   0.5,
			MaxHops: maxHops,
			Decay:   0.5,
		}

		results, err := vs.Recommend(ctx, req)
		if err != nil {
			t.Logf("Recommend error with maxHops=%d: %v", maxHops, err)
			return
		}

		assert.NotNil(t, results)
		assert.LessOrEqual(t, len(results), 3)
	})
}

func TestRecommend(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	vs := NewVectorStore(mem, logger, 1024*1024, 0, 0)
	defer func() { _ = vs.Close() }()
	ctx := context.Background()

	datasetName := "rec_test"
	dims := 2

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	vectors := [][]float32{{1, 0}, {1.1, 0.1}, {0, 1}, {-1, -1}}
	b.Field(0).(*array.Uint64Builder).AppendValues([]uint64{10, 11, 20, 30}, nil)
	vb := b.Field(1).(*array.FixedSizeListBuilder)
	vvb := vb.ValueBuilder().(*array.Float32Builder)

	for _, v := range vectors {
		vb.Append(true)
		vvb.AppendValues(v, nil)
	}

	rec := b.NewRecordBatch()
	defer rec.Release()

	err := vs.applyReplayBatch(datasetName, rec, 1, time.Now().UnixNano())
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	ds, err := vs.GetDataset(datasetName)
	require.NoError(t, err)

	ds.dataMu.Lock()
	if ds.Graph == nil {
		ds.Graph = NewGraphStore()
	}
	_ = ds.Graph.AddEdge(Edge{Subject: 0, Predicate: "related", Object: 1, Weight: 1.0})
	_ = ds.Graph.AddEdge(Edge{Subject: 1, Predicate: "related", Object: 2, Weight: 1.0})
	ds.dataMu.Unlock()

	t.Run("SingleSeed_Hybrid", func(t *testing.T) {
		req := &query.RecommendRequest{
			Dataset: datasetName,
			SeedIDs: []string{"10"},
			K:       2,
			Alpha:   0.5,
			MaxHops: 1,
			Decay:   0.5,
		}
		results, err := vs.Recommend(ctx, req)
		require.NoError(t, err)
		assert.Len(t, results, 2)
		assert.Equal(t, VectorID(10), results[0].ID)
		assert.Equal(t, VectorID(11), results[1].ID)
	})

	t.Run("MultiSeed_Centroid", func(t *testing.T) {
		req := &query.RecommendRequest{
			Dataset: datasetName,
			SeedIDs: []string{"10", "20"},
			K:       2,
			Alpha:   1.0,
		}
		results, err := vs.Recommend(ctx, req)
		require.NoError(t, err)
		assert.Len(t, results, 2)
		ids := []VectorID{results[0].ID, results[1].ID}
		assert.Contains(t, ids, VectorID(10))
		assert.Contains(t, ids, VectorID(20))
	})

	t.Run("AlphaTuning_PureANN", func(t *testing.T) {
		req := &query.RecommendRequest{
			Dataset: datasetName,
			SeedIDs: []string{"30"},
			K:       1,
			Alpha:   1.0,
		}
		results, err := vs.Recommend(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, VectorID(30), results[0].ID)
	})

	t.Run("InvalidSeed", func(t *testing.T) {
		req := &query.RecommendRequest{
			Dataset: datasetName,
			SeedIDs: []string{"999"},
			K:       5,
		}
		_, err := vs.Recommend(ctx, req)
		assert.Error(t, err)
	})
}
