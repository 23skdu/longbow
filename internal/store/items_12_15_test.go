package store

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func buildRichBatch(t testing.TB, alloc memory.Allocator, rows []struct {
	id       string
	category string
	price    float64
	text     string
	vec      []float32
	metaJSON string
}) arrow.RecordBatch {
	t.Helper()
	dim := len(rows[0].vec)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "category", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "price", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "metadata", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32), Nullable: false},
	}, nil)

	idB := array.NewStringBuilder(alloc)
	defer idB.Release()
	catB := array.NewStringBuilder(alloc)
	defer catB.Release()
	priceB := array.NewFloat64Builder(alloc)
	defer priceB.Release()
	textB := array.NewStringBuilder(alloc)
	defer textB.Release()
	metaB := array.NewStringBuilder(alloc)
	defer metaB.Release()
	vecB := array.NewFixedSizeListBuilder(alloc, int32(dim), arrow.PrimitiveTypes.Float32)
	defer vecB.Release()
	vecValB := vecB.ValueBuilder().(*array.Float32Builder)

	for _, r := range rows {
		idB.Append(r.id)
		catB.Append(r.category)
		priceB.Append(r.price)
		textB.Append(r.text)
		metaB.Append(r.metaJSON)
		vecB.Append(true)
		for _, v := range r.vec {
			vecValB.Append(v)
		}
	}

	cols := []arrow.Array{
		idB.NewArray(),
		catB.NewArray(),
		priceB.NewArray(),
		textB.NewArray(),
		metaB.NewArray(),
		vecB.NewArray(),
	}
	for _, c := range cols {
		defer c.Release()
	}
	return array.NewRecordBatch(schema, cols, int64(len(rows)))
}

func TestItem12SchemaConstraintsEvolutionAndMigration(t *testing.T) {
	mem := memory.NewGoAllocator()
	reg := NewSchemaConstraintRegistry(nil)
	minP, maxP := 0.0, 500.0
	ver1, err := reg.RegisterSchema(DatasetSchemaSpec{
		Dataset:        "products",
		Compatibility:  CompatFull,
		StrictMetadata: true,
		MetadataTypes:  map[string]string{"tier": "string", "stock": "int"},
		Constraints: []FieldConstraint{
			{Field: "id", Type: "string", NotNull: true, Unique: true},
			{Field: "category", Type: "string", Enum: []string{"books", "electronics"}},
			{Field: "price", Type: "number", Min: &minP, Max: &maxP},
			{Field: "vector", VectorDim: 4},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, ver1)

	ctx := context.Background()
	good := buildRichBatch(t, mem, []struct {
		id       string
		category string
		price    float64
		text     string
		vec      []float32
		metaJSON string
	}{
		{"sku-1", "books", 19.99, "vector book", []float32{1, 0, 0, 0}, `{"tier":"pro","stock":12}`},
	})
	defer good.Release()
	require.NoError(t, reg.ValidateBatch(ctx, "products", good))

	// Duplicate UNIQUE id must fail
	require.Error(t, reg.ValidateBatch(ctx, "products", good))

	// Range CHECK failure (price=999 > 500)
	badRange := buildRichBatch(t, mem, []struct {
		id       string
		category string
		price    float64
		text     string
		vec      []float32
		metaJSON string
	}{
		{"sku-2", "books", 999.0, "expensive", []float32{1, 0, 0, 0}, `{"tier":"pro","stock":1}`},
	})
	defer badRange.Release()
	require.Error(t, reg.ValidateBatch(ctx, "products", badRange))

	// Backward/Full incompatible evolution (adding required NOT NULL field) must fail
	_, err = reg.RegisterSchema(DatasetSchemaSpec{
		Dataset:       "products",
		Compatibility: CompatFull,
		Constraints: []FieldConstraint{
			{Field: "id", Type: "string", NotNull: true, Unique: true},
			{Field: "new_required", Type: "string", NotNull: true},
		},
	})
	require.Error(t, err)
}

func TestItem13MaterializedViewsAndBench(t *testing.T) {
	mem := memory.NewGoAllocator()
	vs := NewVectorStore(mem, zerolog.Nop(), 64*1024*1024, 64*1024*1024, 10*time.Millisecond)
	vs.dataPath = t.TempDir()
	vs.StartIngestionWorkers(2)
	vs.StartIndexingWorkers(2)
	defer func() { _ = vs.Close() }()

	rec := buildRichBatch(t, mem, []struct {
		id       string
		category string
		price    float64
		text     string
		vec      []float32
		metaJSON string
	}{
		{"1", "us", 10.0, "item one", []float32{1, 0, 0, 0}, `{"region":"us"}`},
		{"2", "us", 30.0, "item two", []float32{0.9, 0.1, 0, 0}, `{"region":"us"}`},
		{"3", "eu", 50.0, "item three", []float32{0, 1, 0, 0}, `{"region":"eu"}`},
	})
	defer rec.Release()
	ctx := context.Background()
	require.NoError(t, vs.StoreRecordBatch(ctx, "orders_mv", rec))
	require.Eventually(t, func() bool {
		ds, ok := vs.getDataset("orders_mv")
		return ok && ds.GetRowCount() >= 3
	}, 2*time.Second, 20*time.Millisecond)

	mgr := vs.GetMaterializedViewManager()
	snap, err := mgr.CreateView(ctx, MaterializedViewSpec{
		Name:          "orders_by_cat",
		SourceDataset: "orders_mv",
		GroupBy:       []string{"category"},
		RefreshPolicy: RefreshStreaming,
		Aggregations: []AggSpec{
			{Alias: "cnt", Op: AggCount},
			{Alias: "total", Field: "price", Op: AggSum},
			{Alias: "mean", Field: "price", Op: AggAvg},
			{Alias: "lo", Field: "price", Op: AggMin},
			{Alias: "hi", Field: "price", Op: AggMax},
		},
	})
	require.NoError(t, err)
	require.Len(t, snap.Rows, 2)

	usRes, err := mgr.QueryView("orders_by_cat", map[string]string{"category": "us"})
	require.NoError(t, err)
	require.Len(t, usRes.Rows, 1)
	require.Equal(t, float64(2), usRes.Rows[0].Metrics["cnt"])
	require.Equal(t, float64(40), usRes.Rows[0].Metrics["total"])
	require.Equal(t, float64(20), usRes.Rows[0].Metrics["mean"])
	require.Equal(t, float64(10), usRes.Rows[0].Metrics["lo"])
	require.Equal(t, float64(30), usRes.Rows[0].Metrics["hi"])

	// Query planner integration hit
	_, hit := mgr.TryPlanQuery("orders_mv", []string{"category"}, []AggSpec{{Field: "price", Op: AggSum}})
	require.True(t, hit)
}

func BenchmarkMaterializedViewAggregation(b *testing.B) {
	mem := memory.NewGoAllocator()
	rec := buildRichBatch(b, mem, []struct {
		id       string
		category string
		price    float64
		text     string
		vec      []float32
		metaJSON string
	}{
		{"1", "us", 10.0, "a", []float32{1, 0, 0, 0}, `{}`},
		{"2", "eu", 20.0, "b", []float32{0, 1, 0, 0}, `{}`},
		{"3", "us", 30.0, "c", []float32{1, 0, 0, 0}, `{}`},
	})
	defer rec.Release()
	spec := MaterializedViewSpec{
		GroupBy:      []string{"category"},
		Aggregations: []AggSpec{{Alias: "sum_p", Field: "price", Op: AggSum}},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := AggregateBatches([]arrow.RecordBatch{rec}, nil, spec)
		if len(rows) != 2 {
			b.Fatal("unexpected groups")
		}
	}
}

func TestItems14And15TextSearchFacetsAnalyticsAndProvenance(t *testing.T) {
	mem := memory.NewGoAllocator()
	vs := NewVectorStore(mem, zerolog.Nop(), 64*1024*1024, 64*1024*1024, 10*time.Millisecond)
	vs.dataPath = t.TempDir()
	vs.StartIngestionWorkers(2)
	vs.StartIndexingWorkers(2)
	defer func() { _ = vs.Close() }()

	rec := buildRichBatch(t, mem, []struct {
		id       string
		category string
		price    float64
		text     string
		vec      []float32
		metaJSON string
	}{
		{"doc-1", "security", 0, "Distributed vector database encryption with zero trust controls", []float32{1, 0, 0, 0}, `{"category":"security","author":"alice"}`},
		{"doc-2", "security", 0, "Basic password auth without vector database encryption", []float32{0.8, 0.2, 0, 0}, `{"category":"security","author":"bob"}`},
		{"doc-3", "storage", 0, "Columnar Apache Arrow snapshot storage engine", []float32{0, 1, 0, 0}, `{"category":"storage","author":"alice"}`},
	})
	defer rec.Release()

	ctx := context.Background()
	require.NoError(t, vs.StoreRecordBatch(ctx, "kb_docs", rec))
	require.Eventually(t, func() bool {
		ds, ok := vs.getDataset("kb_docs")
		return ok && ds.IndexLen() >= 3
	}, 2*time.Second, 20*time.Millisecond)

	// Item 14: Tune BM25, execute Phrase + Boolean NOT + Fuzzy query + Facets + Analytics
	tsm := vs.GetTextSearchManager()
	require.NoError(t, tsm.SetBM25Params("kb_docs", BM25TuningParams{K1: 1.5, B: 0.6}))
	resp, err := tsm.Search(ctx, AdvancedTextQueryRequest{
		Dataset:     "kb_docs",
		Query:       `"vector database" AND encrytpion~1 AND NOT password`,
		K:           5,
		FacetFields: []string{"category", "author"},
	})
	require.NoError(t, err)
	require.Equal(t, 1, resp.TotalHits)
	require.Equal(t, "doc-1", resp.Hits[0].ID)
	require.Contains(t, resp.Hits[0].Snippet, "<mark>")
	require.Equal(t, 1, resp.Facets["category"][0].Count)

	// Zero-result query recorded in search analytics
	zeroResp, err := tsm.Search(ctx, AdvancedTextQueryRequest{Dataset: "kb_docs", Query: "nonexistenttermxyz", K: 5})
	require.NoError(t, err)
	require.Equal(t, 0, zeroResp.TotalHits)

	stats := tsm.Analytics(10)
	require.Equal(t, int64(2), stats.TotalQueries)
	require.Equal(t, int64(1), stats.ZeroResultQueries)
	require.NotEmpty(t, stats.ZeroResultTop)

	// Item 15: Record Provenance & ExplainSearch (with HNSW hop trace + score breakdown)
	pm := vs.GetProvenanceManager()
	require.NoError(t, pm.Record(VectorProvenance{
		Dataset:      "kb_docs",
		VectorID:     "0",
		SourceURI:    "s3://docs/security.pdf",
		Model:        "text-embedding-3-large",
		ModelVersion: "2026-01",
		ChunkIndex:   4,
	}))
	exp, err := pm.ExplainSearch(ctx, ExplainSearchRequest{
		Dataset:   "kb_docs",
		Vector:    []float32{1, 0, 0, 0},
		TextQuery: "encryption",
		K:         2,
		Alpha:     0.6,
	})
	require.NoError(t, err)
	require.NotEmpty(t, exp.Candidates)
	require.Greater(t, exp.Candidates[0].Breakdown.FinalScore, 0.0)
	require.NotEmpty(t, exp.Hops)
	require.NotNil(t, exp.Candidates[0].Provenance)
	require.Equal(t, "s3://docs/security.pdf", exp.Candidates[0].Provenance.SourceURI)
}

func FuzzItems12To15ParsersAndEval(f *testing.F) {
	f.Add(`"vector db" AND (auth~1 OR NOT legacy)`, "vector db auth encryption")
	f.Add(`(((broken`, "")

	f.Fuzz(func(t *testing.T, query, doc string) {
		ast, err := ParseAdvancedTextQuery(query)
		if err == nil && ast != nil {
			_, matched := ast.evaluate(doc, tokenizeAlphaNum(doc), 1)
			_, _ = GenerateSnippetHighlight(doc, matched, "<mark>", "</mark>", 80)
		}
		_ = LevenshteinDistance(query, doc)
		_ = CheckEvolutionCompatibility(
			DatasetSchemaSpec{Constraints: []FieldConstraint{{Field: "a", NotNull: true}}},
			DatasetSchemaSpec{Constraints: []FieldConstraint{{Field: "b", NotNull: false}}},
			CompatBackward,
		)
	})
}
