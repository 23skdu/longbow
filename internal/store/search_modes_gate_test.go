package store

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type gateFixture struct {
	vs     *VectorStore
	alloc  memory.Allocator
	baseTs int64
}

func setupSearchModesGateFixture(tb testing.TB, count int) *gateFixture {
	tb.Helper()
	alloc := memory.NewGoAllocator()
	vs := NewVectorStore(alloc, zerolog.Nop(), 64*1024*1024, 64*1024*1024, 10*time.Millisecond)
	vs.dataPath = tb.TempDir()
	vs.SetHybridSearchConfig(HybridSearchConfig{
		Enabled:     true,
		Alpha:       0.5,
		TextColumns: []string{"text", "category"},
		BM25:        DefaultBM25Config(),
		RRFk:        60,
	})
	tCfg := DefaultTemporalConfig()
	tCfg.Enabled = true
	vs.SetTemporalIndex(tCfg)
	vs.StartIngestionWorkers(2)
	vs.StartIndexingWorkers(2)
	tb.Cleanup(func() { _ = vs.Close() })

	baseTs := int64(1_700_000_000_000_000_000)
	primary := buildSearchGateBatch(alloc, baseTs, count)
	defer primary.Release()
	require.NoError(tb, vs.StoreRecordBatch(context.Background(), "gate_primary", primary))

	secondary := buildSearchGateBatch(alloc, baseTs, 8)
	defer secondary.Release()
	require.NoError(tb, vs.StoreRecordBatch(context.Background(), "gate_secondary", secondary))

	ctx := context.Background()
	waitGateDataset(tb, vs, "gate_primary", count)
	waitGateDataset(tb, vs, "gate_secondary", 8)

	ds, ok := vs.getDataset("gate_primary")
	require.True(tb, ok)
	require.NotNil(tb, ds.Graph)
	require.NoError(tb, ds.Graph.AddEdge(Edge{Subject: 0, Predicate: "related", Object: 1, Weight: 1.0}))
	require.NoError(tb, ds.Graph.AddEdge(Edge{Subject: 1, Predicate: "related", Object: 2, Weight: 0.9}))

	vs.SetCoordinator(NewGlobalSearchCoordinator(zerolog.Nop(), nil))
	if pred := vs.GetIndexPredictor(); pred != nil {
		vs.RecordQueryPerformance(QueryFeatures{VectorDimension: 4, DatasetSize: count, SearchK: 5}, 0.5, 0.99, IndexTypeHNSW, "", "")
	}

	_, err := vs.GetMultiModalPipeline().IngestItems(ctx, "gate_mm", []MultiModalItem{
		{ID: "0", Modality: ModalityText, Caption: "quantum zero trust mesh encryption"},
		{ID: "1", Modality: ModalityText, Caption: "columnar apache arrow storage"},
		{ID: "2", Modality: ModalityText, Caption: "rdma transport fabric"},
	})
	require.NoError(tb, err)
	waitGateDataset(tb, vs, "gate_mm", 3)

	return &gateFixture{vs: vs, alloc: alloc, baseTs: baseTs}
}

func buildSearchGateBatch(alloc memory.Allocator, baseTs int64, count int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "rank", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "category", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "metadata", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: false},
		{Name: "geo_point", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float64), Nullable: false},
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32), Nullable: false},
	}, nil)

	idB := array.NewStringBuilder(alloc)
	defer idB.Release()
	rankB := array.NewInt64Builder(alloc)
	defer rankB.Release()
	activeB := array.NewBooleanBuilder(alloc)
	defer activeB.Release()
	catB := array.NewStringBuilder(alloc)
	defer catB.Release()
	textB := array.NewStringBuilder(alloc)
	defer textB.Release()
	metaB := array.NewStringBuilder(alloc)
	defer metaB.Release()
	tsB := array.NewTimestampBuilder(alloc, arrow.FixedWidthTypes.Timestamp_ns.(*arrow.TimestampType))
	defer tsB.Release()
	geoB := array.NewFixedSizeListBuilder(alloc, 2, arrow.PrimitiveTypes.Float64)
	defer geoB.Release()
	geoValB := geoB.ValueBuilder().(*array.Float64Builder)
	vecB := array.NewFixedSizeListBuilder(alloc, 4, arrow.PrimitiveTypes.Float32)
	defer vecB.Release()
	vecValB := vecB.ValueBuilder().(*array.Float32Builder)

	quarter := count / 4
	if quarter < 1 {
		quarter = 1
	}

	for i := 0; i < count; i++ {
		idB.Append(fmt.Sprintf("%d", i))
		rankB.Append(int64(i * 10))
		// Deterministic booleans: i%2 == 0 is active (so IDs 0,2,4,... are true; IDs 1,3,... are false)
		activeB.Append(i%2 == 0)

		var cat, txt string
		var lat, lon float64
		var v0, v1, v2, v3 float32
		eps := float32(i%quarter) * 0.01

		switch {
		case i < quarter:
			cat = "security"
			if i == 0 {
				txt = "encryption quantum zero trust mesh"
			} else {
				txt = "security perimeter firewall audit"
			}
			// San Francisco
			lat, lon = 37.7749+float64(i)*0.0001, -122.4194+float64(i)*0.0001
			v0, v1 = 1.0-eps, eps
		case i < 2*quarter:
			cat = "electronics"
			txt = "solid state semiconductor wafer fabrication"
			lat, lon = 37.7849, -122.4094
			v0, v1 = 0.8-eps, 0.2+eps
		case i < 3*quarter:
			cat = "storage"
			txt = "columnar apache arrow snapshot storage"
			// NYC (~4130 km from SF, guaranteed outside 5km SF radius and SF box)
			lat, lon = 40.7128, -74.0060
			v1 = 1.0 - eps
			v2 = eps
		default:
			cat = "network"
			txt = "high throughput rdma transport fabric"
			lat, lon = 37.7755, -122.4180
			v2 = 1.0 - eps
			v3 = eps
		}

		// L2 normalize so dot/cosine/l2 distances agree monotonically
		norm := float32(math.Sqrt(float64(v0*v0 + v1*v1 + v2*v2 + v3*v3)))
		if norm > 0 {
			v0 /= norm
			v1 /= norm
			v2 /= norm
			v3 /= norm
		}

		catB.Append(cat)
		textB.Append(txt)
		metaB.Append(fmt.Sprintf(`{"category":%q}`, cat))
		tsB.Append(arrow.Timestamp(baseTs - int64(count-i)*1000))
		geoB.Append(true)
		geoValB.Append(lat)
		geoValB.Append(lon)
		vecB.Append(true)
		vecValB.Append(v0)
		vecValB.Append(v1)
		vecValB.Append(v2)
		vecValB.Append(v3)
	}

	cols := []arrow.Array{
		idB.NewArray(), rankB.NewArray(), activeB.NewArray(), catB.NewArray(),
		textB.NewArray(), metaB.NewArray(), tsB.NewArray(), geoB.NewArray(), vecB.NewArray(),
	}
	for _, c := range cols {
		defer c.Release()
	}
	return array.NewRecordBatch(schema, cols, int64(count))
}

func waitGateDataset(tb testing.TB, vs *VectorStore, dataset string, want int) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ds, ok := vs.getDataset(dataset)
		if ok && ds.IndexLen() >= want {
			if ds.TemporalIndex == nil || ds.TemporalIndex.Size() >= want {
				return
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	tb.Fatalf("dataset %q failed to finish indexing %d rows within 5s", dataset, want)
}

type gateDoGetStream struct {
	*MockDoGetStream
	capture captureFlightStream
}

func (g *gateDoGetStream) Send(fd *flight.FlightData) error {
	return g.capture.Send(fd)
}

func decodeDoGetIDs(t testing.TB, vs *VectorStore, tq query.TicketQuery) []uint64 {
	t.Helper()
	raw, err := json.Marshal(tq)
	require.NoError(t, err)
	stream := &gateDoGetStream{MockDoGetStream: NewMockDoGetStream(context.Background())}
	require.NoError(t, vs.DoGet(&flight.Ticket{Ticket: raw}, stream))
	rdr, err := flight.NewRecordReader(&flightMsgReader{msgs: stream.capture.msgs})
	require.NoError(t, err)
	defer rdr.Release()

	var ids []uint64
	for rdr.Next() {
		rec := rdr.RecordBatch()
		idCol := rec.Column(0).(*array.Uint64)
		for i := 0; i < int(rec.NumRows()); i++ {
			ids = append(ids, idCol.Value(i))
		}
	}
	require.NoError(t, rdr.Err())
	return ids
}

func extractIDs(res []SearchResult) []uint64 {
	ids := make([]uint64, len(res))
	for i, r := range res {
		ids[i] = uint64(r.ID)
	}
	return ids
}

func TestSearchModesGate(t *testing.T) {
	const corpusSize = 64
	fx := setupSearchModesGateFixture(t, corpusSize)
	vs := fx.vs
	ctx := context.Background()
	q := []float32{1, 0, 0, 0}

	// Generous CI-safe per-query latency budget catching pathological stalls / quadratic scans.
	const maxQueryMean = 75 * time.Millisecond
	const timedIters = 5

	type gateCase struct {
		name string
		run  func(subT *testing.T) error
	}

	assertHasHits := func(subT *testing.T, res []SearchResult, mode string) {
		subT.Helper()
		require.NotEmpty(subT, res, "%s must return hits", mode)
	}

	cases := []gateCase{
		{
			name: "Dense",
			run: func(subT *testing.T) error {
				res, err := vs.SearchDatasetVectors(ctx, "gate_primary", q, 5, nil)
				if err != nil {
					return err
				}
				assertHasHits(subT, res, "Dense")
				assert.Equal(subT, uint64(0), uint64(res[0].ID))
				assert.NotContains(subT, extractIDs(res), uint64(32), "orthogonal NYC storage doc should not be top-5")
				return nil
			},
		},
		{
			name: "Hybrid",
			run: func(subT *testing.T) error {
				res, err := vs.SearchHybridWithFilters(ctx, "gate_primary", q, "encryption quantum", 5, 0.5, 60, 0, 0, false, nil)
				if err != nil {
					return err
				}
				assertHasHits(subT, res, "Hybrid")
				assert.Equal(subT, uint64(0), uint64(res[0].ID))
				return nil
			},
		},
		{
			name: "Filtered",
			run: func(subT *testing.T) error {
				filters := []core.Filter{{Field: "rank", Operator: ">=", Value: "200"}}
				res, err := vs.SearchDatasetVectors(ctx, "gate_primary", q, 5, filters)
				if err != nil {
					return err
				}
				assertHasHits(subT, res, "Filtered")
				for _, r := range res {
					assert.GreaterOrEqual(subT, uint64(r.ID), uint64(20), "filtered rank<200 ID leaked: %d", r.ID)
				}
				assert.Equal(subT, uint64(20), uint64(res[0].ID))
				return nil
			},
		},
		{
			name: "FilteredBool",
			run: func(subT *testing.T) error {
				filters := []core.Filter{{Field: "active", Operator: "==", Value: "true"}}
				res, err := vs.SearchDatasetVectors(ctx, "gate_primary", q, 8, filters)
				if err != nil {
					return err
				}
				assertHasHits(subT, res, "FilteredBool")
				for _, r := range res {
					assert.Equal(subT, uint64(0), uint64(r.ID)%2, "inactive (odd) ID leaked: %d", r.ID)
				}
				assert.Contains(subT, extractIDs(res), uint64(0))
				return nil
			},
		},
		{
			name: "FilteredString",
			run: func(subT *testing.T) error {
				filters := []core.Filter{{Field: "category", Operator: "==", Value: "electronics"}}
				res, err := vs.SearchDatasetVectors(ctx, "gate_primary", q, 5, filters)
				if err != nil {
					return err
				}
				assertHasHits(subT, res, "FilteredString")
				for _, r := range res {
					assert.GreaterOrEqual(subT, uint64(r.ID), uint64(16))
					assert.Less(subT, uint64(r.ID), uint64(32))
				}
				assert.Equal(subT, uint64(16), uint64(res[0].ID))
				return nil
			},
		},
		{
			name: "Sparse",
			run: func(subT *testing.T) error {
				res, err := vs.SearchHybridWithFilters(ctx, "gate_primary", nil, "encryption quantum", 5, 0.0, 60, 0, 0, false, nil)
				if err != nil {
					return err
				}
				assertHasHits(subT, res, "Sparse")
				ids := extractIDs(res)
				assert.Equal(subT, uint64(0), ids[0])
				assert.NotContains(subT, ids, uint64(32))
				assert.NotContains(subT, ids, uint64(48))
				return nil
			},
		},
		{
			name: "ByID",
			run: func(subT *testing.T) error {
				res, err := vs.SearchByID(ctx, &core.VectorSearchByIDRequest{Dataset: "gate_primary", ID: "0", K: 4})
				if err != nil {
					return err
				}
				assertHasHits(subT, res, "ByID")
				ids := extractIDs(res)
				assert.Equal(subT, uint64(0), ids[0])
				assert.NotContains(subT, ids, uint64(48))
				return nil
			},
		},
		{
			name: "Range",
			run: func(subT *testing.T) error {
				res, err := vs.RangeSearch(ctx, "gate_primary", q, 0.25, nil, 32)
				if err != nil {
					return err
				}
				assertHasHits(subT, res, "Range")
				ids := extractIDs(res)
				assert.Contains(subT, ids, uint64(0))
				assert.NotContains(subT, ids, uint64(32), "orthogonal vector exceeded range threshold")
				return nil
			},
		},
		{
			name: "AdvancedText",
			run: func(subT *testing.T) error {
				resp, err := vs.GetTextSearchManager().Search(ctx, AdvancedTextQueryRequest{
					Dataset:     "gate_primary",
					Query:       "encryption AND quantum",
					K:           5,
					FacetFields: []string{"category"},
				})
				if err != nil {
					return err
				}
				require.NotEmpty(subT, resp.Hits)
				assert.Equal(subT, "0", resp.Hits[0].ID)
				require.NotEmpty(subT, resp.Facets["category"])
				assert.Equal(subT, "security", resp.Facets["category"][0].Value)
				return nil
			},
		},
		{
			name: "GraphRAG",
			run: func(subT *testing.T) error {
				ids := decodeDoGetIDs(subT, vs, query.TicketQuery{Search: &query.VectorSearchRequest{
					Dataset: "gate_primary", Vector: q, K: 5, GraphAlpha: 0.5, GraphDepth: 2,
				}})
				require.NotEmpty(subT, ids)
				assert.Contains(subT, ids, uint64(0))
				assert.Contains(subT, ids, uint64(1))
				return nil
			},
		},
		{
			name: "GlobalGraphRAG",
			run: func(subT *testing.T) error {
				ids := decodeDoGetIDs(subT, vs, query.TicketQuery{Search: &query.VectorSearchRequest{
					Dataset: "gate_primary", Vector: q, K: 5, GraphAlpha: 0.5, LocalOnly: false,
				}})
				require.NotEmpty(subT, ids)
				assert.Contains(subT, ids, uint64(0))
				return nil
			},
		},
		{
			name: "Recommend",
			run: func(subT *testing.T) error {
				res, err := vs.Recommend(ctx, &query.RecommendRequest{
					Dataset: "gate_primary", SeedIDs: []string{"0", "1"}, K: 4, Alpha: 0.5, MaxHops: 2, Decay: 0.8,
				})
				if err != nil {
					return err
				}
				assertHasHits(subT, res, "Recommend")
				assert.NotContains(subT, extractIDs(res), uint64(48))
				return nil
			},
		},
		{
			name: "Geo",
			run: func(subT *testing.T) error {
				res, err := vs.GeoSearch(ctx, &types.GeoSearchRequest{
					Dataset: "gate_primary", SearchType: "radius",
					Center: core.GeoPoint{Lat: 37.7749, Lon: -122.4194}, RadiusKm: 5.0, K: 20,
				})
				if err != nil {
					return err
				}
				assertHasHits(subT, res, "Geo")
				ids := extractIDs(res)
				assert.NotContains(subT, ids, uint64(32), "NYC point leaked into 5km SF radius")
				return nil
			},
		},
		{
			name: "GeoBox",
			run: func(subT *testing.T) error {
				res, err := vs.GeoSearch(ctx, &types.GeoSearchRequest{
					Dataset: "gate_primary", SearchType: "box",
					Box: &types.GeoBoundingBox{MinLat: 37.7, MaxLat: 37.8, MinLon: -122.5, MaxLon: -122.4}, K: 20,
				})
				if err != nil {
					return err
				}
				assertHasHits(subT, res, "GeoBox")
				ids := extractIDs(res)
				assert.NotContains(subT, ids, uint64(32), "NYC point leaked into SF bounding box")
				return nil
			},
		},
		{
			name: "GeoHybrid",
			run: func(subT *testing.T) error {
				res, err := vs.GeoSearch(ctx, &types.GeoSearchRequest{
					Dataset: "gate_primary", SearchType: "hybrid",
					Center: core.GeoPoint{Lat: 37.7749, Lon: -122.4194}, RadiusKm: 5.0,
					QueryVector: q, K: 10,
				})
				if err != nil {
					return err
				}
				assertHasHits(subT, res, "GeoHybrid")
				assert.NotContains(subT, extractIDs(res), uint64(32))
				return nil
			},
		},
		{
			name: "Temporal",
			run: func(subT *testing.T) error {
				// Timestamp cutoff includes IDs 0..31 and excludes IDs >= 32 (timestamps > cutoff)
				cutoff := fx.baseTs - int64(corpusSize-31)*1000
				res, err := vs.TemporalSearch(ctx, &types.TemporalSearchRequest{
					Dataset: "gate_primary", SearchType: "as_of", Timestamp: cutoff, K: 10,
				})
				if err != nil {
					return err
				}
				assertHasHits(subT, res, "Temporal")
				for _, r := range res {
					assert.LessOrEqual(subT, uint64(r.ID), uint64(31), "future record leaked past as_of cutoff: %d", r.ID)
				}
				return nil
			},
		},
		{
			name: "TemporalRange",
			run: func(subT *testing.T) error {
				// Exact timestamp span covering rows 16..31
				startTs := fx.baseTs - int64(corpusSize-16)*1000
				endTs := fx.baseTs - int64(corpusSize-31)*1000
				res, err := vs.TemporalSearch(ctx, &types.TemporalSearchRequest{
					Dataset: "gate_primary", SearchType: "range", StartTime: startTs, EndTime: endTs, K: 16,
				})
				if err != nil {
					return err
				}
				assertHasHits(subT, res, "TemporalRange")
				for _, r := range res {
					assert.GreaterOrEqual(subT, uint64(r.ID), uint64(16))
					assert.LessOrEqual(subT, uint64(r.ID), uint64(31))
				}
				return nil
			},
		},
		{
			name: "TemporalWindow",
			run: func(subT *testing.T) error {
				res, err := vs.TemporalSearch(ctx, &types.TemporalSearchRequest{
					Dataset: "gate_primary", SearchType: "sliding_window", WindowSize: 4, K: 4,
				})
				if err != nil {
					return err
				}
				assertHasHits(subT, res, "TemporalWindow")
				assert.LessOrEqual(subT, len(res), 4)
				return nil
			},
		},
		{
			name: "MultiModal",
			run: func(subT *testing.T) error {
				res, err := vs.GetMultiModalPipeline().Search(ctx, MultiModalSearchRequest{
					Dataset: "gate_mm", Modality: ModalityText, TextQuery: "quantum encryption mesh", K: 2,
				})
				if err != nil {
					return err
				}
				assertHasHits(subT, res, "MultiModal")
				assert.Equal(subT, uint64(0), uint64(res[0].ID))
				return nil
			},
		},
		{
			name: "Federated",
			run: func(subT *testing.T) error {
				res, err := vs.FederatedSearch(ctx, []string{"gate_primary", "gate_secondary"}, nil, q, 5, 60)
				if err != nil {
					return err
				}
				assertHasHits(subT, res, "Federated")
				assert.Contains(subT, extractIDs(res), uint64(0))
				return nil
			},
		},
		{
			name: "LearnedIndex",
			run: func(subT *testing.T) error {
				ids := decodeDoGetIDs(subT, vs, query.TicketQuery{Search: &query.VectorSearchRequest{
					Dataset: "gate_primary", Vector: q, K: 4, EnableLearnedIndex: true,
				}})
				require.NotEmpty(subT, ids)
				assert.Equal(subT, uint64(0), ids[0])
				return nil
			},
		},
	}

	require.Len(t, cases, 21, "gate must cover all 21 search types")
	for _, tc := range cases {
		t.Run(tc.name, func(subT *testing.T) {
			require.NoError(subT, tc.run(subT), "accuracy check failed on warmup")

			start := time.Now()
			for i := 0; i < timedIters; i++ {
				require.NoError(subT, tc.run(subT))
			}
			mean := time.Since(start) / timedIters
			subT.Logf("search_gate mode=%-16s mean=%-10s budget=%s status=PASS", tc.name, mean.Round(time.Microsecond), maxQueryMean)
			assert.Less(subT, mean, maxQueryMean, "latency regression detected for mode %s", tc.name)
		})
	}
}

func BenchmarkSearchModesGate(b *testing.B) {
	fx := setupSearchModesGateFixture(b, 64)
	vs := fx.vs
	ctx := context.Background()
	q := []float32{1, 0, 0, 0}

	b.Run("Dense", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			res, err := vs.SearchDatasetVectors(ctx, "gate_primary", q, 5, nil)
			if err != nil || len(res) == 0 {
				b.Fatalf("dense failed: err=%v len=%d", err, len(res))
			}
		}
	})

	b.Run("FilteredString", func(b *testing.B) {
		filters := []core.Filter{{Field: "category", Operator: "==", Value: "electronics"}}
		b.ReportAllocs()
		for b.Loop() {
			res, err := vs.SearchDatasetVectors(ctx, "gate_primary", q, 5, filters)
			if err != nil || len(res) == 0 {
				b.Fatalf("filtered failed: err=%v len=%d", err, len(res))
			}
		}
	})

	b.Run("Hybrid", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			res, err := vs.SearchHybridWithFilters(ctx, "gate_primary", q, "encryption quantum", 5, 0.5, 60, 0, 0, false, nil)
			if err != nil || len(res) == 0 {
				b.Fatalf("hybrid failed: err=%v len=%d", err, len(res))
			}
		}
	})

	b.Run("Range", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			res, err := vs.RangeSearch(ctx, "gate_primary", q, 0.25, nil, 16)
			if err != nil || len(res) == 0 {
				b.Fatalf("range failed: err=%v len=%d", err, len(res))
			}
		}
	})

	b.Run("Geo", func(b *testing.B) {
		req := &types.GeoSearchRequest{
			Dataset: "gate_primary", SearchType: "radius",
			Center: core.GeoPoint{Lat: 37.7749, Lon: -122.4194}, RadiusKm: 5.0, K: 10,
		}
		b.ReportAllocs()
		for b.Loop() {
			res, err := vs.GeoSearch(ctx, req)
			if err != nil || len(res) == 0 {
				b.Fatalf("geo failed: err=%v len=%d", err, len(res))
			}
		}
	})

	b.Run("TemporalRange", func(b *testing.B) {
		req := &types.TemporalSearchRequest{
			Dataset: "gate_primary", SearchType: "range",
			StartTime: fx.baseTs - 48*1000, EndTime: fx.baseTs - 33*1000, K: 10,
		}
		b.ReportAllocs()
		for b.Loop() {
			res, err := vs.TemporalSearch(ctx, req)
			if err != nil || len(res) == 0 {
				b.Fatalf("temporal range failed: err=%v len=%d", err, len(res))
			}
		}
	})
}
