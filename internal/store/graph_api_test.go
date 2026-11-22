package store

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/23skdu/longbow/internal/store/cluster"

	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/23skdu/longbow/internal/metrics"
)

// TestGraphAPI_GetGraphStats runs the test graph api_get graph stats operation.
func TestGraphAPI_GetGraphStats(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	s := NewVectorStore(mem, logger, 1<<30, 0, time.Hour)
	meta := cluster.NewMetaServer(s)
	defer func() { _ = s.Close() }()
	defer func() { _ = meta.Close() }()

	// 1. Setup Dataset
	dsName := "graph_stats_test"
	_ = s.CreateNamespace(dsName)
	ds := &Dataset{
		Name:  dsName,
		Graph: NewGraphStore(),
	}
	s.UpdateDatasets(func(m map[string]*Dataset) {
		m[dsName] = ds
	})

	// 2. Add some data
	_ = ds.Graph.AddEdge(Edge{Subject: 1, Predicate: "knows", Object: 2, Weight: 1.0})
	_ = ds.Graph.AddEdge(Edge{Subject: 2, Predicate: "knows", Object: 3, Weight: 1.0})

	// 3. Call GetGraphStats
	req := map[string]string{"dataset": dsName}
	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	action := &flight.Action{
		Type: "GetGraphStats",
		Body: reqBytes,
	}

	stream := &mockFlightServer{}
	err = meta.DoAction(action, stream)
	require.NoError(t, err)

	require.Len(t, stream.results, 1)
	var resp struct {
		EdgeCount  int      `json:"edge_count"`
		NodeCount  int      `json:"node_count"`
		Predicates []string `json:"predicates"`
	}
	err = json.Unmarshal(stream.results[0].Body, &resp)
	require.NoError(t, err)
	require.Equal(t, 2, resp.EdgeCount)
	require.Equal(t, 2, resp.NodeCount, "nodes 1 and 2 have outgoing edges")
	require.Equal(t, []string{"knows"}, resp.Predicates)
}

// TestGraphAPI_GetGraphStats_NotFound runs the test graph api_get graph stats_not found operation.
func TestGraphAPI_GetGraphStats_NotFound(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	s := NewVectorStore(mem, logger, 1<<30, 0, time.Hour)
	defer func() { _ = s.Close() }()
	meta := cluster.NewMetaServer(s)
	defer func() { _ = meta.Close() }()

	req := map[string]string{"dataset": "non_existent"}
	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	action := &flight.Action{
		Type: "GetGraphStats",
		Body: reqBytes,
	}

	stream := &mockFlightServer{}
	err = meta.DoAction(action, stream)
	require.Error(t, err)
}

// TestGraphAPI_DetectCommunities_EmitsMetrics pins the two clustering metrics
// documented in docs/metrics.md. Both were declared but never emitted, so the
// docs promised series that Prometheus never saw.
func TestGraphAPI_DetectCommunities_EmitsMetrics(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	s := NewVectorStore(mem, logger, 1<<30, 0, time.Hour)
	meta := cluster.NewMetaServer(s)
	defer func() { _ = s.Close() }()
	defer func() { _ = meta.Close() }()

	dsName := "graph_metrics_test"
	_ = s.CreateNamespace(dsName)
	ds := &Dataset{
		Name:  dsName,
		Graph: NewGraphStore(),
	}
	s.UpdateDatasets(func(m map[string]*Dataset) {
		m[dsName] = ds
	})

	// Two disjoint triangles over the layer-0 graph the handler reads from.
	// handleDetectCommunities needs ds.Index exposing GetData(), so the
	// community detection runs on the HNSW GraphData, not GraphStore.
	cfg := DefaultArrowHNSWConfig()
	idx := NewArrowHNSW(ds, &cfg, nil)
	t.Cleanup(func() { _ = idx.Close() })
	ds.Index = idx

	gd := idx.GetData()
	require.NotNil(t, gd)
	adj := map[uint32][]uint32{
		1: {2, 3}, 2: {1, 3}, 3: {1, 2},
		10: {11, 12}, 11: {10, 12}, 12: {10, 11},
	}
	for id, nbrs := range adj {
		require.NoError(t, gd.SetNeighbors(id, nbrs))
	}
	before := testutil.ToFloat64(metrics.GraphCommunitiesTotal)
	histBefore := clusteringSampleCount(t)

	raw, err := json.Marshal(map[string]any{"dataset": dsName, "max_iterations": 20})
	require.NoError(t, err)

	stream := &mockFlightServer{}
	require.NoError(t, meta.DoAction(&flight.Action{Type: "detect-communities", Body: raw}, stream))
	require.Len(t, stream.results, 1)

	var res struct {
		CommunityCount int            `json:"CommunityCount"`
		Labels         map[string]any `json:"Labels"`
	}
	require.NoError(t, json.Unmarshal(stream.results[0].Body, &res))
	require.Equal(t, 2, res.CommunityCount, "two disjoint triangles must form two communities")

	after := testutil.ToFloat64(metrics.GraphCommunitiesTotal)
	require.Equal(t, float64(2), after, "longbow_graph_communities_total must report the detected count")
	require.NotEqual(t, before, after, "the gauge must be updated by this call")

	// The duration histogram must have recorded this run. Other tests in the
	// package also exercise the handler, so compare counts rather than
	// asserting a bare non-zero.
	histAfter := clusteringSampleCount(t)
	require.Greater(t, histAfter, histBefore,
		"longbow_graph_clustering_duration_seconds must observe the run")
}

// clusteringSampleCount returns the observation count of the unlabeled
// clustering duration histogram. testutil.ToFloat64 cannot read histograms, so
// gather the metric family directly.
func clusteringSampleCount(t *testing.T) uint64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, fam := range families {
		if fam.GetName() != "longbow_graph_clustering_duration_seconds" {
			continue
		}
		require.Len(t, fam.GetMetric(), 1, "clustering histogram must be unlabeled")
		return fam.GetMetric()[0].GetHistogram().GetSampleCount()
	}
	t.Fatalf("metric family longbow_graph_clustering_duration_seconds not found")
	return 0
}

// Mock Flight Server for capturing responses
type mockFlightServer struct {
	flight.FlightService_DoActionServer
	results []*flight.Result
}

// Send runs the send operation.
func (m *mockFlightServer) Send(r *flight.Result) error {
	m.results = append(m.results, r)
	return nil
}

// Context runs the context operation.
func (m *mockFlightServer) Context() context.Context {
	return context.Background()
}
