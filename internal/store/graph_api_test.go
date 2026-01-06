package store


import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/rs/zerolog"
)

func TestGraphAPI_GetGraphStats(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	s := NewVectorStore(mem, logger, 1<<30, 0, time.Hour)
	meta := NewMetaServer(s)
	defer func() { _ = meta.Close() }()

	// 1. Setup Dataset
	dsName := "graph_stats_test"
	_ = s.CreateNamespace(dsName) // Implicitly creates default namespace? No, just create dataset.
	// Actually VectorStore.CreateNamespace creates a namespace, but datasets are created on write usually.
	// Let's manually inject a dataset.
	ds := &Dataset{
		Name:  dsName,
		Graph: NewGraphStore(),
	}
	s.mu.Lock()
	s.datasets[dsName] = ds
	s.mu.Unlock()

	// 2. Add some data
	_ = ds.Graph.AddEdge(Edge{Subject: 1, Predicate: "knows", Object: 2, Weight: 1.0})
	_ = ds.Graph.AddEdge(Edge{Subject: 2, Predicate: "knows", Object: 3, Weight: 1.0})

	// Mock DetectCommunities result for stats
	// DetectCommunities usually runs on demand or background, let's force it if we want stats,
	// or maybe the stats just return 0 if not run.
	// The requirement for GetGraphStats is edge_count, community_count, predicates.
	_ = ds.Graph.DetectCommunities()

	// 3. Call GetGraphStats
	req := map[string]string{"dataset": dsName}
	reqBytes, _ := json.Marshal(req)

	action := &flight.Action{
		Type: "GetGraphStats",
		Body: reqBytes,
	}

	stream := &mockFlightServer{}
	err := meta.DoAction(action, stream)
	require.NoError(t, err)

	require.Len(t, stream.results, 1)
	var resp struct {
		EdgeCount      int      `json:"edge_count"`
		CommunityCount int      `json:"community_count"`
		Predicates     []string `json:"predicates"`
	}
	err = json.Unmarshal(stream.results[0].Body, &resp)
	require.NoError(t, err)

	assert.Equal(t, 2, resp.EdgeCount)
	assert.GreaterOrEqual(t, resp.CommunityCount, 1)
	assert.Contains(t, resp.Predicates, "knows")
}

func TestGraphAPI_GetGraphStats_NotFound(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	s := NewVectorStore(mem, logger, 1<<30, 0, time.Hour)
	meta := NewMetaServer(s)

	req := map[string]string{"dataset": "non_existent"}
	reqBytes, _ := json.Marshal(req)

	action := &flight.Action{
		Type: "GetGraphStats",
		Body: reqBytes,
	}

	stream := &mockFlightServer{}
	err := meta.DoAction(action, stream)
	require.Error(t, err)
}

// Mock Flight Server for capturing responses
type mockFlightServer struct {
	flight.FlightService_DoActionServer
	results []*flight.Result
}

func (m *mockFlightServer) Send(r *flight.Result) error {
	m.results = append(m.results, r)
	return nil
}
func (m *mockFlightServer) Context() context.Context {
	return context.Background()
}
