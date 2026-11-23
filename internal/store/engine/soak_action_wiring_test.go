package engine

import (
	"encoding/json"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestSoakActionWiring_DataPlane pins the data-plane dispatch of action types
// emitted by AgentSoak but historically handled only on the meta plane.
func TestSoakActionWiring_DataPlane(t *testing.T) {
	s := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<28, 0, 0)
	defer func() { _ = s.Close() }()

	body, err := json.Marshal(map[string]any{
		"dataset":  "definitely-not-a-real-dataset",
		"seed_ids": []string{"seed"},
		"node_ids": []uint32{1},
		"vector":   []float32{0.1, 0.2},
		"k":        1,
	})
	require.NoError(t, err)

	for _, actionType := range []string{
		"TemporalSearch", "TemporalRangeSearch", "TemporalVersionHistory",
		"TemporalAggregation", "GraphRAGExpand", "Recommend",
	} {
		t.Run(actionType, func(t *testing.T) {
			err := s.DoAction(&flight.Action{Type: actionType, Body: body}, &mockFlightServer{})
			require.Error(t, err, "unrouted action must be rejected")
			assert.NotEqual(t, codes.Unimplemented, status.Code(err),
				"%q must be dispatched by the data plane", actionType)
		})
	}
}

// TestSoakActionWiring_RBACPermissions pins read classification for the soak
// analytics actions so scoped reader keys can invoke them.
func TestSoakActionWiring_RBACPermissions(t *testing.T) {
	for _, actionType := range []string{
		"TemporalSearch", "TemporalRangeSearch", "TemporalVersionHistory",
		"TemporalAggregation", "GraphRAGExpand", "Recommend",
	} {
		t.Run(actionType, func(t *testing.T) {
			raw, err := json.Marshal(map[string]any{"dataset": "kb"})
			require.NoError(t, err)
			perm, target := permissionAndDatasetForAction(&flight.Action{Type: actionType, Body: raw})
			assert.Equal(t, PermRead, perm, "%q must be a read action", actionType)
			assert.Equal(t, "kb", target)
		})
	}
}
