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

// Canonical graph action types. These are the literals the Python SDK
// (longbowclientsdk/src/longbow/client.py), the CLI (cmd/cli/graph_cmd.go)
// and the docs (docs/api.md) use. The mirror assertions live in
// cmd/cli/graph_cmd_test.go — the two sides are intentionally duplicated
// rather than shared to keep cmd/cli free of internal/store.
var canonicalGraphActions = []string{
	"add-edge",
	"traverse-graph",
	"GetGraphStats",
	"calculate-pagerank",
	"detect-communities",
}

// graphActionBody returns a body for an action whose dataset does not exist.
// Reaching the handler yields codes.NotFound; failing to dispatch yields
// codes.Unimplemented — which is exactly the wiring bug these tests pin.
func graphActionBody(t *testing.T) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"dataset":        "definitely-not-a-real-dataset",
		"subject":        1,
		"predicate":      "knows",
		"object":         2,
		"weight":         1.0,
		"start":          1,
		"max_hops":       2,
		"max_iterations": 5,
	})
	require.NoError(t, err)
	return raw
}

// TestGraphActionWiring_DataPlane asserts the data-plane dispatcher
// (VectorStore.DoAction) recognises every canonical graph action type.
func TestGraphActionWiring_DataPlane(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	s := NewVectorStore(mem, logger, 1<<28, 0, 0)
	defer func() { _ = s.Close() }()

	for _, actionType := range canonicalGraphActions {
		t.Run(actionType, func(t *testing.T) {
			stream := &mockFlightServer{}
			err := s.DoAction(&flight.Action{Type: actionType, Body: graphActionBody(t)}, stream)

			// Unimplemented means the switch fell through: the action is
			// either missing from the dispatcher or misspelled.
			require.NotEqual(t, codes.Unimplemented, status.Code(err),
				"data plane must dispatch %q (got: %v)", actionType, err)
			// All five reach their handler; a missing dataset is NotFound.
			require.Equal(t, codes.NotFound, status.Code(err),
				"%q should reach its handler and fail on the missing dataset (got: %v)", actionType, err)
		})
	}
}

// TestGraphActionWiring_MetaPlane asserts the meta-plane dispatcher
// (VectorStore.DoMetaAction) recognises every canonical graph action type.
// The Python SDK sends all graph operations through the meta client, so a
// missing case here surfaces as Unimplemented to SDK users.
func TestGraphActionWiring_MetaPlane(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	s := NewVectorStore(mem, logger, 1<<28, 0, 0)
	defer func() { _ = s.Close() }()

	for _, actionType := range canonicalGraphActions {
		t.Run(actionType, func(t *testing.T) {
			stream := &mockFlightServer{}
			err := s.DoMetaAction(&flight.Action{Type: actionType, Body: graphActionBody(t)}, stream)

			require.NotEqual(t, codes.Unimplemented, status.Code(err),
				"meta plane must dispatch %q (got: %v)", actionType, err)
			require.Equal(t, codes.NotFound, status.Code(err),
				"%q should reach its handler and fail on the missing dataset (got: %v)", actionType, err)
		})
	}
}

// TestGraphActionWiring_UnknownActionStillUnimplemented is the control: an
// unknown action type must still be rejected as Unimplemented so the
// positive assertions above prove dispatch, not blanket acceptance.
func TestGraphActionWiring_UnknownActionStillUnimplemented(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	s := NewVectorStore(mem, logger, 1<<28, 0, 0)
	defer func() { _ = s.Close() }()

	stream := &mockFlightServer{}
	err := s.DoAction(&flight.Action{Type: "detect-communitie", Body: graphActionBody(t)}, stream)
	require.Equal(t, codes.Unimplemented, status.Code(err))

	stream = &mockFlightServer{}
	err = s.DoMetaAction(&flight.Action{Type: "detect-communitie", Body: graphActionBody(t)}, stream)
	require.Equal(t, codes.Unimplemented, status.Code(err))
}

// TestGraphActionWiring_RBACPermissions pins the RBAC classification of each
// graph action. add-edge mutates the graph and must require PermWrite; the
// read-only analytics/traversal actions must require PermRead. Anything that
// falls through to the default branch demands PermAdmin, which would lock
// ordinary API keys out of graph analytics.
func TestGraphActionWiring_RBACPermissions(t *testing.T) {
	readActions := []string{"traverse-graph", "GetGraphStats", "calculate-pagerank", "detect-communities"}
	for _, actionType := range readActions {
		t.Run(actionType, func(t *testing.T) {
			raw, err := json.Marshal(map[string]string{"dataset": "kb"})
			require.NoError(t, err)
			perm, target := permissionAndDatasetForAction(&flight.Action{Type: actionType, Body: raw})
			assert.Equal(t, PermRead, perm, "%q must be a read action", actionType)
			assert.Equal(t, "kb", target, "dataset must be extracted for scoping")
		})
	}

	t.Run("add-edge", func(t *testing.T) {
		raw, err := json.Marshal(map[string]string{"dataset": "kb"})
		require.NoError(t, err)
		perm, target := permissionAndDatasetForAction(&flight.Action{Type: "add-edge", Body: raw})
		assert.Equal(t, PermWrite, perm, "add-edge mutates the graph and must be a write action")
		assert.Equal(t, "kb", target)
	})
}

// TestGraphActionWiring_PascalCaseRejected pins the historical bug: the CLI
// used to send PascalCase types (AddEdge, TraverseGraph, ...) which the
// server has never dispatched. If someone "helpfully" adds PascalCase
// aliases to the server this test fails and forces a decision rather than
// silently accepting two spellings.
func TestGraphActionWiring_PascalCaseRejected(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	s := NewVectorStore(mem, logger, 1<<28, 0, 0)
	defer func() { _ = s.Close() }()

	legacy := []string{"AddEdge", "TraverseGraph", "CalculatePageRank", "DetectCommunities"}
	for _, actionType := range legacy {
		t.Run(actionType, func(t *testing.T) {
			stream := &mockFlightServer{}
			err := s.DoAction(&flight.Action{Type: actionType, Body: graphActionBody(t)}, stream)
			assert.Equal(t, codes.Unimplemented, status.Code(err),
				"%q is not a server-side action type; clients must send kebab-case", actionType)
		})
	}
}
