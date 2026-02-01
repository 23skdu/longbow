package store

import (
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockGraph for Strategy Testing
type MockGraph struct {
	mock.Mock
}

func (m *MockGraph) GetNeighbors(layer int, id uint32, buf []uint32) []uint32 {
	args := m.Called(layer, id, buf)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).([]uint32)
}

// ... other mock methods if needed ...

func TestQueryPlanner_Selection(t *testing.T) {
	planner := NewQueryPlanner()

	// Case 1: Start == Target (Trivial) -> BFS (per current logic default fallthrough, wait logic says if != use AStar)
	// If Start == Target, BFS is fine (0 hops). A* is also fine.
	// Current logic: if Target != Start { return astar } else { return bfs }
	s1 := planner.Plan(NavigatorQuery{StartID: 1, TargetID: 1})
	assert.Equal(t, "BFS", s1.Name())

	// Case 2: Start != Target -> A*
	s2 := planner.Plan(NavigatorQuery{StartID: 1, TargetID: 2})
	assert.Equal(t, "AStar", s2.Name())
}

func TestStrategies_Agreement(t *testing.T) {
	// Setup a simple linear graph 1->2->3->4
	// Both BFS and A* should find it.

	// We need a real GraphNavigator for this, backed by a mock/stub GraphData.
	// We can use the logic from graph_navigator_test.go (createGraphData) to make a real one.

	gd := types.NewGraphData(10, 2, false, false, -1, false, false, false, types.VectorTypeFloat32)
	// 1->2
	_ = gd.SetNeighbors(1, []uint32{2})
	_ = gd.SetVector(1, []float32{1.0, 0.0})
	// 2->3
	_ = gd.SetNeighbors(2, []uint32{3})
	_ = gd.SetVector(2, []float32{2.0, 0.0})
	// 3->4
	_ = gd.SetNeighbors(3, []uint32{4})
	_ = gd.SetVector(3, []float32{3.0, 0.0})
	// 4
	_ = gd.SetNeighbors(4, []uint32{})
	_ = gd.SetVector(4, []float32{4.0, 0.0})

	provider := func() *types.GraphData { return gd }
	config := NavigatorConfig{MaxHops: 5, EnableCaching: false}
	nav := NewGraphNavigator("test", provider, config, nil)
	err := nav.Initialize()
	assert.NoError(t, err)

	ctx := context.Background()
	query := NavigatorQuery{StartID: 1, TargetID: 4, MaxHops: 5}

	// Test BFS Explicitly
	bfs := &BFSStrategy{}
	pathBFS, err := bfs.FindPath(ctx, nav, query)
	assert.NoError(t, err)
	assert.True(t, pathBFS.Found)
	assert.Equal(t, 3, pathBFS.Hops) // 1->2->3->4 is 3 hops? 1-2(1), 2-3(2), 3-4(3). Yes.
	assert.Equal(t, []uint32{1, 2, 3, 4}, pathBFS.Path)

	// Test A* Explicitly
	astar := &AStarStrategy{}
	pathAStar, err := astar.FindPath(ctx, nav, query)
	assert.NoError(t, err)
	assert.True(t, pathAStar.Found)
	assert.Equal(t, 3, pathAStar.Hops)
	assert.Equal(t, []uint32{1, 2, 3, 4}, pathAStar.Path)
}
