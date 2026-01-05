package sharding

import (
	"strconv"
	"testing"

	"github.com/23skdu/longbow/internal/mesh"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestRingManager_JoinLeave(t *testing.T) {
	logger := zerolog.Nop()
	rm := NewRingManager("localNode", logger)

	// Add nodes via mesh events
	nodeA := &mesh.Member{ID: "nodeA", Addr: "127.0.0.1:8001", Status: mesh.StatusAlive}
	nodeB := &mesh.Member{ID: "nodeB", Addr: "127.0.0.1:8002", Status: mesh.StatusAlive}

	rm.NotifyJoin(nodeA)
	rm.NotifyJoin(nodeB)

	// Verify ring population
	// Vector ID 1 (stringified) should map to someone
	key := "vector-1"
	owner := rm.GetNode(key)
	assert.Contains(t, []string{"nodeA", "nodeB"}, owner)

	// Node Leave
	rm.NotifyLeave(nodeA)

	// Check all route to B now (if A was owner, it should move. If B was owner, stays B)
	// To force check, we iterate effectively or just check membership.
	members := rm.GetMembers()
	assert.Len(t, members, 1)
	assert.Equal(t, "nodeB", members[0])

	owner2 := rm.GetNode(key)
	assert.Equal(t, "nodeB", owner2, "Should route to remaining node")
}

func TestRingManager_Consistency(t *testing.T) {
	logger := zerolog.Nop()
	rm := NewRingManager("localNode", logger)

	// TWS: Time Window Sharding strategy test if applicable
	// Currently ShardManager uses Consistent Hashing (implied by ring usage)
	// But let's verify GetNode consistency

	key := strconv.Itoa(12345)

	n1 := rm.GetNode(key)
	n2 := rm.GetNode(key)

	assert.Equal(t, n1, n2)
}
