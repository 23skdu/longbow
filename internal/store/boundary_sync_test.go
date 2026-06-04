package store

import (
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/mesh"
	"github.com/23skdu/longbow/internal/sharding"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBoundarySyncAndRouting(t *testing.T) {
	logger := zerolog.Nop()

	// 1. Create two RingManagers (simulating nodes)
	rm1 := sharding.NewRingManager("node-1", logger)
	rm2 := sharding.NewRingManager("node-2", logger)

	// 2. Setup gossip configurations
	gcfg1 := mesh.GossipConfig{
		ID:            "node-1",
		Port:          12001,
		AdvertiseAddr: "127.0.0.1:12001",
		Delegate:      rm1,
	}
	gcfg2 := mesh.GossipConfig{
		ID:            "node-2",
		Port:          12002,
		AdvertiseAddr: "127.0.0.1:12002",
		Delegate:      rm2,
	}

	g1 := mesh.NewGossip(&gcfg1)
	err := g1.Start()
	require.NoError(t, err)
	defer g1.Stop()

	g2 := mesh.NewGossip(&gcfg2)
	err = g2.Start()
	require.NoError(t, err)
	defer g2.Stop()

	// 3. Connect/Join
	err = g1.Join("127.0.0.1:12002")
	require.NoError(t, err)

	// Wait for gossip membership sync
	time.Sleep(500 * time.Millisecond)

	// Verify they see each other
	require.Len(t, g1.GetMembers(), 2)
	require.Len(t, g2.GetMembers(), 2)

	// 4. Test Local Tags Update propagation
	tags1 := map[string]string{
		"geo:dataset1:centroid": "37.7749,-122.4194", // SF
		"geo:dataset1:radius":   "10.0",
		"temporal:dataset1:min": "1000",
		"temporal:dataset1:max": "2000",
	}

	g1.UpdateLocalTags(tags1)

	// Wait for gossip piggyback propagation
	time.Sleep(1 * time.Second)

	// Node 2 should have received the tag updates and updated its RingManager boundaries
	// Let's verify RouteGeo and RouteTemporal on rm2
	// SF query center (37.7749, -122.4194). Radius 5km.
	matchedGeo := rm2.RouteGeo("dataset1", 37.7749, -122.4194, 5.0)
	assert.Contains(t, matchedGeo, "node-1")

	// Query far away (NY: 40.7128, -74.0060)
	matchedGeoNY := rm2.RouteGeo("dataset1", 40.7128, -74.0060, 5.0)
	assert.NotContains(t, matchedGeoNY, "node-1")

	// Query overlapping temporal range [1500, 1600]
	matchedTemporal := rm2.RouteTemporal("dataset1", 1500, 1600)
	assert.Contains(t, matchedTemporal, "node-1")

	// Query non-overlapping temporal range [2500, 3000]
	matchedTemporalOut := rm2.RouteTemporal("dataset1", 2500, 3000)
	assert.NotContains(t, matchedTemporalOut, "node-1")

	// 5. Test Region Cleanup on Member Departure
	g1.Stop()

	// Wait for suspicion/dead propagation
	// In SWIM suspicion timeout is configurable. Since default is 5s, let's manually trigger NotifyLeave
	// to verify the logic directly.
	member1 := &mesh.Member{
		ID: "node-1",
	}
	rm2.NotifyLeave(member1)

	// Node-1 boundaries should be cleared from Node-2
	matchedGeoAfterLeave := rm2.RouteGeo("dataset1", 37.7749, -122.4194, 5.0)
	assert.NotContains(t, matchedGeoAfterLeave, "node-1")

	matchedTemporalAfterLeave := rm2.RouteTemporal("dataset1", 1500, 1600)
	assert.NotContains(t, matchedTemporalAfterLeave, "node-1")
}
