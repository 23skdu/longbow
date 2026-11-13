package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestElasticShardingSplitMergeFailoverAndAutoScale(t *testing.T) {
	ctrl := NewElasticShardingController(nil)
	now := time.Now()
	ctrl.RegisterNode(ClusterNodeHealth{NodeID: "node-a", Addr: "10.0.0.1:9000", Healthy: true, LastHeartbeat: now, LoadScore: 0.7})
	ctrl.RegisterNode(ClusterNodeHealth{NodeID: "node-b", Addr: "10.0.0.2:9000", Healthy: true, LastHeartbeat: now, LoadScore: 0.1})
	ctrl.RegisterNode(ClusterNodeHealth{NodeID: "node-c", Addr: "10.0.0.3:9000", Healthy: true, LastHeartbeat: now, LoadScore: 0.2})

	shards := ctrl.EnsureDatasetShards("vectors", "node-a", []string{"node-c"})
	require.Len(t, shards, 1)
	rootID := shards[0].ShardID

	// Record 200 keys across token space
	for i := 0; i < 200; i++ {
		_, err := ctrl.RecordKeyPlacement("vectors", fmt.Sprintf("vec-%04d", i), 512)
		require.NoError(t, err)
	}

	require.NoError(t, ctrl.SetPolicy(ElasticScalingPolicy{
		Enabled:              true,
		MinShards:            1,
		MaxShards:            16,
		SplitVectorThreshold: 150,
		MergeVectorThreshold: 300,
		HeartbeatTimeoutSec:  10,
	}))

	// Auto-scale triggers split when vector count >= 150
	decisions, err := ctrl.ReconcileAutoScale("vectors", true)
	require.NoError(t, err)
	require.Len(t, decisions, 1)
	require.Equal(t, "split", decisions[0].Action)
	require.NotEqual(t, "", rootID)

	topo := ctrl.TopologySnapshot()
	dsShards := topo["shards"].(map[string][]ShardDescriptor)["vectors"]
	require.Len(t, dsShards, 2)
	require.Equal(t, int64(200), dsShards[0].VectorCount+dsShards[1].VectorCount)

	// Rebalance across nodes
	_, err = ctrl.RebalanceShards("vectors")
	require.NoError(t, err)

	// Simulate node-b and node-c sending fresh heartbeats while node-a misses heartbeat -> failover promotes node-c replica
	ctrl.RegisterNode(ClusterNodeHealth{NodeID: "node-b", Addr: "10.0.0.2:9000", Healthy: true, LastHeartbeat: now.Add(18 * time.Second), LoadScore: 0.1})
	ctrl.RegisterNode(ClusterNodeHealth{NodeID: "node-c", Addr: "10.0.0.3:9000", Healthy: true, LastHeartbeat: now.Add(18 * time.Second), LoadScore: 0.2})
	failDecisions := ctrl.CheckHealthAndFailover(now.Add(20 * time.Second))
	require.NotEmpty(t, failDecisions)
	require.Equal(t, "failover", failDecisions[0].Action)

	// Merge shards back together cleanly
	merged, _, err := ctrl.MergeShards("vectors", dsShards[0].ShardID, dsShards[1].ShardID)
	require.NoError(t, err)
	require.Equal(t, int64(200), merged.VectorCount)
}

func BenchmarkElasticShardSplitAndRoute(b *testing.B) {
	ctrl := NewElasticShardingController(nil)
	ctrl.RegisterNode(ClusterNodeHealth{NodeID: "n1", Healthy: true, LastHeartbeat: time.Now(), LoadScore: 0.2})
	ctrl.EnsureDatasetShards("bench_ds", "n1", nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctrl.RouteKey("bench_ds", fmt.Sprintf("item-%d", i))
	}
}

func FuzzElasticShardingRouting(f *testing.F) {
	f.Add("dataset-1", "key-alpha", 4)
	f.Add("ds", "", 1)

	f.Fuzz(func(t *testing.T, dataset, key string, splits int) {
		if dataset == "" {
			dataset = "default"
		}
		ctrl := NewElasticShardingController(nil)
		ctrl.RegisterNode(ClusterNodeHealth{NodeID: "n1", Healthy: true, LastHeartbeat: time.Now()})
		shards := ctrl.EnsureDatasetShards(dataset, "n1", nil)
		curID := shards[0].ShardID
		if splits > 0 && splits <= 4 {
			left, _, _, err := ctrl.SplitShard(dataset, curID)
			if err == nil {
				_ = left
			}
		}
		sd, ok := ctrl.RouteKey(dataset, key)
		if !ok || sd.ShardID == "" {
			t.Fatalf("failed to route key %q in %q", key, dataset)
		}
	})
}
