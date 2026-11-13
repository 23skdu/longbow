package store

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestCloudControlPlane_LifecycleScaleAndSLA(t *testing.T) {
	mem := memory.NewGoAllocator()
	vs := NewVectorStore(mem, zerolog.Nop(), 64*1024*1024, 64*1024*1024, 10*time.Millisecond)
	defer func() { _ = vs.Close() }()

	ctrl := vs.GetCloudControlPlane()
	cl, err := ctrl.CreateCluster(CreateCloudClusterRequest{
		ID:              "cls-ent-1",
		Name:            "prod-vectors",
		TenantID:        "acme",
		Tier:            CloudTierEnterprise,
		PrimaryRegion:   "us-east-1",
		ReplicaRegions:  map[string]string{"eu-west-1": "local"},
		Nodes:           3,
		ReplicationMode: ReplicationSemiSync,
	})
	require.NoError(t, err)
	require.Equal(t, CloudTierEnterprise, cl.Tier)
	require.Equal(t, 99.99, cl.SLAPolicy.AvailabilityTarget)
	require.Equal(t, 3, cl.Nodes)
	require.Len(t, cl.Regions, 2)

	scaled, err := ctrl.ScaleCluster("cls-ent-1", 6, CloudTierDedicated)
	require.NoError(t, err)
	require.Equal(t, 6, scaled.Nodes)
	require.Equal(t, CloudTierDedicated, scaled.Tier)
	require.Equal(t, 99.95, scaled.SLAPolicy.AvailabilityTarget)

	rep, err := ctrl.EvaluateSLA("cls-ent-1", 12.5)
	require.NoError(t, err)
	require.True(t, rep.Compliant)
	require.True(t, rep.RPOMet)
	require.True(t, rep.RTOMet)

	// Simulate lag violation -> SLA breach recorded
	require.NoError(t, ctrl.MarkRegionHealth("cls-ent-1", "eu-west-1", true, 5000))
	breachRep, err := ctrl.EvaluateSLA("cls-ent-1", 10.0)
	require.NoError(t, err)
	require.False(t, breachRep.Compliant)
	require.False(t, breachRep.RPOMet)
	require.NotEmpty(t, breachRep.BreachReasons)

	require.NoError(t, ctrl.DeleteCluster("cls-ent-1"))
	_, ok := ctrl.GetCluster("cls-ent-1")
	require.False(t, ok)
}

func TestCloudControlPlane_MultiRegionHTTPReplicationAndFailover(t *testing.T) {
	mem := memory.NewGoAllocator()
	primaryStore := NewVectorStore(mem, zerolog.Nop(), 64*1024*1024, 64*1024*1024, 10*time.Millisecond)
	primaryStore.dataPath = t.TempDir()
	primaryStore.StartIngestionWorkers(2)
	primaryStore.StartIndexingWorkers(2)
	defer func() { _ = primaryStore.Close() }()

	replicaStore := NewVectorStore(mem, zerolog.Nop(), 64*1024*1024, 64*1024*1024, 10*time.Millisecond)
	replicaStore.dataPath = t.TempDir()
	replicaStore.StartIngestionWorkers(2)
	replicaStore.StartIndexingWorkers(2)
	defer func() { _ = replicaStore.Close() }()

	mux := http.NewServeMux()
	replicaStore.RegisterHTTPHandlers(mux)
	replicaSrv := httptest.NewServer(mux)
	defer replicaSrv.Close()

	ctrl := primaryStore.GetCloudControlPlane()
	_, err := ctrl.CreateCluster(CreateCloudClusterRequest{
		ID:              "cls-global",
		Name:            "global-search",
		TenantID:        "tenant-a",
		Tier:            CloudTierEnterprise,
		PrimaryRegion:   "us-east-1",
		ReplicaRegions:  map[string]string{"eu-central-1": replicaSrv.URL},
		ReplicationMode: ReplicationSync,
	})
	require.NoError(t, err)

	ctx := context.Background()
	n, err := ctrl.ReplicateMutation(ctx, "cls-global", ReplicationEvent{
		Dataset: "products",
		Op:      "upsert",
		ID:      "sku-1",
		Vector:  []float32{1, 0, 0, 0},
		Text:    "ergonomic keyboard",
		Metadata: map[string]any{
			"category": "peripherals",
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// Confirm the replicated vector is indexed & searchable in the secondary region store
	res, err := replicaStore.SearchDatasetVectors(ctx, "products", []float32{1, 0, 0, 0}, 5, nil)
	require.NoError(t, err)
	require.NotEmpty(t, res)

	// Tampered checksum is rejected
	err = replicaStore.GetCloudControlPlane().ApplyIncomingReplication(ctx, ReplicationEvent{
		Dataset:  "products",
		Op:       "upsert",
		ID:       "sku-bad",
		Vector:   []float32{0, 1, 0, 0},
		Checksum: strings.Repeat("0", 64),
	})
	require.Error(t, err)

	// Replicate delete and ensure read routing & failover promotion work cleanly
	_, err = ctrl.ReplicateMutation(ctx, "cls-global", ReplicationEvent{
		Dataset: "products",
		Op:      "delete",
		ID:      "sku-1",
	})
	require.NoError(t, err)

	require.NoError(t, ctrl.MarkRegionHealth("cls-global", "us-east-1", false, 0))
	routedReg, _, err := ctrl.SelectReadRegion("cls-global", "us-east-1")
	require.NoError(t, err)
	require.Equal(t, "eu-central-1", routedReg)

	promoted, err := ctrl.PromoteRegion(ctx, "cls-global", "", "primary outage")
	require.NoError(t, err)
	require.Equal(t, "eu-central-1", promoted.PrimaryRegion)
	require.Equal(t, "primary", promoted.Regions["eu-central-1"].Role)
	require.Equal(t, "replica", promoted.Regions["us-east-1"].Role)
}

func TestCloudControlPlane_DoActionDispatch(t *testing.T) {
	mem := memory.NewGoAllocator()
	vs := NewVectorStore(mem, zerolog.Nop(), 64*1024*1024, 64*1024*1024, 10*time.Millisecond)
	vs.dataPath = t.TempDir()
	vs.StartIngestionWorkers(1)
	vs.StartIndexingWorkers(1)
	defer func() { _ = vs.Close() }()

	callAction := func(actionType string, body any) []byte {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		stream := &mockDoActionServer{}
		handled, err := vs.handleFeatureAction(&flight.Action{Type: actionType, Body: raw}, stream)
		require.True(t, handled)
		require.NoError(t, err)
		require.NotEmpty(t, stream.results)
		return stream.results[0].Body
	}

	callAction("CreateCloudCluster", CreateCloudClusterRequest{
		ID:            "cls-act-1",
		Name:          "act-cluster",
		PrimaryRegion: "us-west-2",
	})
	callAction("AddReplicaRegion", map[string]string{
		"id":     "cls-act-1",
		"region": "ap-northeast-1",
	})
	repOut := callAction("ReplicateClusterMutation", map[string]any{
		"cluster_id": "cls-act-1",
		"event": map[string]any{
			"dataset": "cloud_docs",
			"id":      "doc-1",
			"vector":  []float32{0.5, 0.5, 0, 0},
		},
	})
	require.Contains(t, string(repOut), `"replicated_regions":1`)

	slaOut := callAction("GetCloudSLAReport", map[string]any{
		"cluster_id": "cls-act-1",
	})
	require.Contains(t, string(slaOut), `"compliant":true`)
}

func FuzzCloudControlPlaneRequest(f *testing.F) {
	f.Add("cls-fuzz", "us-east-1", "enterprise", int32(3), "ds1", "vec1", float32(0.25))
	f.Add("", "", "serverless", int32(0), "ds2", "", float32(1.0))

	mem := memory.NewGoAllocator()
	vs := NewVectorStore(mem, zerolog.Nop(), 32*1024*1024, 32*1024*1024, 10*time.Millisecond)
	defer func() { _ = vs.Close() }()
	ctrl := vs.GetCloudControlPlane()

	f.Fuzz(func(t *testing.T, id, region, tier string, nodes int32, ds, vecID string, v float32) {
		cl, err := ctrl.CreateCluster(CreateCloudClusterRequest{
			ID:            id,
			Name:          "fuzz-" + id,
			Tier:          CloudTier(tier),
			PrimaryRegion: region,
			Nodes:         int(nodes),
		})
		if err == nil && cl != nil {
			_, _ = ctrl.EvaluateSLA(cl.ID, float64(v))
			ev := ReplicationEvent{
				ClusterID: cl.ID,
				Dataset:   ds,
				ID:        vecID,
				Vector:    []float32{v, 1.0},
			}
			ev.Checksum = computeReplicationChecksum(ev)
			_ = verifyReplicationChecksum(ev)
			_ = ctrl.DeleteCluster(cl.ID)
		}
	})
}
