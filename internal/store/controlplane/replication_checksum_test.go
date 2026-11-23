package controlplane

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultSLAPolicyByTier(t *testing.T) {
	enterprise := DefaultSLAPolicy(CloudTierEnterprise)
	require.Equal(t, 99.99, enterprise.AvailabilityTarget)
	require.Equal(t, int64(5), enterprise.RPOSeconds)

	dedicated := DefaultSLAPolicy(CloudTierDedicated)
	require.Equal(t, 99.95, dedicated.AvailabilityTarget)
	require.Greater(t, dedicated.MaxReplicationLagMs, enterprise.MaxReplicationLagMs)

	serverless := DefaultSLAPolicy(CloudTierServerless)
	require.Equal(t, 99.9, serverless.AvailabilityTarget)
	require.Greater(t, serverless.RTOSeconds, dedicated.RTOSeconds)
}

func TestReplicationChecksumVerification(t *testing.T) {
	event := ReplicationEvent{
		ClusterID:    "cls-1",
		SourceRegion: "us-east-1",
		LSN:          7,
		Dataset:      "docs",
		Op:           "upsert",
		ID:           "doc-1",
		Vector:       []float32{0.25, 1},
	}

	checksum := ComputeReplicationChecksum(event)
	require.Len(t, checksum, 64)
	require.True(t, VerifyReplicationChecksum(event))

	event.Checksum = checksum
	require.True(t, VerifyReplicationChecksum(event))

	event.Checksum = strings.Repeat("0", 64)
	require.False(t, VerifyReplicationChecksum(event))
}

func FuzzReplicationChecksum(f *testing.F) {
	f.Add("cls-1", "us-east-1", uint64(7), "docs", "upsert", "doc-1", float32(0.25))
	f.Add("", "", uint64(0), "", "", "", float32(0))
	f.Add("cls-2", "eu-west-1", uint64(42), "vectors", "delete", "v-1", float32(-1.5))

	f.Fuzz(func(t *testing.T, cluster, region string, lsn uint64, dataset, op, id string, value float32) {
		event := ReplicationEvent{
			ClusterID:    cluster,
			SourceRegion: region,
			LSN:          lsn,
			Dataset:      dataset,
			Op:           op,
			ID:           id,
			Vector:       []float32{value},
		}

		checksum := ComputeReplicationChecksum(event)
		if len(checksum) != 64 {
			t.Fatalf("checksum length = %d, want 64", len(checksum))
		}
		if again := ComputeReplicationChecksum(event); again != checksum {
			t.Fatalf("checksum is not deterministic: %q != %q", again, checksum)
		}
		event.Checksum = checksum
		if !VerifyReplicationChecksum(event) {
			t.Fatal("computed checksum must verify")
		}
	})
}
