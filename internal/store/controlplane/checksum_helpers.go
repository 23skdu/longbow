// alias-only: not re-exported
package controlplane

// ComputeReplicationChecksum exposes the replication checksum helper to host
// packages that need to validate event payloads in white-box tests.
func ComputeReplicationChecksum(ev ReplicationEvent) string {
	return computeReplicationChecksum(ev)
}

// VerifyReplicationChecksum exposes replication checksum verification to host
// packages that need to validate event payloads in white-box tests.
func VerifyReplicationChecksum(ev ReplicationEvent) bool {
	return verifyReplicationChecksum(ev)
}
