package storage

import "context"

// WALReplicator defines the interface for replicating WAL batches to remote nodes.
type WALReplicator interface {
	// Replicate synchronously replicates a WAL batch to remote nodes.
	// It returns an error if the replication fails to meet the quorum.
	// data is the serialized (and potentially compressed) WAL batch.
	Replicate(ctx context.Context, data []byte) error
}
