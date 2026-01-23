package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetricsInitialization(t *testing.T) {
	assert.NotNil(t, FlightOpsTotal)
	assert.NotNil(t, FlightDurationSeconds)
	assert.NotNil(t, FlightBytesReadTotal)
	assert.NotNil(t, FlightBytesWrittenTotal)
	assert.NotNil(t, WalWritesTotal)
	assert.NotNil(t, WalBytesWritten)
	assert.NotNil(t, WalReplayDurationSeconds)
	assert.NotNil(t, SnapshotTotal)
	assert.NotNil(t, SnapshotDurationSeconds)
	assert.NotNil(t, EvictionsTotal)

	// Lock Contention Metrics
	assert.NotNil(t, LockContentionDuration)
	assert.NotNil(t, WALLockWaitDuration)
	assert.NotNil(t, PoolLockWaitDuration)
	assert.NotNil(t, IndexLockWaitDuration)
	assert.NotNil(t, DatasetLockWaitDuration)
	assert.NotNil(t, ShardLockWaitDuration)
}
