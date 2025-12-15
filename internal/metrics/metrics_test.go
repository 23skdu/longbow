package metrics

import (
"testing"

"github.com/stretchr/testify/assert"
)

func TestMetricsInitialization(t *testing.T) {
assert.NotNil(t, FlightOperationsTotal)
assert.NotNil(t, FlightDurationSeconds)
assert.NotNil(t, FlightBytesProcessed)
assert.NotNil(t, WalWritesTotal)
assert.NotNil(t, WalBytesWritten)
assert.NotNil(t, WalReplayDurationSeconds)
assert.NotNil(t, SnapshotTotal)
assert.NotNil(t, SnapshotDurationSeconds)
assert.NotNil(t, EvictionsTotal)
}
