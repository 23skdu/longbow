package store

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func countTemporalIngestWorkers() int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			stacks := string(buf[:n])
			return strings.Count(stacks, "TemporalIndex).runIngestWorker") +
				strings.Count(stacks, "TemporalIndex).startIngestWorker")
		}
		buf = make([]byte, len(buf)*2)
	}
}

func waitForZeroTemporalWorkers(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if countTemporalIngestWorkers() == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected 0 temporal ingest workers, found %d", countTemporalIngestWorkers())
}

// TestVectorStore_NoTemporalGoroutineLeakAcrossNamespaces runs the test vector store_no temporal goroutine leak across namespaces operation.
func TestVectorStore_NoTemporalGoroutineLeakAcrossNamespaces(t *testing.T) {
	waitForZeroTemporalWorkers(t, time.Second)
	baseline := runtime.NumGoroutine()

	vs := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), 1<<30, 0, 0)
	vs.SetTemporalIndex(TemporalConfig{Enabled: true})

	const numNamespaces = 21
	const datasetsPerNS = 3
	baseTs := time.Now().UnixNano()

	for nsIdx := 0; nsIdx < numNamespaces; nsIdx++ {
		nsName := fmt.Sprintf("ns-%02d", nsIdx)
		require.NoError(t, vs.CreateNamespace(nsName))
		for dsIdx := 0; dsIdx < datasetsPerNS; dsIdx++ {
			dsName := fmt.Sprintf("%s/ds-%d", nsName, dsIdx)
			ds, created := vs.getOrCreateDataset(dsName, func() *Dataset {
				return NewDataset(dsName, nil)
			})
			require.True(t, created)
			require.NotNil(t, ds.TemporalIndex)

			ids := []uint64{1, 2, 3, 4}
			norms := []float32{1, 2, 3, 4}
			ts := []int64{baseTs, baseTs + 1, baseTs + 2, baseTs + 3}
			require.NoError(t, ds.TemporalIndex.AddBatchNorms(ids, norms, ts, 4))
		}
	}

	// All 63 datasets are alive; on-demand temporal workers should drain and exit.
	waitForZeroTemporalWorkers(t, 2*time.Second)

	// Delete half the namespaces and then shut down the store cleanly.
	for nsIdx := 0; nsIdx < numNamespaces/2; nsIdx++ {
		require.NoError(t, vs.DeleteNamespace(fmt.Sprintf("ns-%02d", nsIdx)))
	}

	require.NoError(t, vs.Shutdown(context.Background()))
	waitForZeroTemporalWorkers(t, time.Second)

	// Ensure Shutdown cleared remaining datasets and closed their resources.
	remaining := 0
	vs.IterateDatasets(func(_ string, _ *Dataset) {
		remaining++
	})
	assert.Equal(t, 0, remaining)
	assert.LessOrEqual(t, runtime.NumGoroutine(), baseline+16)
}
