package store

import (
	"testing"

	"github.com/23skdu/longbow/internal/metrics"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func getCounterValue(c interface {
	Write(*io_prometheus_client.Metric) error
}) float64 {
	m := &io_prometheus_client.Metric{}
	_ = c.Write(m)
	return m.GetCounter().GetValue()
}

func TestArrowHNSW_PoolMetrics(t *testing.T) {
	config := DefaultArrowHNSWConfig()
	config.M = 16
	config.Dims = 128

	ds := &Dataset{Name: "metrics_test"}
	idx := NewArrowHNSW(ds, config, nil)

	// Baseline
	getBefore := getCounterValue(metrics.HNSWInsertPoolGetTotal)
	putBefore := getCounterValue(metrics.HNSWInsertPoolPutTotal)
	growBefore := getCounterValue(metrics.HNSWBitsetGrowTotal)

	// Add a vector
	vec := make([]float32, 128)
	err := idx.InsertWithVector(0, vec, 0)
	require.NoError(t, err)

	// Verify increments
	getAfter := getCounterValue(metrics.HNSWInsertPoolGetTotal)
	putAfter := getCounterValue(metrics.HNSWInsertPoolPutTotal)

	require.Equal(t, getBefore+1, getAfter, "InsertPoolGet should increment")
	require.Equal(t, putBefore+1, putAfter, "InsertPoolPut should increment")

	// Trigger Search
	searchGetBefore := getCounterValue(metrics.HNSWSearchPoolGetTotal)
	searchPutBefore := getCounterValue(metrics.HNSWSearchPoolPutTotal)

	_, err = idx.Search(vec, 1, 10, nil)
	require.NoError(t, err)

	searchGetAfter := getCounterValue(metrics.HNSWSearchPoolGetTotal)
	searchPutAfter := getCounterValue(metrics.HNSWSearchPoolPutTotal)

	require.Equal(t, searchGetBefore+1, searchGetAfter, "SearchPoolGet should increment")
	require.Equal(t, searchPutBefore+1, searchPutAfter, "SearchPoolPut should increment")

	// Check Bitset growth (Initial 10000 might not grow for 1st node, but let's check it's >= before)
	growAfter := getCounterValue(metrics.HNSWBitsetGrowTotal)
	require.GreaterOrEqual(t, growAfter, growBefore)
}
