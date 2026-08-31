package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordGraphStoreExport_Success(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGraphStoreExport(5*time.Millisecond, 1000, 4096, true)
	})
}

func TestRecordGraphStoreExport_Error(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGraphStoreExport(1*time.Millisecond, 0, 0, false)
	})
}

func TestRecordGraphStoreExport_ZeroBytes(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGraphStoreExport(2*time.Millisecond, 50, -1, true)
	})
}

func TestRecordGraphStoreImport_Success(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGraphStoreImport(3*time.Millisecond, 500, true)
	})
}

func TestRecordGraphStoreImport_Error(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGraphStoreImport(1*time.Millisecond, 0, false)
	})
}

func TestRecordGraphStorePredicateCount(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGraphStorePredicateCount(42)
	})
}

func TestRecordGraphStoreEdgeCount(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGraphStoreEdgeCount(1024)
	})
}

func TestNewGraphStoreMetricsCollector(t *testing.T) {
	c := NewGraphStoreMetricsCollector()
	require.NotNil(t, c)
}

func TestGraphStoreMetricsCollector_UpdateEdgeCount(t *testing.T) {
	c := NewGraphStoreMetricsCollector()
	assert.NotPanics(t, func() {
		c.UpdateEdgeCount(100)
		c.UpdateEdgeCount(0)
	})
}

func TestGraphStoreMetricsCollector_UpdatePredicateCount(t *testing.T) {
	c := NewGraphStoreMetricsCollector()
	assert.NotPanics(t, func() {
		c.UpdatePredicateCount(10)
		c.UpdatePredicateCount(0)
	})
}

func TestRecordNamespaceQuery(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordNamespaceQuery("default", 0.005)
	})
}

func TestRecordNamespaceStorage(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordNamespaceStorage("default", 1024*1024)
	})
}

func TestRecordNamespaceVectors(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordNamespaceVectors("default", 5000)
	})
}

func TestRecordNamespaceIngestRate(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordNamespaceIngestRate("default", 100.0)
	})
}

func TestSetNamespaceQuotaLimit(t *testing.T) {
	assert.NotPanics(t, func() {
		SetNamespaceQuotaLimit("default", "vectors", 1e6)
	})
}

func TestSetNamespaceQuotaUsed(t *testing.T) {
	assert.NotPanics(t, func() {
		SetNamespaceQuotaUsed("default", "vectors", 5e5)
	})
}

func TestRecordNamespaceRateLimitHit(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordNamespaceRateLimitHit("default")
	})
}

func TestRecordNamespaceCacheHit(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordNamespaceCacheHit("default")
	})
}

func TestRecordNamespaceCacheMiss(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordNamespaceCacheMiss("default")
	})
}

func TestRecordSimdBatch(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordSimdBatch("avx2", "euclidean", 1000)
	})
}
