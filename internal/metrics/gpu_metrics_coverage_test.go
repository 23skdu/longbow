package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewExporter runs the test new exporter operation.
func TestNewExporter(t *testing.T) {
	e := NewExporter(time.Second)
	require.NotNil(t, e)
	assert.Equal(t, time.Second, e.interval)
}

// TestExporter_StartHTTPServer_Stop runs the test exporter_start http server_stop operation.
func TestExporter_StartHTTPServer_Stop(t *testing.T) {
	e := NewExporter(time.Second)
	err := e.StartHTTPServer("127.0.0.1:0")
	require.NoError(t, err)
	require.NotNil(t, e.httpServer)
	err = e.Stop()
	assert.NoError(t, err)
}

// TestExporter_Stop_NilServer runs the test exporter_stop_nil server operation.
func TestExporter_Stop_NilServer(t *testing.T) {
	e := NewExporter(time.Second)
	err := e.Stop()
	assert.NoError(t, err)
}

// TestExporter_HealthHandler runs the test exporter_health handler operation.
func TestExporter_HealthHandler(t *testing.T) {
	e := NewExporter(time.Second)
	req := httptest.NewRequest(http.MethodGet, "/gpu/health", nil)
	w := httptest.NewRecorder()
	e.healthHandler(w, req)
	resp := w.Result()
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "status")
}

// TestRecordGPUSearch runs the test record gpu search operation.
func TestRecordGPUSearch(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUSearch(5*time.Millisecond, "cpu", 10)
	})
}

// TestRecordGPUSearchError runs the test record gpu search error operation.
func TestRecordGPUSearchError(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUSearchError("oom")
	})
}

// TestRecordGPUSync runs the test record gpu sync operation.
func TestRecordGPUSync(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUSync(2*time.Millisecond, 100)
	})
}

// TestRecordGPUSyncError runs the test record gpu sync error operation.
func TestRecordGPUSyncError(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUSyncError()
	})
}

// TestRecordGPUIndexSize runs the test record gpu index size operation.
func TestRecordGPUIndexSize(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUIndexSize(0, 1000)
	})
}

// TestRecordGPUMemory runs the test record gpu memory operation.
func TestRecordGPUMemory(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUMemory(0, 4*1024*1024, 2*1024*1024, 2*1024*1024)
	})
}

// TestRecordGPUUtilization runs the test record gpu utilization operation.
func TestRecordGPUUtilization(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUUtilization(0, 75.5)
	})
}

// TestRecordGPUTemperature runs the test record gpu temperature operation.
func TestRecordGPUTemperature(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUTemperature(0, 72.0)
	})
}

// TestRecordGPUPower runs the test record gpu power operation.
func TestRecordGPUPower(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUPower(0, 150.0)
	})
}

// TestRecordMetalInit runs the test record metal init operation.
func TestRecordMetalInit(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMetalInit(10*time.Millisecond, true)
		RecordMetalInit(10*time.Millisecond, false)
	})
}

// TestRecordMetalSearch runs the test record metal search operation.
func TestRecordMetalSearch(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMetalSearch(3*time.Millisecond, 10, 128)
	})
}

// TestRecordMetalSearchError runs the test record metal search error operation.
func TestRecordMetalSearchError(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMetalSearchError("timeout")
	})
}

// TestRecordMetalAdd runs the test record metal add operation.
func TestRecordMetalAdd(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMetalAdd(2*time.Millisecond, 64, 128)
	})
}

// TestRecordMetalAddError runs the test record metal add error operation.
func TestRecordMetalAddError(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMetalAddError("oom")
	})
}

// TestRecordMetalIndexSize runs the test record metal index size operation.
func TestRecordMetalIndexSize(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMetalIndexSize(0, 2048, 128)
	})
}

// TestRecordMetalMemory runs the test record metal memory operation.
func TestRecordMetalMemory(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMetalMemory(512*1024, 256*1024)
	})
}

// TestRecordMetalShaderCompile runs the test record metal shader compile operation.
func TestRecordMetalShaderCompile(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMetalShaderCompile(50*time.Millisecond, true, 3)
		RecordMetalShaderCompile(50*time.Millisecond, false, 0)
	})
}

// TestRecordMultiGPUQuery runs the test record multi gpu query operation.
func TestRecordMultiGPUQuery(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMultiGPUQuery(10*time.Millisecond, 2, "round_robin")
	})
}

// TestRecordMultiGPUQueryError runs the test record multi gpu query error operation.
func TestRecordMultiGPUQueryError(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMultiGPUQueryError("round_robin", "oom")
	})
}

// TestRecordMultiGPUReplicate runs the test record multi gpu replicate operation.
func TestRecordMultiGPUReplicate(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMultiGPUReplicate(5*time.Millisecond, 2, 1000)
	})
}

// TestRecordMultiGPUReplicateError runs the test record multi gpu replicate error operation.
func TestRecordMultiGPUReplicateError(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMultiGPUReplicateError()
	})
}

// TestRecordMultiGPUDeviceStats runs the test record multi gpu device stats operation.
func TestRecordMultiGPUDeviceStats(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMultiGPUDeviceStats(0, 100, 5)
	})
}

// TestRecordGPUHNSWBuild runs the test record gpuhnsw build operation.
func TestRecordGPUHNSWBuild(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUHNSWBuild(100*time.Millisecond, 1000, true)
		RecordGPUHNSWBuild(10*time.Millisecond, 0, false)
	})
}

// TestRecordGPUHNSWBuildBatch runs the test record gpuhnsw build batch operation.
func TestRecordGPUHNSWBuildBatch(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUHNSWBuildBatch(50*time.Millisecond, 500)
	})
}

// TestRecordGPUHNSWBuildFallback runs the test record gpuhnsw build fallback operation.
func TestRecordGPUHNSWBuildFallback(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUHNSWBuildFallback("oom")
	})
}
