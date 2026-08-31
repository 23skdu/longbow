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

func TestNewExporter(t *testing.T) {
	e := NewExporter(time.Second)
	require.NotNil(t, e)
	assert.Equal(t, time.Second, e.interval)
}

func TestExporter_StartHTTPServer_Stop(t *testing.T) {
	e := NewExporter(time.Second)
	err := e.StartHTTPServer("127.0.0.1:0")
	require.NoError(t, err)
	require.NotNil(t, e.httpServer)
	err = e.Stop()
	assert.NoError(t, err)
}

func TestExporter_Stop_NilServer(t *testing.T) {
	e := NewExporter(time.Second)
	err := e.Stop()
	assert.NoError(t, err)
}

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

func TestRecordGPUSearch(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUSearch(5*time.Millisecond, "cpu", 10)
	})
}

func TestRecordGPUSearchError(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUSearchError("oom")
	})
}

func TestRecordGPUSync(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUSync(2*time.Millisecond, 100)
	})
}

func TestRecordGPUSyncError(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUSyncError()
	})
}

func TestRecordGPUIndexSize(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUIndexSize(0, 1000)
	})
}

func TestRecordGPUMemory(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUMemory(0, 4*1024*1024, 2*1024*1024, 2*1024*1024)
	})
}

func TestRecordGPUUtilization(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUUtilization(0, 75.5)
	})
}

func TestRecordGPUTemperature(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUTemperature(0, 72.0)
	})
}

func TestRecordGPUPower(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUPower(0, 150.0)
	})
}

func TestRecordMetalInit(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMetalInit(10*time.Millisecond, true)
		RecordMetalInit(10*time.Millisecond, false)
	})
}

func TestRecordMetalSearch(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMetalSearch(3*time.Millisecond, 10, 128)
	})
}

func TestRecordMetalSearchError(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMetalSearchError("timeout")
	})
}

func TestRecordMetalAdd(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMetalAdd(2*time.Millisecond, 64, 128)
	})
}

func TestRecordMetalAddError(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMetalAddError("oom")
	})
}

func TestRecordMetalIndexSize(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMetalIndexSize(0, 2048, 128)
	})
}

func TestRecordMetalMemory(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMetalMemory(512*1024, 256*1024)
	})
}

func TestRecordMetalShaderCompile(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMetalShaderCompile(50*time.Millisecond, true, 3)
		RecordMetalShaderCompile(50*time.Millisecond, false, 0)
	})
}

func TestRecordMultiGPUQuery(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMultiGPUQuery(10*time.Millisecond, 2, "round_robin")
	})
}

func TestRecordMultiGPUQueryError(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMultiGPUQueryError("round_robin", "oom")
	})
}

func TestRecordMultiGPUReplicate(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMultiGPUReplicate(5*time.Millisecond, 2, 1000)
	})
}

func TestRecordMultiGPUReplicateError(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMultiGPUReplicateError()
	})
}

func TestRecordMultiGPUDeviceStats(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordMultiGPUDeviceStats(0, 100, 5)
	})
}

func TestRecordGPUHNSWBuild(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUHNSWBuild(100*time.Millisecond, 1000, true)
		RecordGPUHNSWBuild(10*time.Millisecond, 0, false)
	})
}

func TestRecordGPUHNSWBuildBatch(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUHNSWBuildBatch(50*time.Millisecond, 500)
	})
}

func TestRecordGPUHNSWBuildFallback(t *testing.T) {
	assert.NotPanics(t, func() {
		RecordGPUHNSWBuildFallback("oom")
	})
}
