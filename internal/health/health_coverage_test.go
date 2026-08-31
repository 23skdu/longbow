package health

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

func makeLogger() zerolog.Logger {
	return zerolog.Nop()
}

func makeTracer() noop.Tracer {
	return noop.Tracer{}
}

// HealthManager tests
func TestNewHealthManager(t *testing.T) {
	hm := NewHealthManager("1.0.0", makeLogger(), makeTracer())
	require.NotNil(t, hm)
}

func TestHealthManager_RegisterChecker(t *testing.T) {
	hm := NewHealthManager("1.0.0", makeLogger(), makeTracer())
	checker := NewMetricsChecker(makeLogger(), makeTracer())
	hm.RegisterChecker(checker)
}

func TestHealthManager_GetRegistry(t *testing.T) {
	hm := NewHealthManager("1.0.0", makeLogger(), makeTracer())
	reg := hm.GetRegistry()
	assert.NotNil(t, reg)
}

func TestHealthManager_CheckHealth_Empty(t *testing.T) {
	hm := NewHealthManager("1.0.0", makeLogger(), makeTracer())
	health := hm.CheckHealth(context.Background())
	require.NotNil(t, health)
	assert.Equal(t, StatusHealthy, health.Status)
}

func TestHealthManager_CheckHealth_WithCheckers(t *testing.T) {
	hm := NewHealthManager("1.0.0", makeLogger(), makeTracer())
	hm.RegisterChecker(NewMetricsChecker(makeLogger(), makeTracer()))
	hm.RegisterChecker(NewLoggingChecker(makeLogger(), makeTracer()))
	health := hm.CheckHealth(context.Background())
	require.NotNil(t, health)
	assert.NotEmpty(t, health.Components)
}

func TestHealthManager_HTTPHandler(t *testing.T) {
	hm := NewHealthManager("1.0.0", makeLogger(), makeTracer())
	handler := hm.HTTPHandler()
	require.NotNil(t, handler)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result SystemHealth
	err := json.NewDecoder(resp.Body).Decode(&result)
	assert.NoError(t, err)
}

// Tracing checker
func TestNewTracingChecker(t *testing.T) {
	tc := NewTracingChecker(makeLogger(), makeTracer())
	require.NotNil(t, tc)
	assert.Equal(t, "tracing", tc.Name())
}

func TestTracingChecker_Check(t *testing.T) {
	tc := NewTracingChecker(makeLogger(), makeTracer())
	result := tc.Check(context.Background())
	require.NotNil(t, result)
	assert.Equal(t, "tracing", result.Name)
}

// DatabaseChecker — coverage
func TestDatabaseChecker_Coverage(t *testing.T) {
	logger := makeLogger()
	tracer := makeTracer()
	dc := NewDatabaseChecker(&logger, tracer)
	require.NotNil(t, dc)
	assert.Equal(t, "database", dc.Name())
}

func TestDatabaseChecker_Check_Coverage(t *testing.T) {
	logger := makeLogger()
	tracer := makeTracer()
	dc := NewDatabaseChecker(&logger, tracer)
	result := dc.Check(context.Background())
	require.NotNil(t, result)
}

// Storage checker — coverage
func TestStorageChecker_Coverage(t *testing.T) {
	sc := NewStorageChecker(makeLogger(), makeTracer())
	require.NotNil(t, sc)
	assert.Equal(t, "storage", sc.Name())
}

func TestStorageChecker_Check_Coverage(t *testing.T) {
	sc := NewStorageChecker(makeLogger(), makeTracer())
	result := sc.Check(context.Background())
	require.NotNil(t, result)
}

// mockHealthChecker implements HealthChecker with configurable status
type mockHealthChecker struct {
	name   string
	status HealthStatus
}

func (m *mockHealthChecker) Name() string {
	return m.name
}

func (m *mockHealthChecker) Check(_ context.Context) *ComponentHealth {
	return &ComponentHealth{
		Name:   m.name,
		Status: m.status,
	}
}

func TestCheckHealth_WithDegradedChecker(t *testing.T) {
	hm := NewHealthManager("1.0.0", makeLogger(), makeTracer())
	hm.RegisterChecker(&mockHealthChecker{name: "mock", status: StatusDegraded})
	health := hm.CheckHealth(context.Background())
	require.NotNil(t, health)
	assert.Equal(t, StatusDegraded, health.Status)
}

func TestCheckHealth_WithUnhealthyChecker(t *testing.T) {
	hm := NewHealthManager("1.0.0", makeLogger(), makeTracer())
	hm.RegisterChecker(&mockHealthChecker{name: "mock", status: StatusUnhealthy})
	health := hm.CheckHealth(context.Background())
	require.NotNil(t, health)
	assert.Equal(t, StatusUnhealthy, health.Status)
}

func TestHTTPHandler_UnhealthyReturns503(t *testing.T) {
	hm := NewHealthManager("1.0.0", makeLogger(), makeTracer())
	hm.RegisterChecker(&mockHealthChecker{name: "mock", status: StatusUnhealthy})
	handler := hm.HTTPHandler()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	var result SystemHealth
	err := json.NewDecoder(w.Body).Decode(&result)
	require.NoError(t, err)
	assert.Equal(t, StatusUnhealthy, result.Status)
}

func TestGetSystemInfo_WithGC(t *testing.T) {
	hm := NewHealthManager("1.0.0", makeLogger(), makeTracer())
	runtime.GC()
	info := hm.getSystemInfo()
	require.NotNil(t, info)
	assert.False(t, info.LastGC.IsZero(), "LastGC should be non-zero after GC")
	assert.NotEmpty(t, info.GoVersion)
	assert.Positive(t, info.NumGoroutines)
}
