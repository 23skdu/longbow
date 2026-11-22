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

// TestNewHealthManager tests
func TestNewHealthManager(t *testing.T) {
	hm := NewHealthManager("1.0.0", makeLogger(), makeTracer())
	require.NotNil(t, hm)
}

// TestHealthManager_RegisterChecker runs the test health manager_register checker operation.
func TestHealthManager_RegisterChecker(t *testing.T) {
	hm := NewHealthManager("1.0.0", makeLogger(), makeTracer())
	checker := NewMetricsChecker(makeLogger(), makeTracer())
	hm.RegisterChecker(checker)
}

// TestHealthManager_GetRegistry runs the test health manager_get registry operation.
func TestHealthManager_GetRegistry(t *testing.T) {
	hm := NewHealthManager("1.0.0", makeLogger(), makeTracer())
	reg := hm.GetRegistry()
	assert.NotNil(t, reg)
}

// TestHealthManager_CheckHealth_Empty runs the test health manager_check health_empty operation.
func TestHealthManager_CheckHealth_Empty(t *testing.T) {
	hm := NewHealthManager("1.0.0", makeLogger(), makeTracer())
	health := hm.CheckHealth(context.Background())
	require.NotNil(t, health)
	assert.Equal(t, StatusHealthy, health.Status)
}

// TestHealthManager_CheckHealth_WithCheckers runs the test health manager_check health_with checkers operation.
func TestHealthManager_CheckHealth_WithCheckers(t *testing.T) {
	hm := NewHealthManager("1.0.0", makeLogger(), makeTracer())
	hm.RegisterChecker(NewMetricsChecker(makeLogger(), makeTracer()))
	hm.RegisterChecker(NewLoggingChecker(makeLogger(), makeTracer()))
	health := hm.CheckHealth(context.Background())
	require.NotNil(t, health)
	assert.NotEmpty(t, health.Components)
}

// TestHealthManager_HTTPHandler runs the test health manager_http handler operation.
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

// TestNewTracingChecker checker
func TestNewTracingChecker(t *testing.T) {
	tc := NewTracingChecker(makeLogger(), makeTracer())
	require.NotNil(t, tc)
	assert.Equal(t, "tracing", tc.Name())
}

// TestTracingChecker_Check runs the test tracing checker_check operation.
func TestTracingChecker_Check(t *testing.T) {
	tc := NewTracingChecker(makeLogger(), makeTracer())
	result := tc.Check(context.Background())
	require.NotNil(t, result)
	assert.Equal(t, "tracing", result.Name)
}

// TestDatabaseChecker_Coverage — coverage
func TestDatabaseChecker_Coverage(t *testing.T) {
	logger := makeLogger()
	tracer := makeTracer()
	dc := NewDatabaseChecker(&logger, tracer)
	require.NotNil(t, dc)
	assert.Equal(t, "database", dc.Name())
}

// TestDatabaseChecker_Check_Coverage runs the test database checker_check_coverage operation.
func TestDatabaseChecker_Check_Coverage(t *testing.T) {
	logger := makeLogger()
	tracer := makeTracer()
	dc := NewDatabaseChecker(&logger, tracer)
	result := dc.Check(context.Background())
	require.NotNil(t, result)
}

// TestStorageChecker_Coverage checker — coverage
func TestStorageChecker_Coverage(t *testing.T) {
	sc := NewStorageChecker(makeLogger(), makeTracer())
	require.NotNil(t, sc)
	assert.Equal(t, "storage", sc.Name())
}

// TestStorageChecker_Check_Coverage runs the test storage checker_check_coverage operation.
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

// Name runs the name operation.
func (m *mockHealthChecker) Name() string {
	return m.name
}

// Check runs the check operation.
func (m *mockHealthChecker) Check(_ context.Context) *ComponentHealth {
	return &ComponentHealth{
		Name:   m.name,
		Status: m.status,
	}
}

// TestCheckHealth_WithDegradedChecker runs the test check health_with degraded checker operation.
func TestCheckHealth_WithDegradedChecker(t *testing.T) {
	hm := NewHealthManager("1.0.0", makeLogger(), makeTracer())
	hm.RegisterChecker(&mockHealthChecker{name: "mock", status: StatusDegraded})
	health := hm.CheckHealth(context.Background())
	require.NotNil(t, health)
	assert.Equal(t, StatusDegraded, health.Status)
}

// TestCheckHealth_WithUnhealthyChecker runs the test check health_with unhealthy checker operation.
func TestCheckHealth_WithUnhealthyChecker(t *testing.T) {
	hm := NewHealthManager("1.0.0", makeLogger(), makeTracer())
	hm.RegisterChecker(&mockHealthChecker{name: "mock", status: StatusUnhealthy})
	health := hm.CheckHealth(context.Background())
	require.NotNil(t, health)
	assert.Equal(t, StatusUnhealthy, health.Status)
}

// TestHTTPHandler_UnhealthyReturns503 runs the test http handler_unhealthy returns503 operation.
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

// TestGetSystemInfo_WithGC runs the test get system info_with gc operation.
func TestGetSystemInfo_WithGC(t *testing.T) {
	hm := NewHealthManager("1.0.0", makeLogger(), makeTracer())
	runtime.GC()
	info := hm.getSystemInfo()
	require.NotNil(t, info)
	assert.False(t, info.LastGC.IsZero(), "LastGC should be non-zero after GC")
	assert.NotEmpty(t, info.GoVersion)
	assert.Positive(t, info.NumGoroutines)
}
