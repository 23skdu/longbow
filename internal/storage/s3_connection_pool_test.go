package storage

import (
	"net/http"
	"testing"
	"time"
)

// TestS3ConnectionPoolConfig tests connection pool configuration
func TestS3ConnectionPoolConfig_Defaults(t *testing.T) {
	cfg := &S3BackendConfig{
		Endpoint:        "http://localhost:9000",
		Bucket:          "test-bucket",
		AccessKeyID:     "testkey",
		SecretAccessKey: "testsecret",
		Region:          "us-east-1",
	}

	backend, err := NewS3Backend(cfg)
	if err != nil {
		t.Fatalf("NewS3Backend failed: %v", err)
	}

	// Verify backend was created
	if backend == nil {
		t.Fatal("Backend is nil")
	}

	// Verify transport has default pool settings
	transport := backend.GetHTTPTransport()
	if transport == nil {
		t.Fatal("Transport is nil - connection pooling not configured")
	}

	_ = http.MethodGet // Silences "unused" lint

	// Check default pool values
	if transport.MaxIdleConns != DefaultMaxIdleConns {
		t.Errorf("MaxIdleConns = %d, want %d", transport.MaxIdleConns, DefaultMaxIdleConns)
	}
	if transport.MaxIdleConnsPerHost != DefaultMaxIdleConnsPerHost {
		t.Errorf("MaxIdleConnsPerHost = %d, want %d", transport.MaxIdleConnsPerHost, DefaultMaxIdleConnsPerHost)
	}
	if transport.IdleConnTimeout != DefaultIdleConnTimeout {
		t.Errorf("IdleConnTimeout = %v, want %v", transport.IdleConnTimeout, DefaultIdleConnTimeout)
	}
}

// TestS3ConnectionPoolConfig_Custom tests custom connection pool settings
func TestS3ConnectionPoolConfig_Custom(t *testing.T) {
	cfg := &S3BackendConfig{
		Endpoint:            "http://localhost:9000",
		Bucket:              "test-bucket",
		AccessKeyID:         "testkey",
		SecretAccessKey:     "testsecret",
		Region:              "us-east-1",
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 50,
		IdleConnTimeout:     120 * time.Second,
	}

	backend, err := NewS3Backend(cfg)
	if err != nil {
		t.Fatalf("NewS3Backend failed: %v", err)
	}

	transport := backend.GetHTTPTransport()
	if transport == nil {
		t.Fatal("Transport is nil")
	}

	if transport.MaxIdleConns != 200 {
		t.Errorf("MaxIdleConns = %d, want 200", transport.MaxIdleConns)
	}
	if transport.MaxIdleConnsPerHost != 50 {
		t.Errorf("MaxIdleConnsPerHost = %d, want 50", transport.MaxIdleConnsPerHost)
	}
	if transport.IdleConnTimeout != 120*time.Second {
		t.Errorf("IdleConnTimeout = %v, want 120s", transport.IdleConnTimeout)
	}
}

// TestS3ConnectionPoolConfig_ZeroValues tests that zero values use defaults
func TestS3ConnectionPoolConfig_ZeroValues(t *testing.T) {
	cfg := &S3BackendConfig{
		Endpoint:            "http://localhost:9000",
		Bucket:              "test-bucket",
		AccessKeyID:         "testkey",
		SecretAccessKey:     "testsecret",
		Region:              "us-east-1",
		MaxIdleConns:        0, // Should use default
		MaxIdleConnsPerHost: 0, // Should use default
		IdleConnTimeout:     0, // Should use default
	}

	backend, err := NewS3Backend(cfg)
	if err != nil {
		t.Fatalf("NewS3Backend failed: %v", err)
	}

	transport := backend.GetHTTPTransport()
	if transport == nil {
		t.Fatal("Transport is nil")
	}

	// Zero values should fall back to defaults
	if transport.MaxIdleConns != DefaultMaxIdleConns {
		t.Errorf("MaxIdleConns = %d, want default %d", transport.MaxIdleConns, DefaultMaxIdleConns)
	}
	if transport.MaxIdleConnsPerHost != DefaultMaxIdleConnsPerHost {
		t.Errorf("MaxIdleConnsPerHost = %d, want default %d", transport.MaxIdleConnsPerHost, DefaultMaxIdleConnsPerHost)
	}
	if transport.IdleConnTimeout != DefaultIdleConnTimeout {
		t.Errorf("IdleConnTimeout = %v, want default %v", transport.IdleConnTimeout, DefaultIdleConnTimeout)
	}
}

// TestS3ConnectionPool_TransportReuse tests that transport is reused across operations
func TestS3ConnectionPool_TransportReuse(t *testing.T) {
	cfg := &S3BackendConfig{
		Endpoint:        "http://localhost:9000",
		Bucket:          "test-bucket",
		AccessKeyID:     "testkey",
		SecretAccessKey: "testsecret",
		Region:          "us-east-1",
	}

	backend, err := NewS3Backend(cfg)
	if err != nil {
		t.Fatalf("NewS3Backend failed: %v", err)
	}

	// Get transport multiple times - should be same instance
	transport1 := backend.GetHTTPTransport()
	transport2 := backend.GetHTTPTransport()

	if transport1 != transport2 {
		t.Error("Transport should be reused, got different instances")
	}
}

// TestS3ConnectionPool_HTTPClientConfigured tests HTTP client uses pooled transport
func TestS3ConnectionPool_HTTPClientConfigured(t *testing.T) {
	cfg := &S3BackendConfig{
		Endpoint:        "http://localhost:9000",
		Bucket:          "test-bucket",
		AccessKeyID:     "testkey",
		SecretAccessKey: "testsecret",
		Region:          "us-east-1",
	}

	backend, err := NewS3Backend(cfg)
	if err != nil {
		t.Fatalf("NewS3Backend failed: %v", err)
	}

	httpClient := backend.GetHTTPClient()
	if httpClient == nil {
		t.Fatal("HTTP client is nil")
	}

	// Verify client uses our pooled transport
	if httpClient.Transport == nil {
		t.Error("HTTP client transport is nil")
	}

	transport, ok := httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatal("HTTP client transport is not *http.Transport")
	}

	// Verify it's our configured transport
	if transport.MaxIdleConns != DefaultMaxIdleConns {
		t.Errorf("HTTP client transport MaxIdleConns = %d, want %d", transport.MaxIdleConns, DefaultMaxIdleConns)
	}
}

// TestS3ConnectionPool_MultipleBackendsShareNothing tests backends don't share transports
func TestS3ConnectionPool_MultipleBackendsShareNothing(t *testing.T) {
	cfg1 := &S3BackendConfig{
		Endpoint:        "http://localhost:9000",
		Bucket:          "bucket1",
		AccessKeyID:     "testkey",
		SecretAccessKey: "testsecret",
		Region:          "us-east-1",
	}

	cfg2 := &S3BackendConfig{
		Endpoint:        "http://localhost:9001",
		Bucket:          "bucket2",
		AccessKeyID:     "testkey",
		SecretAccessKey: "testsecret",
		Region:          "us-west-2",
	}

	backend1, err := NewS3Backend(cfg1)
	if err != nil {
		t.Fatalf("NewS3Backend(cfg1) failed: %v", err)
	}

	backend2, err := NewS3Backend(cfg2)
	if err != nil {
		t.Fatalf("NewS3Backend(cfg2) failed: %v", err)
	}

	// Each backend should have its own transport
	transport1 := backend1.GetHTTPTransport()
	transport2 := backend2.GetHTTPTransport()

	if transport1 == transport2 {
		t.Error("Different backends should have different transports")
	}
}
