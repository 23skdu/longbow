package security

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestValidateInput runs the test validate input operation.
func TestValidateInput(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
		description string
	}{
		{
			name:        "Valid short input",
			input:       "valid-input",
			expectError: false,
			description: "Should accept valid input",
		},
		{
			name:        "Too long input",
			input:       string(make([]byte, 1025)),
			expectError: true,
			description: "Should reject input > 1024 chars",
		},
		{
			name:        "HTML characters",
			input:       "<script>alert('xss')</script>",
			expectError: true,
			description: "Should reject HTML characters",
		},
		{
			name:        "SQL injection attempt",
			input:       "'; DROP TABLE users; --",
			expectError: true,
			description: "Should reject SQL injection",
		},
		{
			name:        "Path traversal attempt",
			input:       "../../../etc/passwd",
			expectError: true,
			description: "Should reject path traversal",
		},
		{
			name:        "Null byte",
			input:       "input\x00injection",
			expectError: true,
			description: "Should reject null bytes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateInput(tt.input)
			if tt.expectError {
				assert.Error(t, err, tt.description)
			} else {
				assert.NoError(t, err, tt.description)
			}
		})
	}
}

// TestAuditLogger runs the test audit logger operation.
func TestAuditLogger(t *testing.T) {
	auditLogger := NewAuditLogger()

	t.Run("SuccessfulEntry", func(t *testing.T) {
		entry := AuditEntry{
			Timestamp: time.Now(),
			UserID:    "test-user",
			Operation: "login",
			Resource:  "/api/login",
			IPAddress: "192.168.1.1",
			Success:   true,
			Reason:    "",
		}

		assert.NotPanics(t, func() {
			auditLogger.LogAuditEntry(context.Background(), &entry)
		})
	})

	t.Run("FailedEntry", func(t *testing.T) {
		entry := AuditEntry{
			Timestamp: time.Now(),
			UserID:    "test-user",
			Operation: "delete",
			Resource:  "/api/data/123",
			IPAddress: "192.168.1.1",
			Success:   false,
			Reason:    "insufficient_permissions",
		}

		assert.NotPanics(t, func() {
			auditLogger.LogAuditEntry(context.Background(), &entry)
		})
	})
}

// TestAuthenticationMiddleware runs the test authentication middleware operation.
func TestAuthenticationMiddleware(t *testing.T) {
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("protected content")); err != nil {
			t.Logf("failed to write response: %v", err)
		}
	})

	protectedHandler := AuthenticationMiddleware(nextHandler)

	tests := []struct {
		name           string
		headers        map[string]string
		expectedStatus int
		expectedBody   string
	}{
		{
			name:           "Missing API key",
			headers:        map[string]string{},
			expectedStatus: http.StatusUnauthorized,
			expectedBody:   "Unauthorized: Missing API key\n",
		},
		{
			name:           "Invalid API key format",
			headers:        map[string]string{"Authorization": "invalid-token"},
			expectedStatus: http.StatusUnauthorized,
			expectedBody:   "Unauthorized: Invalid API key format\n",
		},
		{
			name:           "Valid Bearer token",
			headers:        map[string]string{"Authorization": "Bearer valid-token-123"},
			expectedStatus: http.StatusOK,
			expectedBody:   "protected content",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test", http.NoBody)

			for key, value := range tt.headers {
				req.Header.Set(key, value)
			}

			w := httptest.NewRecorder()
			protectedHandler.ServeHTTP(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code)
			assert.Equal(t, tt.expectedBody, w.Body.String())
		})
	}
}

// TestGetClientIP runs the test get client ip operation.
func TestGetClientIP(t *testing.T) {
	tests := []struct {
		name       string
		headers    map[string]string
		remoteAddr string
		expectedIP string
	}{
		{
			name:       "X-Forwarded-For single IP",
			headers:    map[string]string{"X-Forwarded-For": "192.168.1.100"},
			remoteAddr: "10.0.0.1:12345",
			expectedIP: "192.168.1.100",
		},
		{
			name:       "X-Forwarded-For multiple IPs",
			headers:    map[string]string{"X-Forwarded-For": "192.168.1.100,10.0.0.1"},
			remoteAddr: "10.0.0.1:12345",
			expectedIP: "192.168.1.100",
		},
		{
			name:       "X-Real-IP",
			headers:    map[string]string{"X-Real-IP": "192.168.1.200"},
			remoteAddr: "10.0.0.1:12345",
			expectedIP: "192.168.1.200",
		},
		{
			name:       "RemoteAddr fallback",
			headers:    map[string]string{},
			remoteAddr: "192.168.1.50:12345",
			expectedIP: "192.168.1.50",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test", http.NoBody)

			req.RemoteAddr = tt.remoteAddr
			for key, value := range tt.headers {
				req.Header.Set(key, value)
			}

			ip := getClientIP(req)
			assert.Equal(t, tt.expectedIP, ip)
		})
	}
}

// TestAuthenticationMiddlewareRedactsBearerToken verifies that a raw API key
// never reaches an audit record, either in the queryable buffer or in JSON.
func TestAuthenticationMiddlewareRedactsBearerToken(t *testing.T) {
	original := middlewareAuditLogger
	middlewareAuditLogger = NewAuditLogger()
	t.Cleanup(func() { middlewareAuditLogger = original })

	const rawToken = "super-secret-raw-token-9f8e7d"
	handler := AuthenticationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/secure", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+rawToken)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	entries := middlewareAuditLogger.RecentEntries(10)
	require.Len(t, entries, 1)
	require.NotEqual(t, rawToken, entries[0].UserID)
	require.NotContains(t, entries[0].UserID, rawToken)

	sum := sha256.Sum256([]byte(rawToken))
	require.Equal(t, "token_"+hex.EncodeToString(sum[:4]), entries[0].UserID)

	encoded, err := json.Marshal(entries)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), rawToken)
	require.Equal(t, "redacted", redactToken(""))
}

// TestAuditDenialCategorySanitizer pins the fixed denial category vocabulary.
func TestAuditDenialCategorySanitizer(t *testing.T) {
	tests := []struct {
		reason   string
		expected string
	}{
		{"permission denied", "permission_denied"},
		{"insufficient_permissions for secret-key-123", "permission_denied"},
		{"invalid API key for secret-key-123", "invalid_api_key"},
		{"API key disabled", "disabled_api_key"},
		{"tenant isolation boundary", "namespace_mismatch"},
		{"namespace mismatch: tenant-secret", "namespace_mismatch"},
		{"dataset not authorized", "dataset_not_authorized"},
		{"missing HTTP credentials", "missing_credentials"},
		{"invalid input: secret-key-123", "invalid_input"},
		{"unrecognized rejection reason for secret-key-123", "other"},
		{"", "other"},
	}
	for _, tt := range tests {
		t.Run(tt.reason, func(t *testing.T) {
			got := auditDenialCategory(tt.reason)
			require.Equal(t, tt.expected, got)
			require.NotContains(t, got, "secret-key-123")
		})
	}
}

// TestAuditDenialCountsAndMetric checks that only failures are counted, that
// denial reasons are reduced to safe label values, and that the queryable view
// exposes no user IDs or tokens.
func TestAuditDenialCountsAndMetric(t *testing.T) {
	const secret = "super-secret-raw-token-9f8e7d"
	auditLogger := NewAuditLogger()
	entries := []AuditEntry{
		{UserID: "user-secret-id", Operation: "read", IPAddress: "flight", Success: false, Reason: "permission denied"},
		{UserID: "user-secret-id", Operation: "read", IPAddress: "flight", Success: false, Reason: "permission denied; key=" + secret},
		{UserID: "user-secret-id", Operation: "read", IPAddress: "flight", Success: true, Reason: ""},
		{UserID: "user-secret-id", Operation: "authorize:read", IPAddress: "rpc", Success: false, Reason: "invalid API key " + secret},
	}

	metric := metrics.SecurityAuditDenials
	permission := metric.WithLabelValues("permission_denied", "read", "flight")
	invalidKey := metric.WithLabelValues("invalid_api_key", "authorize:read", "rpc")
	otherOp := metric.WithLabelValues("permission_denied", "write", "flight")
	beforePermission, beforeInvalidKey, beforeOtherOp := testutil.ToFloat64(permission), testutil.ToFloat64(invalidKey), testutil.ToFloat64(otherOp)
	for i := range entries {
		auditLogger.LogAuditEntry(context.Background(), &entries[i])
	}
	require.Equal(t, beforePermission+2, testutil.ToFloat64(permission), "unsuccessful entries increment their denial category")
	require.Equal(t, beforeInvalidKey+1, testutil.ToFloat64(invalidKey))
	require.Equal(t, beforeOtherOp, testutil.ToFloat64(otherOp), "successes and other operations must not increment this series")

	counts := auditLogger.DenialCounts()
	require.Equal(t, 2, counts["permission_denied|read|flight"])
	require.Equal(t, 1, counts["invalid_api_key|authorize:read|rpc"])
	require.Len(t, counts, 2)
	for key := range counts {
		require.NotContains(t, key, secret)
		require.NotContains(t, key, "user-secret-id")
	}
	require.Empty(t, (*AuditLogger)(nil).DenialCounts())
}
