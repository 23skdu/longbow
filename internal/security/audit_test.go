package security

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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
