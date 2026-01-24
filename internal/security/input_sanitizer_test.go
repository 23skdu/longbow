package security

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRateLimiter(t *testing.T) {
	rl := NewRateLimiter(10, 60) // 10 requests per minute

	t.Run("WithinLimit", func(t *testing.T) {
		assert.False(t, rl.CheckRateLimit("127.0.0.1"))
		assert.False(t, rl.CheckRateLimit("127.0.0.1"))
	})

	t.Run("ExceedsLimit", func(t *testing.T) {
		// Simulate exceeding limit
		for i := 0; i < 11; i++ {
			rl.CheckRateLimit("192.168.1.1")
		}
		assert.True(t, rl.CheckRateLimit("192.168.1.1"))
	})
}

func TestInputSanitizer(t *testing.T) {
	is := NewInputSanitizer()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "HTML tags",
			input:    "<script>alert('xss')</script>",
			expected: "&lt;script&gt;alert('xss')&lt;/script&gt;",
		},
		{
			name:     "JavaScript protocol",
			input:    "javascript:alert('xss')",
			expected: "alert('xss')",
		},
		{
			name:     "Data protocol",
			input:    "data:text/html,<script>alert('xss')</script>",
			expected: "text/html,&lt;script&gt;alert('xss')&lt;/script&gt;",
		},
		{
			name:     "Extra whitespace",
			input:    "  test  ",
			expected: "test",
		},
		{
			name:     "Clean input",
			input:    "normal-text",
			expected: "normal-text",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := is.SanitizeString(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSecurityHeaders(t *testing.T) {
	// Create a mock handler
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Wrap with security headers middleware
	protectedHandler := SecurityHeaders(nextHandler)

	req := httptest.NewRequest("GET", "/test", nil)
	require.NoError(t, err)

	w := httptest.NewRecorder()
	protectedHandler.ServeHTTP(w, req)

	// Check that security headers are set
	assert.Equal(t, "DENY", w.Header().Get("X-Frame-Options"))
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
	assert.Equal(t, "1; mode=block", w.Header().Get("X-XSS-Protection"))
	assert.Equal(t, "max-age=31536000; includeSubDomains", w.Header().Get("Strict-Transport-Security"))
	assert.Equal(t, "default-src 'self'", w.Header().Get("Content-Security-Policy"))
}
