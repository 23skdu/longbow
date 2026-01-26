package security

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// AuditEntry represents a security audit log entry
type AuditEntry struct {
	Timestamp time.Time
	UserID    string
	Operation string
	Resource  string
	IPAddress string
	Success   bool
	Reason    string
}

// AuditLogger handles security audit logging
type AuditLogger struct {
	logger zerolog.Logger
}

// NewAuditLogger creates a new audit logger
func NewAuditLogger() *AuditLogger {
	logger := log.With().Str("component", "security_audit")
	return &AuditLogger{
		logger: logger.Logger(),
	}
}

// LogAuditEntry logs an audit entry
func (a *AuditLogger) LogAuditEntry(ctx context.Context, entry AuditEntry) {
	event := a.logger.Info().
		Str("operation", entry.Operation).
		Str("resource", entry.Resource).
		Str("user_id", entry.UserID).
		Str("ip_address", entry.IPAddress).
		Bool("success", entry.Success).
		Time("timestamp", entry.Timestamp)

	if entry.Reason != "" {
		event.Str("reason", entry.Reason)
	}

	if !entry.Success {
		event.Str("status", "failed")
	} else {
		event.Str("status", "success")
	}

	event.Send()
}

// ValidateInput validates and sanitizes input parameters
func ValidateInput(input string) error {
	// Length checks
	if len(input) > 1024 {
		return fmt.Errorf("input too long: max 1024 characters")
	}

	// Character validation
	for _, char := range []string{"<", ">", "&", "\"", "'", "\x00"} {
		if strings.Contains(input, char) {
			return fmt.Errorf("input contains invalid characters")
		}
	}

	// SQL injection prevention (basic)
	if strings.Contains(strings.ToLower(input), "select") ||
		strings.Contains(strings.ToLower(input), "insert") ||
		strings.Contains(strings.ToLower(input), "delete") ||
		strings.Contains(strings.ToLower(input), "drop") {
		return fmt.Errorf("input contains potentially malicious SQL keywords")
	}

	// Path traversal protection
	if strings.Contains(input, "..") || strings.Contains(input, "\\") {
		return fmt.Errorf("input contains potentially malicious path patterns")
	}

	return nil
}

// AuthenticationMiddleware provides basic authentication middleware
func AuthenticationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check for API key in headers
		apiKey := r.Header.Get("Authorization")
		if apiKey == "" {
			http.Error(w, "Unauthorized: Missing API key", http.StatusUnauthorized)
			return
		}

		// Validate API key format (simplified)
		if !strings.HasPrefix(apiKey, "Bearer ") {
			http.Error(w, "Unauthorized: Invalid API key format", http.StatusUnauthorized)
			return
		}

		// Extract and validate token
		token := strings.TrimPrefix(apiKey, "Bearer ")
		if err := ValidateInput(token); err != nil {
			http.Error(w, "Unauthorized: Invalid token", http.StatusUnauthorized)
			return
		}

		// Log authentication attempt
		auditLogger := NewAuditLogger()
		auditLogger.LogAuditEntry(r.Context(), AuditEntry{
			Timestamp: time.Now(),
			UserID:    token,
			Operation: "authenticate",
			Resource:  r.URL.Path,
			IPAddress: getClientIP(r),
			Success:   true,
			Reason:    "",
		})

		// Call next handler
		next.ServeHTTP(w, r)
	})
}

// getClientIP extracts the real client IP address
func getClientIP(r *http.Request) string {
	// Check X-Forwarded-For header first (proxy/load balancer)
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// X-Forwarded-For can contain multiple IPs, take the first one
		if idx := strings.Index(xff, ","); idx != -1 {
			return xff[:idx]
		}
		return xff
	}

	// Fall back to X-Real-IP
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}

	// Finally use RemoteAddr
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
