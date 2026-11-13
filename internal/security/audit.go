package security

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// AuditEntry represents a security audit log entry
type AuditEntry struct {
	Timestamp time.Time `json:"timestamp"`
	UserID    string    `json:"user_id"`
	TenantID  string    `json:"tenant_id,omitempty"`
	Operation string    `json:"operation"`
	Resource  string    `json:"resource"`
	IPAddress string    `json:"ip_address"`
	Success   bool      `json:"success"`
	Reason    string    `json:"reason,omitempty"`
}

const defaultAuditBufferSize = 4096

// AuditLogger handles security audit logging with an in-memory queryable ring buffer.
type AuditLogger struct {
	logger  zerolog.Logger
	mu      sync.RWMutex
	entries []AuditEntry
	maxSize int
}

// NewAuditLogger creates a new audit logger
func NewAuditLogger() *AuditLogger {
	logger := log.With().Str("component", "security_audit")
	return &AuditLogger{
		logger:  logger.Logger(),
		maxSize: defaultAuditBufferSize,
		entries: make([]AuditEntry, 0, 256),
	}
}

// LogAuditEntry logs an audit entry and records it in the ring buffer.
func (a *AuditLogger) LogAuditEntry(ctx context.Context, entry *AuditEntry) {
	if a == nil || entry == nil {
		return
	}
	rec := *entry
	if rec.Timestamp.IsZero() {
		rec.Timestamp = time.Now()
	}

	a.mu.Lock()
	if a.maxSize <= 0 {
		a.maxSize = defaultAuditBufferSize
	}
	if len(a.entries) >= a.maxSize {
		copy(a.entries, a.entries[1:])
		a.entries[len(a.entries)-1] = rec
	} else {
		a.entries = append(a.entries, rec)
	}
	a.mu.Unlock()

	event := a.logger.Info().
		Str("operation", rec.Operation).
		Str("resource", rec.Resource).
		Str("user_id", rec.UserID).
		Str("tenant_id", rec.TenantID).
		Str("ip_address", rec.IPAddress).
		Bool("success", rec.Success).
		Time("timestamp", rec.Timestamp)

	if rec.Reason != "" {
		event.Str("reason", rec.Reason)
	}

	if !rec.Success {
		event.Str("status", "failed")
	} else {
		event.Str("status", "success")
	}

	event.Send()
}

// RecentEntries returns up to limit most recent audit log entries (newest first).
func (a *AuditLogger) RecentEntries(limit int) []AuditEntry {
	if a == nil {
		return nil
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	n := len(a.entries)
	if limit <= 0 || limit > n {
		limit = n
	}
	out := make([]AuditEntry, 0, limit)
	for i := n - 1; i >= n-limit; i-- {
		out = append(out, a.entries[i])
	}
	return out
}

// RecordEvent creates and logs a structured AuditEntry.
func (a *AuditLogger) RecordEvent(ctx context.Context, userID, tenantID, op, resource, ip string, success bool, reason string) {
	a.LogAuditEntry(ctx, &AuditEntry{
		Timestamp: time.Now(),
		UserID:    userID,
		TenantID:  tenantID,
		Operation: op,
		Resource:  resource,
		IPAddress: ip,
		Success:   success,
		Reason:    reason,
	})
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
		entry := AuditEntry{
			Timestamp: time.Now(),
			UserID:    token,
			Operation: "authenticate",
			Resource:  r.URL.Path,
			IPAddress: getClientIP(r),
			Success:   true,
			Reason:    "",
		}
		auditLogger.LogAuditEntry(r.Context(), &entry)

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
