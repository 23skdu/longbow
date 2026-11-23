package security

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
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
	Transport string    `json:"transport,omitempty"`
	Success   bool      `json:"success"`
	Reason    string    `json:"reason,omitempty"`
}

const defaultAuditBufferSize = 4096

// middlewareAuditLogger retains the HTTP middleware's audit entries so they
// can be queried (and tested) rather than being discarded per request.
var middlewareAuditLogger = NewAuditLogger()

// knownAuditTransports are the transport tokens callers pass through the
// legacy RecordEvent ip argument. Other non-empty values are remote addresses.
var knownAuditTransports = map[string]bool{
	"auth":        true,
	"flight":      true,
	"grpc":        true,
	"grpc-stream": true,
	"http":        true,
	"internal":    true,
	"rpc":         true,
	"websocket":   true,
}

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

	// Only failures count as denials. The label values are sanitized before
	// they reach Prometheus, so no raw reason or API key is ever exported.
	if !rec.Success {
		metrics.SecurityAuditDenials.WithLabelValues(
			auditDenialCategory(rec.Reason),
			safeAuditLabel(rec.Operation, "unknown"),
			auditTransport(rec),
		).Inc()
	}

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

// DenialCounts summarizes unsuccessful entries held in the in-memory ring
// buffer, keyed by "category|operation|transport". It is intended for operators
// and soaks that need denial counts without scraping raw audit logs: no user
// IDs, tokens, reasons or resource paths are included in the result.
func (a *AuditLogger) DenialCounts() map[string]int {
	counts := make(map[string]int)
	if a == nil {
		return counts
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, rec := range a.entries {
		if rec.Success {
			continue
		}
		key := strings.Join([]string{
			auditDenialCategory(rec.Reason),
			safeAuditLabel(rec.Operation, "unknown"),
			auditTransport(rec),
		}, "|")
		counts[key]++
	}
	return counts
}

// auditDenialCategory maps a free-form (and potentially sensitive) denial
// reason to a small, fixed set of label values. Raw reason strings are never
// used as Prometheus label values.
func auditDenialCategory(reason string) string {
	r := strings.ToLower(strings.TrimSpace(reason))
	switch {
	case r == "":
		return "other"
	case containsAny(r, "missing credential", "missing http credential", "missing api key", "missing authentication", "missing token", "unauthenticated"):
		return "missing_credentials"
	case containsAny(r, "invalid api key", "invalid credential", "invalid token", "invalid jwt", "malformed token"):
		return "invalid_api_key"
	case containsAny(r, "disabled api key", "api key disabled", "key disabled", "revoked"):
		return "disabled_api_key"
	case containsAny(r, "permission denied", "insufficient permission", "insufficient_permission", "not permitted", "forbidden", "admin required"):
		return "permission_denied"
	case containsAny(r, "namespace mismatch", "tenant isolation", "tenant mismatch", "cross-tenant"):
		return "namespace_mismatch"
	case containsAny(r, "dataset not authorized", "dataset"):
		return "dataset_not_authorized"
	case containsAny(r, "invalid input", "invalid json", "invalid request", "validation failed"):
		return "invalid_input"
	default:
		return "other"
	}
}

// containsAny reports whether s contains at least one of the needles.
func containsAny(s string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(s, needle) {
			return true
		}
	}
	return false
}

// safeAuditLabel bounds an operation or transport label to a short, known
// character set. This prevents caller-controlled values from creating
// unbounded Prometheus series or leaking tokens into labels.
func safeAuditLabel(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	var b strings.Builder
	for _, r := range value {
		if b.Len() >= 64 {
			break
		}
		switch {
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_', r == '-', r == '.', r == ':':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return fallback
	}
	return b.String()
}

// auditTransport resolves the transport label for an entry. Newer callers may
// set AuditEntry.Transport directly; older RecordEvent callers pass a transport
// token in the ip argument and everything else is treated as a remote address.
func auditTransport(rec AuditEntry) string {
	if t := strings.TrimSpace(rec.Transport); t != "" {
		return safeAuditLabel(t, "unknown")
	}
	ip := strings.ToLower(strings.TrimSpace(rec.IPAddress))
	if knownAuditTransports[ip] {
		return safeAuditLabel(ip, "unknown")
	}
	if ip != "" {
		return "http"
	}
	return "unknown"
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

// redactToken turns a raw API key into a non-reversible identifier suitable
// for audit records. The raw key is never retained.
func redactToken(token string) string {
	if token == "" {
		return "redacted"
	}
	sum := sha256.Sum256([]byte(token))
	return "token_" + hex.EncodeToString(sum[:4])
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

		// Log authentication attempt with a non-reversible token identifier.
		entry := AuditEntry{
			Timestamp: time.Now(),
			UserID:    redactToken(token),
			Operation: "authenticate",
			Resource:  r.URL.Path,
			IPAddress: getClientIP(r),
			Transport: "http",
			Success:   true,
			Reason:    "",
		}
		middlewareAuditLogger.LogAuditEntry(r.Context(), &entry)

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
