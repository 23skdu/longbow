package security

import (
	"net/http"
	"strings"
)

// RateLimiter provides basic rate limiting functionality
type RateLimiter struct {
	requests      map[string]int
	maxRequests   int
	windowSeconds int
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(maxRequests, windowSeconds int) *RateLimiter {
	return &RateLimiter{
		requests:      make(map[string]int),
		maxRequests:   maxRequests,
		windowSeconds: windowSeconds,
	}
}

// CheckRateLimit checks if a request exceeds rate limits
func (rl *RateLimiter) CheckRateLimit(ip string) bool {
	// Simple implementation - in production, use sliding window or token bucket
	rl.requests[ip]++
	return rl.requests[ip] > rl.maxRequests
}

// SecurityHeaders adds security-related HTTP headers
func SecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Prevent clickjacking
		w.Header().Set("X-Frame-Options", "DENY")

		// Prevent MIME type sniffing
		w.Header().Set("X-Content-Type-Options", "nosniff")

		// XSS protection
		w.Header().Set("X-XSS-Protection", "1; mode=block")

		// HSTS
		w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")

		// Content Security Policy
		w.Header().Set("Content-Security-Policy", "default-src 'self'")

		next.ServeHTTP(w, r)
	})
}

// InputSanitizer provides input sanitization functions
type InputSanitizer struct{}

// NewInputSanitizer creates a new input sanitizer
func NewInputSanitizer() *InputSanitizer {
	return &InputSanitizer{}
}

// SanitizeString removes potentially dangerous characters
func (is *InputSanitizer) SanitizeString(input string) string {
	// Remove HTML tags
	sanitized := strings.ReplaceAll(input, "<", "&lt;")
	sanitized = strings.ReplaceAll(sanitized, ">", "&gt;")

	// Remove potentially dangerous attributes
	sanitized = strings.ReplaceAll(sanitized, "javascript:", "")
	sanitized = strings.ReplaceAll(sanitized, "data:", "")

	// Trim whitespace
	return strings.TrimSpace(sanitized)
}
