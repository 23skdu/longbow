package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	OIDCAuthAttempts = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_oidc_auth_attempts_total",
			Help: "Total OIDC federation authentication attempts",
		},
		[]string{"provider", "issuer"},
	)

	OIDCAuthFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_oidc_auth_failures_total",
			Help: "Total OIDC federation authentication failures",
		},
		[]string{"provider", "issuer", "reason"},
	)

	OIDCAuthSuccesses = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_oidc_auth_successes_total",
			Help: "Total successful OIDC federation authentications",
		},
		[]string{"provider", "issuer"},
	)

	OIDCJWKSFetchFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_oidc_jwks_fetch_failures_total",
			Help: "Total failures fetching OIDC JWKS",
		},
		[]string{"issuer"},
	)

	OIDCTokenValidationLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_oidc_token_validation_seconds",
			Help:    "OIDC token validation latency",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"provider", "issuer"},
	)
)
