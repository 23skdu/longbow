package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// SecurityAuditDenials counts unsuccessful security audit entries.
	//
	// All label values are sanitized by the security package before they are
	// written: category comes from a fixed vocabulary, operation and transport
	// are bounded/sanitized. Raw denial reasons, user IDs and API keys are
	// never used as label values.
	SecurityAuditDenials = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_security_audit_denials_total",
			Help: "Total unsuccessful security audit entries by category, operation and transport",
		},
		[]string{"category", "operation", "transport"},
	)
)
