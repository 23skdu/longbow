package security

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

const tenantContextKey contextKey = "longbow_tenant_context"

var (
	ErrUnauthorized    = errors.New("unauthorized")
	ErrForbidden       = errors.New("forbidden")
	ErrTenantIsolation = errors.New("cross-tenant access denied")
	ErrRLSValidation   = errors.New("row-level security policy violation")
)

// TenantContext holds authenticated identity, tenant boundary, roles, and RLS metadata.
type TenantContext struct {
	TenantID    string            `json:"tenant_id"`
	Namespace   string            `json:"namespace"`
	UserID      string            `json:"user_id"`
	KeyID       string            `json:"key_id"`
	Roles       []string          `json:"roles"`
	Permissions []string          `json:"permissions"`
	Datasets    []string          `json:"datasets,omitempty"`
	RowFilters  map[string]string `json:"row_filters,omitempty"`
	IsAdmin     bool              `json:"is_admin"`
}

// ContextWithTenant returns a derived context carrying TenantContext.
func ContextWithTenant(ctx context.Context, tc *TenantContext) context.Context {
	if tc == nil {
		return ctx
	}
	return context.WithValue(ctx, tenantContextKey, tc)
}

// TenantFromContext retrieves TenantContext from ctx if present.
func TenantFromContext(ctx context.Context) (*TenantContext, bool) {
	if ctx == nil {
		return nil, false
	}
	tc, ok := ctx.Value(tenantContextKey).(*TenantContext)
	if !ok || tc == nil {
		return nil, false
	}
	return tc, true
}

// CanAccessDataset verifies whether this TenantContext can access the given namespace/dataset.
func (tc *TenantContext) CanAccessDataset(namespace, dataset string) bool {
	if tc == nil {
		return false
	}
	if tc.IsAdmin {
		return true
	}
	if tc.TenantID != "" && namespace != "" && !strings.EqualFold(tc.TenantID, namespace) {
		if tc.Namespace == "" || tc.Namespace != namespace {
			return false
		}
	}
	if tc.Namespace != "" && namespace != "" && tc.Namespace != namespace {
		return false
	}
	if len(tc.Datasets) > 0 && dataset != "" {
		for _, allowed := range tc.Datasets {
			if allowed == "*" || allowed == dataset || strings.TrimPrefix(dataset, namespace+"/") == allowed {
				return true
			}
		}
		return false
	}
	return true
}

// HasPermission checks whether TenantContext carries a permission string.
func (tc *TenantContext) HasPermission(perm string) bool {
	if tc == nil {
		return false
	}
	if tc.IsAdmin {
		return true
	}
	for _, p := range tc.Permissions {
		if p == perm || p == "*" {
			return true
		}
	}
	return false
}

// AccessValidator validates raw API keys or bearer tokens and resolves TenantContext + authorization.
type AccessValidator interface {
	AuthorizeToken(ctx context.Context, rawToken, permission, namespace, dataset string) (*TenantContext, error)
}

// RLSOperator defines supported comparison operators for row-level security predicates.
type RLSOperator string

const (
	RLSOpEq     RLSOperator = "eq"
	RLSOpNeq    RLSOperator = "neq"
	RLSOpPrefix RLSOperator = "prefix"
	RLSOpIn     RLSOperator = "in"
)

// RLSRule represents a single predicate on record metadata.
// When ExpectedFromClaim is set (e.g. "tenant_id", "user_id"), the expected value is taken from TenantContext.
type RLSRule struct {
	Field             string      `json:"field"`
	Operator          RLSOperator `json:"operator"`
	Value             string      `json:"value,omitempty"`
	ExpectedFromClaim string      `json:"expected_from_claim,omitempty"`
}

// RLSPolicy binds row-level security rules to a dataset or namespace wildcard.
type RLSPolicy struct {
	Name      string    `json:"name"`
	Namespace string    `json:"namespace,omitempty"`
	Dataset   string    `json:"dataset,omitempty"`
	Rules     []RLSRule `json:"rules"`
	Enabled   bool      `json:"enabled"`
}

// RLSEvaluator evaluates row-level security policies across metadata maps.
type RLSEvaluator struct {
	mu       sync.RWMutex
	policies map[string]RLSPolicy // key: namespace + ":" + dataset
}

// NewRLSEvaluator creates a new row-level security evaluator.
func NewRLSEvaluator() *RLSEvaluator {
	return &RLSEvaluator{
		policies: make(map[string]RLSPolicy),
	}
}

func policyKey(namespace, dataset string) string {
	return namespace + ":" + dataset
}

// SetPolicy registers or replaces an RLS policy.
func (e *RLSEvaluator) SetPolicy(policy RLSPolicy) error {
	if policy.Name == "" {
		return fmt.Errorf("%w: policy name is required", ErrRLSValidation)
	}
	for _, r := range policy.Rules {
		if r.Field == "" {
			return fmt.Errorf("%w: rule field is required", ErrRLSValidation)
		}
		switch r.Operator {
		case "", RLSOpEq, RLSOpNeq, RLSOpPrefix, RLSOpIn:
		default:
			return fmt.Errorf("%w: unsupported operator %q", ErrRLSValidation, r.Operator)
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.policies[policyKey(policy.Namespace, policy.Dataset)] = policy
	return nil
}

// DeletePolicy deletes an RLS policy by namespace and dataset.
func (e *RLSEvaluator) DeletePolicy(namespace, dataset string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.policies, policyKey(namespace, dataset))
}

// GetPolicies returns all configured RLS policies.
func (e *RLSEvaluator) GetPolicies() []RLSPolicy {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]RLSPolicy, 0, len(e.policies))
	for _, p := range e.policies {
		out = append(out, p)
	}
	return out
}

// EvaluateMetadata checks whether a record's metadata passes all applicable RLS rules and TenantContext row filters.
func (e *RLSEvaluator) EvaluateMetadata(tc *TenantContext, namespace, dataset string, meta map[string]any) bool {
	if tc == nil {
		return true
	}
	if tc.IsAdmin {
		return true
	}

	// Enforce explicit tenant row filters carried directly on the TenantContext/API key.
	for k, expected := range tc.RowFilters {
		actual, ok := meta[k]
		if !ok || fmt.Sprintf("%v", actual) != expected {
			return false
		}
	}

	// Enforce implicit tenant_id isolation when metadata specifies tenant_id and caller has a TenantID.
	if tc.TenantID != "" {
		if rawTenant, exists := meta["tenant_id"]; exists {
			if fmt.Sprintf("%v", rawTenant) != tc.TenantID {
				return false
			}
		}
	}

	if e == nil {
		return true
	}

	e.mu.RLock()
	candidates := make([]RLSPolicy, 0, 3)
	for _, key := range []string{
		policyKey(namespace, dataset),
		policyKey(namespace, "*"),
		policyKey("*", "*"),
	} {
		if p, ok := e.policies[key]; ok && p.Enabled {
			candidates = append(candidates, p)
		}
	}
	e.mu.RUnlock()

	for _, p := range candidates {
		for _, rule := range p.Rules {
			if !evalRule(tc, rule, meta) {
				return false
			}
		}
	}
	return true
}

func evalRule(tc *TenantContext, rule RLSRule, meta map[string]any) bool {
	if meta == nil {
		return false
	}
	rawVal, exists := meta[rule.Field]
	if !exists {
		return false
	}
	actual := fmt.Sprintf("%v", rawVal)

	expected := rule.Value
	switch rule.ExpectedFromClaim {
	case "tenant_id":
		expected = tc.TenantID
	case "user_id":
		expected = tc.UserID
	case "namespace":
		expected = tc.Namespace
	}

	op := rule.Operator
	if op == "" {
		op = RLSOpEq
	}

	switch op {
	case RLSOpEq:
		return actual == expected
	case RLSOpNeq:
		return actual != expected
	case RLSOpPrefix:
		return strings.HasPrefix(actual, expected)
	case RLSOpIn:
		for _, part := range strings.Split(expected, ",") {
			if strings.TrimSpace(part) == actual {
				return true
			}
		}
		return false
	default:
		return false
	}
}
