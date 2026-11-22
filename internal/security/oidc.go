package security

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
)

// OIDCProviderKind identifies the cloud identity provider backing an issuer.
type OIDCProviderKind string

const (
	// OIDCProviderGeneric is a oidc provider generic constant.
	OIDCProviderGeneric OIDCProviderKind = "generic"
	// OIDCProviderAWS is a oidc provider aws constant.
	OIDCProviderAWS OIDCProviderKind = "aws"
	// OIDCProviderGoogle is a oidc provider google constant.
	OIDCProviderGoogle OIDCProviderKind = "google"
)

// OIDCIssuerConfig describes a trusted OIDC issuer used for federation.
type OIDCIssuerConfig struct {
	Provider        OIDCProviderKind `json:"provider"`
	IssuerURL       string           `json:"issuer_url"`
	JWKSURL         string           `json:"jwks_url,omitempty"`
	Audience        string           `json:"audience"`
	AllowedAlgs     []string         `json:"allowed_algs,omitempty"`
	RoleClaim       string           `json:"role_claim,omitempty"`
	TenantClaim     string           `json:"tenant_claim,omitempty"`
	NamespaceClaim  string           `json:"namespace_claim,omitempty"`
	DatasetsClaim   string           `json:"datasets_claim,omitempty"`
	RowFilterClaim  string           `json:"row_filter_claim,omitempty"`
	AdminClaim      string           `json:"admin_claim,omitempty"`
	AdminValue      string           `json:"admin_value,omitempty"`
	ClockSkew       time.Duration    `json:"clock_skew,omitempty"`
	RefreshInterval time.Duration    `json:"refresh_interval,omitempty"`
	TrustPolicyARN  string           `json:"trust_policy_arn,omitempty"` // AWS role ARN / Google principal
}

// OIDCConfig holds federation settings for the security layer.
type OIDCConfig struct {
	Enabled  bool               `json:"enabled"`
	Issuers  []OIDCIssuerConfig `json:"issuers"`
	JTICache int                `json:"jti_cache_size,omitempty"`
}

// OIDCClaims is the validated claim set extracted from a federated token.
type OIDCClaims struct {
	Subject   string                 `json:"sub"`
	Issuer    string                 `json:"iss"`
	Audience  []string               `json:"aud"`
	Expiry    int64                  `json:"exp"`
	IssuedAt  int64                  `json:"iat"`
	NotBefore int64                  `json:"nbf,omitempty"`
	JTI       string                 `json:"jti,omitempty"`
	Email     string                 `json:"email,omitempty"`
	Name      string                 `json:"name,omitempty"`
	Groups    []string               `json:"groups,omitempty"`
	Raw       map[string]interface{} `json:"-"`
	Provider  OIDCProviderKind       `json:"-"`
	IssuerCfg OIDCIssuerConfig       `json:"-"`
}

// OIDCManager validates federated OIDC tokens and maps them to TenantContext.
type OIDCManager struct {
	mu      sync.RWMutex
	config  OIDCConfig
	cache   map[string]*jwksCache
	jtiMu   sync.Mutex
	jtiSeen map[string]time.Time
	jtiMax  int
	httpc   *http.Client
}

type jwksCache struct {
	keys     map[string]interface{}
	fetched  time.Time
	endpoint string
}

// NewOIDCManager creates a manager from config.
func NewOIDCManager(cfg OIDCConfig) *OIDCManager {
	if cfg.JTICache <= 0 {
		cfg.JTICache = 1024
	}
	m := &OIDCManager{
		config:  cfg,
		cache:   make(map[string]*jwksCache),
		jtiSeen: make(map[string]time.Time),
		jtiMax:  cfg.JTICache,
		httpc:   &http.Client{Timeout: 10 * time.Second},
	}
	return m
}

func (m *OIDCManager) allowedAlgs(cfg OIDCIssuerConfig) []string {
	if len(cfg.AllowedAlgs) > 0 {
		return cfg.AllowedAlgs
	}
	return []string{"RS256", "ES256"}
}

func (m *OIDCManager) skew(cfg OIDCIssuerConfig) time.Duration {
	if cfg.ClockSkew > 0 {
		return cfg.ClockSkew
	}
	return 60 * time.Second
}

func (m *OIDCManager) refreshInterval(cfg OIDCIssuerConfig) time.Duration {
	if cfg.RefreshInterval > 0 {
		return cfg.RefreshInterval
	}
	return 5 * time.Minute
}

// findIssuer returns the config matching the token issuer.
func (m *OIDCManager) findIssuer(iss string) (OIDCIssuerConfig, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, c := range m.config.Issuers {
		if c.IssuerURL == iss {
			return c, true
		}
	}
	return OIDCIssuerConfig{}, false
}

// jwksEndpoint resolves the JWKS URL for an issuer.
func (m *OIDCManager) jwksEndpoint(ctx context.Context, cfg OIDCIssuerConfig) (string, error) {
	if cfg.JWKSURL != "" {
		return cfg.JWKSURL, nil
	}
	// OpenID Connect discovery.
	discURL := strings.TrimRight(cfg.IssuerURL, "/") + "/.well-known/openid-configuration"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, discURL, nil) // #nosec G704 - issuer/JWKS URL is admin-configured trusted input
	if err != nil {
		return "", err
	}
	resp, err := m.httpc.Do(req) // #nosec G704 - issuer/JWKS URL is admin-configured trusted input
	if err != nil {
		return "", fmt.Errorf("oidc discovery failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("oidc discovery status %d", resp.StatusCode)
	}
	var doc struct {
		JWKSURI string `json:"jwks_uri"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return "", fmt.Errorf("oidc discovery decode: %w", err)
	}
	if doc.JWKSURI == "" {
		return "", errors.New("oidc discovery missing jwks_uri")
	}
	return doc.JWKSURI, nil
}

func (m *OIDCManager) getJWKS(ctx context.Context, cfg OIDCIssuerConfig) (map[string]interface{}, error) {
	endpoint, err := m.jwksEndpoint(ctx, cfg)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	c, ok := m.cache[endpoint]
	if ok && time.Since(c.fetched) < m.refreshInterval(cfg) {
		keys := c.keys
		m.mu.Unlock()
		return keys, nil
	}
	m.mu.Unlock()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil) // #nosec G704 - admin-configured JWKS endpoint
	if err != nil {
		return nil, err
	}
	resp, err := m.httpc.Do(req) // #nosec G704 - admin-configured JWKS endpoint
	if err != nil {
		metrics.OIDCJWKSFetchFailures.WithLabelValues(cfg.IssuerURL).Inc()
		return nil, fmt.Errorf("jwks fetch failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		metrics.OIDCJWKSFetchFailures.WithLabelValues(cfg.IssuerURL).Inc()
		return nil, fmt.Errorf("jwks status %d", resp.StatusCode)
	}
	var jwks jose.JSONWebKeySet
	if err := json.NewDecoder(resp.Body).Decode(&jwks); err != nil {
		return nil, fmt.Errorf("jwks decode: %w", err)
	}
	keys := make(map[string]interface{}, len(jwks.Keys))
	for _, k := range jwks.Keys {
		kid := k.KeyID
		if kid == "" {
			continue
		}
		pub := k.Public()
		if pub.Key == nil {
			continue
		}
		keys[kid] = pub.Key
	}
	if len(keys) == 0 {
		return nil, errors.New("jwks contained no usable keys")
	}

	m.mu.Lock()
	m.cache[endpoint] = &jwksCache{keys: keys, fetched: time.Now(), endpoint: endpoint}
	m.mu.Unlock()
	return keys, nil
}

const maxOIDCTokenBytes = 16 * 1024

// ValidateToken validates a federated OIDC JWT and returns claims.
func (m *OIDCManager) ValidateToken(ctx context.Context, rawToken string) (claims *OIDCClaims, err error) {
	if !m.Enabled() {
		return nil, errors.New("oidc federation disabled")
	}
	if len(rawToken) == 0 || len(rawToken) > maxOIDCTokenBytes {
		return nil, errors.New("invalid token length")
	}
	start := time.Now()
	// peek issuer for metrics labels
	parser := jwt.NewParser()
	unverified, _, perr := parser.ParseUnverified(rawToken, jwt.MapClaims{})
	issuerLabel := "unknown"
	providerLabel := "unknown"
	if perr == nil {
		if mc, ok := unverified.Claims.(jwt.MapClaims); ok {
			if iss, _ := mc["iss"].(string); iss != "" {
				issuerLabel = iss
				if cfg, ok := m.findIssuer(iss); ok {
					providerLabel = string(cfg.Provider)
				}
			}
		}
	}
	metrics.OIDCAuthAttempts.WithLabelValues(providerLabel, issuerLabel).Inc()
	defer func() {
		metrics.OIDCTokenValidationLatency.WithLabelValues(providerLabel, issuerLabel).Observe(time.Since(start).Seconds())
		if err != nil {
			metrics.OIDCAuthFailures.WithLabelValues(providerLabel, issuerLabel, err.Error()).Inc()
		} else {
			metrics.OIDCAuthSuccesses.WithLabelValues(providerLabel, issuerLabel).Inc()
		}
	}()
	if perr != nil {
		return nil, fmt.Errorf("token parse: %w", perr)
	}
	mc, ok := unverified.Claims.(jwt.MapClaims)
	if !ok {
		return nil, errors.New("invalid claims")
	}
	iss, _ := mc["iss"].(string)
	if iss == "" {
		return nil, errors.New("missing iss")
	}
	cfg, ok := m.findIssuer(iss)
	if !ok {
		return nil, ErrInvalidProvider
	}
	audience := cfg.Audience
	algs := m.allowedAlgs(cfg)
	skew := m.skew(cfg)

	keys, err := m.getJWKS(ctx, cfg)
	if err != nil {
		return nil, err
	}
	keyFunc := func(token *jwt.Token) (interface{}, error) {
		// alg allowlist enforced via WithValidMethods, but double-check.
		alg, _ := token.Header["alg"].(string)
		allowed := false
		for _, a := range algs {
			if a == alg {
				allowed = true
				break
			}
		}
		if !allowed {
			return nil, fmt.Errorf("disallowed alg %s", alg)
		}
		kid, _ := token.Header["kid"].(string)
		if kid == "" {
			return nil, errors.New("missing kid")
		}
		key, ok := keys[kid]
		if !ok {
			// refresh once
			fresh, err := m.getJWKS(ctx, cfg)
			if err != nil {
				return nil, err
			}
			key, ok = fresh[kid]
			if !ok {
				return nil, errors.New("unknown kid")
			}
		}
		return key, nil
	}

	token, err := jwt.Parse(rawToken, keyFunc,
		jwt.WithValidMethods(algs),
		jwt.WithIssuer(cfg.IssuerURL),
		jwt.WithAudience(audience),
		jwt.WithLeeway(skew),
	)
	if err != nil {
		return nil, fmt.Errorf("token validation: %w", err)
	}
	if !token.Valid {
		return nil, ErrInvalidToken
	}
	mapClaims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, errors.New("invalid claims type")
	}

	oidc := &OIDCClaims{
		Issuer:    iss,
		Raw:       map[string]interface{}(mapClaims),
		Provider:  cfg.Provider,
		IssuerCfg: cfg,
	}
	if sub, _ := mapClaims["sub"].(string); sub != "" {
		oidc.Subject = sub
	} else {
		return nil, errors.New("missing sub")
	}
	if aud, ok := mapClaims["aud"]; ok {
		switch v := aud.(type) {
		case string:
			oidc.Audience = []string{v}
		case []interface{}:
			for _, a := range v {
				if s, ok := a.(string); ok {
					oidc.Audience = append(oidc.Audience, s)
				}
			}
		}
	}
	if exp, ok := mapClaims["exp"].(float64); ok {
		oidc.Expiry = int64(exp)
	}
	if iat, ok := mapClaims["iat"].(float64); ok {
		oidc.IssuedAt = int64(iat)
	}
	if nbf, ok := mapClaims["nbf"].(float64); ok {
		oidc.NotBefore = int64(nbf)
	}
	if jti, _ := mapClaims["jti"].(string); jti != "" {
		oidc.JTI = jti
	}
	if email, _ := mapClaims["email"].(string); email != "" {
		oidc.Email = email
	}
	if name, _ := mapClaims["name"].(string); name != "" {
		oidc.Name = name
	}
	// groups
	if g, ok := mapClaims["groups"]; ok {
		switch v := g.(type) {
		case []interface{}:
			for _, x := range v {
				if s, ok := x.(string); ok {
					oidc.Groups = append(oidc.Groups, s)
				}
			}
		case string:
			oidc.Groups = append(oidc.Groups, v)
		}
	}

	// Replay protection on jti.
	if oidc.JTI != "" {
		if m.seenJTI(oidc.JTI) {
			return nil, errors.New("replayed token")
		}
	}

	return oidc, nil
}

func (m *OIDCManager) seenJTI(jti string) bool {
	m.jtiMu.Lock()
	defer m.jtiMu.Unlock()
	if _, ok := m.jtiSeen[jti]; ok {
		return true
	}
	// evict expired/oldest if full
	if len(m.jtiSeen) >= m.jtiMax {
		// simple eviction: remove one arbitrary
		for k := range m.jtiSeen {
			delete(m.jtiSeen, k)
			break
		}
	}
	m.jtiSeen[jti] = time.Now().Add(10 * time.Minute)
	return false
}

// ToTenantContext maps validated claims into a TenantContext using issuer config.
func (c *OIDCClaims) ToTenantContext() *TenantContext {
	cfg := c.IssuerCfg
	tc := &TenantContext{
		UserID: c.Subject,
		KeyID:  "oidc:" + string(cfg.Provider) + ":" + c.Subject,
	}
	// tenant
	if cfg.TenantClaim != "" {
		if v, ok := c.Raw[cfg.TenantClaim].(string); ok {
			tc.TenantID = v
		}
	} else if v, ok := c.Raw["tenant_id"].(string); ok {
		tc.TenantID = v
	}
	if cfg.NamespaceClaim != "" {
		if v, ok := c.Raw[cfg.NamespaceClaim].(string); ok {
			tc.Namespace = v
		}
	} else if v, ok := c.Raw["namespace"].(string); ok {
		tc.Namespace = v
	}
	if cfg.DatasetsClaim != "" {
		switch v := c.Raw[cfg.DatasetsClaim].(type) {
		case []interface{}:
			for _, x := range v {
				if s, ok := x.(string); ok {
					tc.Datasets = append(tc.Datasets, s)
				}
			}
		case string:
			tc.Datasets = append(tc.Datasets, v)
		}
	} else if v, ok := c.Raw["datasets"].([]interface{}); ok {
		for _, x := range v {
			if s, ok := x.(string); ok {
				tc.Datasets = append(tc.Datasets, s)
			}
		}
	}
	// row filters
	rfClaim := cfg.RowFilterClaim
	if rfClaim == "" {
		rfClaim = "row_filters"
	}
	if v, ok := c.Raw[rfClaim]; ok {
		switch val := v.(type) {
		case map[string]interface{}:
			tc.RowFilters = make(map[string]string, len(val))
			for k, x := range val {
				if s, ok := x.(string); ok {
					tc.RowFilters[k] = s
				}
			}
		}
	}

	// roles
	roleClaim := cfg.RoleClaim
	if roleClaim == "" {
		roleClaim = "groups"
	}
	switch v := c.Raw[roleClaim].(type) {
	case []interface{}:
		for _, x := range v {
			if s, ok := x.(string); ok {
				tc.Roles = append(tc.Roles, s)
			}
		}
	case string:
		tc.Roles = append(tc.Roles, v)
	}
	// admin
	adminClaim := cfg.AdminClaim
	if adminClaim == "" {
		adminClaim = "role"
	}
	if cfg.AdminValue != "" {
		if v, ok := c.Raw[adminClaim].(string); ok && v == cfg.AdminValue {
			tc.IsAdmin = true
		}
	}
	if len(tc.Roles) == 0 {
		tc.Roles = append(tc.Roles, "reader")
	}
	// default permissions based on roles (aligned with store.rolePermissions)
	for _, r := range tc.Roles {
		switch r {
		case "admin":
			tc.Permissions = append(tc.Permissions, "read", "write", "delete", "ingest", "admin", "backup", "manage-keys", "manage-tenant")
			tc.IsAdmin = true
		case "tenant-admin":
			tc.Permissions = append(tc.Permissions, "read", "write", "delete", "ingest", "backup", "manage-keys", "manage-tenant")
		case "writer", "read-write":
			tc.Permissions = append(tc.Permissions, "read", "write", "delete", "ingest")
		case "ingest-only":
			tc.Permissions = append(tc.Permissions, "read", "ingest")
		case "reader", "read-only":
			tc.Permissions = append(tc.Permissions, "read")
		}
	}
	// de-dupe permissions
	seen := map[string]bool{}
	out := []string{}
	for _, p := range tc.Permissions {
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	tc.Permissions = out
	if len(tc.Permissions) == 0 {
		tc.Permissions = []string{"read"}
	}
	return tc
}

// Enabled returns whether OIDC federation is active.
func (m *OIDCManager) Enabled() bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.Enabled
}

// ListIssuers returns a copy of all configured OIDC issuers.
func (m *OIDCManager) ListIssuers() []OIDCIssuerConfig {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]OIDCIssuerConfig, len(m.config.Issuers))
	copy(out, m.config.Issuers)
	return out
}

// AddIssuer registers or updates a trusted OIDC issuer configuration.
func (m *OIDCManager) AddIssuer(cfg OIDCIssuerConfig) error {
	if cfg.IssuerURL == "" {
		return errors.New("issuer_url is required")
	}
	if !isAllowedOIDCEndpoint(cfg.IssuerURL) {
		return errors.New("issuer_url must use https")
	}
	if cfg.JWKSURL != "" && !isAllowedOIDCEndpoint(cfg.JWKSURL) {
		return errors.New("jwks_url must use https")
	}
	if cfg.Audience == "" {
		return errors.New("audience is required")
	}
	if cfg.Provider == "" {
		cfg.Provider = OIDCProviderGeneric
	}
	for _, alg := range cfg.AllowedAlgs {
		upper := strings.ToUpper(alg)
		if upper == "NONE" || strings.HasPrefix(upper, "HS") {
			return fmt.Errorf("disallowed signing algorithm %q: only asymmetric algorithms allowed", alg)
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.config.Enabled = true
	for i, existing := range m.config.Issuers {
		if existing.IssuerURL == cfg.IssuerURL {
			m.config.Issuers[i] = cfg
			delete(m.cache, cfg.JWKSURL)
			return nil
		}
	}
	m.config.Issuers = append(m.config.Issuers, cfg)
	return nil
}

func isAllowedOIDCEndpoint(raw string) bool {
	return strings.HasPrefix(raw, "https://") ||
		strings.HasPrefix(raw, "http://127.0.0.1") ||
		strings.HasPrefix(raw, "http://localhost")
}

// RemoveIssuer removes a configured issuer by URL.
func (m *OIDCManager) RemoveIssuer(issuerURL string) bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, existing := range m.config.Issuers {
		if existing.IssuerURL == issuerURL {
			m.config.Issuers = append(m.config.Issuers[:i], m.config.Issuers[i+1:]...)
			return true
		}
	}
	return false
}

// AWSOIDCIssuerConfig returns a sensible default config for AWS IAM / IRSA
// style OIDC federation. issuerURL is the cluster/provider issuer (e.g.
// https://oidc.eks.<region>.amazonaws.com/id/<id>).
func AWSOIDCIssuerConfig(issuerURL, audience string) OIDCIssuerConfig {
	if audience == "" {
		audience = "sts.amazonaws.com"
	}
	return OIDCIssuerConfig{
		Provider:       OIDCProviderAWS,
		IssuerURL:      issuerURL,
		Audience:       audience,
		AllowedAlgs:    []string{"RS256"},
		TenantClaim:    "tenant_id",
		NamespaceClaim: "namespace",
		DatasetsClaim:  "datasets",
		RoleClaim:      "groups",
		RowFilterClaim: "row_filters",
		AdminClaim:     "role",
		AdminValue:     "admin",
		ClockSkew:      time.Minute,
	}
}

// GoogleOIDCIssuerConfig returns a sensible default config for Google IAM /
// Workload Identity Federation.
func GoogleOIDCIssuerConfig(clientID string) OIDCIssuerConfig {
	return OIDCIssuerConfig{
		Provider:       OIDCProviderGoogle,
		IssuerURL:      "https://accounts.google.com",
		Audience:       clientID,
		AllowedAlgs:    []string{"RS256"},
		TenantClaim:    "tenant_id",
		NamespaceClaim: "namespace",
		DatasetsClaim:  "datasets",
		RoleClaim:      "groups",
		RowFilterClaim: "row_filters",
		AdminClaim:     "role",
		AdminValue:     "admin",
		ClockSkew:      time.Minute,
	}
}
