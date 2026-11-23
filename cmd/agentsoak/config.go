package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// defaultConfigPath is resolved from the repository root, where the soak
// tooling is normally invoked. A missing file falls back to built-in defaults.
const defaultConfigPath = "scripts/configs/multiagent_soak.json"

// soakGeoOptions configures proximity searches.
type soakGeoOptions struct {
	Lat      float64 `json:"lat"`
	Lon      float64 `json:"lon"`
	RadiusKM float64 `json:"radius_km"`
}

// soakGraphOptions configures graph traversal and analytics operations.
type soakGraphOptions struct {
	MaxHops   int    `json:"max_hops"`
	StartNode uint32 `json:"start_node"`
}

// soakWorkloadOptions supplies parameters shared by workload operations.
type soakWorkloadOptions struct {
	K                  int              `json:"k"`
	Alpha              float64          `json:"alpha"`
	RRFK               int              `json:"rrf_k"`
	FederatedTags      []string         `json:"federated_tags"`
	TemporalWindowSize int              `json:"temporal_window_size"`
	Geo                soakGeoOptions   `json:"geo"`
	Graph              soakGraphOptions `json:"graph"`
}

// soakTLSOptions mirrors security.TLSConfig fields relevant to clients.
type soakTLSOptions struct {
	Enabled    bool   `json:"enabled"`
	CertFile   string `json:"cert_file"`
	KeyFile    string `json:"key_file"`
	CAFile     string `json:"ca_file"`
	ServerName string `json:"server_name"`
	MinVersion string `json:"min_version"`
}

// soakOIDCOptions selects an OIDC workload-identity source. "none" keeps the
// scoped API keys issued by provisioning; "aws" and "google" exchange a
// federated JWT for a short-lived Longbow session key before the soak starts.
type soakOIDCOptions struct {
	Mode         string `json:"mode"`
	Audience     string `json:"audience"`
	TTLSeconds   int    `json:"ttl_seconds"`
	TokenFileEnv string `json:"token_file_env"`
}

// soakSecurityOptions describes the security controls under test. RBAC is
// always enforced regardless of variant.
type soakSecurityOptions struct {
	Variant  string          `json:"variant"`
	TLS      soakTLSOptions  `json:"tls"`
	OIDC     soakOIDCOptions `json:"oidc"`
	TenantID string          `json:"tenant_id"`
}

// soakConfig mirrors scripts/configs/multiagent_soak.json, the config shared by the Go
// and Python simulators.
type soakConfig struct {
	Agents int `json:"agents"`
	// AgentsPerNamespace groups agents into shared namespaces. Zero or one
	// keeps the default private/shared/common layout with one private
	// namespace per agent.
	AgentsPerNamespace int                 `json:"agents_per_namespace"`
	URI                string              `json:"uri"`
	MetaURI            string              `json:"meta_uri"`
	DurationHours      float64             `json:"duration_hours"`
	Dim                int                 `json:"dim"`
	Seed               int64               `json:"seed"`
	ThinkTimeMS        int                 `json:"think_time_ms"`
	SampleIntervalS    int                 `json:"sample_interval_s"`
	Workload           map[string]float64  `json:"workload"`
	WorkloadOptions    soakWorkloadOptions `json:"workload_options"`
	Security           soakSecurityOptions `json:"security"`
	Namespaces         struct {
		PrivatePrefix string `json:"private_prefix"`
		SharedPrefix  string `json:"shared_prefix"`
		Common        string `json:"common"`
	} `json:"namespaces"`
	RBAC struct {
		AdminAPIKeyEnv string `json:"admin_api_key_env"`
	} `json:"rbac"`
}

func defaultWorkload() map[string]float64 {
	return map[string]float64{
		"dense": 25, "hybrid": 10, "sparse": 5, "filtered": 5, "range": 3,
		"byid": 3, "temporal_asof": 3, "temporal_range": 3, "temporal_window": 2,
		"geo_radius": 2, "geo_box": 2, "geo_hybrid": 1,
		"graphrag": 2, "traverse": 2, "recommend": 1,
		"multimodal": 2, "federated": 2, "text": 3,
		"insert": 12, "upsert": 5, "delete": 3,
		"delete_ns": 0.5, "pagerank": 0.25, "communities": 0.25,
		"get_vector": 0.5, "graph_stats": 0.5,
	}
}

func defaultConfig() *soakConfig {
	c := &soakConfig{
		Agents:          3,
		URI:             "grpc://127.0.0.1:3000",
		MetaURI:         "grpc://127.0.0.1:3001",
		DurationHours:   1,
		Dim:             128,
		Seed:            42,
		ThinkTimeMS:     10,
		SampleIntervalS: 60,
		Workload:        defaultWorkload(),
		WorkloadOptions: soakWorkloadOptions{
			K: 5, Alpha: 0.5, RRFK: 60,
			FederatedTags:      []string{"soak"},
			TemporalWindowSize: 50,
			Geo:                soakGeoOptions{Lat: 37.7, Lon: -122.4, RadiusKM: 5},
			Graph:              soakGraphOptions{MaxHops: 2, StartNode: 1},
		},
		Security: soakSecurityOptions{
			Variant: "plaintext",
			OIDC: soakOIDCOptions{ // #nosec G101 - default audience and env name are not credentials
				Mode:         "none",
				Audience:     "longbow",
				TTLSeconds:   900,
				TokenFileEnv: "AWS_WEB_IDENTITY_TOKEN_FILE",
			},
		},
	}
	c.Namespaces.PrivatePrefix = "private_"
	c.Namespaces.SharedPrefix = "shared_"
	c.Namespaces.Common = "master"
	c.RBAC.AdminAPIKeyEnv = "LONGBOW_RBAC_BOOTSTRAP_KEY"
	return c
}

// loadConfig reads the shared JSON config and applies it over the built-in
// defaults. An empty path returns the defaults without touching the filesystem.
func loadConfig(path string) (*soakConfig, error) {
	cfg := defaultConfig()
	if path == "" {
		return cfg, nil
	}
	// The path comes from the operator-supplied -config flag (or the in-repo
	// default), never from untrusted input.
	data, err := os.ReadFile(path) // #nosec G304 -- operator-supplied config path
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var raw struct {
		Agents             *int                `json:"agents"`
		AgentsPerNamespace *int                `json:"agents_per_namespace"`
		URI                *string             `json:"uri"`
		MetaURI            *string             `json:"meta_uri"`
		DurationHours      *float64            `json:"duration_hours"`
		Dim                *int                `json:"dim"`
		Seed               *int64              `json:"seed"`
		ThinkTimeMS        *int                `json:"think_time_ms"`
		SampleIntervalS    *int                `json:"sample_interval_s"`
		Workload           *map[string]float64 `json:"workload"`
		WorkloadOptions    *struct {
			K                  *int      `json:"k"`
			Alpha              *float64  `json:"alpha"`
			RRFK               *int      `json:"rrf_k"`
			FederatedTags      *[]string `json:"federated_tags"`
			TemporalWindowSize *int      `json:"temporal_window_size"`
			Geo                *struct {
				Lat      *float64 `json:"lat"`
				Lon      *float64 `json:"lon"`
				RadiusKM *float64 `json:"radius_km"`
			} `json:"geo"`
			Graph *struct {
				MaxHops   *int    `json:"max_hops"`
				StartNode *uint32 `json:"start_node"`
			} `json:"graph"`
		} `json:"workload_options"`
		Security *struct {
			Variant *string `json:"variant"`
			TLS     *struct {
				Enabled    *bool   `json:"enabled"`
				CertFile   *string `json:"cert_file"`
				KeyFile    *string `json:"key_file"`
				CAFile     *string `json:"ca_file"`
				ServerName *string `json:"server_name"`
				MinVersion *string `json:"min_version"`
			} `json:"tls"`
			OIDC *struct {
				Mode         *string `json:"mode"`
				Audience     *string `json:"audience"`
				TTLSeconds   *int    `json:"ttl_seconds"`
				TokenFileEnv *string `json:"token_file_env"`
			} `json:"oidc"`
			TenantID *string `json:"tenant_id"`
		} `json:"security"`
		Namespaces *struct {
			PrivatePrefix *string `json:"private_prefix"`
			SharedPrefix  *string `json:"shared_prefix"`
			Common        *string `json:"common"`
		} `json:"namespaces"`
		RBAC *struct {
			AdminAPIKeyEnv *string `json:"admin_api_key_env"`
		} `json:"rbac"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if raw.Agents != nil {
		cfg.Agents = *raw.Agents
	}
	if raw.AgentsPerNamespace != nil {
		cfg.AgentsPerNamespace = *raw.AgentsPerNamespace
	}
	if raw.URI != nil {
		cfg.URI = *raw.URI
	}
	if raw.MetaURI != nil {
		cfg.MetaURI = *raw.MetaURI
	}
	if raw.DurationHours != nil {
		cfg.DurationHours = *raw.DurationHours
	}
	if raw.Dim != nil {
		cfg.Dim = *raw.Dim
	}
	if raw.Seed != nil {
		cfg.Seed = *raw.Seed
	}
	if raw.ThinkTimeMS != nil {
		cfg.ThinkTimeMS = *raw.ThinkTimeMS
	}
	if raw.SampleIntervalS != nil {
		cfg.SampleIntervalS = *raw.SampleIntervalS
	}
	if raw.Workload != nil {
		cfg.Workload = *raw.Workload
	}
	if raw.WorkloadOptions != nil {
		o := raw.WorkloadOptions
		if o.K != nil {
			cfg.WorkloadOptions.K = *o.K
		}
		if o.Alpha != nil {
			cfg.WorkloadOptions.Alpha = *o.Alpha
		}
		if o.RRFK != nil {
			cfg.WorkloadOptions.RRFK = *o.RRFK
		}
		if o.FederatedTags != nil {
			cfg.WorkloadOptions.FederatedTags = *o.FederatedTags
		}
		if o.TemporalWindowSize != nil {
			cfg.WorkloadOptions.TemporalWindowSize = *o.TemporalWindowSize
		}
		if o.Geo != nil {
			if o.Geo.Lat != nil {
				cfg.WorkloadOptions.Geo.Lat = *o.Geo.Lat
			}
			if o.Geo.Lon != nil {
				cfg.WorkloadOptions.Geo.Lon = *o.Geo.Lon
			}
			if o.Geo.RadiusKM != nil {
				cfg.WorkloadOptions.Geo.RadiusKM = *o.Geo.RadiusKM
			}
		}
		if o.Graph != nil {
			if o.Graph.MaxHops != nil {
				cfg.WorkloadOptions.Graph.MaxHops = *o.Graph.MaxHops
			}
			if o.Graph.StartNode != nil {
				cfg.WorkloadOptions.Graph.StartNode = *o.Graph.StartNode
			}
		}
	}
	if raw.Security != nil {
		s := raw.Security
		if s.Variant != nil {
			cfg.Security.Variant = *s.Variant
		}
		if s.TenantID != nil {
			cfg.Security.TenantID = *s.TenantID
		}
		if s.TLS != nil {
			if s.TLS.Enabled != nil {
				cfg.Security.TLS.Enabled = *s.TLS.Enabled
			}
			if s.TLS.CertFile != nil {
				cfg.Security.TLS.CertFile = *s.TLS.CertFile
			}
			if s.TLS.KeyFile != nil {
				cfg.Security.TLS.KeyFile = *s.TLS.KeyFile
			}
			if s.TLS.CAFile != nil {
				cfg.Security.TLS.CAFile = *s.TLS.CAFile
			}
			if s.TLS.ServerName != nil {
				cfg.Security.TLS.ServerName = *s.TLS.ServerName
			}
			if s.TLS.MinVersion != nil {
				cfg.Security.TLS.MinVersion = *s.TLS.MinVersion
			}
		}
		if s.OIDC != nil {
			if s.OIDC.Mode != nil {
				cfg.Security.OIDC.Mode = *s.OIDC.Mode
			}
			if s.OIDC.Audience != nil {
				cfg.Security.OIDC.Audience = *s.OIDC.Audience
			}
			if s.OIDC.TTLSeconds != nil {
				cfg.Security.OIDC.TTLSeconds = *s.OIDC.TTLSeconds
			}
			if s.OIDC.TokenFileEnv != nil {
				cfg.Security.OIDC.TokenFileEnv = *s.OIDC.TokenFileEnv
			}
		}
	}
	if raw.Namespaces != nil {
		if raw.Namespaces.PrivatePrefix != nil {
			cfg.Namespaces.PrivatePrefix = *raw.Namespaces.PrivatePrefix
		}
		if raw.Namespaces.SharedPrefix != nil {
			cfg.Namespaces.SharedPrefix = *raw.Namespaces.SharedPrefix
		}
		if raw.Namespaces.Common != nil {
			cfg.Namespaces.Common = *raw.Namespaces.Common
		}
	}
	if raw.RBAC != nil {
		if raw.RBAC.AdminAPIKeyEnv != nil {
			cfg.RBAC.AdminAPIKeyEnv = *raw.RBAC.AdminAPIKeyEnv
		}
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config %s: %w", path, err)
	}
	return cfg, nil
}

func (c *soakConfig) validate() error {
	if c.Agents < 2 {
		return fmt.Errorf("agents must be at least 2 (got %d)", c.Agents)
	}
	if c.AgentsPerNamespace < 0 {
		return fmt.Errorf("agents_per_namespace must not be negative")
	}
	if c.AgentsPerNamespace > 1 && c.AgentsPerNamespace >= c.Agents {
		return fmt.Errorf("agents_per_namespace must be smaller than agents so RBAC has at least two groups")
	}
	if c.Dim < 1 {
		return fmt.Errorf("dim must be positive (got %d)", c.Dim)
	}
	if c.DurationHours <= 0 {
		return fmt.Errorf("duration_hours must be positive (got %f)", c.DurationHours)
	}
	if c.SampleIntervalS < 1 {
		return fmt.Errorf("sample_interval_s must be positive (got %d)", c.SampleIntervalS)
	}
	if strings.TrimSpace(c.URI) == "" {
		return fmt.Errorf("uri must not be empty")
	}
	if len(c.Workload) == 0 {
		return fmt.Errorf("workload must contain at least one operation")
	}
	total := 0.0
	for op, weight := range c.Workload {
		if !knownWorkloadOp(op) {
			return fmt.Errorf("unknown workload operation %q", op)
		}
		if weight < 0 {
			return fmt.Errorf("workload weight for %q must be non-negative", op)
		}
		total += weight
	}
	if total <= 0 {
		return fmt.Errorf("workload weights must sum to a positive value")
	}
	if c.WorkloadOptions.K < 1 {
		return fmt.Errorf("workload_options.k must be positive")
	}
	if c.WorkloadOptions.Alpha < 0 || c.WorkloadOptions.Alpha > 1 {
		return fmt.Errorf("workload_options.alpha must be between 0 and 1")
	}
	switch c.Security.Variant {
	case "plaintext", "tls", "mtls", "oidc", "encrypted", "combined":
	default:
		return fmt.Errorf("unknown security variant %q", c.Security.Variant)
	}
	switch c.Security.OIDC.Mode {
	case "none", "aws", "google":
	default:
		return fmt.Errorf("unknown oidc mode %q", c.Security.OIDC.Mode)
	}
	if c.Security.OIDC.TTLSeconds < 1 {
		return fmt.Errorf("security.oidc.ttl_seconds must be positive")
	}
	seen := map[string]bool{}
	for _, ns := range newTopologyUnchecked(c).Namespaces() {
		if strings.TrimSpace(ns) == "" {
			return fmt.Errorf("namespace names must not be empty")
		}
		if seen[ns] {
			return fmt.Errorf("namespace names must be unique (duplicate %q)", ns)
		}
		seen[ns] = true
	}
	return nil
}

// topology derives the namespace layout and per-agent permission sets. The
// default layout has a private namespace per agent (2N+1 namespaces); the
// grouped layout gives each group of AgentsPerNamespace agents one shared
// namespace that they can all write, while other groups remain read-only.
type topology struct {
	agents     int
	perGroup   int
	private    []string
	shared     []string
	common     string
	groupCount int
}

func newTopology(cfg *soakConfig) (*topology, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return newTopologyUnchecked(cfg), nil
}

func newTopologyUnchecked(cfg *soakConfig) *topology {
	t := &topology{
		agents:   cfg.Agents,
		perGroup: cfg.AgentsPerNamespace,
		common:   cfg.Namespaces.Common,
	}
	if t.perGroup <= 1 {
		t.perGroup = 1
		t.groupCount = cfg.Agents
		t.private = make([]string, cfg.Agents)
		t.shared = make([]string, cfg.Agents)
		for i := 0; i < cfg.Agents; i++ {
			t.private[i] = fmt.Sprintf("%s%d", cfg.Namespaces.PrivatePrefix, i)
			t.shared[i] = fmt.Sprintf("%s%d", cfg.Namespaces.SharedPrefix, i)
		}
		return t
	}
	t.groupCount = (cfg.Agents + t.perGroup - 1) / t.perGroup
	t.shared = make([]string, t.groupCount)
	for group := 0; group < t.groupCount; group++ {
		t.shared[group] = fmt.Sprintf("%s%d", cfg.Namespaces.SharedPrefix, group)
	}
	return t
}

// Namespaces returns private namespaces (default layout only), then shared
// namespaces, then the common namespace.
func (t *topology) Namespaces() []string {
	all := make([]string, 0, len(t.private)+len(t.shared)+1)
	all = append(all, t.private...)
	all = append(all, t.shared...)
	return append(all, t.common)
}

func (t *topology) valid(i int) bool { return i >= 0 && i < t.agents }

func (t *topology) group(i int) int {
	if t.perGroup <= 1 {
		return i
	}
	return i / t.perGroup
}

// Writable returns namespaces agent i may read and write.
func (t *topology) Writable(i int) []string {
	if !t.valid(i) {
		return nil
	}
	if t.perGroup <= 1 {
		return []string{t.private[i], t.shared[i], t.common}
	}
	return []string{t.shared[t.group(i)], t.common}
}

// ReadOnly returns namespaces agent i may read but not write.
func (t *topology) ReadOnly(i int) []string {
	if !t.valid(i) {
		return nil
	}
	out := make([]string, 0, len(t.shared))
	for group, ns := range t.shared {
		if t.perGroup > 1 && group == t.group(i) {
			continue
		}
		if t.perGroup <= 1 && group == i {
			continue
		}
		out = append(out, ns)
	}
	return out
}

// Readable returns every namespace agent i may read.
func (t *topology) Readable(i int) []string {
	if !t.valid(i) {
		return nil
	}
	out := t.Writable(i)
	return append(out, t.ReadOnly(i)...)
}

// forbidden returns one namespace agent i may neither read nor write, followed
// by one it may read but not write. RBAC verification uses both probes.
func (t *topology) forbidden(i int) (private, shared string) {
	other := (i + 1) % t.agents
	if t.perGroup <= 1 {
		return t.private[other], t.shared[other]
	}
	otherGroup := (t.group(i) + 1) % t.groupCount
	return t.shared[otherGroup], t.shared[otherGroup]
}

func (t *topology) contains(list []string, ns string) bool {
	for _, item := range list {
		if item == ns {
			return true
		}
	}
	return false
}
