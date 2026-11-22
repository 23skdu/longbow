package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// defaultConfigPath is resolved from the repository root, where the soak
// tooling is normally invoked. A missing file falls back to built-in defaults.
const defaultConfigPath = "configs/multiagent_soak.json"

// soakConfig mirrors configs/multiagent_soak.json, the config shared by the Go
// and Python simulators.
type soakConfig struct {
	Agents        int     `json:"agents"`
	URI           string  `json:"uri"`
	MetaURI       string  `json:"meta_uri"`
	DurationHours float64 `json:"duration_hours"`
	Dim           int     `json:"dim"`
	Seed          int64   `json:"seed"`
	ThinkTimeMS   int     `json:"think_time_ms"`
	Namespaces    struct {
		PrivatePrefix string `json:"private_prefix"`
		SharedPrefix  string `json:"shared_prefix"`
		Common        string `json:"common"`
	} `json:"namespaces"`
	RBAC struct {
		AdminAPIKeyEnv string `json:"admin_api_key_env"`
		Require        bool   `json:"require"`
	} `json:"rbac"`
}

func defaultConfig() *soakConfig {
	c := &soakConfig{
		Agents:        3,
		URI:           "grpc://127.0.0.1:3000",
		MetaURI:       "grpc://127.0.0.1:3001",
		DurationHours: 1,
		Dim:           128,
		Seed:          42,
		ThinkTimeMS:   10,
	}
	c.Namespaces.PrivatePrefix = "private_"
	c.Namespaces.SharedPrefix = "shared_"
	c.Namespaces.Common = "master"
	c.RBAC.AdminAPIKeyEnv = "LONGBOW_RBAC_BOOTSTRAP_KEY"
	c.RBAC.Require = true
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
		Agents        *int     `json:"agents"`
		URI           *string  `json:"uri"`
		MetaURI       *string  `json:"meta_uri"`
		DurationHours *float64 `json:"duration_hours"`
		Dim           *int     `json:"dim"`
		Seed          *int64   `json:"seed"`
		ThinkTimeMS   *int     `json:"think_time_ms"`
		Namespaces    *struct {
			PrivatePrefix *string `json:"private_prefix"`
			SharedPrefix  *string `json:"shared_prefix"`
			Common        *string `json:"common"`
		} `json:"namespaces"`
		RBAC *struct {
			AdminAPIKeyEnv *string `json:"admin_api_key_env"`
			Require        *bool   `json:"require"`
		} `json:"rbac"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if raw.Agents != nil {
		cfg.Agents = *raw.Agents
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
		if raw.RBAC.Require != nil {
			cfg.RBAC.Require = *raw.RBAC.Require
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
	if c.Dim < 1 {
		return fmt.Errorf("dim must be positive (got %d)", c.Dim)
	}
	if c.DurationHours <= 0 {
		return fmt.Errorf("duration_hours must be positive (got %f)", c.DurationHours)
	}
	if strings.TrimSpace(c.URI) == "" {
		return fmt.Errorf("uri must not be empty")
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

// topology derives the 2N+1 namespace layout and per-agent permission sets.
type topology struct {
	agents  int
	private []string
	shared  []string
	common  string
}

func newTopology(cfg *soakConfig) (*topology, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return newTopologyUnchecked(cfg), nil
}

func newTopologyUnchecked(cfg *soakConfig) *topology {
	t := &topology{
		agents:  cfg.Agents,
		private: make([]string, cfg.Agents),
		shared:  make([]string, cfg.Agents),
		common:  cfg.Namespaces.Common,
	}
	for i := 0; i < cfg.Agents; i++ {
		t.private[i] = fmt.Sprintf("%s%d", cfg.Namespaces.PrivatePrefix, i)
		t.shared[i] = fmt.Sprintf("%s%d", cfg.Namespaces.SharedPrefix, i)
	}
	return t
}

// Namespaces returns private namespaces, then shared namespaces, then common.
func (t *topology) Namespaces() []string {
	all := make([]string, 0, 2*t.agents+1)
	all = append(all, t.private...)
	all = append(all, t.shared...)
	return append(all, t.common)
}

func (t *topology) valid(i int) bool { return i >= 0 && i < t.agents }

// Writable returns the private, owned-shared, and common namespaces for agent i.
func (t *topology) Writable(i int) []string {
	if !t.valid(i) {
		return nil
	}
	return []string{t.private[i], t.shared[i], t.common}
}

// ReadOnly returns the other agents' shared namespaces for agent i.
func (t *topology) ReadOnly(i int) []string {
	if !t.valid(i) {
		return nil
	}
	out := make([]string, 0, t.agents-1)
	out = append(out, t.shared[:i]...)
	out = append(out, t.shared[i+1:]...)
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

func (t *topology) contains(list []string, ns string) bool {
	for _, item := range list {
		if item == ns {
			return true
		}
	}
	return false
}
