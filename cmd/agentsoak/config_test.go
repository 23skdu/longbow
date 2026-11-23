package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// repoConfigPath locates the shared config from this package's directory.
func repoConfigPath(t *testing.T) string {
	t.Helper()
	return filepath.Clean(filepath.Join("..", "..", defaultConfigPath))
}

// TestDefaultConfigIsThreeAgentsSevenNamespaces runs the test default config is three agents seven namespaces operation.
func TestDefaultConfigIsThreeAgentsSevenNamespaces(t *testing.T) {
	cfg := defaultConfig()
	if cfg.Agents != 3 {
		t.Fatalf("default agents = %d, want 3", cfg.Agents)
	}
	topo, err := newTopology(cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"private_0", "private_1", "private_2",
		"shared_0", "shared_1", "shared_2",
		"master",
	}
	if !reflect.DeepEqual(topo.Namespaces(), want) {
		t.Fatalf("namespaces = %v, want %v", topo.Namespaces(), want)
	}
}

// TestLoadSharedConfig runs the test load shared config operation.
func TestLoadSharedConfig(t *testing.T) {
	cfg, err := loadConfig(repoConfigPath(t))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Agents != 3 || cfg.Dim != 128 || cfg.Seed != 42 {
		t.Fatalf("unexpected shared config: %+v", cfg)
	}
	if cfg.RBAC.AdminAPIKeyEnv != "LONGBOW_RBAC_BOOTSTRAP_KEY" {
		t.Fatalf("admin key env = %q", cfg.RBAC.AdminAPIKeyEnv)
	}
}

// TestLoadConfigJSONOverrides runs the test load config json overrides operation.
func TestLoadConfigJSONOverrides(t *testing.T) {
	raw := map[string]any{
		"agents":        4,
		"think_time_ms": 25,
		"namespaces":    map[string]any{"private_prefix": "own_", "shared_prefix": "pool_", "common": "all"},
		"rbac":          map[string]any{"admin_api_key_env": "MY_ADMIN_KEY"},
	}
	data, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "multiagent.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Agents != 4 || cfg.ThinkTimeMS != 25 {
		t.Fatalf("overrides not applied: %+v", cfg)
	}
	if cfg.Namespaces.Common != "all" {
		t.Fatalf("nested overrides not applied: %+v", cfg)
	}
	topo, err := newTopology(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(topo.Namespaces()) != 9 {
		t.Fatalf("namespaces = %d, want 9", len(topo.Namespaces()))
	}
}

// TestLoadSharedWorkloadAndSecuritySchema verifies the Go side of the config
// shared with the Python soak simulators.
func TestLoadSharedWorkloadAndSecuritySchema(t *testing.T) {
	raw := map[string]any{
		"workload":          map[string]any{"dense": 10, "federated": 2.5, "pagerank": 0},
		"workload_options":  map[string]any{"k": 7, "rrf_k": 30, "federated_tags": []string{"soak", "ci"}, "geo": map[string]any{"radius_km": 3}, "graph": map[string]any{"max_hops": 4, "start_node": 9}},
		"security":          map[string]any{"variant": "mtls", "tenant_id": "tenant-a", "tls": map[string]any{"enabled": true, "cert_file": "client.crt", "ca_file": "ca.crt"}, "oidc": map[string]any{"mode": "aws", "ttl_seconds": 600}},
		"sample_interval_s": 30,
	}
	data, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "workload.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Workload) != 3 || cfg.Workload["federated"] != 2.5 {
		t.Fatalf("workload not applied: %v", cfg.Workload)
	}
	if cfg.WorkloadOptions.K != 7 || cfg.WorkloadOptions.RRFK != 30 || cfg.WorkloadOptions.Geo.RadiusKM != 3 {
		t.Fatalf("workload options not applied: %+v", cfg.WorkloadOptions)
	}
	if cfg.WorkloadOptions.Graph.StartNode != 9 || cfg.WorkloadOptions.Graph.MaxHops != 4 {
		t.Fatalf("graph options not applied: %+v", cfg.WorkloadOptions.Graph)
	}
	if !reflect.DeepEqual(cfg.WorkloadOptions.FederatedTags, []string{"soak", "ci"}) {
		t.Fatalf("federated tags = %v", cfg.WorkloadOptions.FederatedTags)
	}
	if cfg.Security.Variant != "mtls" || cfg.Security.TenantID != "tenant-a" || !cfg.Security.TLS.Enabled {
		t.Fatalf("security config not applied: %+v", cfg.Security)
	}
	if cfg.Security.OIDC.Mode != "aws" || cfg.Security.OIDC.TTLSeconds != 600 {
		t.Fatalf("oidc config not applied: %+v", cfg.Security.OIDC)
	}
	if cfg.SampleIntervalS != 30 {
		t.Fatalf("sample interval = %d", cfg.SampleIntervalS)
	}
}

// TestLoadConfigMissingAndInvalid runs the test load config missing and invalid operation.
func TestLoadConfigMissingAndInvalid(t *testing.T) {
	if _, err := loadConfig(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatal("expected error for missing config")
	}
	path := filepath.Join(t.TempDir(), "invalid.json")
	if err := os.WriteFile(path, []byte(`{"agents": 1}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path); err == nil {
		t.Fatal("expected validation error for one agent")
	}
}

// TestTopologyPermissionMatrix runs the test topology permission matrix operation.
func TestTopologyPermissionMatrix(t *testing.T) {
	topo, err := newTopology(defaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := topo.Writable(0), []string{"private_0", "shared_0", "master"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("writable(0) = %v, want %v", got, want)
	}
	if got, want := topo.ReadOnly(0), []string{"shared_1", "shared_2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("readOnly(0) = %v, want %v", got, want)
	}
	if topo.contains(topo.Readable(0), "private_1") {
		t.Fatal("agent 0 must not read agent 1's private namespace")
	}
	if topo.contains(topo.Readable(0), "shared_1") != true {
		t.Fatal("agent 0 must read agent 1's shared namespace")
	}
	if topo.contains(topo.Writable(0), "shared_1") {
		t.Fatal("agent 0 must not write agent 1's shared namespace")
	}
	for i := 0; i < topo.agents; i++ {
		if !topo.contains(topo.Writable(i), "master") {
			t.Fatalf("agent %d must write the common namespace", i)
		}
	}
}

// TestTopologyGroupedNamespaces covers the bounded CI layout with two agents
// sharing each namespace. Cross-group reads stay allowed and writes stay denied.
func TestTopologyGroupedNamespaces(t *testing.T) {
	cfg := defaultConfig()
	cfg.Agents = 8
	cfg.AgentsPerNamespace = 2
	topo, err := newTopology(cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"shared_0", "shared_1", "shared_2", "shared_3", "master"}
	if !reflect.DeepEqual(topo.Namespaces(), want) {
		t.Fatalf("namespaces = %v, want %v", topo.Namespaces(), want)
	}
	if got, want := topo.Writable(1), []string{"shared_0", "master"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("writable(1) = %v, want %v", got, want)
	}
	if got, want := topo.ReadOnly(0), []string{"shared_1", "shared_2", "shared_3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("readOnly(0) = %v, want %v", got, want)
	}
	forbiddenRead, forbiddenWrite := topo.forbidden(0)
	if forbiddenRead != "shared_1" || forbiddenWrite != "shared_1" {
		t.Fatalf("forbidden(0) = %q, %q", forbiddenRead, forbiddenWrite)
	}
	if topo.contains(topo.Writable(0), "shared_1") {
		t.Fatal("group 0 must not write group 1's namespace")
	}
	if !topo.contains(topo.Readable(0), "shared_1") {
		t.Fatal("group 0 must read group 1's namespace")
	}

	cfg.AgentsPerNamespace = 8
	if _, err := newTopology(cfg); err == nil {
		t.Fatal("expected error when all agents share one group")
	}
}

// TestTopologyScaleAndNames runs the test topology scale and names operation.
func TestTopologyScaleAndNames(t *testing.T) {
	for _, n := range []int{2, 3, 5, 10, 50} {
		cfg := defaultConfig()
		cfg.Agents = n
		cfg.Namespaces.PrivatePrefix = "p_"
		cfg.Namespaces.SharedPrefix = "s_"
		cfg.Namespaces.Common = "c"
		topo, err := newTopology(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if len(topo.Namespaces()) != 2*n+1 {
			t.Fatalf("agents=%d: %d namespaces, want %d", n, len(topo.Namespaces()), 2*n+1)
		}
	}
	cfg := defaultConfig()
	cfg.Agents = 2
	cfg.Namespaces.PrivatePrefix = "ns_"
	cfg.Namespaces.SharedPrefix = "ns_"
	cfg.Namespaces.Common = "ns_0"
	if _, err := newTopology(cfg); err == nil {
		t.Fatal("expected duplicate namespace error")
	}
}

// TestResolveConfigFlagOverrides runs the test resolve config flag overrides operation.
func TestResolveConfigFlagOverrides(t *testing.T) {
	originalFlagSet := flagSet
	originalConfigPath, originalAgents, originalSeed := *configPath, *agents, *seed
	originalPrivatePrefix := *privatePrefix
	t.Cleanup(func() {
		flagSet = originalFlagSet
		*configPath, *agents, *seed = originalConfigPath, originalAgents, originalSeed
		*privatePrefix = originalPrivatePrefix
	})

	*configPath = repoConfigPath(t)
	*agents = 5
	*seed = 0
	*privatePrefix = "custom_"
	flagSet = func(name string) bool {
		return name == "agents" || name == "seed" || name == "private-prefix"
	}
	cfg, err := resolveConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Agents != 5 || cfg.Seed != 0 || cfg.Namespaces.PrivatePrefix != "custom_" {
		t.Fatalf("flag overrides not applied: %+v", cfg)
	}
	if cfg.Namespaces.Common != "master" {
		t.Fatalf("non-overridden config value changed: %+v", cfg)
	}
}

// TestLoadConfigEmptyPathUsesDefaults pins the no-config path used when the
// shared file is absent and no -config flag was supplied.
func TestLoadConfigEmptyPathUsesDefaults(t *testing.T) {
	cfg, err := loadConfig("")
	if err != nil {
		t.Fatal(err)
	}
	want := defaultConfig()
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("loadConfig(\"\") = %+v, want %+v", cfg, want)
	}
}

// TestLoadConfigValidationMatrix covers the validation rules that keep a soak
// from starting with a config that cannot produce a meaningful report.
func TestLoadConfigValidationMatrix(t *testing.T) {
	cases := []struct {
		name, raw, wantErr string
	}{
		{"malformed json", `{`, "parse config"},
		{"wrong field type", `{"agents":"three"}`, "parse config"},
		{"too few agents", `{"agents":1}`, "agents must be at least 2"},
		{"negative group size", `{"agents_per_namespace":-1}`, "must not be negative"},
		{"group size equals agents", `{"agents":3,"agents_per_namespace":3}`, "smaller than agents"},
		{"non-positive dimension", `{"dim":0}`, "dim must be positive"},
		{"non-positive duration", `{"duration_hours":0}`, "duration_hours must be positive"},
		{"non-positive sample interval", `{"sample_interval_s":0}`, "sample_interval_s must be positive"},
		{"blank uri", `{"uri":"   "}`, "uri must not be empty"},
		{"empty workload", `{"workload":{}}`, "workload must contain at least one"},
		{"zero total weight", `{"workload":{"dense":0}}`, "weights must sum to a positive value"},
		{"unknown op", `{"workload":{"unsupported":1}}`, "unknown workload operation"},
		{"negative weight", `{"workload":{"dense":-1}}`, "must be non-negative"},
		{"non-positive k", `{"workload_options":{"k":0}}`, "k must be positive"},
		{"alpha out of range", `{"workload_options":{"alpha":1.5}}`, "between 0 and 1"},
		{"unknown security variant", `{"security":{"variant":"bogus"}}`, "unknown security variant"},
		{"unknown oidc mode", `{"security":{"oidc":{"mode":"bogus"}}}`, "unknown oidc mode"},
		{"non-positive oidc ttl", `{"security":{"oidc":{"ttl_seconds":0}}}`, "ttl_seconds must be positive"},
		{"duplicate namespace", `{"agents":2,"namespaces":{"private_prefix":"ns_","shared_prefix":"ns_"}}`, "must be unique"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, []byte(tc.raw), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := loadConfig(path)
			if err == nil {
				t.Fatalf("expected error containing %q, got config %+v", tc.wantErr, cfg)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %q, want substring %q", err, tc.wantErr)
			}
		})
	}
}

// saveResolveFlags snapshots the flag-backed globals mutated by resolveConfig
// tests and resets them to their CLI defaults. Every test restores them even
// when it fails, so flag state never leaks between subtests.
func saveResolveFlags(t *testing.T) {
	t.Helper()
	flagSetOrig := flagSet
	configPathOrig, uriOrig, agentsOrig, agentsPerNSOrig := *configPath, *uri, *agents, *agentsPerNS
	durationOrig, dimOrig, seedOrig, thinkTimeOrig, sampleIntervalOrig := *duration, *dim, *seed, *thinkTime, *sampleInterval
	privatePrefixOrig, sharedPrefixOrig, commonOrig := *privatePrefix, *sharedPrefix, *common
	adminAPIKeyOrig, workloadFlagOrig, federatedOnlyOrig := *adminAPIKey, *workloadFlag, *federatedOnly
	tlsEnabledOrig, tlsCertOrig, tlsKeyOrig, tlsCAOrig := *tlsEnabled, *tlsCert, *tlsKey, *tlsCA
	tlsServerNameOrig, tlsMinVersionOrig := *tlsServerName, *tlsMinVersion
	oidcModeOrig, oidcAudienceOrig, oidcTTLOrig, tenantIDOrig := *oidcMode, *oidcAudience, *oidcTTL, *tenantID
	outputJSONOrig, verboseOrig := *outputJSON, *verbose
	t.Cleanup(func() {
		flagSet = flagSetOrig
		*configPath, *uri, *agents, *agentsPerNS = configPathOrig, uriOrig, agentsOrig, agentsPerNSOrig
		*duration, *dim, *seed, *thinkTime, *sampleInterval = durationOrig, dimOrig, seedOrig, thinkTimeOrig, sampleIntervalOrig
		*privatePrefix, *sharedPrefix, *common = privatePrefixOrig, sharedPrefixOrig, commonOrig
		*adminAPIKey, *workloadFlag, *federatedOnly = adminAPIKeyOrig, workloadFlagOrig, federatedOnlyOrig
		*tlsEnabled, *tlsCert, *tlsKey, *tlsCA = tlsEnabledOrig, tlsCertOrig, tlsKeyOrig, tlsCAOrig
		*tlsServerName, *tlsMinVersion = tlsServerNameOrig, tlsMinVersionOrig
		*oidcMode, *oidcAudience, *oidcTTL, *tenantID = oidcModeOrig, oidcAudienceOrig, oidcTTLOrig, tenantIDOrig
		*outputJSON, *verbose = outputJSONOrig, verboseOrig
	})

	*configPath, *uri, *agents, *agentsPerNS = defaultConfigPath, "", 0, 0
	*duration, *dim, *seed, *thinkTime, *sampleInterval = 0, 0, 0, 0, 0
	*privatePrefix, *sharedPrefix, *common, *adminAPIKey, *workloadFlag = "", "", "", "", ""
	*federatedOnly, *tlsEnabled, *tlsCert, *tlsKey, *tlsCA = false, false, "", "", ""
	*tlsServerName, *tlsMinVersion, *oidcMode, *oidcAudience = "", "", "", ""
	*oidcTTL = 0
	*tenantID, *outputJSON, *verbose = "", "", false
}

// TestResolveConfigFlagPrecedence verifies that explicit flags win over the
// shared config (including zero values), that validation runs after overrides,
// and that a missing shared file only falls back when -config was not set.
func TestResolveConfigFlagPrecedence(t *testing.T) {
	cases := []struct {
		name    string
		config  string
		missing bool
		set     map[string]bool
		apply   func()
		wantErr string
		check   func(*testing.T, *soakConfig)
	}{
		{
			name: "workload flag replaces shared workload",
			set:  map[string]bool{"workload": true},
			apply: func() {
				*workloadFlag = " dense=25, hybrid = 5 "
			},
			check: func(t *testing.T, cfg *soakConfig) {
				if len(cfg.Workload) != 2 || cfg.Workload["dense"] != 25 || cfg.Workload["hybrid"] != 5 {
					t.Fatalf("workload override not applied: %v", cfg.Workload)
				}
			},
		},
		{
			name: "federated only wins over workload flag",
			set:  map[string]bool{"workload": true},
			apply: func() {
				*workloadFlag = "dense=25"
				*federatedOnly = true
			},
			check: func(t *testing.T, cfg *soakConfig) {
				if len(cfg.Workload) != 1 || cfg.Workload["federated"] != 1 {
					t.Fatalf("federated-only workload = %v", cfg.Workload)
				}
			},
		},
		{
			name:    "malformed workload override fails",
			set:     map[string]bool{"workload": true},
			apply:   func() { *workloadFlag = "dense=maybe" },
			wantErr: "invalid weight",
		},
		{
			name:    "invalid flag override fails validation",
			set:     map[string]bool{"agents": true},
			apply:   func() { *agents = 1 },
			wantErr: "agents must be at least 2",
		},
		{
			name:  "zero seed remains a valid override",
			set:   map[string]bool{"seed": true},
			apply: func() { *seed = 0 },
			check: func(t *testing.T, cfg *soakConfig) {
				if cfg.Seed != 0 {
					t.Fatalf("seed = %d, want explicit zero", cfg.Seed)
				}
			},
		},
		{
			name: "duration and topology flags override config",
			set: map[string]bool{
				"duration": true, "think-time": true, "sample-interval": true,
				"agents-per-namespace": true, "common": true, "uri": true, "dim": true,
			},
			apply: func() {
				*duration = 90 * time.Second
				*thinkTime = 25 * time.Millisecond
				*sampleInterval = 30 * time.Second
				*agentsPerNS = 2
				*common = "all"
				*uri = "grpc://example:3000"
				*dim = 64
			},
			check: func(t *testing.T, cfg *soakConfig) {
				if cfg.DurationHours != 0.025 || cfg.ThinkTimeMS != 25 || cfg.SampleIntervalS != 30 {
					t.Fatalf("timing overrides not applied: %+v", cfg)
				}
				if cfg.AgentsPerNamespace != 2 || cfg.Namespaces.Common != "all" || cfg.URI != "grpc://example:3000" || cfg.Dim != 64 {
					t.Fatalf("topology/connection overrides not applied: %+v", cfg)
				}
			},
		},
		{
			name:   "tls flag promotes plaintext variant",
			config: `{"agents":2,"security":{"variant":"plaintext"}}`,
			set:    map[string]bool{"tls-enabled": true},
			apply:  func() { *tlsEnabled = true },
			check: func(t *testing.T, cfg *soakConfig) {
				if cfg.Security.Variant != "tls" || !cfg.Security.TLS.Enabled {
					t.Fatalf("TLS variant = %q, enabled = %v", cfg.Security.Variant, cfg.Security.TLS.Enabled)
				}
			},
		},
		{
			name:   "tls flag promotes oidc variant to combined",
			config: `{"agents":2,"security":{"variant":"oidc","oidc":{"mode":"aws","ttl_seconds":600}}}`,
			set:    map[string]bool{"tls-enabled": true},
			apply:  func() { *tlsEnabled = true },
			check: func(t *testing.T, cfg *soakConfig) {
				if cfg.Security.Variant != "combined" {
					t.Fatalf("security variant = %q, want combined", cfg.Security.Variant)
				}
			},
		},
		{
			name:    "explicit missing config is fatal",
			missing: true,
			set:     map[string]bool{"config": true},
			wantErr: "read config",
		},
		{
			name:    "missing shared config falls back to defaults",
			missing: true,
			set:     map[string]bool{"agents": true},
			apply:   func() { *agents = 4 },
			check: func(t *testing.T, cfg *soakConfig) {
				if cfg.Agents != 4 || cfg.Seed != 42 || cfg.Dim != 128 {
					t.Fatalf("fallback config lost defaults or flag override: %+v", cfg)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			saveResolveFlags(t)
			switch {
			case tc.config != "":
				path := filepath.Join(t.TempDir(), "config.json")
				if err := os.WriteFile(path, []byte(tc.config), 0600); err != nil {
					t.Fatal(err)
				}
				*configPath = path
			case tc.missing:
				*configPath = filepath.Join(t.TempDir(), "missing.json")
			default:
				*configPath = repoConfigPath(t)
			}
			flagSet = func(name string) bool { return tc.set[name] }
			if tc.apply != nil {
				tc.apply()
			}
			cfg, err := resolveConfig()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want substring %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.check != nil {
				tc.check(t, cfg)
			}
		})
	}
}

// FuzzLoadConfigJSON exercises the shared config parser with arbitrary JSON,
// checking that every accepted config is internally consistent and yields a
// usable topology. Oversized inputs are skipped to keep fuzzing bounded.
func FuzzLoadConfigJSON(f *testing.F) {
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"agents":2,"workload":{"dense":1}}`))
	f.Add([]byte(`{"agents":1}`))
	f.Add([]byte(`{"workload":{"unsupported":1}}`))
	f.Add([]byte(`{"agents":3,"agents_per_namespace":3}`))
	f.Add([]byte(`{"security":{"variant":"mtls","oidc":{"mode":"aws","ttl_seconds":600}}}`))
	f.Add([]byte(`{"namespaces":{"private_prefix":"p_","shared_prefix":"s_","common":"all"}}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 8192 {
			return
		}
		path := filepath.Join(t.TempDir(), "config.json")
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		cfg, err := loadConfig(path)
		if err != nil {
			return
		}
		if cfg == nil {
			t.Fatal("loadConfig returned nil config without an error")
		}
		if cfg.Agents > 512 {
			return // keep topology allocation bounded for random large agents
		}
		if err := cfg.validate(); err != nil {
			t.Fatalf("loaded config failed validation: %v", err)
		}
		topo, err := newTopology(cfg)
		if err != nil {
			t.Fatalf("loaded config has invalid topology: %v", err)
		}
		seen := make(map[string]bool)
		for _, ns := range topo.Namespaces() {
			if seen[ns] {
				t.Fatalf("duplicate namespace %q", ns)
			}
			seen[ns] = true
		}
		for i := 0; i < cfg.Agents; i++ {
			if !topo.contains(topo.Readable(i), topo.common) {
				t.Fatalf("agent %d cannot read common namespace %q", i, topo.common)
			}
			blockedRead, blockedWrite := topo.forbidden(i)
			if blockedRead == "" || blockedWrite == "" {
				t.Fatalf("agent %d has no forbidden probe namespaces", i)
			}
		}
	})
}
