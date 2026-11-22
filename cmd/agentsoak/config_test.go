package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// repoConfigPath locates the shared config from this package's directory.
func repoConfigPath(t *testing.T) string {
	t.Helper()
	return filepath.Clean(filepath.Join("..", "..", defaultConfigPath))
}

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
