package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kelseyhightower/envconfig"
)

func TestParseStartupNamespacePoliciesEmpty(t *testing.T) {
	for _, raw := range []string{"", "   "} {
		policies, err := ParseStartupNamespacePolicies(raw)
		if err != nil {
			t.Fatalf("ParseStartupNamespacePolicies(%q) error = %v", raw, err)
		}
		if len(policies) != 0 {
			t.Fatalf("ParseStartupNamespacePolicies(%q) returned %d policies, want 0", raw, len(policies))
		}
	}
}

func TestParseStartupNamespacePolicies(t *testing.T) {
	raw := `[
		{
			"namespace": "team-a",
			"admin_key_ids": ["bootstrap-admin"],
			"writer_key_ids": ["writer-1"],
			"reader_key_ids": ["reader-1"],
			"public_permissions": ["read"]
		},
		{
			"namespace": "scratch",
			"public_permissions": ["write"],
			"ephemeral": true
		}
	]`
	policies, err := ParseStartupNamespacePolicies(raw)
	if err != nil {
		t.Fatalf("ParseStartupNamespacePolicies() error = %v", err)
	}
	if len(policies) != 2 {
		t.Fatalf("got %d policies, want 2", len(policies))
	}
	if policies[0].Namespace != "team-a" || policies[0].Ephemeral {
		t.Errorf("unexpected first policy: %+v", policies[0])
	}
	if got := policies[0].PublicPermissions; len(got) != 1 || got[0] != "read" {
		t.Errorf("first policy permissions = %v, want [read]", got)
	}
	if policies[1].Namespace != "scratch" || !policies[1].Ephemeral {
		t.Errorf("unexpected second policy: %+v", policies[1])
	}
}

func TestParseStartupNamespacePoliciesErrors(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{name: "invalid JSON", raw: `[{"namespace":`},
		{name: "unknown field", raw: `[{"namespace":"team-a","unknown":true}]`},
		{name: "trailing data", raw: `[{"namespace":"team-a"}] {}`},
		{name: "empty namespace", raw: `[{"namespace":"  "}]`},
		{name: "duplicate namespace", raw: `[{"namespace":"team-a"},{"namespace":"team-a"}]`},
		{name: "path separator", raw: `[{"namespace":"team/a"}]`},
		{name: "invalid permission", raw: `[{"namespace":"team-a","public_permissions":["admin"]}]`},
		{name: "wildcard permission", raw: `[{"namespace":"team-a","public_permissions":["*"]}]`},
		{name: "invalid key ID", raw: `[{"namespace":"team-a","writer_key_ids":["bad/key"]}]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseStartupNamespacePolicies(tt.raw)
			if !errors.Is(err, ErrInvalidStartupNamespacePolicies) {
				t.Fatalf("error = %v, want ErrInvalidStartupNamespacePolicies", err)
			}
		})
	}
}

func TestValidateConfigStartupNamespacePolicies(t *testing.T) {
	cfg := Config{
		KeepAliveTime:    2 * time.Hour,
		ListenAddr:       "0.0.0.0:3000",
		MetaAddr:         "0.0.0.0:3001",
		MetricsAddr:      "0.0.0.0:9090",
		MaxMemory:        1073741824,
		DataPath:         "./data",
		SnapshotInterval: time.Hour,
		MaxWALSize:       104857600,
		LogFormat:        "json",
		LogLevel:         "info",
	}
	cfg.StartupNamespacePolicies = `[{"namespace":"team-a","ephemeral":true}]`
	if err := ValidateConfig(&cfg); err != nil {
		t.Fatalf("ValidateConfig() with valid startup policies error = %v", err)
	}

	cfg.StartupNamespacePolicies = `not-json`
	if err := ValidateConfig(&cfg); !errors.Is(err, ErrInvalidStartupNamespacePolicies) {
		t.Fatalf("ValidateConfig() error = %v, want ErrInvalidStartupNamespacePolicies", err)
	}
}

func TestEnvStartupNamespacePolicies(t *testing.T) {
	t.Setenv("LONGBOW_STARTUP_NAMESPACE_POLICIES", `[{"namespace":"team-a","ephemeral":true}]`)
	var cfg Config
	if err := envconfig.Process("LONGBOW", &cfg); err != nil {
		t.Fatalf("envconfig.Process() error = %v", err)
	}
	if !strings.Contains(cfg.StartupNamespacePolicies, "team-a") {
		t.Fatalf("startup policies = %q, want team-a", cfg.StartupNamespacePolicies)
	}
	policies, err := ParseStartupNamespacePolicies(cfg.StartupNamespacePolicies)
	if err != nil {
		t.Fatalf("ParseStartupNamespacePolicies() error = %v", err)
	}
	if len(policies) != 1 || !policies[0].Ephemeral {
		t.Fatalf("policies = %+v, want one ephemeral policy", policies)
	}
}
