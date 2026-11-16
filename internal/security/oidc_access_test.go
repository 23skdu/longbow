package security

import (
	"context"
	"testing"
	"time"
)

func TestOIDCAccessValidatorAuthorize(t *testing.T) {
	m2, priv2, _ := testOIDCSetup(t)
	v := NewOIDCAccessValidator(m2)

	// Use signToken from oidc_test.go
	raw := signToken(t, priv2, map[string]interface{}{
		"iss":       "https://issuer.example",
		"aud":       "longbow",
		"sub":       "user-1",
		"exp":       time.Now().Unix() + 3600,
		"iat":       time.Now().Unix(),
		"jti":       "jti-access-1",
		"tenant_id": "acme",
		"namespace": "acme",
		"datasets":  []interface{}{"acme/docs"},
		"groups":    []interface{}{"writer"},
	})
	// signToken expects jwt.MapClaims; map[string]interface{} ok? jwt.MapClaims is map[string]interface{}
	tc, err := v.AuthorizeToken(context.Background(), raw, "write", "acme", "acme/docs")
	if err != nil {
		t.Fatalf("AuthorizeToken: %v", err)
	}
	if tc.TenantID != "acme" {
		t.Fatalf("tenant mismatch: %s", tc.TenantID)
	}

	// permission denied
	raw2 := signToken(t, priv2, map[string]interface{}{
		"iss":       "https://issuer.example",
		"aud":       "longbow",
		"sub":       "user-2",
		"exp":       time.Now().Unix() + 3600,
		"iat":       time.Now().Unix(),
		"jti":       "jti-access-2",
		"tenant_id": "acme",
		"namespace": "acme",
		"groups":    []interface{}{"reader"},
	})
	if _, err := v.AuthorizeToken(context.Background(), raw2, "write", "acme", "acme/docs"); err == nil {
		t.Fatal("expected permission denied")
	}

	// cross-tenant dataset denied
	raw3 := signToken(t, priv2, map[string]interface{}{
		"iss":       "https://issuer.example",
		"aud":       "longbow",
		"sub":       "user-3",
		"exp":       time.Now().Unix() + 3600,
		"iat":       time.Now().Unix(),
		"jti":       "jti-access-3",
		"tenant_id": "acme",
		"namespace": "acme",
		"datasets":  []interface{}{"acme/docs"},
		"groups":    []interface{}{"reader"},
	})
	if _, err := v.AuthorizeToken(context.Background(), raw3, "read", "other", "other/docs"); err == nil {
		t.Fatal("expected dataset/namespace denial")
	}
}
