package secrets

import (
	"errors"
	"testing"
)

// FuzzValidateKeyID verifies that accepted key IDs are provider-safe and that
// deterministic namespace-policy IDs are always accepted.
func FuzzValidateKeyID(f *testing.F) {
	for _, seed := range []string{
		"admin",
		"team-a_admin.1",
		"",
		"../escape",
		"a..b",
		"a/b",
		" leading-space",
		string(make([]byte, 129)),
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, id string) {
		err := ValidateKeyID(id)
		if err != nil {
			if !errors.Is(err, ErrInvalidKeyID) {
				t.Fatalf("ValidateKeyID(%q) returned unexpected error: %v", id, err)
			}
			return
		}

		if len(id) == 0 || len(id) > 128 {
			t.Fatalf("ValidateKeyID accepted invalid length %d for %q", len(id), id)
		}
		if !isASCIIAlnum(id[0]) {
			t.Fatalf("ValidateKeyID accepted leading byte %q in %q", id[0], id)
		}
		for i := 0; i < len(id); i++ {
			if !isASCIIAlnum(id[i]) && id[i] != '_' && id[i] != '.' && id[i] != '-' {
				t.Fatalf("ValidateKeyID accepted byte %q in %q", id[i], id)
			}
		}
		if containsDotDot(id) {
			t.Fatalf("ValidateKeyID accepted traversal sequence in %q", id)
		}

		policyID := NamespacePolicyKeyID(id)
		if err := ValidateKeyID(policyID); err != nil {
			t.Fatalf("NamespacePolicyKeyID(%q) produced invalid ID %q: %v", id, policyID, err)
		}
		if !IsNamespacePolicyKeyID(policyID) {
			t.Fatalf("NamespacePolicyKeyID(%q) produced %q without policy prefix", id, policyID)
		}
	})
}

func isASCIIAlnum(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

func containsDotDot(s string) bool {
	for i := 0; i+1 < len(s); i++ {
		if s[i] == '.' && s[i+1] == '.' {
			return true
		}
	}
	return false
}
