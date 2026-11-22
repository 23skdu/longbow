package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/23skdu/longbow/internal/security/secrets"
	"github.com/23skdu/longbow/internal/store"
	"github.com/23skdu/longbow/internal/store/rbac"
)

// ErrInvalidStartupNamespacePolicies indicates malformed declarative namespace ACLs.
var ErrInvalidStartupNamespacePolicies = errors.New("startup_namespace_policies is invalid")

// ParseStartupNamespacePolicies decodes and validates the JSON array of
// namespace ACLs that should be provisioned before the server accepts traffic.
// An empty value is valid and means no startup namespaces are declared.
func ParseStartupNamespacePolicies(raw string) ([]store.NamespacePolicy, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}

	var policies []store.NamespacePolicy
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&policies); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidStartupNamespacePolicies, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("%w: unexpected data after JSON array", ErrInvalidStartupNamespacePolicies)
		}
		return nil, fmt.Errorf("%w: %v", ErrInvalidStartupNamespacePolicies, err)
	}

	seen := make(map[string]struct{}, len(policies))
	for i := range policies {
		policy := &policies[i]
		policy.Namespace = strings.TrimSpace(policy.Namespace)
		if err := policy.Validate(); err != nil {
			return nil, fmt.Errorf("%w: policy %d: %v", ErrInvalidStartupNamespacePolicies, i, err)
		}
		if err := rbac.ValidateNamespaceName(policy.Namespace); err != nil {
			return nil, fmt.Errorf("%w: policy %d: %v", ErrInvalidStartupNamespacePolicies, i, err)
		}
		if _, duplicate := seen[policy.Namespace]; duplicate {
			return nil, fmt.Errorf("%w: duplicate namespace %q", ErrInvalidStartupNamespacePolicies, policy.Namespace)
		}
		seen[policy.Namespace] = struct{}{}

		for _, list := range [][]string{policy.AdminKeyIDs, policy.WriterKeyIDs, policy.ReaderKeyIDs} {
			for _, keyID := range list {
				if err := secrets.ValidateKeyID(keyID); err != nil {
					return nil, fmt.Errorf("%w: policy %d: %v", ErrInvalidStartupNamespacePolicies, i, err)
				}
			}
		}
	}
	return policies, nil
}
