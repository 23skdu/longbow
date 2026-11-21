"""RBAC API keys, row-level security, audit logs, and OIDC federation helpers."""

from typing import List, Dict, Any, Optional
from ..exceptions import (
    LongbowAuthenticationError,
)


class SecurityMixin:
    def create_api_key(
        self,
        name: str,
        role: str = "reader",
        tenant_id: str = "",
        namespace: str = "",
        datasets: Optional[List[str]] = None,
        row_filters: Optional[Dict[str, str]] = None,
        ttl_seconds: int = 0,
    ) -> Dict[str, Any]:
        """Issue a new RBAC API key scoped to a tenant/namespace."""
        payload = {
            "name": name,
            "role": role,
            "tenant_id": tenant_id or namespace,
            "namespace": namespace or tenant_id,
            "datasets": datasets or [],
            "row_filters": row_filters or {},
            "ttl_seconds": ttl_seconds,
        }
        return self._do_action_json("CreateAPIKey", payload, default={})
    def rotate_api_key(self, api_key: str, grace_period_seconds: int = 3600) -> Dict[str, Any]:
        """Rotate an existing API key with an optional grace period."""
        return self._do_action_json(
            "RotateAPIKey",
            {"api_key": api_key, "grace_period_seconds": grace_period_seconds},
            default={},
        )
    def revoke_api_key(self, key_or_id: str) -> Dict[str, Any]:
        """Revoke an API key by secret or KeyID."""
        return self._do_action_json("RevokeAPIKey", {"api_key": key_or_id}, default={})
    def set_rls_policy(
        self,
        name: str,
        namespace: str,
        dataset: str,
        rules: List[Dict[str, Any]],
        enabled: bool = True,
    ) -> Dict[str, Any]:
        """Configure a row-level security (RLS) policy for a namespace/dataset."""
        return self._do_action_json(
            "SetRLSPolicy",
            {
                "name": name,
                "namespace": namespace,
                "dataset": dataset,
                "rules": rules,
                "enabled": enabled,
            },
            default={},
        )
    def list_api_keys(self) -> List[Dict[str, Any]]:
        """List registered RBAC API keys."""
        return self._do_action_json("ListAPIKeys", {}, default={}).get("keys", [])
    def get_audit_log(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Fetch recent RBAC security audit log entries."""
        return self._do_action_json("GetAuditLog", {"limit": limit}, default={}).get(
            "entries", []
        )
    def list_oidc_providers(self) -> List[Dict[str, Any]]:
        """List configured OIDC federation providers."""
        return self._do_action_json("ListOIDCProviders", {}, default={}).get(
            "providers", []
        )
    def add_oidc_provider(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Register or update an OIDC federation provider."""
        return self._do_action_json("AddOIDCProvider", config, default={})
    def remove_oidc_provider(self, issuer_url: str) -> Dict[str, Any]:
        """Remove a configured OIDC federation provider."""
        return self._do_action_json(
            "RemoveOIDCProvider", {"issuer_url": issuer_url}, default={}
        )
    def add_aws_oidc_provider(
        self,
        issuer_url: str,
        audience: str = "longbow",
        *,
        jwks_url: str = "",
        trust_policy_arn: str = "",
        tenant_claim: str = "tenant_id",
        namespace_claim: str = "namespace",
        datasets_claim: str = "datasets",
        role_claim: str = "groups",
        row_filter_claim: str = "row_filters",
        admin_claim: str = "role",
        admin_value: str = "admin",
        http_endpoint: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Register an AWS IRSA / EKS / Cognito OIDC federation provider."""
        cfg: Dict[str, Any] = {
            "provider": "aws",
            "issuer_url": issuer_url,
            "audience": audience,
            "allowed_algs": ["RS256", "ES256"],
            "tenant_claim": tenant_claim,
            "namespace_claim": namespace_claim,
            "datasets_claim": datasets_claim,
            "role_claim": role_claim,
            "row_filter_claim": row_filter_claim,
            "admin_claim": admin_claim,
            "admin_value": admin_value,
        }
        if jwks_url:
            cfg["jwks_url"] = jwks_url
        if trust_policy_arn:
            cfg["trust_policy_arn"] = trust_policy_arn
        if http_endpoint:
            return self.rest_add_oidc_provider(cfg, http_endpoint=http_endpoint)
        return self.add_oidc_provider(cfg)
    def add_google_oidc_provider(
        self,
        audience: str = "longbow",
        *,
        issuer_url: str = "https://accounts.google.com",
        jwks_url: str = "https://www.googleapis.com/oauth2/v3/certs",
        trust_policy_arn: str = "",
        tenant_claim: str = "tenant_id",
        namespace_claim: str = "namespace",
        datasets_claim: str = "datasets",
        role_claim: str = "groups",
        row_filter_claim: str = "row_filters",
        admin_claim: str = "role",
        admin_value: str = "admin",
        http_endpoint: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Register a Google IAM / GKE Workload Identity OIDC federation provider."""
        cfg: Dict[str, Any] = {
            "provider": "google",
            "issuer_url": issuer_url,
            "jwks_url": jwks_url,
            "audience": audience,
            "allowed_algs": ["RS256", "ES256"],
            "tenant_claim": tenant_claim,
            "namespace_claim": namespace_claim,
            "datasets_claim": datasets_claim,
            "role_claim": role_claim,
            "row_filter_claim": row_filter_claim,
            "admin_claim": admin_claim,
            "admin_value": admin_value,
        }
        if trust_policy_arn:
            cfg["trust_policy_arn"] = trust_policy_arn
        if http_endpoint:
            return self.rest_add_oidc_provider(cfg, http_endpoint=http_endpoint)
        return self.add_oidc_provider(cfg)
    def rest_list_oidc_providers(
        self, http_endpoint: str = "http://127.0.0.1:6000"
    ) -> List[Dict[str, Any]]:
        """List configured OIDC federation providers via REST v1."""
        return self._rest_request(
            "GET", "/api/v1/oidc/providers", http_endpoint=http_endpoint
        ).get("providers", [])
    def rest_add_oidc_provider(
        self, config: Dict[str, Any], http_endpoint: str = "http://127.0.0.1:6000"
    ) -> Dict[str, Any]:
        """Register or update an OIDC federation provider via REST v1."""
        return self._rest_request(
            "POST", "/api/v1/oidc/providers", config, http_endpoint=http_endpoint
        )
    def rest_remove_oidc_provider(
        self, issuer_url: str, http_endpoint: str = "http://127.0.0.1:6000"
    ) -> Dict[str, Any]:
        """Remove an OIDC federation provider via REST v1."""
        import urllib.parse

        qs = urllib.parse.urlencode({"issuer_url": issuer_url})
        return self._rest_request(
            "DELETE", f"/api/v1/oidc/providers?{qs}", http_endpoint=http_endpoint
        )
    def exchange_oidc_token(
        self,
        oidc_token: str,
        ttl_seconds: int = 900,
        http_endpoint: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Exchange a federated OIDC JWT for a short-lived Longbow session token."""
        if not oidc_token or not oidc_token.strip():
            raise LongbowAuthenticationError(
                "OIDC token is required for token exchange",
                operation="ExchangeOIDCToken",
                hint="Pass a valid federated JWT or call acquire_aws_web_identity_token() / acquire_google_id_token().",
            )
        if http_endpoint:
            resp = self._rest_request(
                "POST",
                "/api/v1/oidc/token",
                {"token": oidc_token, "ttl_seconds": ttl_seconds},
                http_endpoint=http_endpoint,
            )
        else:
            resp = self._do_action_json(
                "ExchangeOIDCToken",
                {"token": oidc_token, "ttl_seconds": ttl_seconds},
                default={},
                bearer_override=oidc_token,
            )
        if resp.get("token"):
            self.api_key = resp["token"]
        return resp
    def authenticate_with_aws_oidc(
        self,
        ttl_seconds: int = 900,
        http_endpoint: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Acquire an AWS IRSA/EKS JWT and exchange it for a short-lived Longbow session token."""
        tok = self.acquire_aws_web_identity_token()
        return self.exchange_oidc_token(tok, ttl_seconds=ttl_seconds, http_endpoint=http_endpoint)
    def authenticate_with_google_oidc(
        self,
        audience: str = "longbow",
        ttl_seconds: int = 900,
        http_endpoint: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Acquire a Google Cloud IAM identity token and exchange it for a short-lived Longbow session token."""
        tok = self.acquire_google_id_token(audience=audience)
        return self.exchange_oidc_token(tok, ttl_seconds=ttl_seconds, http_endpoint=http_endpoint)
    @staticmethod
    def acquire_aws_web_identity_token() -> str:
        """Read an AWS IRSA / EKS Pod Identity JWT from AWS_WEB_IDENTITY_TOKEN_FILE."""
        import os

        token_file = os.environ.get("AWS_WEB_IDENTITY_TOKEN_FILE", "").strip()
        if not token_file:
            raise ValueError("AWS_WEB_IDENTITY_TOKEN_FILE is not set")
        with open(token_file, "r", encoding="utf-8") as fh:
            tok = fh.read().strip()
        if not tok:
            raise ValueError("AWS web identity token file is empty")
        return tok
    @staticmethod
    def acquire_google_id_token(
        audience: str,
        metadata_url: str = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity",
    ) -> str:
        """Fetch a Google Cloud IAM identity token from the GCE metadata server."""
        import urllib.parse
        import urllib.request

        if not audience:
            raise ValueError("audience is required")
        qs = urllib.parse.urlencode({"audience": audience, "format": "full"})
        req = urllib.request.Request(
            f"{metadata_url}?{qs}",
            headers={"Metadata-Flavor": "Google"},
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:  # nosec B310 - GCE metadata URL
            tok = resp.read().decode("utf-8").strip()
        if not tok:
            raise ValueError("empty Google ID token response")
        return tok
