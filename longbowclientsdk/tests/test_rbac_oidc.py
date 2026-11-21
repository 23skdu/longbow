"""Offline unit tests for LongbowClient RBAC, OIDC federation, and auth error handling."""

from __future__ import annotations

import io
import json
import sys
import types
import unittest
import urllib.error
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch


def _ensure_optional_test_deps() -> bool:
    """Provide minimal in-memory stubs when numpy/pyarrow/pandas/pydantic are absent."""
    try:
        import numpy  # noqa: F401
        import pandas  # noqa: F401
        import pyarrow  # noqa: F401
        import pyarrow.flight  # noqa: F401
        import pydantic  # noqa: F401

        if not hasattr(pyarrow.flight, "FlightDescriptor"):
            class _FlightDescriptor:
                @staticmethod
                def for_path(path: str):
                    return ("path", path)

            pyarrow.flight.FlightDescriptor = _FlightDescriptor  # type: ignore[attr-defined]
        if not hasattr(pyarrow.Table, "schema"):
            pyarrow.Table.schema = "stub-schema"  # type: ignore[attr-defined]
        return True
    except ImportError:
        np_mod = types.ModuleType("numpy")
        np_mod.ndarray = list  # type: ignore[attr-defined]
        np_mod.float32 = float  # type: ignore[attr-defined]
        np_mod.float64 = float  # type: ignore[attr-defined]
        np_mod.complex64 = complex  # type: ignore[attr-defined]
        np_mod.complexfloating = ()  # type: ignore[attr-defined]
        np_mod.issubdtype = lambda *a: False  # type: ignore[attr-defined]
        sys.modules.setdefault("numpy", np_mod)

        pa_mod = types.ModuleType("pyarrow")
        flight_mod = types.ModuleType("pyarrow.flight")

        class _Ticket:
            def __init__(self, data: bytes):
                self.ticket = data

        class _Action:
            def __init__(self, typ: str, body: bytes):
                self.type = typ
                self.body = body

        class _FlightDescriptor:
            @staticmethod
            def for_path(path: str):
                return ("path", path)

        class _Table:
            def __init__(self, rows: List[Dict[str, Any]]):
                self._rows = rows
                self.schema = "stub-schema"

            def to_pandas(self):
                return self._rows

            def to_pylist(self):
                return list(self._rows)

            @classmethod
            def from_pandas(cls, df):
                return cls(list(df))

        pa_mod.Table = _Table  # type: ignore[attr-defined]
        pa_mod.Schema = object  # type: ignore[attr-defined]
        pa_mod.schema = lambda *a, **kw: None  # type: ignore[attr-defined]
        pa_mod.field = lambda *a, **kw: None  # type: ignore[attr-defined]
        pa_mod.list_ = lambda *a, **kw: None  # type: ignore[attr-defined]
        pa_mod.float32 = lambda: None  # type: ignore[attr-defined]
        pa_mod.int64 = lambda: None  # type: ignore[attr-defined]
        pa_mod.string = lambda: None  # type: ignore[attr-defined]
        flight_mod.Ticket = _Ticket  # type: ignore[attr-defined]
        flight_mod.Action = _Action  # type: ignore[attr-defined]
        flight_mod.FlightDescriptor = _FlightDescriptor  # type: ignore[attr-defined]
        flight_mod.FlightCallOptions = lambda **kw: kw  # type: ignore[attr-defined]
        pa_mod.flight = flight_mod  # type: ignore[attr-defined]

        pd_mod = types.ModuleType("pandas")
        pd_mod.DataFrame = lambda rows: rows  # type: ignore[attr-defined]

        pydantic_mod = types.ModuleType("pydantic")

        class _BaseModel:
            pass

        pydantic_mod.BaseModel = _BaseModel  # type: ignore[attr-defined]
        pydantic_mod.Field = lambda **kw: None  # type: ignore[attr-defined]

        sys.modules.setdefault("pyarrow", pa_mod)
        sys.modules.setdefault("pyarrow.flight", flight_mod)
        sys.modules.setdefault("pandas", pd_mod)
        sys.modules.setdefault("pydantic", pydantic_mod)
        return False


_HAS_NATIVE_DEPS = _ensure_optional_test_deps()

import pyarrow as pa  # noqa: E402

from longbow import (  # noqa: E402
    LongbowAuthenticationError,
    LongbowAuthorizationError,
    LongbowClient,
    LongbowNotFoundError,
    LongbowPermissionDeniedError,
    LongbowQueryError,
)


def _make_mock_client(
    api_key: str | None = "lb_admin_key",
    oidc_token: str | None = None,
    namespace: str | None = None,
    action_payload: Dict[str, Any] | None = None,
):
    client = LongbowClient(
        uri="grpc://localhost:3000",
        api_key=api_key,
        oidc_token=oidc_token,
        namespace=namespace,
    )
    data_client = MagicMock()
    meta_client = MagicMock()

    action_res = MagicMock()
    action_res.body.to_pybytes.return_value = json.dumps(
        action_payload if action_payload is not None else {"status": "ok"}
    ).encode("utf-8")
    meta_client.do_action.return_value = [action_res]

    client._data_client = data_client
    client._meta_client = meta_client
    return client, data_client, meta_client


class _FakeHTTPResponse:
    def __init__(self, payload: Dict[str, Any]):
        self._body = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TestLongbowClientRBACAndOIDC(unittest.TestCase):
    def test_init_oidc_token_and_namespace_headers(self):
        client, _, _ = _make_mock_client(
            api_key=None,
            oidc_token="eyJ.oidc.jwt",
            namespace="team-a",
        )
        self.assertEqual(client.api_key, "eyJ.oidc.jwt")
        self.assertEqual(client.namespace, "team-a")

        opts = client._get_call_options()
        self.assertIsNotNone(opts)
        headers = opts["headers"] if isinstance(opts, dict) else opts.headers
        self.assertIn((b"authorization", b"Bearer eyJ.oidc.jwt"), headers)
        self.assertIn((b"x-namespace", b"team-a"), headers)

    def test_create_api_key_supports_all_roles_and_row_filters(self):
        client, _, meta_client = _make_mock_client(
            action_payload={"api_key": "lb_new", "role": "ingest-only"}
        )
        res = client.create_api_key(
            name="etl-worker",
            role="ingest-only",
            tenant_id="acme",
            namespace="acme",
            datasets=["acme/docs"],
            row_filters={"env": "prod"},
            ttl_seconds=3600,
        )
        self.assertEqual(res["api_key"], "lb_new")
        action = meta_client.do_action.call_args[0][0]
        self.assertEqual(action.type, "CreateAPIKey")
        body = json.loads(action.body.decode("utf-8"))
        self.assertEqual(body["role"], "ingest-only")
        self.assertEqual(body["tenant_id"], "acme")
        self.assertEqual(body["namespace"], "acme")
        self.assertEqual(body["datasets"], ["acme/docs"])
        self.assertEqual(body["row_filters"], {"env": "prod"})
        self.assertEqual(body["ttl_seconds"], 3600)

    def test_rbac_lifecycle_actions(self):
        client, _, meta_client = _make_mock_client(action_payload={"ok": True})
        client.rotate_api_key("lb_old", grace_period_seconds=120)
        self.assertEqual(meta_client.do_action.call_args[0][0].type, "RotateAPIKey")

        client.revoke_api_key("lb_old")
        self.assertEqual(meta_client.do_action.call_args[0][0].type, "RevokeAPIKey")

        client.list_api_keys()
        self.assertEqual(meta_client.do_action.call_args[0][0].type, "ListAPIKeys")

        client.set_rls_policy(
            name="prod-only",
            namespace="acme",
            dataset="acme/docs",
            rules=[{"field": "env", "value": "prod"}],
        )
        rls_action = meta_client.do_action.call_args[0][0]
        self.assertEqual(rls_action.type, "SetRLSPolicy")
        self.assertEqual(json.loads(rls_action.body.decode("utf-8"))["dataset"], "acme/docs")

        client.get_audit_log(limit=25)
        self.assertEqual(meta_client.do_action.call_args[0][0].type, "GetAuditLog")

    def test_oidc_provider_management_flight_helpers(self):
        client, _, meta_client = _make_mock_client(action_payload={"providers": []})

        client.list_oidc_providers()
        self.assertEqual(meta_client.do_action.call_args[0][0].type, "ListOIDCProviders")

        client.add_aws_oidc_provider(
            issuer_url="https://oidc.eks.us-west-2.amazonaws.com/id/EXAMPLED539D",
            audience="sts.amazonaws.com",
            trust_policy_arn="arn:aws:iam::123456789012:role/longbow-writer",
            tenant_claim="custom_tenant",
        )
        aws_action = meta_client.do_action.call_args[0][0]
        self.assertEqual(aws_action.type, "AddOIDCProvider")
        aws_body = json.loads(aws_action.body.decode("utf-8"))
        self.assertEqual(aws_body["provider"], "aws")
        self.assertEqual(aws_body["audience"], "sts.amazonaws.com")
        self.assertEqual(aws_body["trust_policy_arn"], "arn:aws:iam::123456789012:role/longbow-writer")
        self.assertEqual(aws_body["tenant_claim"], "custom_tenant")

        client.add_google_oidc_provider(
            audience="123456.apps.googleusercontent.com",
            namespace_claim="gcp_ns",
        )
        gcp_action = meta_client.do_action.call_args[0][0]
        gcp_body = json.loads(gcp_action.body.decode("utf-8"))
        self.assertEqual(gcp_body["provider"], "google")
        self.assertEqual(gcp_body["issuer_url"], "https://accounts.google.com")
        self.assertEqual(gcp_body["jwks_url"], "https://www.googleapis.com/oauth2/v3/certs")
        self.assertEqual(gcp_body["namespace_claim"], "gcp_ns")

        client.remove_oidc_provider("https://accounts.google.com")
        rm_action = meta_client.do_action.call_args[0][0]
        self.assertEqual(rm_action.type, "RemoveOIDCProvider")
        self.assertEqual(json.loads(rm_action.body.decode("utf-8"))["issuer_url"], "https://accounts.google.com")

    def test_exchange_oidc_token_flight_uses_bearer_override_and_updates_key(self):
        client, _, meta_client = _make_mock_client(
            api_key="lb_existing",
            action_payload={"token": "lb_session_999", "role": "writer"},
        )
        res = client.exchange_oidc_token("eyJ.provider.jwt", ttl_seconds=900)
        self.assertEqual(res["token"], "lb_session_999")
        self.assertEqual(client.api_key, "lb_session_999")

        action, opts = meta_client.do_action.call_args[0][0], meta_client.do_action.call_args[1]["options"]
        self.assertEqual(action.type, "ExchangeOIDCToken")
        headers = opts["headers"] if isinstance(opts, dict) else opts.headers
        self.assertIn((b"authorization", b"Bearer eyJ.provider.jwt"), headers)

    def test_exchange_oidc_token_empty_raises_authentication_error(self):
        client, _, _ = _make_mock_client(api_key=None)
        with self.assertRaises(LongbowAuthenticationError) as ctx:
            client.exchange_oidc_token("   ")
        self.assertEqual(ctx.exception.status_code, 401)
        self.assertEqual(ctx.exception.code, "UNAUTHENTICATED")
        self.assertEqual(ctx.exception.operation, "ExchangeOIDCToken")

    def test_exchange_oidc_token_rest_and_wrappers(self):
        client, _, _ = _make_mock_client(api_key=None, namespace="team-b")
        with (
            patch.object(LongbowClient, "acquire_google_id_token", return_value="eyJ.google.jwt"),
            patch("urllib.request.urlopen", return_value=_FakeHTTPResponse({"token": "lb_rest_sess"})) as mock_open,
        ):
            res = client.authenticate_with_google_oidc(
                audience="longbow",
                ttl_seconds=600,
                http_endpoint="http://127.0.0.1:6000",
            )
            self.assertEqual(res["token"], "lb_rest_sess")
            self.assertEqual(client.api_key, "lb_rest_sess")
            req = mock_open.call_args[0][0]
            self.assertTrue(req.full_url.endswith("/api/v1/oidc/token"))
            self.assertEqual(req.get_header("X-namespace"), "team-b")

        with (
            patch.object(LongbowClient, "acquire_aws_web_identity_token", return_value="eyJ.aws.jwt"),
            patch("urllib.request.urlopen", return_value=_FakeHTTPResponse({"token": "lb_aws_sess"})),
        ):
            res_aws = client.authenticate_with_aws_oidc(ttl_seconds=600, http_endpoint="http://127.0.0.1:6000")
            self.assertEqual(res_aws["token"], "lb_aws_sess")
            self.assertEqual(client.api_key, "lb_aws_sess")

        with patch("urllib.request.urlopen", return_value=_FakeHTTPResponse({"providers": [{"provider": "aws"}]})):
            self.assertEqual(client.rest_list_oidc_providers(), [{"provider": "aws"}])
            self.assertEqual(
                client.rest_add_oidc_provider(
                    {
                        "provider": "aws",
                        "issuer_url": "https://oidc.eks.us-east-1.amazonaws.com/id/ABC",
                        "audience": "sts.amazonaws.com",
                    }
                ),
                {"providers": [{"provider": "aws"}]},
            )
            self.assertEqual(
                client.rest_remove_oidc_provider("https://oidc.eks.us-east-1.amazonaws.com/id/ABC"),
                {"providers": [{"provider": "aws"}]},
            )

    def test_flight_error_mapping_unauthenticated_and_permission_denied(self):
        client, data_client, meta_client = _make_mock_client()

        # 1. Unauthenticated / expired token on DoAction
        meta_client.do_action.side_effect = RuntimeError("FlightUnauthenticatedError: token replay detected")
        with self.assertRaises(LongbowAuthenticationError) as unauth_ctx:
            client.list_api_keys()
        self.assertNotIsInstance(unauth_ctx.exception, LongbowAuthorizationError)
        self.assertEqual(unauth_ctx.exception.status_code, 401)
        self.assertEqual(unauth_ctx.exception.code, "UNAUTHENTICATED")
        self.assertIn("oidc jwt", unauth_ctx.exception.hint.lower())

        # 2. PermissionDenied on DoGet
        data_client.do_get.side_effect = RuntimeError("gRPC PermissionDenied: dataset access denied for namespace team-b")
        with self.assertRaises(LongbowAuthorizationError) as authz_ctx:
            client._do_get_df({"search": {"dataset": "team-b/docs", "k": 5}}, "search")
        self.assertIs(LongbowPermissionDeniedError, LongbowAuthorizationError)
        self.assertIsInstance(authz_ctx.exception, LongbowAuthenticationError)
        self.assertEqual(authz_ctx.exception.status_code, 403)
        self.assertEqual(authz_ctx.exception.code, "PERMISSION_DENIED")
        self.assertIn("rbac role", authz_ctx.exception.hint.lower())

        # 3. PermissionDenied on DoPut (_upload_batch)
        data_client.do_put.side_effect = RuntimeError("FlightUnauthorizedError: forbidden: insufficient role for ingest")
        table = pa.Table([{"id": 1}])
        with self.assertRaises(LongbowAuthorizationError) as put_ctx:
            client._upload_batch("team-b/docs", table)
        self.assertEqual(put_ctx.exception.status_code, 403)
        self.assertEqual(put_ctx.exception.operation, "DoPut(team-b/docs)")

    def test_rest_error_mapping_401_403_404_500(self):
        client, _, _ = _make_mock_client(api_key="lb_bad")

        def _http_err(code: int, body: str) -> urllib.error.HTTPError:
            return urllib.error.HTTPError(
                url="http://localhost:3000/api/v1/rbac/keys",
                code=code,
                msg="err",
                hdrs=None,  # type: ignore[arg-type]
                fp=io.BytesIO(body.encode("utf-8")),
            )

        with patch("urllib.request.urlopen", side_effect=_http_err(401, '{"error":"invalid or expired api key"}')):
            with self.assertRaises(LongbowAuthenticationError) as ctx401:
                client.rest_list_oidc_providers()
            self.assertNotIsInstance(ctx401.exception, LongbowAuthorizationError)
            self.assertEqual(ctx401.exception.status_code, 401)
            self.assertIn("invalid or expired api key", str(ctx401.exception))

        with patch("urllib.request.urlopen", side_effect=_http_err(403, '{"error":"forbidden: admin required"}')):
            with self.assertRaises(LongbowAuthorizationError) as ctx403:
                client.rest_list_oidc_providers()
            self.assertEqual(ctx403.exception.status_code, 403)
            self.assertIn("admin required", str(ctx403.exception))

        with patch("urllib.request.urlopen", side_effect=_http_err(404, '{"error":"dataset not found"}')):
            with self.assertRaises(LongbowNotFoundError):
                client.rest_list_oidc_providers()

        with patch("urllib.request.urlopen", side_effect=_http_err(500, '{"error":"internal failure"}')):
            with self.assertRaises(LongbowQueryError):
                client.rest_list_oidc_providers()


if __name__ == "__main__":
    unittest.main()
