"""Unit tests for the RBAC-only verification script.

The fake transport implements just enough authorization behavior to confirm
the script checks the intended boundaries; these tests do not start Longbow.
"""

import importlib.util
import os
import sys
import unittest
from urllib.parse import parse_qs, urlsplit

_HERE = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS = os.path.dirname(_HERE)
_MODULE = os.path.join(_SCRIPTS, "verify_rbac.py")


def _load():
    spec = importlib.util.spec_from_file_location("verify_rbac", _MODULE)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {_MODULE}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["verify_rbac"] = module
    spec.loader.exec_module(module)
    return module


vr = _load()

ROLE_PERMS = {
    "admin": {"read", "write", "delete", "ingest", "manage-keys", "manage-tenant"},
    "writer": {"read", "write", "delete", "ingest"},
    "reader": {"read"},
}


class FakeTransport:
    """A small RBAC model used to validate the verification script itself."""

    def __init__(self, allow_unauthenticated=False):
        self.allow_unauthenticated = allow_unauthenticated
        self.keys = {}
        self.key_ids = {}
        self.namespaces = {}
        self.datasets = set()
        self.calls = []
        self.counter = 0
        self.keys["bootstrap-admin"] = {
            "raw": "bootstrap-admin",
            "key_id": "bootstrap-admin",
            "role": "admin",
            "namespace": "",
            "revoked": False,
        }
        self.key_ids["bootstrap-admin"] = "bootstrap-admin"

    def request(self, method, path, api_key=None, namespace=None, body=None):
        self.calls.append((method, path, api_key, namespace, body))
        url = urlsplit(path)
        route = url.path
        query = parse_qs(url.query)

        key = self.keys.get(api_key) if api_key else None
        if key is None and not (self.allow_unauthenticated and method == "GET" and route == "/api/v1/rbac/keys"):
            return 401, {"error": "unauthorized"}
        if key and key.get("revoked"):
            return 401, {"error": "revoked"}

        if route == "/api/v1/rbac/keys":
            if method == "GET":
                if not self._has_permission(key, "", "manage-keys"):
                    return 403, {"error": "forbidden"}
                return 200, {"keys": list(self.keys.values())}
            if method == "POST":
                if not self._has_permission(key, "", "manage-keys"):
                    return 403, {"error": "forbidden"}
                self.counter += 1
                raw = f"key-{self.counter}"
                key_id = f"key-id-{self.counter}"
                role = body["role"]
                self.keys[raw] = {
                    "raw": raw,
                    "key_id": key_id,
                    "role": role,
                    "namespace": body.get("namespace", ""),
                    "revoked": False,
                }
                self.key_ids[key_id] = raw
                return 201, {"api_key": raw, "key_id": key_id}
            if method == "DELETE":
                if not self._has_permission(key, "", "manage-keys"):
                    return 403, {"error": "forbidden"}
                key_id = query.get("key_id", [""])[0]
                raw = self.key_ids.get(key_id)
                if raw is None:
                    return 404, {"error": "not found"}
                self.keys[raw]["revoked"] = True
                return 200, {"status": "revoked"}

        if route == "/api/v1/namespaces" and method == "POST":
            if not self._has_permission(key, "", "manage-tenant"):
                return 403, {"error": "forbidden"}
            self.namespaces[body["name"]] = {
                "reader_key_ids": set(body.get("reader_key_ids", [])),
                "writer_key_ids": set(body.get("writer_key_ids", [])),
                "admin_key_ids": {"bootstrap-admin"},
                "public_permissions": set(body.get("public_permissions", [])),
            }
            return 201, {"name": body["name"], "status": "created"}

        if route == "/api/v1/datasets":
            if method == "POST":
                name = body["name"]
                ns = name.split("/", 1)[0] if "/" in name else ""
                if not self._has_permission(key, ns, "write"):
                    return 403, {"error": "forbidden"}
                self.datasets.add(name)
                return 201, {"name": name, "status": "created"}

        if route.startswith("/api/v1/datasets/"):
            name = route[len("/api/v1/datasets/"):]
            ns = name.split("/", 1)[0] if "/" in name else ""
            if method == "GET":
                if name not in self.datasets:
                    return 404, {"error": "not found"}
                if not self._has_permission(key, ns, "read"):
                    return 403, {"error": "forbidden"}
                return 200, {"name": name}

        return 405, {"error": "method not allowed"}

    def _has_permission(self, key, namespace, perm):
        if key is None:
            return self.allow_unauthenticated
        if key["role"] == "admin":
            return True
        policy = self.namespaces.get(namespace)
        if policy:
            key_id = key["key_id"]
            if key_id in policy["admin_key_ids"]:
                return True
            if key_id in policy["writer_key_ids"] and perm in {"read", "write", "delete", "ingest"}:
                return True
            if key_id in policy["reader_key_ids"] and perm == "read":
                return True
            if perm in policy["public_permissions"]:
                return True
        if key["namespace"] and key["namespace"] != namespace:
            return False
        return perm in ROLE_PERMS.get(key["role"], set())


class VerifyRBACTests(unittest.TestCase):
    def test_expect_status(self):
        vr.expect_status("ok", 200, (200,))
        with self.assertRaisesRegex(AssertionError, "expected HTTP"):
            vr.expect_status("bad", 403, (200,))

    def test_parse_args_defaults(self):
        args = vr.parse_args(["--admin-key", "secret"])
        self.assertEqual(args.admin_key, "secret")
        self.assertEqual(args.timeout, 10.0)

    def test_verifier_passes_against_compliant_server(self):
        transport = FakeTransport()
        verifier = vr.Verifier(transport, "bootstrap-admin", suffix="unit")
        checks = verifier.run()

        self.assertGreaterEqual(len(checks), 15)
        labels = [label for label, _, _ in checks]
        self.assertIn("unauthenticated request is rejected", labels)
        self.assertIn("namespace reader cannot write", labels)
        self.assertIn("writer cannot enter a private namespace", labels)
        self.assertIn("outsider cannot read a private namespace", labels)
        self.assertIn("non-admin cannot create a namespace", labels)
        self.assertIn("revoked key is rejected", labels)
        self.assertEqual(len(transport.namespaces), 4)

    def test_verifier_fails_when_rbac_is_not_enforced(self):
        transport = FakeTransport(allow_unauthenticated=True)
        verifier = vr.Verifier(transport, "bootstrap-admin", suffix="unit")
        with self.assertRaisesRegex(AssertionError, "unauthenticated request is rejected"):
            verifier.run()


if __name__ == "__main__":
    unittest.main()
