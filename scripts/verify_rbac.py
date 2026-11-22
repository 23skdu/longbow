#!/usr/bin/env python3
"""RBAC-only verification for a running Longbow server.

This script exercises authorization behavior and nothing else. It verifies:

* protected REST endpoints reject unauthenticated requests;
* a bootstrap/admin key can create scoped API keys and namespaces;
* namespace reader/writer/admin grants are scoped to that namespace;
* namespace-scoped admins cannot reach global key or tenant administration;
* public namespace permissions work for any authenticated key but never for
  anonymous requests;
* non-admins cannot create namespaces; and
* revoked keys stop working immediately.

Usage:
    LONGBOW_RBAC_BOOTSTRAP_KEY=... python3 scripts/verify_rbac.py \
        --url http://127.0.0.1:9090
"""

import argparse
import json
import os
import random
import string
import sys
import time
import urllib.error
import urllib.request

ALLOWED_DENIED = (401, 403)


class HTTPTransport:
    """Minimal urllib-based transport for the verification requests."""

    def __init__(self, base_url, timeout=10):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def request(self, method, path, api_key=None, namespace=None, body=None):
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if api_key:
            headers["X-API-Key"] = api_key
        if namespace:
            headers["X-Tenant-ID"] = namespace
        req = urllib.request.Request(
            self.base_url + path,
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read().decode("utf-8", "replace")
                return resp.status, _decode_json(raw)
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", "replace")
            return exc.code, _decode_json(raw)


def _decode_json(raw):
    if not raw:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return {"body": raw}


def expect_status(label, status, allowed):
    if status not in allowed:
        raise AssertionError(
            f"{label}: expected HTTP {allowed}, got HTTP {status}"
        )


class Verifier:
    """Runs the RBAC-only verification sequence against a transport."""

    def __init__(self, transport, admin_key, suffix=None):
        self.transport = transport
        self.admin_key = admin_key
        self.suffix = suffix or self._new_suffix()
        self.keys = {}
        self.home_ns = f"rbac_verify_home_{self.suffix}"
        self.acl_ns = f"rbac_verify_acl_{self.suffix}"
        self.public_ns = f"rbac_verify_public_{self.suffix}"
        self.private_ns = f"rbac_verify_private_{self.suffix}"
        self.checks = []

    @staticmethod
    def _new_suffix():
        alphabet = string.ascii_lowercase + string.digits
        tail = "".join(random.SystemRandom().choice(alphabet) for _ in range(8))
        return f"{int(time.time())}_{tail}"

    def _check(self, label, status, allowed, namespace=None):
        expect_status(label, status, allowed)
        self.checks.append((label, status, namespace))

    def _call(self, method, path, key_label=None, namespace=None, body=None):
        api_key = self.admin_key if key_label == "admin" else self.keys.get(key_label)
        return self.transport.request(
            method, path, api_key=api_key, namespace=namespace, body=body
        )

    def _create_key(self, label, role, namespace):
        status, payload = self._call(
            "POST",
            "/api/v1/rbac/keys",
            key_label="admin",
            body={
                "name": f"rbac_verify_{label}_{self.suffix}",
                "role": role,
                "tenant_id": namespace,
                "namespace": namespace,
            },
        )
        self._check(f"create {label} key", status, (201,))
        if not payload or not payload.get("api_key") or not payload.get("key_id"):
            raise AssertionError(f"create {label} key: missing api_key/key_id in response")
        self.keys[label] = payload["api_key"]
        self.keys[label + "_id"] = payload["key_id"]

    def _create_namespace(self, name, **policy):
        body = {"name": name}
        body.update(policy)
        status, _ = self._call(
            "POST", "/api/v1/namespaces", key_label="admin", body=body
        )
        self._check(f"admin creates namespace {name}", status, (201,))

    def _create_dataset(self, dataset, key_label="admin"):
        status, _ = self._call(
            "POST",
            "/api/v1/datasets",
            key_label=key_label,
            body={"name": dataset},
        )
        self._check(f"{key_label} creates dataset {dataset}", status, (201,))

    def _read_dataset(self, dataset, key_label):
        status, _ = self._call(
            "GET",
            "/api/v1/datasets/" + dataset,
            key_label=key_label,
        )
        self._check(f"{key_label} reads dataset {dataset}", status, (200,))

    def run(self):
        # 1. RBAC must reject unauthenticated access to a protected endpoint.
        status, _ = self._call("GET", "/api/v1/rbac/keys")
        self._check("unauthenticated request is rejected", status, ALLOWED_DENIED)

        # 2. The bootstrap key must be a real server admin.
        status, _ = self._call("GET", "/api/v1/rbac/keys", key_label="admin")
        self._check("admin lists API keys", status, (200,))

        # 3. Create namespaced keys and the namespaces that grant them access.
        self._create_namespace(self.home_ns)
        self._create_key("reader", "reader", self.home_ns)
        self._create_key("writer", "writer", self.home_ns)
        self._create_key("outsider", "reader", self.home_ns)
        self._create_key("scoped_admin", "admin", self.home_ns)

        self._create_namespace(
            self.acl_ns,
            admin_key_ids=[self.keys["scoped_admin_id"]],
            reader_key_ids=[self.keys["reader_id"]],
            writer_key_ids=[self.keys["writer_id"]],
        )
        self._create_namespace(self.public_ns, public_permissions=["read"])
        self._create_namespace(self.private_ns)

        # 4. Namespace ACL grants override the key's home namespace.
        self._create_dataset(f"{self.acl_ns}/probe", key_label="writer")
        self._read_dataset(f"{self.acl_ns}/probe", "reader")
        self._create_dataset(f"{self.acl_ns}/admin_probe", key_label="scoped_admin")
        self._read_dataset(f"{self.acl_ns}/admin_probe", "scoped_admin")

        status, _ = self._call(
            "POST",
            "/api/v1/datasets",
            key_label="reader",
            body={"name": f"{self.acl_ns}/denied"},
        )
        self._check("namespace reader cannot write", status, ALLOWED_DENIED)

        status, _ = self._call(
            "POST",
            "/api/v1/datasets",
            key_label="writer",
            body={"name": f"{self.private_ns}/denied"},
        )
        self._check("writer cannot enter a private namespace", status, ALLOWED_DENIED)

        # A namespace ACL admin is a data administrator only. Global key and
        # tenant administration must still require an unscoped admin key.
        status, _ = self._call(
            "GET",
            "/api/v1/rbac/keys",
            key_label="scoped_admin",
            namespace=self.acl_ns,
        )
        self._check(
            "namespace-scoped admin cannot list API keys", status, ALLOWED_DENIED
        )

        status, _ = self._call(
            "POST",
            "/api/v1/namespaces",
            key_label="scoped_admin",
            namespace=self.acl_ns,
            body={"name": f"rbac_verify_scoped_escalation_{self.suffix}"},
        )
        self._check(
            "namespace-scoped admin cannot create a namespace", status, ALLOWED_DENIED
        )

        # 5. Public permissions apply to any authenticated key, but only there.
        self._create_dataset(f"{self.public_ns}/probe")
        self._create_dataset(f"{self.private_ns}/probe")
        status, _ = self._call(
            "GET", "/api/v1/datasets/" + f"{self.public_ns}/probe"
        )
        self._check(
            "public namespace still requires authentication", status, ALLOWED_DENIED
        )
        self._read_dataset(f"{self.public_ns}/probe", "outsider")

        status, _ = self._call(
            "GET", "/api/v1/datasets/" + f"{self.private_ns}/probe", key_label="outsider"
        )
        self._check("outsider cannot read a private namespace", status, ALLOWED_DENIED)

        # 6. Namespace creation is a server-admin-only operation.
        status, _ = self._call(
            "POST",
            "/api/v1/namespaces",
            key_label="writer",
            body={"name": f"rbac_verify_escalation_{self.suffix}"},
        )
        self._check("non-admin cannot create a namespace", status, ALLOWED_DENIED)

        # 7. Revocation takes effect immediately.
        status, _ = self._call(
            "DELETE",
            "/api/v1/rbac/keys?key_id=" + self.keys["outsider_id"],
            key_label="admin",
        )
        self._check("admin revokes a key", status, (200,))
        status, _ = self._call(
            "GET",
            "/api/v1/datasets/" + f"{self.public_ns}/probe",
            key_label="outsider",
        )
        self._check("revoked key is rejected", status, (401,))

        return self.checks


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url",
        default=os.environ.get("LONGBOW_RBAC_VERIFY_URL", "http://127.0.0.1:9090"),
        help="HTTP REST/metrics address of the running Longbow server",
    )
    parser.add_argument(
        "--admin-key",
        default=os.environ.get("LONGBOW_RBAC_BOOTSTRAP_KEY", ""),
        help="Bootstrap admin API key (default: LONGBOW_RBAC_BOOTSTRAP_KEY)",
    )
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout in seconds")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if not args.admin_key:
        print("FAIL: set LONGBOW_RBAC_BOOTSTRAP_KEY or pass --admin-key", file=sys.stderr)
        return 2
    verifier = Verifier(HTTPTransport(args.url, args.timeout), args.admin_key)
    try:
        checks = verifier.run()
    except Exception as exc:  # noqa: BLE001 - report any verification failure cleanly
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    for label, status, _ in checks:
        print(f"PASS {label} (HTTP {status})")
    print(f"RBAC verification passed: {len(checks)} checks")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
