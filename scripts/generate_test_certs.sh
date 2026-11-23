#!/usr/bin/env bash
# Generate a short-lived CA, server certificate, and client certificate for
# bounded TLS/mTLS soak variants. Never use these certificates in production.
set -euo pipefail

OUT_DIR=${1:?usage: generate_test_certs.sh <output-dir>}
mkdir -p "$OUT_DIR"

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout "$OUT_DIR/ca.key" -out "$OUT_DIR/ca.crt" -days 1 \
  -subj "/CN=longbow-soak-ca" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" >/dev/null 2>&1

openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout "$OUT_DIR/server.key" -out "$OUT_DIR/server.csr" \
  -subj "/CN=localhost" >/dev/null 2>&1
openssl x509 -req -in "$OUT_DIR/server.csr" \
  -CA "$OUT_DIR/ca.crt" -CAkey "$OUT_DIR/ca.key" -CAcreateserial \
  -out "$OUT_DIR/server.crt" -days 1 \
  -extfile <(printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\nkeyUsage=digitalSignature,keyEncipherment\n') \
  >/dev/null 2>&1

openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout "$OUT_DIR/client.key" -out "$OUT_DIR/client.csr" \
  -subj "/CN=longbow-soak-client" >/dev/null 2>&1
openssl x509 -req -in "$OUT_DIR/client.csr" \
  -CA "$OUT_DIR/ca.crt" -CAkey "$OUT_DIR/ca.key" -CAcreateserial \
  -out "$OUT_DIR/client.crt" -days 1 \
  -extfile <(printf 'extendedKeyUsage=clientAuth\nkeyUsage=digitalSignature,keyEncipherment\n') \
  >/dev/null 2>&1

chmod 600 "$OUT_DIR"/*.key "$OUT_DIR"/*.csr "$OUT_DIR/ca.srl"
