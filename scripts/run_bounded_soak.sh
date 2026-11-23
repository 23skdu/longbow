#!/usr/bin/env bash
# Start a disposable, RBAC-enforced Longbow server, run one bounded AgentSoak
# variant, sample server RSS, and validate the report against soak gates.
#
# RBAC is always enforced: the server gets a bootstrap admin key and the soak
# client provisions scoped keys. There is no unauthenticated soak mode.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: run_bounded_soak.sh [options] [extra agentsoak flags]

  --duration DURATION        Go duration, e.g. 10m (default: 10m)
  --agents N                 Agent count (default: 3)
  --agents-per-namespace N   Agents per shared namespace; 0 uses private-per-agent (default: 0)
  --report PATH              JSON report output (required)
  --workload SPEC            Optional op=weight,... override
  --federated-only           Run only the federated operation
  --help                     Show this message

Optional environment: LONGBOW_BIN, AGENTSOAK_BIN, LONGBOW_RBAC_BOOTSTRAP_KEY,
LONGBOW_LISTEN_ADDR, LONGBOW_META_ADDR, LONGBOW_METRICS_ADDR, LONGBOW_MAX_MEMORY
(default: 4 GiB), SOAK_SERVER_ENV (a bash snippet of extra server env
assignments for secure variants).
EOF
}

DURATION="10m"
AGENTS="3"
PER_NAMESPACE="0"
REPORT=""
WORKLOAD=""
FEDERATED_ONLY=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --duration) DURATION="$2"; shift 2 ;;
    --agents) AGENTS="$2"; shift 2 ;;
    --agents-per-namespace) PER_NAMESPACE="$2"; shift 2 ;;
    --report) REPORT="$2"; shift 2 ;;
    --workload) WORKLOAD="$2"; shift 2 ;;
    --federated-only) FEDERATED_ONLY="-federated-only"; shift ;;
    --help) usage; exit 0 ;;
    --) shift; break ;;
    *) break ;;
  esac
done
if [[ -z "$REPORT" ]]; then
  usage >&2
  echo "--report is required" >&2
  exit 2
fi

LONGBOW_BIN=${LONGBOW_BIN:-bin/longbow}
AGENTSOAK_BIN=${AGENTSOAK_BIN:-bin/agentsoak}
ADMIN_KEY=${LONGBOW_RBAC_BOOTSTRAP_KEY:-ci-soak-admin-key-0123456789}
LISTEN_ADDR=${LONGBOW_LISTEN_ADDR:-127.0.0.1:31000}
META_ADDR=${LONGBOW_META_ADDR:-127.0.0.1:31001}
METRICS_ADDR=${LONGBOW_METRICS_ADDR:-127.0.0.1:31002}
CONFIG=${SOAK_CONFIG:-scripts/configs/multiagent_soak.json}

for binary in "$LONGBOW_BIN" "$AGENTSOAK_BIN"; do
  if [[ ! -x "$binary" ]]; then
    echo "missing executable $binary; run 'make build && make build-agentsoak' first" >&2
    exit 2
  fi
done

DATA_DIR=$(mktemp -d "${TMPDIR:-/tmp}/longbow-soak.XXXXXX")
RSS_FILE="${REPORT%.json}.rss.jsonl"
SERVER_LOG="${REPORT%.json}.server.log"
SERVER_PID=""
SAMPLER_PID=""
STOP_TIMEOUT_S=30

# stop_process sends SIGTERM, waits for a bounded shutdown, and force-stops a
# child that ignores it. A hung soak server must fail, never stall CI forever.
stop_process() {
  local pid="$1"
  [[ -n "$pid" ]] || return 0
  if ! kill -0 "$pid" 2>/dev/null; then
    wait "$pid" 2>/dev/null || true
    return 0
  fi
  kill "$pid" 2>/dev/null || true
  local i state
  for ((i = 0; i < STOP_TIMEOUT_S; i++)); do
    state=$(ps -o stat= -p "$pid" 2>/dev/null | tr -d ' ' || true)
    if [[ -z "$state" || "$state" == Z* ]]; then
      wait "$pid" 2>/dev/null || true
      return 0
    fi
    sleep 1
  done
  kill -KILL "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  return 1
}

cleanup() {
  stop_process "$SAMPLER_PID" || true
  if ! stop_process "$SERVER_PID"; then
    echo "soak server did not shut down within ${STOP_TIMEOUT_S}s; force-stopped" >&2
  fi
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT

mkdir -p "$(dirname "$REPORT")"
: >"$RSS_FILE"
: >"$SERVER_LOG"

# SOAK_SERVER_ENV lets secure variants set TLS/OIDC/encryption variables
# without weakening the common startup path. Use plain VAR=value assignments.
if [[ -n "${SOAK_SERVER_ENV:-}" ]]; then
  # shellcheck disable=SC2086
  eval "export ${SOAK_SERVER_ENV}"
fi
LONGBOW_LISTEN_ADDR="$LISTEN_ADDR" \
LONGBOW_META_ADDR="$META_ADDR" \
LONGBOW_METRICS_ADDR="$METRICS_ADDR" \
LONGBOW_DATA_PATH="$DATA_DIR" \
LONGBOW_MAX_MEMORY=${LONGBOW_MAX_MEMORY:-4294967296} \
LONGBOW_TESTING=true \
LONGBOW_TEMPORAL_ENABLED=true \
LONGBOW_TEMPORAL_VERSION_HISTORY=true \
LONGBOW_SECRETS_PROVIDER=file \
LONGBOW_SECRETS_PATH="$DATA_DIR/rbac_secrets" \
LONGBOW_RBAC_BOOTSTRAP_KEY="$ADMIN_KEY" \
  "$LONGBOW_BIN" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

for _ in $(seq 1 90); do
  if curl -fsS "http://$METRICS_ADDR/ready" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    cat "$SERVER_LOG" >&2
    exit 1
  fi
  sleep 1
done
if ! curl -fsS "http://$METRICS_ADDR/ready" >/dev/null 2>&1; then
  echo "server did not become ready" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

(
  start=$SECONDS
  while kill -0 "$SERVER_PID" 2>/dev/null; do
    rss=$(ps -o rss= -p "$SERVER_PID" 2>/dev/null | tr -d ' ' || true)
    if [[ -n "$rss" ]]; then
      echo "{\"t_s\":$((SECONDS - start)),\"rss_bytes\":$((rss * 1024))}"
    fi
    sleep 5
  done
) >"$RSS_FILE" &
SAMPLER_PID=$!

soak_args=(
  -config "$CONFIG"
  -uri "grpc://$LISTEN_ADDR"
  -agents "$AGENTS"
  -agents-per-namespace "$PER_NAMESPACE"
  -duration "$DURATION"
  -json "$REPORT"
  -admin-api-key "$ADMIN_KEY"
)
if [[ -n "$WORKLOAD" ]]; then
  soak_args+=(-workload "$WORKLOAD")
fi
if [[ -n "$FEDERATED_ONLY" ]]; then
  soak_args+=("$FEDERATED_ONLY")
fi

status=0
if ! "$AGENTSOAK_BIN" "${soak_args[@]}" "$@"; then
  status=1
fi

stop_process "$SAMPLER_PID" || true
SAMPLER_PID=""

if ! stop_process "$SERVER_PID"; then
  echo "soak server did not shut down within ${STOP_TIMEOUT_S}s; force-stopped" >&2
  status=1
fi
SERVER_PID=""

if ! python3 scripts/check_soak_report.py "$REPORT" --rss-samples "$RSS_FILE"; then
  status=1
fi
if [[ "$status" -ne 0 ]]; then
  echo "--- server log tail ---" >&2
  tail -40 "$SERVER_LOG" >&2
fi
exit "$status"
