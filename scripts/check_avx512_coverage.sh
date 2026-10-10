#!/usr/bin/env bash
# Run the AVX-512 / VBMI / AMX lanes of internal/simd under Intel Software
# Development Emulator and fail if any of them was skipped.
#
# Why this exists: docs/roadmap.md item 5. The AVX-512 parity tests
# (TestPackTQ*AVX512*, the FMA portable tests, TestPackTQ2AVX512VBMI) and the AMX
# tests all guard on GetCPUFeatures() and t.Skip on a GitHub Actions runner,
# which has no AVX-512. That is correct for a developer machine and useless for
# a gate: the widest kernels in the tree are exactly the ones CI never runs.
#
# SDE emulates the instruction set and answers CPUID for the emulated model, so
# the feature detection inside the test binary reports what the emulated CPU
# has and the guarded tests run for real.
#
# Usage:
#   scripts/check_avx512_coverage.sh              # download SDE if needed
#   LONGBOW_SDE=/path/to/sde64 scripts/check_avx512_coverage.sh
#
# Exits non-zero if a test that the emulated model claims to support was
# skipped, which is the only way a silent regression in feature detection - a
# wrong CPUID bit, a build tag, a renamed test - can be caught.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PKG="./internal/simd"
WORK="${LONGBOW_SDE_WORK:-$(mktemp -d)}"

SDE_VERSION="10.13.1-2026-07-28"
SDE_URL="https://downloadmirror.intel.com/924984/sde-external-${SDE_VERSION}-lin.tar.xz"
SDE_SHA256="94E97D623FEC54385686E1E7BA65EBC9941748C05EE451423948334892BF2B50"

# model : feature-label : test-name-include-pattern : test-name-exclude-pattern
#
# A skip is a failure only when the skipped test's name matches include and does
# not match exclude. The exclude arm is what makes a model without a feature
# stop reporting that feature's correctly-skipped tests as gaps: Skylake-X has
# AVX-512 but no VBMI, so TestPackTQ2AVX512VBMI skipping under -skx is expected
# and is only treated as a gap under -icx and -spr.
MODELS=(
  "skx:avx512:AVX512|AVX-512|FMA:VBMI"
  "icx:avx512+vbmi:AVX512|AVX-512|FMA|VBMI|PackTQ2:^$"
  "spr:avx512+vbmi+amx:AVX512|AVX-512|FMA|VBMI|PackTQ2|AMX:^$"
)

# log writes to stderr: resolve_sde's result is captured with $(...), so a log
# line on stdout would be captured along with the path.
log() { printf '==> %s\n' "$*" >&2; }

resolve_sde() {
  if [[ -n "${LONGBOW_SDE:-}" ]]; then
    echo "$LONGBOW_SDE"
    return
  fi
  if [[ -x "$WORK/sde/sde64" ]]; then
    echo "$WORK/sde/sde64"
    return
  fi

  local tarball="$WORK/sde.tar.xz"
  if [[ ! -f "$tarball" ]]; then
    log "downloading Intel SDE ${SDE_VERSION}"
    curl -fsSL --retry 3 -o "$tarball.part" "$SDE_URL"
    mv "$tarball.part" "$tarball"
  fi

  log "verifying SDE checksum"
  local actual
  actual="$(sha256sum "$tarball" | awk '{print $1}')"
  if [[ "${actual,,}" != "${SDE_SHA256,,}" ]]; then
    echo "error: SDE checksum mismatch" >&2
    echo "  expected $SDE_SHA256" >&2
    echo "  actual   $actual" >&2
    exit 1
  fi

  log "unpacking SDE"
  mkdir -p "$WORK/sde"
  tar -xJf "$tarball" --strip-components=1 -C "$WORK/sde"
  echo "$WORK/sde/sde64"
}

SDE64="$(resolve_sde)"
[[ -x "$SDE64" ]] || { echo "error: $SDE64 is not executable" >&2; exit 1; }

cd "$REPO_ROOT"

TESTBIN="$WORK/simd.test"
log "building the internal/simd test binary for host"
go test -c -o "$TESTBIN" "$PKG"

failed=0
declare -a summary

for entry in "${MODELS[@]}"; do
  IFS=: read -r model label include exclude <<<"$entry"

  log "running $PKG under SDE model -$model ($label)"
  out="$WORK/out-$model.txt"

  # SDE exits non-zero when the emulated program does; the test binary's own
  # exit status is what matters, so let it through and read the verdict from the
  # test output rather than from the emulator's status.
  set +e
  "$SDE64" "-$model" -- "$TESTBIN" -test.v -test.timeout=20m >"$out" 2>&1
  set -e

  ran=$(grep -cE '^(=== RUN|--- PASS)' "$out" || true)
  skipped=$(grep -E '^\s*--- SKIP' "$out" | sed -E 's/^ *//' || true)

  # A skip is only a failure when it names a test the emulated model supports.
  offending="$(printf '%s\n' "$skipped" | grep -Ei "$include" | grep -Eiv "$exclude" || true)"

  if [[ -n "$offending" ]]; then
    failed=1
    summary+=("FAIL  -$model ($label): $(printf '%s\n' "$offending" | wc -l | tr -d ' ') test(s) skipped despite the emulator reporting support")
    printf '%s\n' "$offending" | sed 's/^/      /'
  else
    summary+=("ok    -$model ($label): $ran tests run, no supported-feature skips")
  fi

  # The test binary itself must pass, or the run proves nothing.
  if ! grep -qE '^PASS$' "$out"; then
    failed=1
    summary+=("FAIL  -$model ($label): test binary did not report PASS")
    tail -30 "$out" | sed 's/^/      /'
  fi
  cp "$out" "$WORK/sde-$model-verbose.txt" 2>/dev/null || true
done

echo
log "AVX-512 coverage under Intel SDE ${SDE_VERSION}"
for line in "${summary[@]}"; do
  echo "  $line"
done

if [[ "$failed" -ne 0 ]]; then
  echo >&2
  echo "error: AVX-512/VBMI/AMX coverage is incomplete; full output in $WORK" >&2
  exit 1
fi

log "all emulated SIMD lanes ran"