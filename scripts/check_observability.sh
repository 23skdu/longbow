#!/usr/bin/env bash
# Release-artifact conformance gate.
#
# Invoked by `make observability-check` and the CI `observability` job so the
# release artifact list stays in sync with what CI and the Makefile actually
# build: multi-arch linux/amd64 + linux/arm64 CPU images, amd64-only GPU/TPU
# images, and the native darwin/arm64 binary (never a darwin/arm64 OCI image).
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$REPO_ROOT"

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

MAKEFILE=Makefile
WORKFLOW=.github/workflows/ci.yml
DEPLOY_DOC=docs/deploy.md

# image:platform:dockerfile:make-target
# TPU and CUDA variants are amd64-only because their base toolchains require it.
RELEASE_IMAGES=(
  "longbow:linux/amd64:Dockerfile.cpu:docker-build-cpu"
  "longbow:linux/arm64:Dockerfile.cpu:docker-build-cpu-arm64"
  "longbow-emlgo-cpu:linux/amd64:Dockerfile.emlgo-cpu:docker-build-emlgo-cpu"
  "longbow-emlgo-cpu:linux/arm64:Dockerfile.emlgo-cpu:docker-build-emlgo-cpu-arm64"
  "longbow-nvidia:linux/amd64:Dockerfile.nvidia:docker-build-nvidia"
  "longbow-emlgo-nvidia:linux/amd64:Dockerfile.emlgo-nvidia:docker-build-emlgo-nvidia"
  "longbow-tpu:linux/amd64:Dockerfile.tpu:docker-build-tpu"
)
# Native (non-OCI) release artifacts.
NATIVE_ARTIFACTS=(
  "longbow-darwin-arm64:darwin/arm64"
)

[[ -f "$MAKEFILE" && -f "$WORKFLOW" && -f "$DEPLOY_DOC" ]] || fail "missing release gate input files"

echo "Release artifact matrix under check:"
for entry in "${RELEASE_IMAGES[@]}"; do
  echo "  OCI     ${entry}"
done
for entry in "${NATIVE_ARTIFACTS[@]}"; do
  echo "  native  ${entry}"
done

for entry in "${RELEASE_IMAGES[@]}"; do
  IFS=: read -r image platform dockerfile target <<<"$entry"

  grep -Eq "^${target}:" "$MAKEFILE" ||
    fail "$MAKEFILE is missing target ${target} (${image} ${platform})"

  if grep -Fq "file: ./${dockerfile}" "$WORKFLOW"; then
    :
  elif grep -Fq "file: ${dockerfile}" "$WORKFLOW"; then
    :
  else
    fail "$WORKFLOW does not build ${dockerfile} (${image} ${platform})"
  fi

  if [[ "$platform" == "linux/arm64" ]]; then
    grep -Fq -- "--platform linux/arm64" "$MAKEFILE" ||
      fail "$MAKEFILE has no linux/arm64 docker build for ${image}"
    grep -Fq "platforms: linux/amd64,linux/arm64" "$WORKFLOW" ||
      fail "$WORKFLOW does not publish a multi-arch manifest for ${dockerfile}"
  else
    grep -Fq -- "--platform linux/amd64" "$MAKEFILE" ||
      fail "$MAKEFILE has no linux/amd64 docker build for ${image}"
    grep -Fq "platforms: linux/amd64" "$WORKFLOW" ||
      fail "$WORKFLOW has no linux/amd64 build for ${dockerfile}"
  fi
done

# Publishing must be opt-in and sign/attest only pushed digests.
grep -Fq "startsWith(github.ref, 'refs/tags/')" "$WORKFLOW" ||
  fail "$WORKFLOW does not gate publishing on tag refs"
# shellcheck disable=SC2016 # keep the GitHub Actions expression literal
grep -Fq 'push: ${{ env.PUBLISH' "$WORKFLOW" ||
  fail "$WORKFLOW release builds are not publish-gated"
grep -Fq 'outputs.digest' "$WORKFLOW" ||
  fail "$WORKFLOW publishing steps do not use pushed digests"
grep -Fq 'cosign sign --yes' "$WORKFLOW" ||
  fail "$WORKFLOW does not sign published digests"

# darwin/arm64 is shipped as a native binary; OCI images are Linux artifacts.
grep -Fq "name: longbow-darwin-arm64" "$WORKFLOW" ||
  fail "$WORKFLOW no longer produces the longbow-darwin-arm64 artifact"
grep -Fq "GOOS=darwin GOARCH=arm64" "$WORKFLOW" ||
  fail "$WORKFLOW no longer cross-compiles the darwin/arm64 binary"
grep -Fq "darwin/arm64 OCI images are not published" "$DEPLOY_DOC" ||
  fail "$DEPLOY_DOC must state that darwin/arm64 OCI images are not published"
if grep -Eiq "darwin/arm64 (OCI )?(image|container)s? (is|are) published|publish(ed|ing)? [^.]*darwin/arm64 (OCI )?(image|container)" "$DEPLOY_DOC"; then
  fail "$DEPLOY_DOC must not promise a darwin/arm64 OCI image or container"
fi

echo "PASS: release artifact matrix (amd64 + arm64 CPU images, amd64-only GPU/TPU, native darwin/arm64 binary) is consistent."
