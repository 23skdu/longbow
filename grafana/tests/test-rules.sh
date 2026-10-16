#!/usr/bin/env bash
# Validate grafana/rules.yml and run the promtool unit tests.
#
# rules.yml is Grafana-flavored, so it cannot be handed to promtool directly:
# some annotations use humanizeBytes, which is a Grafana template function and
# not a Prometheus one. This script rewrites just those calls to humanize so the
# expressions and templates can be parsed, then runs the checks. It never
# rewrites the checked-in rules file.
#
# Requires promtool. Install from a Prometheus release archive, or point
# PROMTOOL at an existing binary.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
grafana_dir="$(dirname "$here")"

PROMTOOL="${PROMTOOL:-promtool}"
if ! command -v "$PROMTOOL" >/dev/null 2>&1; then
  echo "promtool not found. Set PROMTOOL=/path/to/promtool and re-run." >&2
  exit 1
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

sed 's/humanizeBytes/humanize/g' "$grafana_dir/rules.yml" > "$work/rules.yml"
cp "$here"/*_test.yml "$work/"

# Extract one alert group in isolation. promtool needs the top-level "groups:" key
# that the extraction drops, so it is re-added. Grafana template functions are
# rewritten to their Prometheus equivalents because promtool only knows the latter.
#
# The end of a group is detected structurally - the next "  - name: " line -
# rather than by naming the following group. Naming the neighbour meant every
# insertion or reordering in rules.yml silently turned an existing test into a
# no-op that still passed.
#
# This uses a single awk rather than `awk | awk` with an early exit in the second
# stage. When the second stage exits early the first receives SIGPIPE, and under
# `set -o pipefail` that aborts the script - but only when the timings happen to
# line up, so it passes interactively and fails in a redirect.
extract_group() {
  local start="$1" out="$2"
  awk -v s="  - name: $start" '
    $0 == s { f = 1; print; next }
    f && /^  - name: / { exit }
    f { print }
  ' "$work/rules.yml" \
    | sed '/runbook_url:/d; s/{{ \$value | humanizeDuration }}/V/; s/{{ \$value | humanizePercentage }}/V/; s/{{ \$value | humanize }}/V/; s/{{ \$value }}/V/' \
    > "$work/group.yml"
  { echo "groups:"; cat "$work/group.yml"; } > "$work/$out"
  # A group that did not match would extract nothing and promtool would report
  # a file with no rules, which passes for an empty test. Fail loudly instead.
  if ! grep -q '^  - name: ' "$work/group.yml"; then
    echo "extract_group: no rule group named '$start' in rules.yml" >&2
    exit 1
  fi
}

# Every group with a matching <name>_test.yml gets a unit test.
# Adding a group without a test is reported by the conformance gate
# (TestRuleGroupsHaveUnitTests) rather than failing here, because parse-checking
# an untested group is still worth doing.
for testfile in "$here"/*_test.yml; do
  [ -e "$testfile" ] || continue
  name="$(basename "$testfile" .yml)"
  group="longbow_${name%_test}"
  echo "==> extracting $group for $name"
  extract_group "$group" "${name%_test}.rules.yml"
done

echo "==> checking all rules parse"
"$PROMTOOL" check rules "$work/rules.yml"

# Each group's unit test asserts that an alert fires when it should and stays
# silent when it should not, so an expression that merely never fires is a
# failure rather than a silent pass.
status=0
for testfile in "$here"/*_test.yml; do
  [ -e "$testfile" ] || continue
  name="$(basename "$testfile" .yml)"
  echo "==> running $name"
  ( cd "$work" && "$PROMTOOL" test rules "$name.yml" ) || status=1
done
exit $status
