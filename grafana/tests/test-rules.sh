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
cp "$here"/*_alerts_test.yml "$work/"

# Extract one alert group in isolation. promtool needs the top-level "groups:" key
# that the extraction drops, so it is re-added. Grafana template functions are
# rewritten to their Prometheus equivalents because promtool only knows the latter.
#
# This uses a single awk rather than `awk | awk` with an early exit in the second
# stage. When the second stage exits early the first receives SIGPIPE, and under
# `set -o pipefail` that aborts the script - but only when the timings happen to
# line up, so it passes interactively and fails in a redirect.
extract_group() {
  local start="$1" stop="$2" out="$3"
  awk -v s="  - name: $start" -v e="  - name: $stop" '
    $0 == s { f = 1 }
    f && $0 == e { exit }
    f { print }
  ' "$work/rules.yml" \
    | sed 's/{{ \$value | humanizeDuration }}/V/; s/{{ \$value | humanizePercentage }}/V/; s/{{ \$value | humanize }}/V/' \
    > "$work/group.yml"
  { echo "groups:"; cat "$work/group.yml"; } > "$work/$out"
}

extract_group longbow_bulk_insert_alerts longbow_hnsw_contention_alerts bulk_insert_rules.yml
extract_group longbow_temporal_alerts longbow_layer_eviction_alerts temporal_rules.yml

echo "==> checking all rules parse"
"$PROMTOOL" check rules "$work/rules.yml"

# Each group's unit test lives in a file named after the group. The assertions
# exercise that an alert fires when it should and stays silent when it should not,
# so a broken expression that merely never fires is still a failure.
status=0
for testfile in "$work"/*_alerts_test.yml; do
  name="$(basename "$testfile" .yml)"
  echo "==> running $name"
  ( cd "$work" && "$PROMTOOL" test rules "$name.yml" ) || status=1
done
exit $status
