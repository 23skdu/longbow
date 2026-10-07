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
cp "$here/bulk_insert_alerts_test.yml" "$work/bulk_insert_alerts_test.yml"

# The unit test exercises only the bulk-insert group, so isolate it and rewrite
# the two alert-name templates the assertions match on.
awk '/^  - name: longbow_bulk_insert_alerts$/{f=1} f' "$work/rules.yml" \
  | awk '/^  - name: longbow_hnsw_contention_alerts$/{exit} {print}' \
  | sed 's/{{ \$value | humanizeDuration }}/V/; s/{{ \$value | humanizePercentage }}/V/; s/{{ \$value | humanize }}/V/' \
  > "$work/group.yml"

# The extraction drops the top-level "groups:" key, which promtool requires.
{ echo "groups:"; cat "$work/group.yml"; } > "$work/bulk_insert_rules.yml"

echo "==> checking all rules parse"
"$PROMTOOL" check rules "$work/rules.yml"

echo "==> running bulk-insert alert unit tests"
( cd "$work" && "$PROMTOOL" test rules bulk_insert_alerts_test.yml )
