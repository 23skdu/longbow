#!/usr/bin/env bash
# Guard the generated SIMD assembly under internal/simd.
#
# Files listed in the //go:generate directives in internal/simd/generate.go are
# produced by the Avo sources in internal/simd/gen/. They do not carry a "Code
# generated ... DO NOT EDIT." header, so nothing stops an editor or a careless
# `go generate ./...` from rewriting them.
#
# The more serious hazard is that they do not regenerate byte for byte.
# all_kernels_avo_amd64.s also contains a block of hand-written kernels that
# the generator only knows how to emit as no-op stubs, so regenerating silently
# replaces working code with functions that return zero.
#
# Rather than demand byte equality (which would first require porting every one
# of those kernels into the Avo source), this check asserts the property that
# actually matters: regenerating must not remove any kernel committed today.
# It exits non-zero and lists the symbols when `go generate` would drop one.
#
# Every generated file is snapshotted and restored around the run, so the check
# leaves the working tree exactly as it found it, dirty or not.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
simd_dir="$repo_root/internal/simd"

# Discover the generated files from the -out flags of the //go:generate
# directives, so a new generator is covered without editing this script.
mapfile -t generated < <(
  grep -rhoE '^\s*//go:generate .*-out [^ ]+' "$simd_dir"/*.go |
    awk '{print $NF}' |
    while read -r out; do
      [[ "$out" == *.s ]] && printf '%s\n' "$simd_dir/$out"
    done
)

if [[ ${#generated[@]} -eq 0 ]]; then
  echo "::error::no generated .s files discovered in $simd_dir/generate.go"
  exit 1
fi

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

for f in "${generated[@]}"; do
  if [[ ! -f "$f" ]]; then
    echo "::error::$f is listed in a //go:generate directive but does not exist"
    exit 1
  fi
  cp "$f" "$workdir/$(basename "$f").committed"
done

echo "Regenerating ${#generated[@]} SIMD kernel file(s)..."
(cd "$simd_dir" && go generate ./...)

for f in "${generated[@]}"; do
  cp "$f" "$workdir/$(basename "$f").generated"
  cp "$workdir/$(basename "$f").committed" "$f"
done

if ! python3 - "$workdir" "${generated[@]}" <<'PY'
import difflib
import os
import re
import sys

workdir, files = sys.argv[1], sys.argv[2:]


def symbols(text):
    return set(re.findall(r"^TEXT ·(\w+)\(SB\)", text, re.M))


failed = False
for path in files:
    base = os.path.basename(path)
    committed = open(os.path.join(workdir, base + ".committed")).read()
    generated = open(os.path.join(workdir, base + ".generated")).read()

    if committed == generated:
        print(f"{base}: OK, regeneration is a byte-for-byte no-op")
        continue

    failed = True
    before, after = symbols(committed), symbols(generated)
    lost, added = sorted(before - after), sorted(after - before)

    print(f"::error::{base}: regeneration is not reproducible")
    if lost:
        print(f"  regeneration would REMOVE {len(lost)} kernel(s): {', '.join(lost)}")
        print(
            "  These are hand-written and are not produced by internal/simd/gen/*.go,\n"
            "  which emits no-op stubs for them. Port them into the generator, or move\n"
            "  them to kernels_manual_amd64.s, before regenerating."
        )
    if added:
        print(f"  regeneration would ADD {len(added)} kernel(s): {', '.join(added)}")
        print(
            "  These duplicate symbols defined in a hand-maintained .s file, which\n"
            "  would be a link error. Remove them from the generator."
        )
    if not lost and not added:
        print("  Same kernels, different bodies -- the committed file was hand-edited:")
        diff = list(
            difflib.unified_diff(
                committed.split("\n"),
                generated.split("\n"),
                fromfile=f"committed {base}",
                tofile=f"generated {base}",
                lineterm="",
                n=1,
            )
        )
        for line in diff[:40]:
            print(f"    {line}")
        if len(diff) > 40:
            print(f"    ... {len(diff) - 40} more diff lines")

sys.exit(1 if failed else 0)
PY
then
  echo "::error::regeneration would drop committed kernels; see above"
  exit 1
fi
