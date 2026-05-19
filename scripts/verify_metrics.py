#!/usr/bin/env python3
import os
import re
import sys

def get_documented_metrics(filepath):
    metrics = set()
    if not os.path.exists(filepath):
        print(f"Warning: {filepath} not found.")
        return metrics
    
    with open(filepath, 'r') as f:
        content = f.read()
        # Find all `longbow_...` in the first column of the table
        # | `longbow_active_search_contexts` | Description |
        matches = re.findall(r'\| `(longbow_[a-z0-9_]+)` \|', content)
        for m in matches:
            metrics.add(m)
    return metrics

def get_code_metrics(root_dir):
    metrics = set()
    # Pattern to match Name: "longbow_..."
    pattern = re.compile(r'Name:\s*"([a-z0-9_]+)"')
    
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.endswith('.go'):
                with open(os.path.join(root, file), 'r', errors='ignore') as f:
                    content = f.read()
                    matches = pattern.findall(content)
                    for m in matches:
                        if m.startswith('longbow_'):
                            metrics.add(m)
    return metrics

def main():
    docs_file = 'docs/metrics.md'
    internal_dir = 'internal'
    pkg_dir = 'pkg'
    cmd_dir = 'cmd'

    documented = get_documented_metrics(docs_file)
    
    code_metrics = set()
    for d in [internal_dir, pkg_dir, cmd_dir]:
        if os.path.exists(d):
            code_metrics.update(get_code_metrics(d))

    # Also check if they are documented but not in code (maybe deprecated or renamed)
    not_in_code = documented - code_metrics
    # Check if they are in code but not documented
    not_documented = code_metrics - documented

    success = True
    
    if not_documented:
        print("Error: The following metrics are defined in code but not documented in docs/metrics.md:")
        for m in sorted(not_documented):
            print(f"  - {m}")
        success = False

    if not_in_code:
        print("Warning: The following metrics are documented in docs/metrics.md but not found in code (may be false positives or deprecated):")
        for m in sorted(not_in_code):
            print(f"  - {m}")
        # We don't fail for this, as some metrics might be generated dynamically or be in vendors (unlikely here)
        # or just be legacy docs. But we should report it.

    if not success:
        print("\nPlease update docs/metrics.md to include all metrics defined in the source code.")
        sys.exit(1)
    
    print("All metrics are properly documented.")
    sys.exit(0)

if __name__ == "__main__":
    main()
