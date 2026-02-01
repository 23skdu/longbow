import os
import re
import sys

def extract_metrics_from_docs(docs_path):
    metrics = set()
    if not os.path.exists(docs_path):
        print(f"Warning: {docs_path} not found.")
        return metrics
    
    with open(docs_path, 'r') as f:
        content = f.read()
        # Find all ### longbow_...
        matches = re.findall(r'###\s+(longbow_[a-zA-Z0-9_]+)', content)
        for m in matches:
            metrics.add(m)
    return metrics

def extract_metrics_from_code(metrics_dir):
    metrics = set()
    if not os.path.isdir(metrics_dir):
        print(f"Warning: {metrics_dir} not found.")
        return metrics
    
    for root, _, files in os.walk(metrics_dir):
        for file in files:
            if file.endswith('.go'):
                with open(os.path.join(root, file), 'r') as f:
                    content = f.read()
                    # Find all Name: "longbow_..."
                    matches = re.findall(r'Name:\s+"(longbow_[a-zA-Z0-9_]+)"', content)
                    for m in matches:
                        metrics.add(m)
    return metrics

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Verify Prometheus metrics synchronization.')
    parser.add_argument('--strict', action='store_true', help='Fail if any metric is missing in either docs or code.')
    args = parser.parse_args()

    docs_path = 'docs/metrics.md'
    metrics_dir = 'internal/metrics'
    
    doc_metrics = extract_metrics_from_docs(docs_path)
    code_metrics = extract_metrics_from_code(metrics_dir)
    
    print(f"Found {len(doc_metrics)} metrics in {docs_path}")
    print(f"Found {len(code_metrics)} metrics in {metrics_dir}")
    
    missing_in_docs = code_metrics - doc_metrics
    missing_in_code = doc_metrics - code_metrics
    
    success = True
    
    if missing_in_code:
        print("\n❌ Metrics in documentation but MISSING in code (BROKEN LINKS):")
        for m in sorted(missing_in_code):
            print(f"  {m}")
        success = False
    
    if missing_in_docs:
        print(f"\n⚠️  Metrics in code but MISSING in documentation ({len(missing_in_docs)} missing):")
        if args.strict:
            for m in sorted(missing_in_docs):
                print(f"  {m}")
            success = False
        else:
            print("  (Run with --strict to see the full list)")
        
    if success:
        print("\n✅ Metric synchronization check passed!")
        sys.exit(0)
    else:
        print("\n❌ Metric synchronization check failed.")
        sys.exit(1)

if __name__ == "__main__":
    main()
