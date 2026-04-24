import os
import json
import pandas as pd
from datetime import datetime
import platform

def parse_results(log_dir):
    all_results = []
    for root, dirs, files in os.walk(log_dir):
        for f in files:
            if f.endswith('.json') and f.startswith('perf_matrix_'):
                try:
                    with open(os.path.join(root, f)) as j:
                        data = json.load(j)
                        if isinstance(data, dict) and 'results' in data:
                            raw_items = data['results']
                            pform = data.get('platform', 'unknown')
                            mode = data.get('mode', 'unknown')
                        elif isinstance(data, list):
                            raw_items = data
                            pform = 'unknown'
                            mode = 'unknown'
                        else:
                            raw_items = [data]
                            pform = 'unknown'
                            mode = 'unknown'
                        
                        for item in raw_items:
                            if not isinstance(item, dict):
                                continue
                            if 'ingest' in item and 'vec_per_sec' in item['ingest']:
                                item['ingest_vec_per_sec'] = item['ingest']['vec_per_sec']
                            if 'platform' not in item:
                                item['platform'] = pform
                            if 'mode' not in item:
                                item['mode'] = mode
                            all_results.append(item)
                except Exception as e:
                    print(f"Error parsing {f}: {e}")
    return all_results

def generate_markdown(results, output_file):
    if not results:
        print("No results found.")
        return
    
    df = pd.DataFrame(results)
    
    with open(output_file, 'w') as f:
        f.write("# Longbow Performance Benchmark Matrix (LATEST)\n\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Summary Section
        f.write("## Executive Summary\n\n")
        f.write("Benchmarks are still in progress. The following data represents partial results collected so far.\n\n")
        
        # 1. Ingest Matrix (Standard Modes)
        standard_df = df[df['mode'].isin(['cpu', 'metal', 'cuda'])]
        if not standard_df.empty and 'ingest_vec_per_sec' in standard_df.columns:
            f.write("## 1. Ingest Performance (vec/s)\n\n")
            ingest_df = standard_df.pivot_table(
                index=['platform', 'mode', 'dtype'], 
                columns=['count', 'dim'], 
                values='ingest_vec_per_sec'
            )
            f.write(ingest_df.to_markdown())
            f.write("\n\n")
        
        # 2. Search Matrix (Standard Modes)
        search_data = []
        for r in results:
            # Aggregate search metrics from all modes that produce them
            if r.get('mode') in ['cpu', 'metal', 'cuda', 'geo', 'temporal', 'graphrag', 'recommend']:
                for mode_name, metrics in r.get('search', {}).items():
                    if isinstance(metrics, dict) and metrics.get('qps', 0) > 0:
                        search_data.append({
                            'platform': r.get('platform', 'unknown'),
                            'mode': r['mode'],
                            'dtype': r['dtype'],
                            'dim': r['dim'],
                            'count': r['count'],
                            'search_mode': mode_name,
                            'qps': metrics.get('qps', 0)
                        })
        
        if search_data:
            f.write("## 2. Standard Search Performance (QPS)\n\n")
            sdf = pd.DataFrame(search_data)
            # Prioritize standard search modes, then specialized ones
            all_search_modes = sorted(sdf['search_mode'].unique())
            for smode in all_search_modes:
                f.write(f"### {smode.upper()} QPS\n\n")
                sub_df = sdf[sdf['search_mode'] == smode].pivot_table(
                    index=['platform', 'mode', 'dtype'],
                    columns=['count', 'dim'],
                    values='qps'
                )
                f.write(sub_df.to_markdown())
                f.write("\n\n")

        # 3. Specialized Search Modes
        spec_modes = ['geo', 'temporal', 'graphrag', 'recommend']
        spec_search_data = []
        for r in results:
            search_metrics = r.get('search', {})
            for smode in spec_modes:
                # The Go bench-tool might prefix with 'Search_' or use lowercase
                metrics = search_metrics.get(smode) or search_metrics.get(f"Search_{smode.capitalize()}")
                if isinstance(metrics, dict) and metrics.get('qps', 0) > 0:
                    spec_search_data.append({
                        'platform': r.get('platform', 'unknown'),
                        'dtype': r['dtype'],
                        'dim': r['dim'],
                        'count': r['count'],
                        'mode': r['mode'],
                        'search_type': smode,
                        'qps': metrics.get('qps', 0)
                    })
        
        if spec_search_data:
            f.write("## 3. Specialized Search Performance\n\n")
            spec_df = pd.DataFrame(spec_search_data)
            for smode in spec_modes:
                mode_df = spec_df[spec_df['search_type'] == smode]
                if not mode_df.empty:
                    f.write(f"### {smode.upper()} Results\n\n")
                    pivot_df = mode_df.pivot_table(
                        index=['platform', 'mode', 'dtype'],
                        columns=['count', 'dim'],
                        values='qps'
                    )
                    f.write(pivot_df.to_markdown())
                    f.write("\n\n")
        
        # 4. Learned Index Results
        learned_df = df[df['mode'] == 'learned_index']
        if not learned_df.empty:
            f.write("## 4. Learned Index Adaptation Results\n\n")
            for _, row in learned_df.iterrows():
                pform = row.get('platform', 'unknown')
                res = row.get('results', {})
                f.write(f"### Platform: {pform} (Dim: {row.get('dim')})\n\n")
                
                # Prediction Accuracy
                prom = res.get('prometheus', {})
                if prom:
                    f.write("| Metric | Value |\n| :--- | :--- |\n")
                    f.write(f"| k-NN Predictions | {prom.get('knn_predictions', 0)} |\n")
                    f.write(f"| Heuristic Predictions | {prom.get('default_predictions', 0)} |\n")
                    f.write(f"| Correct Predictions | {prom.get('correct_predictions', 0)} |\n")
                    acc = (prom.get('correct_predictions', 0) / prom.get('knn_predictions', 1)) * 100 if prom.get('knn_predictions', 0) > 0 else 0
                    f.write(f"| Accuracy | {acc:.2f}% |\n\n")
                
                # Latency Gain
                comp = res.get('latency_comparison', {})
                if comp:
                    f.write("| Latency Type | P50 (ms) | P99 (ms) |\n| :--- | :--- | :--- |\n")
                    f.write(f"| Heuristic (Baseline) | {comp.get('heuristic_p50_ms', 0):.2f} | - |\n")
                    f.write(f"| Learned Index | {comp.get('knn_p50_ms', 0):.2f} | {comp.get('knn_p99_ms', 0):.2f} |\n")
                    gain = comp.get('latency_gain_p50_ms', 0)
                    f.write(f"| **Gain** | **{gain:+.2f}ms** | |\n\n")

if __name__ == "__main__":
    local_logs = "data/perf_logs"
    results = parse_results(local_logs)
    generate_markdown(results, "docs/performance.md")
