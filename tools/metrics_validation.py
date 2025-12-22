#!/usr/bin/env python3
"""
Metrics Validation Tool

Validates that all defined Pro

metheus metrics in metrics.go are:
1. Registered correctly
2. Emitting non-zero values in running cluster
3. Represented in Grafana dashboard

Usage:
    python3 metrics_validation.py --cluster-ports 9090,9091,9092
"""

import re
import requests
import sys
from collections import defaultdict
from typing import Dict, List, Set, Tuple

class MetricsValidator:
    def __init__(self, metrics_file: str, dashboard_file: str, cluster_ports: List[int]):
        self.metrics_file = metrics_file
        self.dashboard_file = dashboard_file
        self.cluster_ports = cluster_ports
        self.defined_metrics = set()
        self.emitting_metrics = defaultdict(dict)
        self.dashboard_metrics = set()
        
    def extract_defined_metrics(self) -> Set[str]:
        """Extract all metric names from metrics.go"""
        with open(self.metrics_file, 'r') as f:
            content = f.read()
        
        # Match metric definitions
        pattern = r'Name:\s+"(longbow_[^"]+)"'
        matches = re.findall(pattern, content)
        self.defined_metrics = set(matches)
        return self.defined_metrics
    
    def scrape_cluster_metrics(self) -> Dict[str, Dict]:
        """Scrape metrics from all cluster nodes"""
        for port in self.cluster_ports:
            try:
                url = f"http://localhost:{port}/metrics"
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    for line in response.text.split('\n'):
                        if line.startswith('longbow_'):
                            metric_name = line.split('{')[0].split(' ')[0]
                            value = line.split(' ')[-1]
                            try:
                                value_float = float(value)
                                if metric_name not in self.emitting_metrics[port]:
                                    self.emitting_metrics[port][metric_name] = value_float
                            except ValueError:
                                pass
            except Exception as e:
                print(f"Warning: Could not scrape metrics from port {port}: {e}", file=sys.stderr)
        
        return self.emitting_metrics
    
    def extract_dashboard_metrics(self) -> Set[str]:
        """Extract metrics used in Grafana dashboard"""
        try:
            import json
            with open(self.dashboard_file, 'r') as f:
                dashboard = json.load(f)
            
            # Extract from panels
            content_str = json.dumps(dashboard)
            pattern = r'longbow_[a-z_0-9]+'
            matches = re.findall(pattern, content_str)
            self.dashboard_metrics = set(matches)
        except Exception as e:
            print(f"Warning: Could not parse dashboard: {e}", file=sys.stderr)
        
        return self.dashboard_metrics
    
    def validate(self) -> Dict:
        """Run comprehensive validation"""
        print("🔍 Extracting defined metrics from metrics.go...")
        defined = self.extract_defined_metrics()
        print(f"   Found {len(defined)} defined metrics")
        
        print("\n📊 Scraping cluster metrics...")
        emitting = self.scrape_cluster_metrics()
        all_emitting = set()
        for port_metrics in emitting.values():
            all_emitting.update(port_metrics.keys())
        print(f"   Found {len(all_emitting)} emitting metrics across cluster")
        
        print("\n📈 Extracting dashboard metrics...")
        dashboard = self.extract_dashboard_metrics()
        print(f"   Found {len(dashboard)} metrics in dashboard")
        
        # Analysis
        not_emitting = defined - all_emitting
        not_in_dashboard = defined - dashboard
        undefined_in_dashboard = dashboard - defined
        
        # Count non-zero emitting
        non_zero_metrics = set()
        for port_metrics in emitting.values():
            for metric, value in port_metrics.items():
                if value != 0:
                    non_zero_metrics.add(metric)
        
        return {
            'total_defined': len(defined),
            'total_emitting': len(all_emitting),
            'total_non_zero': len(non_zero_metrics),
            'total_in_dashboard': len(dashboard),
            'not_emitting': sorted(not_emitting),
            'not_in_dashboard': sorted(not_in_dashboard),
            'undefined_in_dashboard': sorted(undefined_in_dashboard),
            'coverage_percent': (len(dashboard) / len(defined) * 100) if defined else 0
        }
    
    def generate_report(self, results: Dict):
        """Generate validation report"""
        print("\n" + "="*80)
        print("📋 METRICS VALIDATION REPORT")
        print("="*80)
        
        print(f"\n✅ Total Defined Metrics: {results['total_defined']}")
        print(f"📡 Total Emitting Metrics: {results['total_emitting']}")
        print(f"💫 Total Non-Zero Metrics: {results['total_non_zero']}")
        print(f"📊 Total Dashboard Metrics: {results['total_in_dashboard']}")
        print(f"📈 Dashboard Coverage: {results['coverage_percent']:.1f}%")
        
        if results['not_emitting']:
            print(f"\n⚠️  Metrics NOT Emitting ({len(results['not_emitting'])}):")
            for metric in results['not_emitting'][:20]:
                print(f"   - {metric}")
            if len(results['not_emitting']) > 20:
                print(f"   ... and {len(results['not_emitting']) - 20} more")
        
        if results['not_in_dashboard']:
            print(f"\n📉 Metrics NOT in Dashboard ({len(results['not_in_dashboard'])}):")
            for metric in results['not_in_dashboard'][:30]:
                print(f"   - {metric}")
            if len(results['not_in_dashboard']) > 30:
                print(f"   ... and {len(results['not_in_dashboard']) - 30} more")
        
        if results['undefined_in_dashboard']:
            print(f"\n⚠️  Dashboard uses undefined metrics ({len(results['undefined_in_dashboard'])}):")
            for metric in results['undefined_in_dashboard']:
                print(f"   - {metric}")
        
        print("\n" + "="*80)
        
        # Recommendations
        print("\n💡 RECOMMENDATIONS:")
        if results['coverage_percent'] < 50:
            print("   🔴 CRITICAL: Dashboard coverage is very low!")
            print("      Action: Add panels for high-value metrics")
        elif results['coverage_percent'] < 80:
            print("   🟡 WARNING: Dashboard coverage could be improved")
            print("      Action: Review missing metrics and add important ones")
        else:
            print("   🟢 GOOD: Dashboard coverage is adequate")
        
        if len(results['not_emitting']) > 20:
            print("   🟡 Many metrics not emitting - may need workload generation")
        
        print("="*80)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Validate Longbow metrics')
    parser.add_argument('--metrics-file', default='internal/metrics/metrics.go')
    parser.add_argument('--dashboard-file', default='grafana/dashboards/longbow.json')
    parser.add_argument('--cluster-ports', default='9090,9091,9092')
    
    args = parser.parse_args()
    
    ports = [int(p) for p in args.cluster_ports.split(',')]
    
    validator = MetricsValidator(
        metrics_file=args.metrics_file,
        dashboard_file=args.dashboard_file,
        cluster_ports=ports
    )
    
    results = validator.validate()
    validator.generate_report(results)
    
    # Exit with error if coverage is too low
    sys.exit(0 if results['coverage_percent'] >= 30 else 1)
