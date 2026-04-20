#!/usr/bin/env python3
"""
Dashboard Generator - Adds comprehensive panels to Grafana dashboard
"""

import json
import sys

def create_row(title, y_pos, panel_id):
    """Create a row panel"""
    return {
        "collapsed": False,
        "gridPos": {"h": 1, "w": 24, "x": 0, "y": y_pos},
        "id": panel_id,
        "panels": [],
        "title": title,
        "type": "row"
    }

def create_timeseries_panel(title, expr, legend_format, x, y, w, h, panel_id, unit="short"):
    """Create a timeseries panel"""
    return {
        "datasource": "${datasource}",
        "fieldConfig": {
            "defaults": {
                "custom": {
                    "drawStyle": "line",
                    "lineInterpolation": "linear",
                    "showPoints": "auto"
                },
                "unit": unit
            }
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "targets": [{
            "expr": expr,
            "legendFormat": legend_format,
            "refId": "A"
        }],
        "title": title,
        "type": "timeseries"
    }

def create_stat_panel(title, expr, x, y, w, h, panel_id, unit="short"):
    """Create a stat panel"""
    return {
        "datasource": "${datasource}",
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "thresholds"},
                "unit": unit,
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"color": "green", "value": None}
                    ]
                }
            }
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "targets": [{
            "expr": expr,
            "refId": "A"
        }],
        "title": title,
        "type": "stat"
    }

def enhance_dashboard(dashboard_file):
    """Add comprehensive panels to dashboard"""
    
    with open(dashboard_file, 'r') as f:
        dashboard = json.load(f)
    
    # Starting panel ID and Y position after existing panels
    max_id = max(p.get('id', 0) for p in dashboard['panels'])
    max_y = max((p['gridPos']['y'] + p['gridPos']['h']) for p in dashboard['panels'])
    
    next_id = max_id + 1
    y = max_y 
    
    new_panels = []
    
    # WAL & Persistence row
    new_panels.append(create_row("WAL & Persistence", y, next_id))
    next_id += 1
    y += 1
    
    new_panels.append(create_timeseries_panel(
        "WAL Write Rate", "rate(longbow_wal_writes_total[1m])",
        "{{status}}", 0, y, 6, 6, next_id, "wps"
    ))
    next_id += 1
    
    new_panels.append(create_timeseries_panel(
        "WAL Fsync Duration P99", 
        "histogram_quantile(0.99, sum(rate(longbow_wal_fsync_duration_seconds_bucket[5m])) by (le))",
        "P99", 6, y, 6, 6, next_id, "s"
    ))
    next_id += 1
    
    new_panels.append(create_timeseries_panel(
        "WAL Batch Size",
        "avg(longbow_wal_batch_size)",
        "Avg Batch Size", 12, y, 6, 6, next_id, "short"
    ))
    next_id += 1
    
    new_panels.append(create_stat_panel(
        "WAL Pending Entries",
        "longbow_wal_pending_entries",
        18, y, 6, 6, next_id, "short"
    ))
    next_id += 1
    y += 6
    
    # Index Operations row
    new_panels.append(create_row("Index Operations", y, next_id))
    next_id += 1
    y += 1
    
    new_panels.append(create_timeseries_panel(
        "Index Job Latency P95",
        "histogram_quantile(0.95, sum(rate(longbow_index_job_latency_seconds_bucket[5m])) by (le))",
        "P95", 0, y, 6, 6, next_id, "s"
    ))
    next_id += 1
    
    new_panels.append(create_stat_panel(
        "Index Queue Depth",
        "longbow_index_queue_depth",
        6, y, 6, 6, next_id, "short"
    ))
    next_id += 1
    
    new_panels.append(create_timeseries_panel(
        "HNSW Nodes Visited",
        "rate(longbow_hnsw_nodes_visited[1m])",
        "Nodes/sec", 12, y, 6, 6, next_id, "short"
    ))
    next_id += 1
    
    new_panels.append(create_timeseries_panel(
        "Index Build Latency",
        "histogram_quantile(0.99, sum(rate(longbow_index_build_latency_seconds_bucket[5m])) by (le))",
        "P99", 18, y, 6, 6, next_id, "s"
    ))
    next_id += 1
    y += 6
    
    # Flight RPC Details row
    new_panels.append(create_row("Flight RPC Details", y, next_id))
    next_id += 1
    y += 1
    
    new_panels.append(create_timeseries_panel(
        "DoGet Zero-Copy Ratio",
        "rate(longbow_doget_zero_copy_total{type=\"zero_copy_retain\"}[1m]) / (rate(longbow_doget_zero_copy_total[1m]) + 1)",
        "Zero-Copy %", 0, y, 8, 6, next_id, "percentunit"
    ))
    next_id += 1
    
    new_panels.append(create_timeseries_panel(
        "DoPut Payload Size P99",
        "histogram_quantile(0.99, sum(rate(longbow_doput_payload_size_bytes_bucket[5m])) by (le))",
        "P99", 8, y, 8, 6, next_id, "bytes"
    ))
    next_id += 1
    
    new_panels.append(create_stat_panel(
        "Flight Pool Connections",
        "longbow_flight_pool_connections_active",
        16, y, 8, 6, next_id, "short"
    ))
    next_id += 1
    y += 6
    
    # Global Search row
    new_panels.append(create_row("Global Search", y, next_id))
    next_id += 1
    y += 1
    
    new_panels.append(create_timeseries_panel(
        "Global Search Duration P95",
        "histogram_quantile(0.95, sum(rate(longbow_global_search_duration_seconds_bucket[5m])) by (le))",
        "P95", 0, y, 8, 6, next_id, "s"
    ))
    next_id += 1
    
    new_panels.append(create_timeseries_panel(
        "Global Search Fanout Size",
        "histogram_quantile(0.95, sum(rate(longbow_global_search_fanout_size_bucket[5m])) by (le))",
        "P95 Fanout", 8, y, 8, 6, next_id, "short"
    ))
    next_id += 1
    
    new_panels.append(create_timeseries_panel(
        "Global Search Rate",
        "rate(longbow_global_search_requests_total[1m])",
        "Requests/s", 16, y, 8, 6, next_id, "reqps"
    ))
    next_id += 1
    y += 6
    
    # Sharding row
    new_panels.append(create_row("Sharding & Partitioning", y, next_id))
    next_id += 1
    y += 1
    
    new_panels.append(create_stat_panel(
        "Total Shards",
        "sum(longbow_sharded_hnsw_shard_size)",
        0, y, 6, 6, next_id, "short"
    ))
    next_id += 1
    
    new_panels.append(create_timeseries_panel(
        "Shard Load Factor",
        "longbow_sharded_hnsw_load_factor",
        "{{dataset}}", 6, y, 9, 6, next_id, "percentunit"
    ))
    next_id += 1
    
    new_panels.append(create_timeseries_panel(
        "Shard Lock Contention",
        "rate(longbow_shard_lock_wait_seconds_sum[1m])",
        "{{dataset}}", 15, y, 9, 6, next_id, "s"
    ))
    next_id += 1
    y += 6
    
    # Replication row
    new_panels.append(create_row("Replication & Quorum", y, next_id))
    next_id += 1
    y += 1
    
    new_panels.append(create_timeseries_panel(
        "Replication Lag",
        "longbow_replication_lag_seconds",
        "{{node}}", 0, y, 8, 6, next_id, "s"
    ))
    next_id += 1
    
    new_panels.append(create_timeseries_panel(
        "Quorum Success Rate",
        "rate(longbow_quorum_success_total[1m]) / (rate(longbow_quorum_success_total[1m]) + rate(longbow_quorum_failure_total[1m]))",
        "Success %", 8, y, 8, 6, next_id, "percentunit"
    ))
    next_id += 1
    
    new_panels.append(create_timeseries_panel(
        "Quorum Operation Latency P99",
        "histogram_quantile(0.99, sum(rate(longbow_quorum_operation_duration_seconds_bucket[5m])) by (le, consistency))",
        "{{consistency}} P99", 16, y, 8, 6, next_id, "s"
    ))
    next_id += 1
    y += 6
    
    # Compaction row
    new_panels.append(create_row("Compaction", y, next_id))
    next_id += 1
    y += 1
    
    new_panels.append(create_timeseries_panel(
        "Compaction Rate",
        "rate(longbow_compaction_operations_total[5m])",
        "{{dataset}}", 0, y, 12, 6, next_id, "ops"
    ))
    next_id += 1
    
    new_panels.append(create_timeseries_panel(
        "Auto Compaction Triggers",
        "rate(longbow_compaction_auto_triggers_total[5m])",
        "Triggers/min", 12, y, 12, 6, next_id, "short"
    ))
    next_id += 1
    y += 6
    
    # Add all new panels
    dashboard['panels'].extend(new_panels)
    
    # Write back
    with open(dashboard_file, 'w') as f:
        json.dump(dashboard, f, indent=2)
    
    print(f"✅ Enhanced dashboard with {len(new_panels)} new panels")
    print(f"   - Old panel count: {len(dashboard['panels']) - len(new_panels)}")
    print(f"   - New panel count: {len(dashboard['panels'])}")
    print(f"   - Coverage improvement: Significant increase in observability")

if __name__ == "__main__":
    dashboard_file = "grafana/dashboards/longbow.json"
    enhance_dashboard(dashboard_file)
