import json
import os

DASHBOARD_PATH = 'grafana/dashboards/longbow.json'

def get_next_id(panels):
    ids = [p['id'] for p in panels if 'id' in p]
    return (max(ids) + 1) if ids else 1

def create_stat_panel(id, title, expr, legend=""):
    return {
        "datasource": "${datasource}",
        "fieldConfig": { "defaults": { "color": { "mode": "thresholds" }, "thresholds": { "mode": "absolute", "steps": [{ "color": "green", "value": None }] } } },
        "gridPos": { "h": 6, "w": 6, "x": 0, "y": 0 }, 
        "id": id,
        "targets": [{ "expr": expr, "legendFormat": legend, "refId": "A" }],
        "title": title,
        "type": "stat"
    }

def create_timeseries_panel(id, title, expr, legend="", unit="short"):
    return {
        "datasource": "${datasource}",
        "fieldConfig": { "defaults": { "custom": { "drawStyle": "line" }, "unit": unit } },
        "gridPos": { "h": 6, "w": 8, "x": 0, "y": 0 },
        "id": id,
        "targets": [{ "expr": expr, "legendFormat": legend, "refId": "A" }],
        "title": title,
        "type": "timeseries"
    }

# Load
with open(DASHBOARD_PATH) as f:
    dash = json.load(f)

panels = dash['panels']
existing_exprs = set()
for p in panels:
    if 'targets' in p:
        for t in p['targets']:
            existing_exprs.add(t['expr'])

# Define missing metrics mapping
new_panels_map = {
    "Memory & Systems": [
        ("longbow_arena_allocated_bytes", "Arena Memory", "timeseries", "bytes"),
        ("longbow_arena_slabs_total", "Total Slabs", "stat", "short"),
        ("rate(longbow_eviction_rejected_queries_total[1m])", "Eviction Rejections/s", "timeseries", "ops")
    ],
    "Vector Index & Storage": [
        ("longbow_bq_vectors_total", "BQ Vectors", "stat", "short")
    ],
    "Pipeline & Filtering": [
        ("rate(longbow_pipeline_operations_total[1m])", "Pipeline Ops/sec", "timeseries", "ops"),
        ("histogram_quantile(0.99, sum(rate(longbow_pipeline_duration_seconds_bucket[5m])) by (le))", "Pipeline Duration P99", "timeseries", "s"),
        ("longbow_pipeline_batches_per_second", "Pipeline Throughput", "timeseries", "ops"),
        ("longbow_pipeline_worker_utilization", "Worker Util", "timeseries", "percentunit")
    ],
    "Distributed Internals": [
        ("rate(longbow_dataset_update_retries_total[1m])", "Dataset CAS Retries/s", "timeseries", "ops"),
        ("longbow_namespace_datasets_total", "Namespace Datasets", "stat", "short")
    ],
    "SIMD & Hardware Accelerations": [
        ("rate(longbow_simd_dispatch_count[1m])", "SIMD Dispatches/s", "timeseries", "ops"),
        ("rate(longbow_simd_cosine_batch_calls_total[1m])", "Cosine Batch Calls/s", "timeseries", "ops"),
        ("rate(longbow_simd_dot_product_batch_calls_total[1m])", "DotProduct Batch Calls/s", "timeseries", "ops")
    ],
    "Warmup Status": [ 
        ("longbow_warmup_progress_percent", "Warmup Progress", "stat", "percent"),
        ("longbow_warmup_datasets_completed", "Datasets Warmed", "stat", "short")
    ]
}

def find_row_index(title):
    for i, p in enumerate(panels):
        if p.get('type') == 'row' and p.get('title') == title:
            return i
    return -1

# Insert panels
for row_title, metric_defs in new_panels_map.items():
    row_idx = find_row_index(row_title)
    if row_idx == -1:
        # Create new row
        row_id = get_next_id(panels)
        new_row = {
            "collapsed": False,
            "gridPos": { "h": 1, "w": 24, "x": 0, "y": 0 },
            "id": row_id,
            "panels": [],
            "title": row_title,
            "type": "row"
        }
        panels.append(new_row)
        row_idx = len(panels) - 1
    
    # Calculate insertion index (end of this group)
    insert_idx = len(panels)
    for i in range(row_idx + 1, len(panels)):
        if panels[i].get('type') == 'row':
            insert_idx = i
            break
    
    # Add metrics
    for m_expr, m_title, m_type, m_unit in metric_defs:
        if m_expr in existing_exprs:
            continue
            
        pid = get_next_id(panels) # Get fresh ID each time to avoid conflict
        if m_type == 'stat':
            p = create_stat_panel(pid, m_title, m_expr)
        else:
            p = create_timeseries_panel(pid, m_title, m_expr, unit=m_unit)
        
        panels.insert(insert_idx, p)
        insert_idx += 1
        existing_exprs.add(m_expr)

# Re-flow Layout
current_y = 0
x_cursor = 0
ROW_HEIGHT = 1
PANEL_HEIGHT = 6
PANEL_WIDTH = 8 # Default width for 3-column layout

for p in panels:
    if p.get('type') == 'row':
        # New Row Header
        # If we were in the middle of a row of panels, advance Y
        if x_cursor > 0:
            current_y += PANEL_HEIGHT
            x_cursor = 0
            
        p['gridPos'] = { "h": ROW_HEIGHT, "w": 24, "x": 0, "y": current_y }
        current_y += ROW_HEIGHT
        x_cursor = 0
    else:
        # Panel
        # Check width from existing config or default
        w = p.get('gridPos', {}).get('w', PANEL_WIDTH)
        h = p.get('gridPos', {}).get('h', PANEL_HEIGHT)
        
        # Wrap if needed
        if x_cursor + w > 24:
            current_y += max(h, PANEL_HEIGHT) # simplified
            x_cursor = 0
            
        p['gridPos'] = { "h": h, "w": w, "x": x_cursor, "y": current_y }
        x_cursor += w

# Check if last row didn't wrap
if x_cursor > 0:
    current_y += PANEL_HEIGHT

# Save
with open(DASHBOARD_PATH, 'w') as f:
    json.dump(dash, f, indent=2, sort_keys=True)

print("Dashboard updated successfully.")
