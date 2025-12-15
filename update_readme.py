import sys

with open('README.md', 'r') as f:
    lines = f.readlines()

new_lines = []
in_table = False
table_updated = False

for line in lines:
    if '| Metric Name | Type | Description |' in line:
        in_table = True
        new_lines.append(line)
        continue
    
    if in_table and line.strip().startswith('|'):
        # Skip existing table rows, we will rewrite the whole table
        continue
    
    if in_table and not line.strip().startswith('|'):
        in_table = False
        # Insert new table content
        new_lines.append('| :--- | :--- | :--- |\n')
        new_lines.append('| `longbow_flight_operations_total` | Counter | Total number of Flight operations (DoGet, DoPut, etc.) |\n')
        new_lines.append('| `longbow_flight_duration_seconds` | Histogram | Latency distribution of Flight operations |\n')
        new_lines.append('| `longbow_flight_bytes_processed_total` | Counter | Total bytes processed in Flight operations |\n')
        new_lines.append('| `longbow_vector_index_size` | Gauge | Current number of vectors in the index |\n')
        new_lines.append('| `longbow_average_vector_norm` | Gauge | Average L2 norm of vectors in the index |\n')
        new_lines.append('| `longbow_index_build_latency_seconds` | Histogram | Latency of vector index build operations |\n')
        new_lines.append('| `longbow_memory_fragmentation_ratio` | Gauge | Ratio of system memory reserved vs used |\n')
        new_lines.append('| `longbow_wal_writes_total` | Counter | Total number of write operations to the WAL. |\n')
        new_lines.append('| `longbow_wal_bytes_written_total` | Counter | Total bytes written to the WAL file. |\n')
        new_lines.append('| `longbow_wal_replay_duration_seconds` | Histogram | Time taken to replay the WAL during startup. |\n')
        new_lines.append('| `longbow_snapshot_operations_total` | Counter | Total number of snapshot attempts. |\n')
        new_lines.append('| `longbow_snapshot_duration_seconds` | Histogram | Duration of the snapshot process. |\n')
        new_lines.append('| `longbow_evictions_total` | Counter | Total number of evicted records. |\n')
        new_lines.append('<!-- markdownlint-enable MD013 -->\n\n')
        new_lines.append('For a detailed explanation of each metric, see [Metrics Documentation](docs/metrics.md).\n\n')
        
        # If the current line was the closing comment or empty line, we might have duplicated it, so check
        if 'markdownlint-enable' in line:
            continue
        new_lines.append(line)
        continue

    new_lines.append(line)

with open('README.md', 'w') as f:
    f.writelines(new_lines)
