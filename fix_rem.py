import re

files_to_fix = [
    "internal/store/adaptive_index_zerocopy_test.go",
    "internal/store/arrow_bulk_bench_test.go",
    "internal/store/arrow_concurrent_test.go",
    "internal/store/arrow_search_bench_test.go",
    "internal/store/benchmark_parallel_test.go",
    "internal/store/doget_pipeline_integration_test.go",
    "internal/store/doput_sharding_test.go",
    "internal/store/index_search_adaptive_test.go",
    "internal/store/schema_lock_test.go",
    "internal/store/store_test.go",
    "internal/store/vacuum_test.go"
]

for file in files_to_fix:
    with open(file, "r") as f:
        content = f.read()

    # Fix ds1 := &Dataset{Name: "ds1", Records: []arrow.RecordBatch{rec}}
    content = re.sub(r'Records:\s*\[\]arrow\.RecordBatch{([^}]+)}}', r'Records: NewLockFreeSliceFrom([]arrow.RecordBatch{\1})}', content)

    with open(file, "w") as f:
        f.write(content)
