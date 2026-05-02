import os
import re

for root, dirs, files in os.walk("internal/store"):
    # Skip internal/core
    if "internal/core" in root:
        continue
    for file in files:
        if file.endswith("_test.go"):
            path = os.path.join(root, file)
            with open(path, "r") as f:
                content = f.read()

            original = content
            
            # Cases like: Records: []arrow.RecordBatch{rec},
            content = re.sub(r'Records:\s*\[\]arrow\.RecordBatch{([^}]+)},', r'Records: NewLockFreeSliceFrom([]arrow.RecordBatch{\1}),', content)
            
            # Cases like: ds.Records = []arrow.RecordBatch{rec}
            content = re.sub(r'\.Records\s*=\s*\[\]arrow\.RecordBatch{([^}]+)}', r'.Records = NewLockFreeSliceFrom([]arrow.RecordBatch{\1})', content)
            
            # Cases like: Records: []arrow.RecordBatch{}
            content = re.sub(r'Records:\s*\[\]arrow\.RecordBatch{}', r'Records: NewLockFreeSlice[arrow.RecordBatch]()', content)
            
            # Cases like: ds.Records = make([]arrow.RecordBatch, numVectors)
            content = re.sub(r'\.Records\s*=\s*make\(\[\]arrow\.RecordBatch, ([a-zA-Z0-9_]+)\)', r'.Records = NewLockFreeSliceFrom(make([]arrow.RecordBatch, \1))', content)

            if original != content:
                with open(path, "w") as f:
                    f.write(content)
