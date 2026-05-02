import os
import re

for root, dirs, files in os.walk("internal/store/internal/core"):
    for file in files:
        if file.endswith("_test.go") or file == "mock_test.go":
            path = os.path.join(root, file)
            with open(path, "r") as f:
                content = f.read()

            original = content
            
            # Reverse NewLockFreeSliceFrom
            content = re.sub(r'NewLockFreeSliceFrom\(\[\]arrow\.RecordBatch{(.*?)}\)', r'[]arrow.RecordBatch{\1}', content, flags=re.DOTALL)
            content = re.sub(r'NewLockFreeSliceFrom\((.*?)\)', r'\1', content)
            content = re.sub(r'NewLockFreeSlice\[arrow\.RecordBatch\]\(\)', r'make([]arrow.RecordBatch, 0)', content)
            
            # Reverse append update
            content = re.sub(r'([a-zA-Z0-9_\.]+)\.Records\.UpdateInPlace\(append\(append\(\[\]arrow\.RecordBatch{}, \1\.Records\.Read\(\)\.\.\.\), ([a-zA-Z0-9_]+)\)\)', 
                             r'\1.Records = append(\1.Records, \2)', content)
                             
            # Reverse Read()
            content = re.sub(r'([a-zA-Z0-9_\.]+)\.Records\.Read\(\)', r'\1.Records', content)
            
            if original != content:
                with open(path, "w") as f:
                    f.write(content)
