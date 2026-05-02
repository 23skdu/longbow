import os
import re
import glob
import subprocess

def process_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()

    original = content

    # Fix append calls
    # ds.Records = append(ds.Records, rec) -> ds.Records.UpdateInPlace(append(append([]arrow.RecordBatch{}, ds.Records.Read()...), rec))
    content = re.sub(r'([a-zA-Z0-9_\.]+)\.Records\s*=\s*append\(\1\.Records,\s*([a-zA-Z0-9_]+)\)', 
                     r'\1.Records.UpdateInPlace(append(append([]arrow.RecordBatch{}, \1.Records.Read()...), \2))', content)
                     
    content = re.sub(r'([a-zA-Z0-9_\.]+)\.BatchNodes\s*=\s*append\(\1\.BatchNodes,\s*([a-zA-Z0-9_]+)\)', 
                     r'\1.BatchNodes.UpdateInPlace(append(append([]int{}, \1.BatchNodes.Read()...), \2))', content)

    # Fix len(importedDS.Records) -> len(importedDS.Records.Read())
    content = re.sub(r'len\(([a-zA-Z0-9_\.]+)\.Records\)', r'len(\1.Records.Read())', content)
    
    # Fix importedDS.Records[0] -> importedDS.Records.Read()[0]
    content = re.sub(r'([a-zA-Z0-9_\.]+)\.Records\[([^\]]+)\]', r'\1.Records.Read()[\2]', content)

    # Fix Records assignment in tests
    content = re.sub(r'Records:\s*\[\]arrow\.RecordBatch{}', r'Records: NewLockFreeSlice[arrow.RecordBatch]()', content)
    content = re.sub(r'Records:\s*make\(\[\]arrow\.RecordBatch,\s*0\)', r'Records: NewLockFreeSlice[arrow.RecordBatch]()', content)
    content = re.sub(r'Records:\s*make\(\[\]arrow\.RecordBatch,\s*numVectors\)', r'Records: NewLockFreeSliceFrom(make([]arrow.RecordBatch, numVectors))', content)
    content = re.sub(r'Records:\s*make\(\[\]arrow\.RecordBatch,\s*0,\s*len\(records\)\)', r'Records: NewLockFreeSlice[arrow.RecordBatch]()', content)
    content = re.sub(r'Records:\s*batches', r'Records: NewLockFreeSliceFrom(batches)', content)

    # ExtractPushablePredicate(filterExpr, ds.Records) -> ExtractPushablePredicate(filterExpr, ds.Records.Read())
    content = content.replace('ExtractPushablePredicate(filterExpr, ds.Records)', 'ExtractPushablePredicate(filterExpr, ds.Records.Read())')
    
    # Assigning to ds.Records
    content = re.sub(r'dataset\.Records\s*=\s*make\(\[\]arrow\.RecordBatch,\s*11\)', r'dataset.Records = NewLockFreeSliceFrom(make([]arrow.RecordBatch, 11))', content)
    content = re.sub(r'([a-zA-Z0-9_\.]+)\.Records\s*=\s*\[\]arrow\.RecordBatch\{', r'\1.Records = NewLockFreeSliceFrom([]arrow.RecordBatch{', content)

    # h.dataset.Records.Read() = ... -> h.dataset.Records.UpdateInPlace(...)
    content = re.sub(r'([a-zA-Z0-9_\.]+)\.Records\.Read\(\)\s*=\s*(.*)', r'\1.Records.UpdateInPlace(\2)', content)

    # In struct literals where we have Records: []arrow.RecordBatch{ ... }
    # This is tricky because we have to find the matching brace.
    # Actually, if we just look for `Records: []arrow.RecordBatch{` and the matching brace...
    # We can use a simpler approach: finding the line `Records: []arrow.RecordBatch{` and then finding the closing brace `},` or `}` at the same indentation level.
    # Alternatively, we can use ast parser or just manually do it for the remaining errors.
    
    # For now let's write back if changed
    if original != content:
        with open(filepath, 'w') as f:
            f.write(content)

if __name__ == "__main__":
    for root, dirs, files in os.walk("internal/store"):
        for file in files:
            if file.endswith("_test.go"):
                process_file(os.path.join(root, file))
