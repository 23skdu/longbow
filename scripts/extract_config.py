import re
import sys

target_methods = [
    "GetNUMATopology",
    "IsNUMAEnabled",
    "SetGCTuner",
    "GetGCTuner",
    "GetAdmissionController",
    "SetAutoScaler",
    "SetCoordinator",
    "SetMesh",
    "GetMeshMembers",
    "SetIndexedColumns",
    "EnableAdaptiveGC",
    "DisableAdaptiveGC",
    "GetIndexedColumns",
    "SetAutoShardingConfig",
    "GetAutoShardingConfig",
    "SetGPUConfig",
    "SetAutoGPUConfig",
    "SetTemporalIndex",
    "GetTemporalIndex",
]

with open("internal/store/store.go", "r") as f:
    lines = f.readlines()

new_store_lines = []
config_lines = []
in_target_method = False

i = 0
while i < len(lines):
    # Check for docstrings before the method
    doc_start = i
    while doc_start < len(lines) and lines[doc_start].strip().startswith("//"):
        doc_start += 1
    
    if doc_start < len(lines):
        match = re.match(r"^func \(vs \*VectorStore\) (\w+)", lines[doc_start])
        if match and match.group(1) in target_methods:
            # We found a target method!
            method_name = match.group(1)
            # Add docstrings
            for j in range(i, doc_start):
                config_lines.append(lines[j])
            
            # Find the end of the method
            brace_count = 0
            method_start = doc_start
            started = False
            for j in range(method_start, len(lines)):
                config_lines.append(lines[j])
                brace_count += lines[j].count('{')
                brace_count -= lines[j].count('}')
                if '{' in lines[j]:
                    started = True
                if started and brace_count == 0:
                    i = j + 1
                    break
            
            # Remove any trailing blank lines from config_lines, we'll add one standard
            config_lines.append("\n")
            continue
    
    # If not a target method, keep it in store.go
    new_store_lines.append(lines[i])
    i += 1

with open("internal/store/store.go", "w") as f:
    f.writelines(new_store_lines)

# Now create store_config.go
imports = """package store

import (
	"context"
	"time"

	"github.com/23skdu/longbow/internal/autoscale"
	"github.com/23skdu/longbow/internal/gc"
	"github.com/23skdu/longbow/internal/gpu"
	lbmem "github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/mesh"
	"github.com/23skdu/longbow/internal/metrics"
)

"""

with open("internal/store/store_config.go", "w") as f:
    f.write(imports)
    f.writelines(config_lines)

print("Extraction complete. Extracted methods:", len([m for m in target_methods if m in "".join(config_lines)]))
