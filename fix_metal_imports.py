import re

with open('internal/gpu/metal/metal_gpu_optimized.go', 'r') as f:
    content = f.read()

content = content.replace('"github.com/23skdu/longbow/internal/gpu/types"', '"github.com/23skdu/longbow/internal/gpu/types"\n\t"github.com/23skdu/longbow/internal/gpu/memory"')
content = content.replace('// MetalIndexOptimized implements GPU-accelerated vector search using Metal compute shaders', 'const vectorsPerPage = 4096\n\n// MetalIndexOptimized implements GPU-accelerated vector search using Metal compute shaders')

with open('internal/gpu/metal/metal_gpu_optimized.go', 'w') as f:
    f.write(content)
