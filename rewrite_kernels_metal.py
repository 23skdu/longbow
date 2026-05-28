import re

with open('internal/gpu/metal/kernels.metal', 'r') as f:
    content = f.read()

# Add PageArgBuffer struct
struct_def = """struct PageArgBuffer {
    device const float* pages[1024];
};
struct PageArgBufferHalf {
    device const half* pages[1024];
};
"""

content = content.replace("kernel void vector_distance_l2(", struct_def + "\nkernel void vector_distance_l2(")

def replace_kernel(name, arg_type, struct_type):
    global content
    
    # 1. Update signature
    old_sig = f"device const {arg_type}* vectors [[buffer(1)]]"
    new_sig = f"constant {struct_type}& vectorPages [[buffer(1)]],\n    constant uint* pageStarts [[buffer(5)]],\n    constant uint& numPages [[buffer(6)]]"
    
    # We need to find the kernel function body and replace the signature and the vector access
    pattern = rf"(kernel void {name}\([^)]*?)device const {arg_type}\* vectors \[\[buffer\(1\)\]\](.*?)\)(.*?)\{{"
    
    match = re.search(pattern, content, re.DOTALL)
    if match:
        full_match = match.group(0)
        new_header = full_match.replace(f"device const {arg_type}* vectors [[buffer(1)]]", new_sig)
        
        # Inject page lookup at the start of the kernel
        page_lookup = f"""{{
    if (gid >= numVectors) return;
    
    uint pageIdx = 0;
    uint localId = 0;
    for (uint i = 0; i < numPages; i++) {{
        if (gid < pageStarts[i+1]) {{
            pageIdx = i;
            localId = gid - pageStarts[i];
            break;
        }}
    }}
    device const {arg_type}* vectors = vectorPages.pages[pageIdx];
    uint offset = localId * dim;"""
        
        # Replace the `if (gid >= numVectors) return;` and `uint offset = gid * dim;` with our lookup
        body = content[match.end():]
        # Find the next closing brace
        brace_count = 1
        end_idx = 0
        for i, char in enumerate(body):
            if char == '{':
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count == 0:
                    end_idx = i
                    break
                    
        kernel_body = body[:end_idx]
        
        # Strip out old offset and bounds check
        kernel_body = re.sub(r'if \(gid >= numVectors\) return;\s*(uint offset = gid \* dim;)?', '', kernel_body)
        kernel_body = re.sub(r'uint offset = gid \* dim;', '', kernel_body)
        
        new_body = page_lookup + kernel_body
        
        content = content[:match.start()] + new_header + new_body + content[match.end()+end_idx:]

# Replace all standard distance kernels
replace_kernel('compute_l2_distances', 'float', 'PageArgBuffer')
replace_kernel('compute_cosine_similarity', 'float', 'PageArgBuffer')
replace_kernel('compute_dot_product', 'float', 'PageArgBuffer')

replace_kernel('compute_l2_distances_fp16', 'half', 'PageArgBufferHalf')
replace_kernel('compute_cosine_similarity_fp16', 'half', 'PageArgBufferHalf')
replace_kernel('compute_dot_product_fp16', 'half', 'PageArgBufferHalf')

with open('internal/gpu/metal/kernels.metal', 'w') as f:
    f.write(content)
