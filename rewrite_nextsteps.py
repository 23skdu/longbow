import re

with open('docs/nextsteps.md', 'r') as f:
    content = f.read()

# I will update P2 — Production Scale Optimization status to Completed.
# And remove the completed steps from the Next Steps for Integration.

new_content = re.sub(
    r'#### 1\. Buffer Eviction & VRAM Management\n\* \*\*Status\*\*: In Progress `\[\/\]`\n\* \*\*Task\*\*: (.*?)\n\* \*\*Progress\*\*:\n(.*?)\* \*\*Next Steps for Integration\*\*:(.*?)\n\n',
    r'#### 1. Buffer Eviction & VRAM Management\n* **Status**: Completed `[x]`\n* **Task**: \1\n* **Progress**:\n\2  * **CUDA Migration**: Removed monolithic buffer logic; modified distance kernels to handle chunk-based memory via `page_ptrs` and `page_starts` arrays.\n  * **Metal Migration (Parity)**: Implemented chunk-based memory allocation using `GPUPager`; updated `kernels.metal` to resolve `.gpuAddress` pointers dynamically using `PageArgBuffer` argument-buffer parity.\n  * **Fuzz Testing**: Implemented FuzzCUDAPagerEviction and FuzzMetalPagerEviction ensuring thrash handling at 1GB limit with chunked ingest workloads.\n\n',
    content,
    flags=re.DOTALL
)

with open('docs/nextsteps.md', 'w') as f:
    f.write(new_content)
