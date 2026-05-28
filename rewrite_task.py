import re

with open('/Users/rsd/.gemini/antigravity-ide/brain/33bfca24-43dc-431f-9611-be827edd455d/task.md', 'r') as f:
    content = f.read()

content = content.replace('[/] P2 — Production Scale Optimization (Buffer Eviction & VRAM Management)', '[x] P2 — Production Scale Optimization (Buffer Eviction & VRAM Management)')
content = content.replace('[/] Metal Parity for Buffer Eviction', '[x] Metal Parity for Buffer Eviction')
content = content.replace('[ ] Refactor `MetalIndexOptimized` to support `GPUPager`', '[x] Refactor `MetalIndexOptimized` to support `GPUPager`')
content = content.replace('[ ] Implement Argument Buffer batched search logic for Metal', '[x] Implement Argument Buffer batched search logic for Metal')
content = content.replace('[/] Fuzz Testing for Pager Eviction', '[x] Fuzz Testing for Pager Eviction')
content = content.replace('[ ] Implement `FuzzCUDAPagerEviction`', '[x] Implement `FuzzCUDAPagerEviction`')
content = content.replace('[ ] Implement `FuzzMetalPagerEviction`', '[x] Implement `FuzzMetalPagerEviction`')

with open('/Users/rsd/.gemini/antigravity-ide/brain/33bfca24-43dc-431f-9611-be827edd455d/task.md', 'w') as f:
    f.write(content)
