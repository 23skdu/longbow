import re

with open('/Users/rsd/.gemini/antigravity-ide/brain/33bfca24-43dc-431f-9611-be827edd455d/task.md', 'r') as f:
    content = f.read()

content = content.replace('`[/]` Phase 1: Cleanup & Preparation', '`[x]` Phase 1: Cleanup & Preparation')
content = content.replace('`[ ]` Remove local artifacts (`data/perf_logs/`, `data/profiles/`, `data/generated/`, `longbow-server`, `bench_tool`).', '`[x]` Remove local artifacts (`data/perf_logs/`, `data/profiles/`, `data/generated/`, `longbow-server`, `bench_tool`).')
content = content.replace('`[ ]` Remove remote artifacts on `ancalagon`.', '`[x]` Remove remote artifacts on `ancalagon`.')
content = content.replace('`[ ]` Phase 2: Native Compilation', '`[x]` Phase 2: Native Compilation')
content = content.replace('`[ ]` Compile local binaries (`make build`).', '`[x]` Compile local binaries (`make build`).')
content = content.replace('`[ ]` Update remote repository and compile natively.', '`[x]` Update remote repository and compile natively.')
content = content.replace('`[ ]` Phase 3: Parallel Benchmark Execution', '`[/]` Phase 3: Parallel Benchmark Execution')

with open('/Users/rsd/.gemini/antigravity-ide/brain/33bfca24-43dc-431f-9611-be827edd455d/task.md', 'w') as f:
    f.write(content)
