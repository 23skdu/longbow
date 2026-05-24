import re

with open('/Users/rsd/.gemini/antigravity-ide/brain/02ea479a-ed7c-4275-8f3f-357b8d8a38e3/task.md', 'r') as f:
    content = f.read()

content = content.replace('- `[/]` Move cluster files to `internal/store/cluster`.', '- `[x]` Move cluster files to `internal/store/cluster`.')
content = content.replace('- `[/]` Phase 3: Split monolithic `internal/store` into `index`, `cluster`, `wal`', '- `[x]` Phase 3: Split monolithic `internal/store` into `index`, `cluster`, `wal`')

with open('/Users/rsd/.gemini/antigravity-ide/brain/02ea479a-ed7c-4275-8f3f-357b8d8a38e3/task.md', 'w') as f:
    f.write(content)
