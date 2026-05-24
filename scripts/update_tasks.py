import re

with open('/Users/rsd/.gemini/antigravity-ide/brain/02ea479a-ed7c-4275-8f3f-357b8d8a38e3/task.md', 'r') as f:
    content = f.read()

content = content.replace('- `[/]` Run cluster tests and verify fix resolves timeouts', '- `[x]` Run cluster tests and verify fix resolves timeouts')

with open('/Users/rsd/.gemini/antigravity-ide/brain/02ea479a-ed7c-4275-8f3f-357b8d8a38e3/task.md', 'w') as f:
    f.write(content)
