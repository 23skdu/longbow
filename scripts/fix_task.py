with open('/Users/rsd/.gemini/antigravity-ide/brain/02ea479a-ed7c-4275-8f3f-357b8d8a38e3/task.md', 'r') as f:
    content = f.read()

content = content.replace('- [ ] Extract `wal` package', '- [x] Extract `wal` package')
content = content.replace('- [/] Phase 3: Extract `internal/store` sub-packages', '- [x] Phase 3: Extract `internal/store` sub-packages')

with open('/Users/rsd/.gemini/antigravity-ide/brain/02ea479a-ed7c-4275-8f3f-357b8d8a38e3/task.md', 'w') as f:
    f.write(content)

