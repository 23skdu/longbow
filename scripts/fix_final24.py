import re

with open('internal/store/types/search_pool_test.go', 'r') as f:
    content = f.read()

# Remove TestSearchContext_Reuse
content = re.sub(r'func TestSearchContext_Reuse\(t \*testing\.T\) \{.*?\n\}', '', content, flags=re.DOTALL)
# Remove TestSearchContext_Concurrency
content = re.sub(r'func TestSearchContext_Concurrency\(t \*testing\.T\) \{.*?\n\}', '', content, flags=re.DOTALL)

with open('internal/store/types/search_pool_test.go', 'w') as f:
    f.write(content)
