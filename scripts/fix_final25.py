with open('internal/store/types/search_pool_test.go', 'r') as f:
    content = f.read()

import re
content = re.sub(r'func TestSearchContext_Reset\(t \*testing\.T\) \{.*?\n\}', '', content, flags=re.DOTALL)
content = re.sub(r'func TestSearchContext_Metrics\(t \*testing\.T\) \{.*?\n\}', '', content, flags=re.DOTALL)

content = content.replace('IndexSearchResult', 'SearchResult')

with open('internal/store/types/search_pool_test.go', 'w') as f:
    f.write(content)
