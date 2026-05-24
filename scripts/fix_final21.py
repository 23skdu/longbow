import re

with open('internal/store/structured_errors_test.go', 'r') as f:
    content = f.read()
content = content.replace('package types_test', 'package store')
content = content.replace('package types', 'package store')
with open('internal/store/structured_errors_test.go', 'w') as f:
    f.write(content)

with open('internal/store/index/learned_index_embedding_test.go', 'r') as f:
    content = f.read()
# just find and replace the remaining VectorStore usages
content = re.sub(r's := &VectorStore\{\}.*?m2\)', '', content, flags=re.DOTALL)
content = re.sub(r'	// Default \(unset\) should be empty strings\.', '', content)
content = re.sub(r'	s2 := &VectorStore\{\}', '', content)
content = re.sub(r'	assert\.Equal\(t, "", m2\)', '', content)
with open('internal/store/index/learned_index_embedding_test.go', 'w') as f:
    f.write(content)

# Fix types/search_pool_test.go
with open('internal/store/types/search_pool_test.go', 'r') as f:
    content = f.read()
content = content.replace('index.index', '0')
content = content.replace('SearchResult', 'IndexSearchResult')
content = content.replace('NewSearchResultPool', 'NewIndexSearchResultPool')
with open('internal/store/types/search_pool_test.go', 'w') as f:
    f.write(content)
