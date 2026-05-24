#!/bin/bash
sed -i '' 's/func NewIVFOPQIndex(cfg IVFOPQConfig) \*index.IVFOPQIndex { return index.NewIVFOPQIndex(cfg) }/func NewIVFOPQIndex(d int, cfg IVFOPQConfig) (\*index.IVFOPQIndex, error) { return index.NewIVFOPQIndex(d, cfg) }/g' internal/store/aliases.go

sed -i '' '/"github.com\/23skdu\/longbow\/internal\/storage"/a\
	"github.com/23skdu/longbow/internal/store/cluster"\
' internal/store/benchmark_test.go

# Fix array import in store_test_helpers.go
sed -i '' '/"github.com\/apache\/arrow-go\/v18\/arrow\/flight"/a\
	"github.com/apache/arrow-go/v18/arrow/array"\
' internal/store/store_test_helpers.go

# remove them from cluster/servers_test.go safely
python3 -c "
import re
with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()
# we just need to delete setupDataServerTest and makeVectorRecord from there, or leave them. Let's leave them for now to avoid breaking it if cluster tests use them, wait they might be in cluster_test too. It's safer to leave them or just not use them in cluster if they don't compile. Oh wait, servers_test uses them! But it's in a different package. If it uses them, we leave them.
"
