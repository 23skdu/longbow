#!/bin/bash
sed -i '' 's/ds.TurboQuantBits/ds.TurboQuantBits()/g' internal/store/store_actions.go
sed -i '' 's/ds.TurboQuantBits()()/ds.TurboQuantBits()/g' internal/store/store_actions.go

sed -i '' 's/func (vs \*VectorStore)/func (s \*VectorStore)/g' internal/store/store_meta_actions.go

sed -i '' 's/func NewPluggableInternalAdapter(i index.PluggableVectorIndex) index.VectorIndex { return index.NewPluggableInternalAdapter(i) }/func NewPluggableInternalAdapter(i index.PluggableVectorIndex, dp types.IndexDataProvider) index.VectorIndex { return index.NewPluggableInternalAdapter(i, dp) }/g' internal/store/aliases.go

# Add import to store_hybrid.go safely
sed -i '' '/import (/a\
	"github.com/23skdu/longbow/internal/store/index"\
' internal/store/store_hybrid.go

