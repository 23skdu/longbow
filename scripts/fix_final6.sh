#!/bin/bash
sed -i '' 's/ds.TurboQuantBits =/ds.turboQuantBits =/g' internal/store/store_actions.go
sed -i '' 's/tokenize(/index.Tokenize(/g' internal/store/store_hybrid.go
sed -i '' 's/tokenize(/index.Tokenize(/g' internal/store/store_lifecycle.go

cat << 'M' >> internal/store/aliases.go
type TrainingSample = index.TrainingSample
type IndexConfig = index.IndexConfig
func NewIndexFactory() *index.IndexFactory { return index.NewIndexFactory() }
func NewPluggableInternalAdapter(i index.PluggableVectorIndex) index.VectorIndex { return index.NewPluggableInternalAdapter(i) }
func NewInvertedIndex() *InvertedIndex { return index.NewInvertedIndex() }
M

sed -i '' '/"github.com\/23skdu\/longbow\/internal\/store\/types"/a\
	"github.com/23skdu/longbow/internal/store/index"\
' internal/store/store_hybrid.go

sed -i '' '/"github.com\/23skdu\/longbow\/internal\/store\/types"/a\
	"github.com/23skdu/longbow/internal/store/index"\
' internal/store/store_lifecycle.go

