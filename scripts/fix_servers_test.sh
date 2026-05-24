#!/bin/bash
sed -i '' '/"github.com\/apache\/arrow-go\/v18\/arrow\/array"/a\
	"github.com/23skdu/longbow/internal/store"\
	"github.com/23skdu/longbow/internal/store/cluster"\
' internal/store/cluster/servers_test.go

sed -i '' 's/VectorStore/store.VectorStore/g' internal/store/cluster/servers_test.go
sed -i '' 's/store\.store\.VectorStore/store.VectorStore/g' internal/store/cluster/servers_test.go
sed -i '' 's/NewVectorStore/store.NewVectorStore/g' internal/store/cluster/servers_test.go
sed -i '' 's/store\.store\.NewVectorStore/store.NewVectorStore/g' internal/store/cluster/servers_test.go
sed -i '' 's/NewDataServer/cluster.NewDataServer/g' internal/store/cluster/servers_test.go
sed -i '' 's/NewMetaServer/cluster.NewMetaServer/g' internal/store/cluster/servers_test.go
sed -i '' 's/Dataset/store.Dataset/g' internal/store/cluster/servers_test.go
sed -i '' 's/NewLockFreeSliceFrom/store.NewLockFreeSliceFrom/g' internal/store/cluster/servers_test.go
