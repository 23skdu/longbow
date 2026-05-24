#!/bin/bash
sed -i '' '/"testing"/a\
	"github.com/23skdu/longbow/internal/store/cluster"\
' internal/store/graph_api_test.go

