#!/bin/bash
sed -i '' 's/store := &VectorStore/store := \&MockCDCStore/g' internal/store/cluster/cdc_test.go
sed -i '' '/import (/a\
	"github.com/apache/arrow-go/v18/arrow"\
' internal/store/cluster/cdc_test.go
