#!/bin/bash
sed -i '' '10d' internal/store/cluster/cdc_test.go
sed -i '' 's/store\.cdcMu/store.mu/g' internal/store/cluster/cdc_test.go
sed -i '' 's/cdcMu/mu/g' internal/store/cluster/cdc_test.go

sed -i '' '/"github.com\/23skdu\/longbow\/internal\/store\/types"/d' internal/store/types/structured_errors_test.go
