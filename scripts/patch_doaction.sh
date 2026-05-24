#!/bin/bash
sed -i '' 's/return status\.Errorf(codes\.Unimplemented, "unimplemented action: %s", action\.Type)/return s.doMetaAction(action, stream)/g' internal/store/store_actions.go
