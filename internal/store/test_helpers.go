package store

import (
"go.uber.org/zap"
)

func mockLogger() *zap.Logger {
return zap.NewNop()
}
