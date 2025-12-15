package store

import (
"log/slog"
"os"
)

func mockLogger() *slog.Logger {
return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
}
